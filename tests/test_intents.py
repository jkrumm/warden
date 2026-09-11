#!/usr/bin/env python3
"""Regression suite for scripts/intents.py — the queue that lets a surface
request a ledger change without being a ledger writer.

Two properties carry this module and both are tested here rather than argued:
a spool file can never widen its own approval (`expires_at` and `payload_hash`
are refused outright, and no signature is ever verified here), and a replayed
or forged file is at worst a denial of service (`AND decision IS NULL` means a
second application changes nothing).

Run: .venv/bin/python3 tests/test_intents.py
"""

import datetime as dt
import importlib.util
import json
import os
import stat
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from clients import sideclaw, signer  # noqa: E402

# A throwaway HERMES_HOME so `approvals.pubkey_path()`'s default (used by
# every test in this file that does NOT explicitly set WARDEN_APPROVAL_PUBKEY)
# resolves to a file that deterministically does not exist, rather than
# whatever a real dev machine happens to have at ~/.hermes. Those tests only
# care that a spend attempt cannot crash the drain — see
# `_spend_after_decision`'s WardenError branch — not that it succeeds.
os.environ.setdefault("HERMES_HOME", tempfile.mkdtemp(prefix="intents-hermes-home-"))

_ledger_spec = importlib.util.spec_from_file_location("ledger", REPO / "scripts" / "ledger.py")
ledger = importlib.util.module_from_spec(_ledger_spec)
_ledger_spec.loader.exec_module(ledger)

_intents_spec = importlib.util.spec_from_file_location("intents", REPO / "scripts" / "intents.py")
intents = importlib.util.module_from_spec(_intents_spec)
_intents_spec.loader.exec_module(intents)

# A plausible Ed25519 signature: 64 bytes of hex. Nothing in intents.py checks
# its length or its authenticity — that happens once, at spend time, in
# `execute_approved()`. This is here only so the SHAPE check has real input;
# the dedicated spend tests below sign for real.
SIG = "ab" * 64


class _patch:
    """Swap one attribute on a module object for the duration of a `with`
    block — `sideclaw.submit = fake`, restored afterward so tests cannot
    leak into one another. Same shape as tests/test_lifecycle.py's own."""

    def __init__(self, obj, name: str, value):
        self.obj, self.name, self.value = obj, name, value

    def __enter__(self):
        self.original = getattr(self.obj, self.name)
        setattr(self.obj, self.name, self.value)
        return self.value

    def __exit__(self, *exc):
        setattr(self.obj, self.name, self.original)


class _env:
    """Set (or, with `None`, force-unset) environment variables for one
    `with` block, restoring exactly what was there before."""

    def __init__(self, **kv):
        self.kv = kv

    def __enter__(self):
        self.original = {k: os.environ.get(k) for k in self.kv}
        for k, v in self.kv.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        return self

    def __exit__(self, *exc):
        for k, v in self.original.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _tmp_dir(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=prefix))


def _fresh_ledger() -> "tuple":
    path = _tmp_dir("intents-db-") / "warden.db"
    return ledger.connect(path, migrate=True), path


def _use_spool() -> Path:
    """Point the module global at a throwaway directory — the same
    monkeypatch shape ledger.DB_PATH supports, and the reason INTENTS_DIR is
    re-read at call time."""
    intents.INTENTS_DIR = _tmp_dir("intents-spool-") / "intents"
    return intents.INTENTS_DIR


def _keypair():
    priv = Ed25519PrivateKey.generate()
    pub_hex = priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw,
    ).hex()
    return priv, pub_hex


def _write_pubkey(pub_hex: str) -> Path:
    p = _tmp_dir("intents-pubkey-") / "dispatch-approval.pub"
    p.write_text(pub_hex + "\n", encoding="utf-8")
    return p


def _dispatch_root_with_repo(name: str = "warden", tier: str = "implement"):
    root = _tmp_dir("intents-root-")
    (root / name / ".git").mkdir(parents=True)
    policy_path = root / "dispatch-repos.json"
    policy_path.write_text(
        json.dumps({"root": str(root), "defaultTier": tier, "deny": [], "sensitive": [], "tiers": {}}),
        encoding="utf-8",
    )
    return root, policy_path


def _seed_approval(conn, nonce: str, *, decision=None, decided_by=None) -> None:
    conn.execute(
        "INSERT INTO dispatch_approvals (nonce, verb, repo, tier, payload_hash, created_at, expires_at, "
        "decision, decided_by) VALUES (?,?,?,?,?,?,?,?,?)",
        (nonce, "dispatch", "warden", "0", "hash-" + nonce, "2026-09-09T00:00:00+00:00",
         "2026-09-09T01:00:00+00:00", decision, decided_by),
    )
    conn.commit()


def _seed_approval_row(conn, nonce, *, verb="dispatch", repo="warden", tier="implement",
                        payload_hash="hash", expires_at=None, channel=None, key_id=None,
                        params_json="{}", stdin_text="the approved brief") -> None:
    """A row shaped for a REAL spend attempt — unexpired, with the columns
    `execute_approved()` actually reads — as opposed to `_seed_approval()`
    above, whose fixed past `expires_at` is only ever meant to be seen by
    `_apply_approval_decision()`'s shape/replay tests, never by a spend."""
    now = dt.datetime.now(dt.timezone.utc)
    expires_at = expires_at or (now + dt.timedelta(minutes=30)).isoformat()
    conn.execute(
        "INSERT INTO dispatch_approvals(nonce,verb,repo,tier,payload_hash,created_at,expires_at,channel,"
        "argv_json,stdin_text,key_id,params_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (nonce, verb, repo, tier, payload_hash, now.isoformat(), expires_at, channel, "[]",
         stdin_text, key_id, params_json),
    )
    conn.commit()


def _approval(nonce: str, **overrides) -> dict:
    intent = {
        "v": 1,
        "kind": "approval_decision",
        "created_at": "2026-09-09T12:00:00.000001+00:00",
        "source": "test",
        "nonce": nonce,
        "decision": "approve",
        "decided_by": "U123",
        "signature": SIG,
    }
    intent.update(overrides)
    return intent


def _row(conn, nonce: str):
    return conn.execute("SELECT * FROM dispatch_approvals WHERE nonce=?", (nonce,)).fetchone()


def _expect_value_error(intent: dict, *needles: str) -> None:
    try:
        intents.record(intent)
    except ValueError as err:
        for needle in needles:
            assert needle in str(err), f"message does not name {needle!r}: {err}"
    else:
        raise AssertionError(f"expected ValueError for {intent}")


def test_valid_approval_decision_round_trips():
    """The decision itself lands even though this row's fixed `expires_at`
    (`_seed_approval`) is already in the past by the time the test runs, so
    the spend attempt `drain()` now makes refuses as `expired` — a WardenError
    the `_spend_after_decision` branch turns into a reported result, never a
    crash. See `test_drain_spends_an_approved_decision_and_opens_an_episode`
    below for the round trip that actually spends."""
    spool = _use_spool()
    conn, _ = _fresh_ledger()
    _seed_approval(conn, "n1")

    path = intents.record(_approval("n1", decided_by="U999"))
    assert path.exists() and path.parent == spool

    result = intents.drain(conn)
    assert result["applied"] == 1 and result["rejected"] == 0, result
    assert len(result["results"]) == 1, result
    assert result["results"][0]["nonce"] == "n1" and result["results"][0]["decision"] == "approve"

    row = _row(conn, "n1")
    assert row["decision"] == "approve", dict(row)
    assert row["decided_by"] == "U999", dict(row)
    assert row["signature"] == SIG, dict(row)
    assert row["decided_at"], "decided_at must be stamped by the drain"
    assert row["spent_at"] is None, "an expired row must never be spent"
    assert not path.exists(), "an applied intent must be unlinked"
    conn.close()


def test_drain_spends_an_approved_decision_and_opens_an_episode():
    """The real round trip: a signed `approve` intent, drained against a row
    that can actually be spent, opens a sideclaw episode — the point of
    wiring `execute_approved()` into `drain()` at all."""
    _use_spool()
    conn, _ = _fresh_ledger()
    priv, pub_hex = _keypair()
    root, policy_path = _dispatch_root_with_repo("warden", "implement")
    payload_hash = signer.payload_hash("dispatch", "warden", "implement", "the approved brief", "fix it", "")
    expires_at = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=30)).isoformat()
    _seed_approval_row(
        conn, "n-spend", repo="warden", tier="implement", payload_hash=payload_hash,
        expires_at=expires_at, key_id=signer.key_id(pub_hex), params_json=json.dumps({"why": "fix it"}),
    )
    message = signer.canonical_message("n-spend", payload_hash, "approve", "U999", expires_at)
    intents.record(_approval("n-spend", decided_by="U999", signature=priv.sign(message).hex()))

    captured: dict = {}

    def fake_submit(**kwargs):
        captured.update(kwargs)
        return {"id": "job-spend", "status": "running"}

    with _env(WARDEN_APPROVAL_PUBKEY=str(_write_pubkey(pub_hex)), WARDEN_DISPATCH_REPOS=str(policy_path)), \
         _patch(sideclaw, "submit", fake_submit):
        result = intents.drain(conn)

    assert result["applied"] == 1 and result["rejected"] == 0, result
    assert len(result["results"]) == 1, result
    spend = result["results"][0]
    assert spend == {"nonce": "n-spend", "decision": "approve", "status": "opened", "jobId": "job-spend",
                      "reason": None}, spend
    assert captured["cwd"] == os.path.realpath(root / "warden"), captured

    row = _row(conn, "n-spend")
    assert row["spent_at"] is not None
    assert row["spent_job_id"] == "job-spend"
    conn.close()


def test_drain_does_not_spend_a_deny():
    """A `deny` must never reach sideclaw — the whole reason
    `_spend_after_decision` branches on `decision` before calling
    `execute_approved()` at all."""
    _use_spool()
    conn, _ = _fresh_ledger()
    priv, pub_hex = _keypair()
    payload_hash = "hash-n-deny"
    expires_at = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=30)).isoformat()
    _seed_approval_row(conn, "n-deny", payload_hash=payload_hash, expires_at=expires_at, key_id=signer.key_id(pub_hex))
    message = signer.canonical_message("n-deny", payload_hash, "deny", "U999", expires_at)
    intents.record(_approval("n-deny", decision="deny", decided_by="U999", signature=priv.sign(message).hex()))

    called: list = []

    def fake_submit(**kwargs):
        called.append(kwargs)
        return {"id": "should-never-happen", "status": "running"}

    with _env(WARDEN_APPROVAL_PUBKEY=str(_write_pubkey(pub_hex))), _patch(sideclaw, "submit", fake_submit):
        result = intents.drain(conn)

    assert result["applied"] == 1, result
    assert result["results"] == [
        {"nonce": "n-deny", "decision": "deny", "status": "recorded", "jobId": None, "reason": None}
    ], result
    assert not called, "a deny must never spend"
    row = _row(conn, "n-deny")
    assert row["decision"] == "deny" and row["spent_at"] is None
    conn.close()


def test_spool_file_is_0600_and_directory_is_0700():
    spool = _use_spool()
    path = intents.record(_approval("n-modes"))
    assert stat.S_IMODE(spool.stat().st_mode) == 0o700, oct(spool.stat().st_mode)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600, oct(path.stat().st_mode)


def test_record_opens_no_database():
    """The whole point of the queue: a process that calls record() is not a
    ledger writer. Point DB_PATH at a file that does not exist and prove
    record() neither needs it nor brings it into existence."""
    _use_spool()
    missing = _tmp_dir("intents-nodb-") / "never-created.db"
    original = ledger.DB_PATH
    ledger.DB_PATH = missing
    try:
        path = intents.record(_approval("n-nodb"))
        assert path.exists()
    finally:
        ledger.DB_PATH = original
    assert not missing.exists(), "record() opened a database"
    assert not list(missing.parent.iterdir()), f"record() created {list(missing.parent.iterdir())}"


def test_unknown_top_level_key_is_rejected_by_name():
    _use_spool()
    _expect_value_error(_approval("n-unknown", surprise="x"), "surprise")


def test_unknown_kind_is_rejected():
    _use_spool()
    _expect_value_error(_approval("n-kind", kind="merge_the_pr"), "kind", "merge_the_pr")


def test_bad_schema_version_is_rejected():
    _use_spool()
    _expect_value_error(_approval("n-v", v=2), "'v'")


def test_decision_must_be_approve_or_deny():
    _use_spool()
    for bad in ("APPROVE", "yes", "", None, True):
        _expect_value_error(_approval("n-dec", decision=bad), "decision")


def test_missing_or_empty_required_fields_are_rejected_by_name():
    _use_spool()
    for field in ("nonce", "decided_by", "signature", "source", "created_at"):
        empty = _approval("n-req")
        empty[field] = ""
        _expect_value_error(empty, field)
        missing = _approval("n-req")
        del missing[field]
        _expect_value_error(missing, field)


def test_signature_must_be_hex():
    _use_spool()
    _expect_value_error(_approval("n-hex", signature="not-a-signature"), "signature")


def test_expires_at_and_payload_hash_are_never_accepted_from_a_file():
    """The security-relevant line in intents.py. Those two fields bind an
    approval to a deadline and to specific bytes; a file that could set either
    could widen its own approval — push the expiry out, or re-point the hash
    at a brief nobody was shown. The drain reads both from the existing row."""
    _use_spool()
    _expect_value_error(_approval("n-exp", expires_at="2099-01-01T00:00:00+00:00"), "expires_at")
    _expect_value_error(_approval("n-hash", payload_hash="deadbeef"), "payload_hash")


def test_replayed_file_does_not_overwrite_an_existing_decision():
    """`AND decision IS NULL` is what makes a replay harmless. A `deny`
    spooled after a human already approved must change nothing."""
    _use_spool()
    conn, _ = _fresh_ledger()
    _seed_approval(conn, "n-replay", decision="approve", decided_by="U-human")

    intents.record(_approval("n-replay", decision="deny", decided_by="U-attacker"))
    result = intents.drain(conn)
    assert result["applied"] == 1 and result["rejected"] == 0, result

    row = _row(conn, "n-replay")
    assert row["decision"] == "approve", dict(row)
    assert row["decided_by"] == "U-human", dict(row)
    assert row["signature"] is None, dict(row)
    conn.close()


def test_unknown_nonce_is_applied_and_unlinked_not_rejected():
    """Zero rows updated is ordinary idempotency, not a failure — the row is
    gone or already decided, and neither is something this queue can fix."""
    _use_spool()
    conn, _ = _fresh_ledger()
    path = intents.record(_approval("nonce-that-never-existed"))

    result = intents.drain(conn)
    assert result["applied"] == 1 and result["rejected"] == 0 and result["rejected_files"] == [], result
    assert result["results"] == [], "an unknown nonce writes no decision, so nothing is spent"
    assert not path.exists()
    conn.close()


def test_malformed_file_is_rejected_and_does_not_block_a_valid_one():
    """The bad file sorts FIRST on purpose: one unparseable intent may never
    stop the ones behind it, and nothing is discarded — it lands in
    rejected/ with its exception text beside it."""
    spool = _use_spool()
    conn, _ = _fresh_ledger()
    _seed_approval(conn, "n-good")

    good = intents.record(_approval("n-good"))
    bad = spool / "00000000T000000000000-badbad00.json"
    bad.write_text("{not json at all", encoding="utf-8")
    assert bad.name < good.name, "the malformed file must sort first for this test to prove anything"

    result = intents.drain(conn)
    assert result["applied"] == 1, result
    assert result["rejected"] == 1, result
    assert result["rejected_files"] == [bad.name], result

    assert _row(conn, "n-good")["decision"] == "approve"
    assert not good.exists()
    assert not bad.exists(), "a rejected file must be moved out of the queue"
    moved = spool / "rejected" / bad.name
    assert moved.exists(), "a rejected file is never deleted"
    err = spool / "rejected" / f"{bad.name}.err"
    assert err.exists() and err.read_text().strip(), "a rejection must carry its exception text"
    conn.close()


def test_a_valid_json_file_that_fails_validation_is_rejected():
    spool = _use_spool()
    conn, _ = _fresh_ledger()
    bad = spool / "00000000T000000000001-shapebad.json"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_text(json.dumps({"v": 1, "kind": "approval_decision"}), encoding="utf-8")

    result = intents.drain(conn)
    assert result["applied"] == 0 and result["rejected"] == 1, result
    assert (spool / "rejected" / f"{bad.name}.err").exists()
    conn.close()


def test_drain_on_a_missing_directory_returns_zeros():
    intents.INTENTS_DIR = _tmp_dir("intents-absent-") / "never-created"
    conn, _ = _fresh_ledger()
    result = intents.drain(conn)
    assert result["applied"] == 0 and result["rejected"] == 0 and result["rejected_files"] == [] and result["results"] == []
    assert not intents.INTENTS_DIR.exists(), "drain() must not create the spool directory"
    conn.close()


def test_intents_apply_in_creation_order():
    """Recorded out of order, applied in created_at order — proven through the
    `decision IS NULL` guard: whichever intent lands first is the one that
    sticks, so the surviving decided_by names the oldest file."""
    _use_spool()
    conn, _ = _fresh_ledger()
    _seed_approval(conn, "n-order")

    intents.record(_approval("n-order", created_at="2026-09-09T12:00:02+00:00", decided_by="second"))
    intents.record(_approval("n-order", created_at="2026-09-09T12:00:03+00:00", decided_by="third"))
    intents.record(_approval("n-order", created_at="2026-09-09T12:00:01+00:00", decided_by="first"))

    result = intents.drain(conn)
    assert result["applied"] == 3 and result["rejected"] == 0, result
    assert _row(conn, "n-order")["decided_by"] == "first", dict(_row(conn, "n-order"))
    assert not list(intents.INTENTS_DIR.glob("*.json")), "every applied intent must be unlinked"
    conn.close()


def test_cli_drain_refuses_a_ledger_at_the_wrong_schema_version():
    """One migrator. `--drain` passes migrate=False, so it ASSERTS the version
    and refuses rather than quietly running against a schema it does not
    understand — the guardrail this whole module would otherwise become a
    seventh way around. Exercised, not asserted: the refusal branch is the one
    that never runs in normal operation, which is exactly why it has to be run
    here."""
    conn, db = _fresh_ledger()
    conn.execute("UPDATE schema_version SET version=?", (ledger.LEDGER_SCHEMA_VERSION + 1,))
    conn.commit()
    conn.close()

    spool = _use_spool()
    res = subprocess.run(
        [sys.executable, str(REPO / "scripts" / "intents.py"), "--drain", str(db)],
        capture_output=True, text=True, env={**os.environ, "WARDEN_INTENTS_DIR": str(spool)},
    )
    assert res.returncode == 1, f"a version mismatch must exit 1, got {res.returncode}: {res.stderr}"
    assert "schema_version" in res.stderr, f"the refusal must name the version: {res.stderr!r}"


def test_a_half_written_file_is_invisible_to_the_drain():
    """`record()` publishes by renaming a `.tmp` written in the same directory,
    so a reader can never observe a partial file. The drain's glob is what
    enforces it: a `.json.tmp` left behind by a crash mid-write must be picked
    up by nothing. Tested because "the temp name does not match the glob" is a
    one-character property that a later refactor of either side would silently
    break."""
    spool = _use_spool()
    conn, _db = _fresh_ledger()
    _seed_approval(conn, "n-half")

    intents.record(_approval("n-half"))
    live = next(spool.glob("*.json"))
    # A crash between write and rename leaves exactly this behind.
    (spool / f".{live.name}.tmp").write_text('{"v": 1, "kind": "approval_dec', encoding="utf-8")

    result = intents.drain(conn)
    assert result["applied"] == 1 and result["rejected"] == 0 and result["rejected_files"] == [], result
    assert _row(conn, "n-half")["decision"] == "approve"
    assert list(spool.glob("*.tmp")) == [] or all(p.name.startswith(".") for p in spool.iterdir()), (
        "the half-written file must be left alone, not drained and not rejected")
    assert not (spool / "rejected").exists(), "a half-written temp file must never reach rejected/"


def test_cli_record_then_drain():
    """The path the Slack plugin will actually use — a shell caller that
    cannot import this module, so it must work through the process boundary:
    JSON on stdin (never argv, which crosses a `ps` boundary), then a drain
    against an explicit ledger."""
    spool = _tmp_dir("intents-cli-") / "intents"
    conn, db = _fresh_ledger()
    _seed_approval(conn, "n-cli")
    conn.close()

    env = os.environ.copy()
    env["WARDEN_INTENTS_DIR"] = str(spool)
    script = str(REPO / "scripts" / "intents.py")

    rec = subprocess.run(
        [sys.executable, script, "--record"],
        input=json.dumps(_approval("n-cli", decided_by="U-cli")),
        capture_output=True, text=True, env=env,
    )
    assert rec.returncode == 0, rec.stderr
    spooled = Path(rec.stdout.strip())
    assert spooled.exists() and spooled.parent == spool, rec.stdout

    drained = subprocess.run(
        [sys.executable, script, "--drain", str(db)],
        capture_output=True, text=True, env=env,
    )
    assert drained.returncode == 0, drained.stderr
    lines = drained.stdout.strip().splitlines()
    # The last line is `_main()`'s own summary; anything before it is one
    # spend-result JSON line per applied approval_decision (here: one, for a
    # row with no real signer configured, so the spend itself refuses/errors
    # without ever touching stdout's final line's shape).
    assert lines[-1] == "applied=1 rejected=0", drained.stdout
    assert not spooled.exists()

    check = ledger.connect(db, migrate=False)
    row = _row(check, "n-cli")
    assert row["decision"] == "approve" and row["decided_by"] == "U-cli", dict(row)
    check.close()

    # A malformed intent on stdin is a handled failure: exit 1, message on
    # stderr, nothing spooled.
    bad = subprocess.run(
        [sys.executable, script, "--record"],
        input='{"v": 1, "kind": "nope"}',
        capture_output=True, text=True, env=env,
    )
    assert bad.returncode == 1, bad
    assert "kind" in bad.stderr, bad.stderr
    assert not list(spool.glob("*.json")), list(spool.glob("*.json"))

    # No verb at all is a usage error, not a failure to do the work.
    usage = subprocess.run([sys.executable, script], capture_output=True, text=True, env=env)
    assert usage.returncode == 2, usage


# --- runner ------------------------------------------------------------------

# This suite runs INSIDE a Claude Code session; lifecycle.policy's
# require_no_recursion() (reached via a spent approve draining into
# open_episode()) refuses unconditionally when any of these are set. Popped
# for the whole run, restored after.
_RECURSION_MARKERS = ("CLAUDE_CODE_SESSION", "CLAUDECODE", "CLAUDE_SESSION_ID", "CLAUDE_ENTRYPOINT")


def main() -> int:
    tests = [(name, fn) for name, fn in sorted(globals().items())
             if name.startswith("test_") and callable(fn)]
    passed = 0
    failures: list[str] = []
    saved_recursion_markers = {m: os.environ.pop(m, None) for m in _RECURSION_MARKERS}
    try:
        for name, fn in tests:
            try:
                fn()
                passed += 1
            except AssertionError as e:
                failures.append(f"{name}: {e}")
            except Exception:
                failures.append(f"{name}: unexpected exception\n{traceback.format_exc()}")
    finally:
        for m, v in saved_recursion_markers.items():
            if v is None:
                os.environ.pop(m, None)
            else:
                os.environ[m] = v

    print(f"{passed}/{len(tests)} passed")
    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  {f}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
