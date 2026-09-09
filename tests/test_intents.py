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

import importlib.util
import json
import os
import stat
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

_ledger_spec = importlib.util.spec_from_file_location("ledger", REPO / "scripts" / "ledger.py")
ledger = importlib.util.module_from_spec(_ledger_spec)
_ledger_spec.loader.exec_module(ledger)

_intents_spec = importlib.util.spec_from_file_location("intents", REPO / "scripts" / "intents.py")
intents = importlib.util.module_from_spec(_intents_spec)
_intents_spec.loader.exec_module(intents)

# A plausible Ed25519 signature: 64 bytes of hex. Nothing in intents.py checks
# its length or its authenticity — that happens once, at spend time, in
# require_signed_approval(). This is here only so the SHAPE check has real
# input.
SIG = "ab" * 64


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


def _seed_approval(conn, nonce: str, *, decision=None, decided_by=None) -> None:
    conn.execute(
        "INSERT INTO dispatch_approvals (nonce, verb, repo, tier, payload_hash, created_at, expires_at, "
        "decision, decided_by) VALUES (?,?,?,?,?,?,?,?,?)",
        (nonce, "dispatch", "warden", "0", "hash-" + nonce, "2026-09-09T00:00:00+00:00",
         "2026-09-09T01:00:00+00:00", decision, decided_by),
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
    spool = _use_spool()
    conn, _ = _fresh_ledger()
    _seed_approval(conn, "n1")

    path = intents.record(_approval("n1", decided_by="U999"))
    assert path.exists() and path.parent == spool

    result = intents.drain(conn)
    assert result == {"applied": 1, "rejected": 0, "rejected_files": []}, result

    row = _row(conn, "n1")
    assert row["decision"] == "approve", dict(row)
    assert row["decided_by"] == "U999", dict(row)
    assert row["signature"] == SIG, dict(row)
    assert row["decided_at"], "decided_at must be stamped by the drain"
    assert not path.exists(), "an applied intent must be unlinked"
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
    assert result == {"applied": 1, "rejected": 0, "rejected_files": []}, result
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
    assert intents.drain(conn) == {"applied": 0, "rejected": 0, "rejected_files": []}
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
    conn.execute("UPDATE schema_version SET version=?", (ledger.SCHEMA_VERSION + 1,))
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
    assert result == {"applied": 1, "rejected": 0, "rejected_files": []}, result
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
    assert drained.stdout.strip() == "applied=1 rejected=0", drained.stdout
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

def main() -> int:
    tests = [(name, fn) for name, fn in sorted(globals().items())
             if name.startswith("test_") and callable(fn)]
    passed = 0
    failures: list[str] = []
    for name, fn in tests:
        try:
            fn()
            passed += 1
        except AssertionError as e:
            failures.append(f"{name}: {e}")
        except Exception:
            failures.append(f"{name}: unexpected exception\n{traceback.format_exc()}")

    print(f"{passed}/{len(tests)} passed")
    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  {f}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
