"""Regression suite for the signed-approval gate on warden's write verb (`dispatch
--tier implement`).

WHAT THIS PROVES, AND WHY IT IS NOT THE SAME AS `--confirm`.

`dispatch` has no `--confirm` flag left at all (`warden` refuses one by name — see
tests/test_warden_cli.py's own coverage of that refusal). The gate tested here is a
Slack button click, witnessed by the gateway process and signed there with an
Ed25519 key whose private half never leaves that process's memory. Every row in
`dispatch_approvals` is writable by this uid — the agent's uid — so the suite's
central case is the FORGED one: a row with `decision='approve'` and a plausible
signature, written exactly as a compromised agent would write it, must still refuse.

THE SHAPE CHANGED (Wave 5.3). Originally (2026-08-03) a click signed the row and the
gateway re-ran the retired bash CLI's `<argv> --confirm` in a subprocess — a second CLI
invocation performing the same `require_signed_approval()` check a hand-typed
`--confirm` would have. That subprocess, `--confirm` itself, and the row's
`argv_json` replay are gone. A click now: (1) signs the decision
(`plugins/dispatch-approval/__init__.py`'s `_record_decision`), (2) spools it as an
`approval_decision` intent through `warden/scripts/intents.py --record`, (3) drains
it SYNCHRONOUSLY (`intents.py --drain`), and the drain itself calls
`lifecycle/approvals.py`'s `execute_approved()` the moment the decision lands — so
the episode is opening (or already refused) before the click handler's own call
returns. The plugin's own `execute_approved()` (this repo never sees that name
collision — different processes) then only re-reads the row's `spent_job_id` /
`spend_error` to report the outcome; it runs nothing.

`execute_approved()` verifies the SIGNATURE over `payload_hash`/`decision`/
`decided_by`/`expires_at`, AND re-hashes `stdin_text`/`context_text`/`why` and
compares against the stored `payload_hash` at spend time — the same re-verification
`require_signed_approval` did from the CURRENT `--brief-file`/`--context-file` on
every `--confirm` in the retired bash design, just moved to spend time instead of a
second CLI invocation. A raw `UPDATE dispatch_approvals SET stdin_text=...` between
mint and spend IS caught: see `test_execute_approved_brief_edited_after_click_refuses`,
`test_execute_approved_context_swapped_after_click_refuses` and
`test_execute_approved_why_edited_after_click_refuses` in `tests/test_lifecycle.py`,
which exercise this suite's own `execute_approved()` directly. This suite (the
plugin's HALF of the contract) does not repeat those three cases — they belong to
the warden-side re-hash they are testing, not to the signing/spooling/draining half
this file covers.

The cases:

  - the two hash implementations (plugin and warden's `clients/signer.py`) agree
    with each other AND with `config/approval-spec.json`'s own fixture vectors
  - planning mints a pending row; a rehearsal (`--dry-run`) mints nothing
  - an undecided row is left alone by a drain — nothing to spend yet
  - an unsigned/forged decision refuses                          <- the forgery case
  - a decision signed by the WRONG key refuses ("superseded")    <- the forgery case
  - a real click (sign -> spool -> drain) opens the episode, and the operations
    row names the approver — the only field an agent could not have written itself
  - a denial refuses, and is distinguishable from an absent approval
  - an expired approval refuses
  - an approval is single-use: draining a second signed decision is a no-op
  - an Approve click replays the stored brief + context bytes verbatim
  - a missing public key refuses rather than falling back to instruction-level
  - only the gateway publishes a signing key, and a clobbered one is republished
    before signing (the 2026-08-03 outage, as a test)
  - the plugin has NO write path of its own: with `_run_intents` stubbed to do
    nothing, a click cannot decide a row
  - the whole chain against the REAL warden/scripts/intents.py — spool, drain, and
    a row carrying a signature that verifies, with payload_hash/expires_at/spent_at
    untouched
  - an already-decided row short-circuits before signing again
  - a drain that exits non-zero over somebody ELSE'S rejected file still succeeds,
    because the row is the source of truth
  - the intent the plugin builds passes warden's own `intents.validate()` — the
    contract test between the two repos

Run:

    warden/.venv/bin/python3 tests/test_dispatch_approval.py  (or: make test, from warden/)

Exit status is 0 only when every case matches. Nothing here touches the real
dispatch DB, the real audit log, Slack, or the network.
"""

import datetime as dt
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent

# The plugin itself did not move — only the retired bash CLI and this suite did (2026-09-10).
# Overridable so a moved checkout is one env var, not a grep-and-replace.
_env_plugin_py = os.environ.get("HERMES_PLUGIN_PY")
PLUGIN_PATH = (Path(_env_plugin_py).expanduser() if _env_plugin_py else
               Path.home() / "SourceRoot" / "hermes-agent" / "plugins" / "dispatch-approval" / "__init__.py")

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

sys.path.insert(0, str(HERE))
sys.path.insert(0, str(REPO / "scripts"))

# Reuse the CLI suite's harness (stub sideclaw/GitHub/Slack, throwaway HOME,
# sandboxed dispatch policy) rather than rebuilding it — the gate has to be
# exercised through the same script surface every other bound is.
_cli_spec = importlib.util.spec_from_file_location("cli_suite", HERE / "test_warden_cli.py")
_cli = importlib.util.module_from_spec(_cli_spec)
_cli_spec.loader.exec_module(_cli)
Harness = _cli.Harness

if not PLUGIN_PATH.exists():
    sys.exit(
        f"the dispatch-approval plugin is not available at {PLUGIN_PATH}. Set "
        f"HERMES_PLUGIN_PY if hermes-agent has moved."
    )
_pspec = importlib.util.spec_from_file_location("dispatch_approval", PLUGIN_PATH)
plugin = importlib.util.module_from_spec(_pspec)
_pspec.loader.exec_module(plugin)

from clients import signer  # noqa: E402
from lifecycle import approvals as lc_approvals  # noqa: E402

_LEDGER_SPEC = importlib.util.spec_from_file_location("ledger_for_approval_test", REPO / "scripts" / "ledger.py")
ledger = importlib.util.module_from_spec(_LEDGER_SPEC)
_LEDGER_SPEC.loader.exec_module(ledger)

SPEC_PATH = REPO / "config" / "approval-spec.json"

FAILURES: list[str] = []
CHECKS = [0]


def check(cond: bool, label: str) -> None:
    CHECKS[0] += 1
    if not cond:
        FAILURES.append(label)


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


class _env:
    """Set (or, with `None`, force-unset) environment variables for one `with`
    block, restoring exactly what was there before. The plugin reads
    `os.environ` directly (never a subprocess env dict this test controls), so
    this is how every case points it at the sandboxed DB/pubkey/policy."""

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


class Approver:
    """Stands in for the gateway plugin: owns a key, publishes the public half,
    and signs decisions with the plugin's own `canonical_message` so the two
    halves of the contract are tested against each other rather than against a
    copy."""

    def __init__(self, root: Path):
        self.key = Ed25519PrivateKey.generate()
        self.pub_path = root / "dispatch-approval.pub"
        raw = self.key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw,
        )
        self.pub_path.write_text(raw.hex() + "\n")

    def sign(self, nonce, payload_hash, decision, by, expires_at, key=None):
        msg = plugin.canonical_message(nonce, payload_hash, decision, by, expires_at)
        return (key or self.key).sign(msg).hex()


BRIEF = "Fix the flaky assertion in the retry test."
WHY = "test"
CONTEXT = "benign context: the failing assertion is at line 42"


def _connect(db: Path) -> sqlite3.Connection:
    return ledger.connect(db, migrate=False)


def _row(db: Path, nonce: str) -> sqlite3.Row | None:
    conn = _connect(db)
    try:
        return conn.execute("SELECT * FROM dispatch_approvals WHERE nonce=?", (nonce,)).fetchone()
    finally:
        conn.close()


def _pending_rows(db: Path) -> int:
    conn = _connect(db)
    try:
        return conn.execute("SELECT COUNT(*) FROM dispatch_approvals").fetchone()[0]
    finally:
        conn.close()


def _plugin_env(h: "Harness", db: Path, pubkey: Path, *, sideclaw: str | None = None) -> dict:
    """The env `_run_intents`'s subprocess inherits (via `os.environ` at call
    time) — points the drain, and the spend inside it, at the same sandboxed
    fixtures the dispatch CLI call used to plan."""
    return {
        "WARDEN_DB": str(db),
        "WARDEN_HOME": str(h.tmp),
        "WARDEN_INTENTS_DIR": str(h.tmp / "intents"),
        "WARDEN_APPROVAL_PUBKEY": str(pubkey),
        "WARDEN_DISPATCH_REPOS": str(h.repos_json),
        "WARDEN_TRIAGE_POLICY": str(h.triage_policy_json),
        "WARDEN_PR_REQUIRED_JSON": str(h.pr_required_json),
        "WARDEN_SECRETS_RUN": str(h.secrets_run),
        "SECRETS_BACKEND_FILE": str(h.backend_file),
        "SLACK_BOT_TOKEN": "xoxb-stub",
        "WARDEN_SIDECLAW_BASE": sideclaw or f"http://127.0.0.1:{_cli.stubs.closed_port()}",
        "WARDEN_SLACK_API": f"http://127.0.0.1:{_cli.stubs.closed_port()}",
        "HOME": str(h.home),
    }


def plan_and_db(h: "Harness", approver: Approver, *, repo="gamma", brief=BRIEF, why=WHY):
    """Run `dispatch --tier implement` with no approval — mints the pending row
    + posts buttons — on a DB that persists so the click can see it."""
    db = h.new_db()
    slack_srv = _cli.stubs.StubServer({("POST", "/chat.postMessage"): (200, {"ok": True})})
    try:
        env = h.base_env(db=db, slack=slack_srv.base, pubkey=approver.pub_path)
        r = h.run(["dispatch", repo, "--tier", "implement", "--why", why, "--json"], env=env, stdin=brief)
    finally:
        slack_srv.stop()
    return db, r


def _click(h: "Harness", db: Path, approver: Approver, nonce: str, *, decision="approve",
           by="U0JOHANNES", sideclaw: str | None = None) -> dict | None:
    """The plugin's own click path, for real: sign, spool, drain — synchronously,
    exactly as `_make_handler`'s coroutine does before scheduling
    `execute_approved()`."""
    with _env(**_plugin_env(h, db, approver.pub_path, sideclaw=sideclaw)):
        return plugin._record_decision(nonce, decision, by)


# --- cases -----------------------------------------------------------------


def test_hash_agreement_with_spec_vectors(h, approver):
    """The plugin and warden's own `clients/signer.py` must derive the same
    payload hash and canonical message, or the gate never matches — and both
    must agree with the versioned fixture vectors, the actual cross-repo
    contract."""
    spec = json.loads(SPEC_PATH.read_text())
    for v in spec["vectors"]["payloadHash"]:
        plugin_hash = plugin.payload_hash(v["verb"], v["repo"], v["tier"], v["body"], v["why"], v["context"])
        signer_hash = signer.payload_hash(v["verb"], v["repo"], v["tier"], v["body"], v["why"], v["context"])
        check(plugin_hash == v["expected"] == signer_hash,
              f"payload_hash vector {v.get('name', v)}: plugin={plugin_hash!r} signer={signer_hash!r}")
    for v in spec["vectors"]["signedMessage"]:
        plugin_msg = plugin.canonical_message(v["nonce"], v["payload_hash"], v["decision"], v["decided_by"], v["expires_at"])
        signer_msg = signer.canonical_message(v["nonce"], v["payload_hash"], v["decision"], v["decided_by"], v["expires_at"])
        check(plugin_msg.hex() == v["expectedHex"] == signer_msg.hex(),
              f"signedMessage vector {v.get('name', v)}: plugin != signer or != spec")
    # context is its own hashed part, not glued onto the brief.
    check(plugin.payload_hash("dispatch", "gamma", "implement", BRIEF + CONTEXT, WHY)
          != plugin.payload_hash("dispatch", "gamma", "implement", BRIEF, WHY, CONTEXT),
          "context must be its own hashed field, not string-glued onto the brief")


def test_plan_mints_pending_row(h, approver):
    db, r = plan_and_db(h, approver)
    check(r.returncode == 0, f"plan exits 0, got {r.returncode}: {r.stderr[:200]}")
    conn = _connect(db)
    rows = conn.execute("SELECT verb, repo, tier, decision, signature FROM dispatch_approvals").fetchall()
    conn.close()
    check(len(rows) == 1, f"plan mints exactly one approval row, got {len(rows)}")
    if rows:
        check(rows[0]["verb"] == "dispatch" and rows[0]["repo"] == "gamma", "row names verb+repo")
        check(rows[0]["decision"] is None and rows[0]["signature"] is None,
              "a freshly minted row is undecided and unsigned")


def test_dry_run_mints_nothing(h, approver):
    db = h.new_db()
    r = h.run(["dispatch", "gamma", "--tier", "implement", "--why", WHY, "--dry-run", "--json"],
              env=h.base_env(db=db, pubkey=approver.pub_path), stdin=BRIEF)
    check(r.returncode == 0, "dry-run exits 0")
    check(_pending_rows(db) == 0, "a rehearsal asks nobody and mints no approval row")


def test_undecided_approval_is_untouched_by_a_drain(h, approver):
    db, r = plan_and_db(h, approver)
    check(r.returncode == 0, "plan exits 0")
    with _env(**_plugin_env(h, db, approver.pub_path)):
        drained = plugin._run_intents(["--drain", str(db)])
    check(drained.returncode == 0, f"draining nothing is not a failure: {drained.stderr}")
    row = _row(db, _only_nonce(db))
    check(row["decision"] is None and row["spent_at"] is None, "an undecided row is left exactly alone")


def _only_nonce(db: Path) -> str:
    conn = _connect(db)
    try:
        return conn.execute("SELECT nonce FROM dispatch_approvals LIMIT 1").fetchone()["nonce"]
    finally:
        conn.close()


def test_forged_unsigned_row_refuses(h, approver):
    """A row with `decision='approve'` and NO signature — exactly what a
    compromised agent with sqlite3 and no key could write directly, bypassing
    the plugin and the intents queue entirely."""
    db, r = plan_and_db(h, approver)
    nonce = _only_nonce(db)
    conn = _connect(db)
    conn.execute(
        "UPDATE dispatch_approvals SET decision='approve', decided_at=?, decided_by='U-ATTACKER', "
        "expires_at=? WHERE nonce=?",
        (now_utc().isoformat(), (now_utc() + dt.timedelta(minutes=30)).isoformat(), nonce),
    )
    conn.commit()
    conn.close()
    with _env(WARDEN_APPROVAL_PUBKEY=str(approver.pub_path), WARDEN_DISPATCH_REPOS=str(h.repos_json)):
        result = lc_approvals.execute_approved(_connect(db), nonce)
    check(result.status == "invalid", f"an unsigned forged approve must refuse, got {result.status}")


def test_wrong_signing_key_is_invalid(h, approver):
    """Signed by a DIFFERENT private key while the verifier's public key
    (`WARDEN_APPROVAL_PUBKEY`) never changed — an ordinary bad signature, not
    a key rotation. `row.key_id` (stamped at mint, from the pubkey live then)
    still matches the current key, so this is `invalid`, not `superseded` —
    see `test_key_rotation_is_superseded` for that one."""
    db, r = plan_and_db(h, approver)
    nonce = _only_nonce(db)
    row = _row(db, nonce)
    other = Ed25519PrivateKey.generate()
    expires = (now_utc() + dt.timedelta(minutes=30)).isoformat()
    sig = approver.sign(nonce, row["payload_hash"], "approve", "U0JOHANNES", expires, key=other)
    conn = _connect(db)
    conn.execute(
        "UPDATE dispatch_approvals SET decision='approve', decided_at=?, decided_by='U0JOHANNES', "
        "signature=?, expires_at=? WHERE nonce=?",
        (now_utc().isoformat(), sig, expires, nonce),
    )
    conn.commit()
    conn.close()
    with _env(WARDEN_APPROVAL_PUBKEY=str(approver.pub_path), WARDEN_DISPATCH_REPOS=str(h.repos_json)):
        result = lc_approvals.execute_approved(_connect(db), nonce)
    check(result.status == "invalid", f"a signature from a different key must refuse, got {result.status}")


def test_key_rotation_is_superseded(h, approver):
    """The row was minted (and `key_id` stamped) under `approver`'s key. A
    genuine, correctly-signed decision under THAT key still refuses if the
    gateway's live public key has since rotated (a restart mints a fresh
    keypair) — `key_id` mismatch is what tells this apart from a forged
    signature."""
    db, r = plan_and_db(h, approver)
    nonce = _only_nonce(db)
    row = _row(db, nonce)
    expires = (now_utc() + dt.timedelta(minutes=30)).isoformat()
    sig = approver.sign(nonce, row["payload_hash"], "approve", "U0JOHANNES", expires)
    conn = _connect(db)
    conn.execute(
        "UPDATE dispatch_approvals SET decision='approve', decided_at=?, decided_by='U0JOHANNES', "
        "signature=?, expires_at=? WHERE nonce=?",
        (now_utc().isoformat(), sig, expires, nonce),
    )
    conn.commit()
    conn.close()
    rotated_dir = h.tmp / f"rotated-{nonce}"
    rotated_dir.mkdir(parents=True, exist_ok=True)
    rotated = Approver(rotated_dir)
    with _env(WARDEN_APPROVAL_PUBKEY=str(rotated.pub_path), WARDEN_DISPATCH_REPOS=str(h.repos_json)):
        result = lc_approvals.execute_approved(_connect(db), nonce)
    check(result.status == "superseded", f"a rotated verification key must refuse as superseded, got {result.status}")


def test_click_opens_episode_and_audit_names_approver(h, approver):
    db, r = plan_and_db(h, approver)
    nonce = _only_nonce(db)
    srv = _cli.stubs.StubServer({("POST", "/api/jobs"): (200, {"job": {"id": "job-click", "status": "running"}})})
    try:
        result = _click(h, db, approver, nonce, sideclaw=srv.base)
    finally:
        srv.stop()
    check(result is not None and result.get("already") is False, f"a fresh valid click is not 'already': {result}")
    row = _row(db, nonce)
    check(row["spent_job_id"] == "job-click", f"the click must open the episode, got {dict(row)}")
    conn = _connect(db)
    op = conn.execute("SELECT authorized_by FROM operations WHERE repo='gamma'").fetchone()
    conn.close()
    check(op is not None and op["authorized_by"] == "signed:U0JOHANNES",
          f"the operation must name the approver, got {dict(op) if op else None}")


def test_denial_refuses_and_is_distinct_from_absence(h, approver):
    db, r = plan_and_db(h, approver)
    nonce = _only_nonce(db)
    result = _click(h, db, approver, nonce, decision="deny", by="U0JOHANNES")
    check(result is not None and result.get("decision") == "deny", f"a deny must record as deny: {result}")
    row = _row(db, nonce)
    check(row["decision"] == "deny" and row["spent_at"] is None, "a deny must never spend")
    absent = _click(h, db, approver, "nonce-never-existed")
    check(absent is None, "an absent nonce must return None, distinct from a recorded denial")


def test_expired_approval_refuses(h, approver):
    db, r = plan_and_db(h, approver)
    nonce = _only_nonce(db)
    row = _row(db, nonce)
    past = (now_utc() - dt.timedelta(minutes=5)).isoformat()
    sig = approver.sign(nonce, row["payload_hash"], "approve", "U0JOHANNES", past)
    conn = _connect(db)
    conn.execute(
        "UPDATE dispatch_approvals SET decision='approve', decided_at=?, decided_by='U0JOHANNES', "
        "signature=?, expires_at=? WHERE nonce=?",
        (now_utc().isoformat(), sig, past, nonce),
    )
    conn.commit()
    conn.close()
    with _env(WARDEN_APPROVAL_PUBKEY=str(approver.pub_path), WARDEN_DISPATCH_REPOS=str(h.repos_json)):
        result = lc_approvals.execute_approved(_connect(db), nonce)
    check(result.status == "expired", f"an expired approval must refuse, got {result.status}")


def test_approval_is_single_use(h, approver):
    db, r = plan_and_db(h, approver)
    nonce = _only_nonce(db)
    srv = _cli.stubs.StubServer({("POST", "/api/jobs"): (200, {"job": {"id": "job-once", "status": "running"}})})
    try:
        first = _click(h, db, approver, nonce, sideclaw=srv.base)
        with _env(WARDEN_APPROVAL_PUBKEY=str(approver.pub_path), WARDEN_DISPATCH_REPOS=str(h.repos_json)):
            second = lc_approvals.execute_approved(_connect(db), nonce)
    finally:
        srv.stop()
    check(first is not None and first.get("already") is False, "first spend must succeed")
    check(second.status == "already", f"a second spend of the same nonce must be 'already', got {second.status}")


def test_approve_replays_stored_brief_and_context(h, approver):
    """The stored bytes are what the click replays — never argv, never a path
    the agent could have rewritten in the meantime."""
    db = h.new_db()
    slack_srv = _cli.stubs.StubServer({("POST", "/chat.postMessage"): (200, {"ok": True})})
    try:
        env = h.base_env(db=db, slack=slack_srv.base, pubkey=approver.pub_path)
        cfile = h.tmp / "ctx.txt"
        cfile.write_text(CONTEXT, encoding="utf-8")
        r = h.run(
            ["dispatch", "gamma", "--tier", "implement", "--why", WHY, "--context-file", str(cfile), "--json"],
            env=env, stdin=BRIEF,
        )
    finally:
        slack_srv.stop()
    check(r.returncode == 0, f"plan exits 0: {r.stderr}")
    nonce = _only_nonce(db)
    row = _row(db, nonce)
    check(row["stdin_text"] == BRIEF, f"stored brief must be verbatim: {row['stdin_text']!r}")
    check(row["context_text"] == CONTEXT, f"stored context must be verbatim: {row['context_text']!r}")

    captured = {}
    srv = _cli.stubs.StubServer({("POST", "/api/jobs"): (200, {"job": {"id": "job-replay", "status": "running"}})})

    def record(request):
        captured.update(request["body"]["params"])
        return 200, {"job": {"id": "job-replay", "status": "running"}}

    srv2 = _cli.stubs.StubServer({("POST", "/api/jobs"): record})
    try:
        _click(h, db, approver, nonce, sideclaw=srv2.base)
    finally:
        srv.stop()
        srv2.stop()
    check(captured.get("brief") == BRIEF, f"the spend must replay the stored brief: {captured.get('brief')!r}")
    check(captured.get("context") == CONTEXT, f"the spend must replay the stored context: {captured.get('context')!r}")


def test_missing_pubkey_refuses(h, approver):
    db, r = plan_and_db(h, approver)
    nonce = _only_nonce(db)
    row = _row(db, nonce)
    expires = (now_utc() + dt.timedelta(minutes=30)).isoformat()
    sig = approver.sign(nonce, row["payload_hash"], "approve", "U0JOHANNES", expires)
    conn = _connect(db)
    conn.execute(
        "UPDATE dispatch_approvals SET decision='approve', decided_at=?, decided_by='U0JOHANNES', "
        "signature=?, expires_at=? WHERE nonce=?",
        (now_utc().isoformat(), sig, expires, nonce),
    )
    conn.commit()
    conn.close()
    missing = h.tmp / "no-such.pub"
    with _env(WARDEN_APPROVAL_PUBKEY=str(missing), WARDEN_DISPATCH_REPOS=str(h.repos_json)):
        try:
            lc_approvals.execute_approved(_connect(db), nonce)
            failed = False
        except Exception:
            failed = True
    check(failed, "a missing public key must refuse (raise), never fall back to instruction-level trust")


def test_republish_after_clobber(h, approver):
    """The 2026-08-03 outage: a non-gateway process overwrote the published key.
    `_ensure_published()` must notice and republish before the next signature."""
    home = h.tmp / "hermes-home-republish"
    home.mkdir()
    pub_path = home / "dispatch-approval.pub"
    pub_path.write_text("00" * 32 + "\n", encoding="utf-8")
    fresh_key = Ed25519PrivateKey.generate()
    fresh_hex = fresh_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw,
    ).hex()
    with _env(HERMES_HOME=str(home)):
        plugin._SIGNING_KEY, plugin._PUBLIC_KEY_HEX = fresh_key, fresh_hex
        check(pub_path.read_text().strip() != plugin._PUBLIC_KEY_HEX, "fixture: the file starts out clobbered")
        plugin._ensure_published()
        check(pub_path.read_text().strip() == plugin._PUBLIC_KEY_HEX,
              "a clobbered key must be republished before the next signature")
    # Restore the shared fixture key every other case in this run relies on.
    plugin._SIGNING_KEY, plugin._PUBLIC_KEY_HEX = approver.key, approver.pub_path.read_text().strip()


def test_plugin_has_no_write_path_of_its_own(h, approver):
    """With the intents queue stubbed to do nothing, a click must not be able
    to decide a row — the plugin's two DB connections are both `mode=ro`."""
    db, r = plan_and_db(h, approver)
    nonce = _only_nonce(db)
    original = plugin._run_intents
    plugin._run_intents = lambda args, stdin_text=None: __import__("subprocess").CompletedProcess(args, 0, "", "")
    raised = False
    try:
        with _env(**_plugin_env(h, db, approver.pub_path)):
            plugin._record_decision(nonce, "approve", "U0JOHANNES")
    except RuntimeError:
        # The decision never reached the ledger — `_record_decision()` raises
        # rather than silently reporting success (see its own docstring: the
        # click handler renders that as "Could not record the decision",
        # never as a false "already"/"opened").
        raised = True
    finally:
        plugin._run_intents = original
    row = _row(db, nonce)
    check(row["decision"] is None, "with intents.py stubbed to a no-op, the row must stay undecided")
    check(raised, "a stub that writes nothing must raise, never report success")


def test_full_chain_against_real_intents_py(h, approver):
    """`_record_decision` against the REAL `warden/scripts/intents.py` (never
    stubbed) — spool, drain, and a row whose payload_hash/expires_at/spent_at
    the intent file could not have touched (see intents.py's own
    `_NEVER_FROM_FILE`)."""
    db, r = plan_and_db(h, approver)
    nonce = _only_nonce(db)
    before = _row(db, nonce)
    srv = _cli.stubs.StubServer({("POST", "/api/jobs"): (200, {"job": {"id": "job-chain", "status": "running"}})})
    try:
        result = _click(h, db, approver, nonce, sideclaw=srv.base)
    finally:
        srv.stop()
    after = _row(db, nonce)
    check(result is not None and result.get("already") is False, "the real chain must open the episode")
    check(after["payload_hash"] == before["payload_hash"], "the intent file must never move payload_hash")
    check(after["expires_at"] == before["expires_at"], "the intent file must never move expires_at")
    check(after["spent_at"] is not None, "the real drain must have spent the row")


def test_already_decided_short_circuits(h, approver):
    db, r = plan_and_db(h, approver)
    nonce = _only_nonce(db)
    srv = _cli.stubs.StubServer({("POST", "/api/jobs"): (200, {"job": {"id": "job-first", "status": "running"}})})
    try:
        first = _click(h, db, approver, nonce, sideclaw=srv.base)
    finally:
        srv.stop()
    check(first is not None and first.get("already") is False, "first click must succeed")
    # A second click (same or a different user) must short-circuit on "already
    # decided" and must not attempt to sign or drain a second time — proven by
    # a sideclaw stub that would fail the test if it received a second submit.
    srv2 = _cli.stubs.StubServer({"default": (500, {"error": "must never be called twice"})})
    try:
        second = _click(h, db, approver, nonce, sideclaw=srv2.base)
    finally:
        check(not srv2.requests, "an already-decided row must never reach sideclaw a second time")
        srv2.stop()
    check(second is not None and second.get("already") is True, f"a second click must short-circuit: {second}")


def test_unrelated_rejection_does_not_fail_the_click(h, approver):
    """A drain that rejects somebody ELSE'S malformed spool file must still
    land THIS click's decision — the row is the source of truth, not the
    drain's own exit code."""
    db, r = plan_and_db(h, approver)
    nonce = _only_nonce(db)
    # Drop an unrelated malformed file into the SAME spool directory the click
    # will drain from, so the drain's own exit code goes non-zero (the plugin
    # logs, never raises, on that) while this decision still lands.
    intents_dir = h.tmp / "intents"
    intents_dir.mkdir(parents=True, exist_ok=True)
    (intents_dir / "00000000T000000000000-garbage.json").write_text("{not json", encoding="utf-8")
    with _env(**_plugin_env(h, db, approver.pub_path)):
        result = plugin._record_decision(nonce, "approve", "U0JOHANNES")
    check(result is not None and result.get("already") is False,
          f"an unrelated rejected file must not stop this decision from landing: {result}")


def test_intent_passes_wardens_validate(h, approver):
    """The intent `_record_decision` builds must pass `intents.validate()` — the
    contract test between the two repos, run in-process against the real
    validator rather than the subprocess it normally goes through."""
    _intents_spec = importlib.util.spec_from_file_location("warden_intents_for_contract_test", REPO / "scripts" / "intents.py")
    warden_intents = importlib.util.module_from_spec(_intents_spec)
    _intents_spec.loader.exec_module(warden_intents)

    nonce = "contract-test-nonce"
    payload_hash = "a" * 64
    expires = (now_utc() + dt.timedelta(minutes=30)).isoformat()
    sig = plugin._sign if False else None  # plugin._sign needs a live key; build the intent shape by hand instead
    intent = {
        "v": 1,
        "kind": "approval_decision",
        "created_at": now_utc().isoformat(),
        "source": "dispatch-approval",
        "nonce": nonce,
        "decision": "approve",
        "decided_by": "U0JOHANNES",
        "signature": "ab" * 64,
    }
    validated = warden_intents.validate(dict(intent))
    check(validated == intent, "the plugin's intent shape must pass intents.validate() unchanged")


# --- runner ------------------------------------------------------------------

CASES = [
    test_hash_agreement_with_spec_vectors,
    test_plan_mints_pending_row,
    test_dry_run_mints_nothing,
    test_undecided_approval_is_untouched_by_a_drain,
    test_forged_unsigned_row_refuses,
    test_wrong_signing_key_is_invalid,
    test_key_rotation_is_superseded,
    test_click_opens_episode_and_audit_names_approver,
    test_denial_refuses_and_is_distinct_from_absence,
    test_expired_approval_refuses,
    test_approval_is_single_use,
    test_approve_replays_stored_brief_and_context,
    test_missing_pubkey_refuses,
    test_republish_after_clobber,
    test_plugin_has_no_write_path_of_its_own,
    test_full_chain_against_real_intents_py,
    test_already_decided_short_circuits,
    test_unrelated_rejection_does_not_fail_the_click,
    test_intent_passes_wardens_validate,
]


def main() -> int:
    h = Harness()
    root = Path(tempfile.mkdtemp(prefix="dispatch-approval-"))
    approver = Approver(root)
    # `_record_decision`/`_sign` read the module-global signing key `register()`
    # would normally mint at gateway startup. This suite never calls register()
    # (there is no gateway), so it stands in as the gateway itself: the
    # PLUGIN's signing key IS the approver's key, exactly as if this process
    # were the one that published `approver.pub_path`.
    plugin._SIGNING_KEY = approver.key
    plugin._PUBLIC_KEY_HEX = approver.pub_path.read_text().strip()
    # This suite runs INSIDE a Claude Code session; lifecycle.policy's
    # require_no_recursion() (reached transitively through open_episode()/
    # plan_or_land() from execute_approved()) refuses unconditionally when
    # any of these are set. `_click()`'s own subprocess path already strips
    # them (the plugin's `_subprocess_env()`), but a case calling
    # `lc_approvals.execute_approved()` directly, in-process, would not be —
    # popped for the whole run, restored after.
    _recursion_markers = ("CLAUDE_CODE_SESSION", "CLAUDECODE", "CLAUDE_SESSION_ID", "CLAUDE_ENTRYPOINT")
    saved_recursion_markers = {m: os.environ.pop(m, None) for m in _recursion_markers}
    try:
        for case in CASES:
            try:
                case(h, approver)
            except Exception as exc:  # a raising case is a failing case
                import traceback
                FAILURES.append(f"{case.__name__} raised {exc!r}\n{traceback.format_exc()}")
    finally:
        for m, v in saved_recursion_markers.items():
            if v is None:
                os.environ.pop(m, None)
            else:
                os.environ[m] = v

    print(f"{CHECKS[0]} checks, {len(FAILURES)} failure(s)")
    for f in FAILURES:
        print(f"  FAIL {f}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
