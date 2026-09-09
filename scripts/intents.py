"""intents — the queue that lets a surface *request* a ledger change without
*being* a ledger writer.

WHY THIS EXISTS. DESIGN.md § The ledger requires **one writer process**: "The
API opens read-only; intents go through the loop's queue." Today six writers
open `warden.db` — the loop, two pollers, `hermes-cc.sh`'s embedded python, the
Slack approval plugin, CLI verbs — and every one of them is a process that can
corrupt, lock or half-migrate the file that decides whether to touch
production. The way out is not "be careful in six places"; it is a spool
directory plus a single drain. A caller writes one file and is done. Exactly
one process opens the database.

WHAT THIS IS NOT: an authority model. Signing happens only where it already
happens — in the gateway's RAM behind Slack Socket Mode, or at a TTY — and
`require_signed_approval()` in `~/.hermes/scripts/hermes-cc.sh` remains the one
and only verifier, trusting the row's Ed25519 signature and never the caller.
**There is deliberately no signature verification in this file.** A second
verifier is a copied contract, and a copied contract drifts; this repo already
refuses that for sideclaw's verdict schema. `drain()` validates SHAPE and
writes the row. The signature is checked where it always was, at spend time.

What that buys, stated plainly so the threat model is not re-derived later: a
forged spool file cannot mint a signature, so it cannot cause an approval. The
worst a writer of this directory can do is set `decision='deny'` on a pending
approval — a denial of service, not an escalation. An episode on this host can
already do worse, and a `deny` that should have been an `approve` is a human
noticing nothing happened, not a merge nobody sanctioned.

Two fields are therefore NOT accepted from a spool file at all — `expires_at`
and `payload_hash`. See `_validate_approval_decision()`; that is the
security-relevant line in this module.
"""

import datetime as dt
import importlib.util
import json
import os
import sqlite3
import sys
import uuid
from pathlib import Path
from typing import Any

# scripts/ledger.py — loaded by path, exactly as triage.py loads it (see the
# `_LEDGER_PATH` block there), because the sibling filenames in scripts/ are
# not importable and a plain `import ledger` resolves to whatever happens to
# be on sys.path — which is either nothing or, worse, a different copy.
_LEDGER_PATH = Path(__file__).resolve().parent / "ledger.py"
_ledger_spec = importlib.util.spec_from_file_location("ledger", _LEDGER_PATH)
assert _ledger_spec and _ledger_spec.loader, "Failed to load scripts/ledger.py"
ledger = importlib.util.module_from_spec(_ledger_spec)
_ledger_spec.loader.exec_module(ledger)

# INTENTS_DIR is a module-level global re-read by every function below at call
# time, never captured at import and never baked into a default argument — the
# same shape (and the same reason) as `ledger.DB_PATH`: a test, or a script's
# own override, rebinds `intents.INTENTS_DIR` and the very next call sees it.
INTENTS_DIR = (
    Path(os.environ["WARDEN_INTENTS_DIR"]).expanduser()
    if os.environ.get("WARDEN_INTENTS_DIR")
    else ledger.WARDEN_HOME / "intents"
)

# Where a file that could not be parsed, validated or applied goes. Never
# deleted — DESIGN.md principle 7, "Nothing is silently discarded to stay under
# a bound." A rejected intent is evidence; a deleted one is a story nobody can
# check.
_REJECTED_SUBDIR = "rejected"

SCHEMA_V = 1

# The envelope every intent carries, and then the fields its own `kind` adds.
# Both lists are CLOSED: an unknown top-level key is a ValueError, never a
# silently ignored one. "A policy file may name and parameterise, never
# express" — the same principle as warden's four other closed allowlists. A
# spool file that could carry an unrecognised key is a spool file that can grow
# meaning this module never agreed to.
_ENVELOPE_FIELDS = ("v", "kind", "created_at", "source")

_KIND_FIELDS: dict[str, tuple[str, ...]] = {
    # The only kind this slice implements. `source` is a free label for
    # whoever spooled the file (the plugin, a CLI, Argo) and carries NO
    # authority whatsoever — it is there to make a rejection readable, not to
    # decide anything.
    "approval_decision": ("nonce", "decision", "decided_by", "signature"),
}

# Accepted from the ROW, never from the file. See
# `_validate_approval_decision()`.
_NEVER_FROM_FILE = ("expires_at", "payload_hash")


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _require_nonempty_str(intent: dict[str, Any], field: str) -> str:
    value = intent.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"intent field {field!r} must be a non-empty string, got {value!r}")
    return value


def _parse_created_at(intent: dict[str, Any]) -> dt.datetime:
    raw = _require_nonempty_str(intent, "created_at")
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError as err:
        raise ValueError(f"intent field 'created_at' is not ISO-8601: {raw!r} ({err})") from err
    # A naive timestamp is read as UTC rather than rejected: every producer of
    # these files is on this host and writes UTC, and the field's only
    # mechanical job is ordering (see `record()`'s filename comment).
    return parsed.astimezone(dt.timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _validate_approval_decision(intent: dict[str, Any]) -> None:
    """Shape only. Nothing here is a security check on the SIGNATURE — that
    happens once, at spend time, in `require_signed_approval()`.

    The security-relevant line in this module is the refusal below.
    `expires_at` and `payload_hash` are what bind an approval to a *deadline*
    and to *specific bytes*. A spool file that could supply either could widen
    its own approval: push the expiry out, or re-point the hash at a brief
    nobody approved, and the row would still verify because the signature it
    carries is over what the file said, not over what Slack showed a human.
    So the file may not name them at all. `drain()` reads both from the
    existing `dispatch_approvals` row — the one written when the human was
    actually asked."""
    for field in _NEVER_FROM_FILE:
        if field in intent:
            raise ValueError(
                f"intent field {field!r} is never accepted from a spool file — it binds the approval to "
                "specific bytes and a specific deadline, so a file that could set it could widen its own "
                "approval. The drain reads it from the dispatch_approvals row."
            )

    _require_nonempty_str(intent, "nonce")
    _require_nonempty_str(intent, "decided_by")

    decision = intent.get("decision")
    if decision not in ("approve", "deny"):
        raise ValueError(f"intent field 'decision' must be exactly 'approve' or 'deny', got {decision!r}")

    signature = _require_nonempty_str(intent, "signature")
    try:
        bytes.fromhex(signature)
    except ValueError as err:
        raise ValueError(f"intent field 'signature' must be hex, got {signature!r} ({err})") from err


_KIND_VALIDATORS = {
    "approval_decision": _validate_approval_decision,
}


def validate(intent: Any) -> dict[str, Any]:
    """Raise ValueError naming the exact offending field, or return the intent
    unchanged. Called by `record()` before a file is written AND by `drain()`
    after one is read back — a file on disk is not evidence that it was ever
    validated, since anything that can write the directory can drop a file in
    it."""
    if not isinstance(intent, dict):
        raise ValueError(f"intent must be a JSON object, got {type(intent).__name__}")

    if intent.get("v") != SCHEMA_V:
        raise ValueError(f"intent field 'v' must be {SCHEMA_V}, got {intent.get('v')!r}")

    kind = intent.get("kind")
    if kind not in _KIND_FIELDS:
        raise ValueError(f"intent field 'kind' must be one of {sorted(_KIND_FIELDS)}, got {kind!r}")

    _parse_created_at(intent)
    _require_nonempty_str(intent, "source")

    allowed = set(_ENVELOPE_FIELDS) | set(_KIND_FIELDS[kind])
    # Kind-specific validators run BEFORE the unknown-key sweep for the two
    # names in `_NEVER_FROM_FILE`, so those get their own explicit message
    # rather than a generic "unknown key".
    _KIND_VALIDATORS[kind](intent)
    unknown = sorted(set(intent) - allowed)
    if unknown:
        raise ValueError(f"unknown intent field(s) {unknown} for kind {kind!r} — the schema is a closed allowlist")

    return intent


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    # mkdir(mode=...) is masked by the process umask; chmod is not. 0700
    # unconditionally, every call, because "the spool is not world-readable"
    # is a property this module asserts rather than one it inherits from
    # whoever happened to create the directory first.
    os.chmod(path, 0o700)
    return path


def record(intent: dict[str, Any]) -> Path:
    """Validate and spool one intent. Returns the path written.

    **Opens no database.** That is the whole point of this module: a process
    that calls `record()` is not a ledger writer, so Argo, a CLI or the Slack
    plugin can request a change without joining the six writers DESIGN.md
    exists to reduce to one.
    """
    validate(intent)
    directory = _ensure_dir(INTENTS_DIR)

    # The name is `<created_at as %Y%m%dT%H%M%S%f>-<8 hex>.json` for two
    # reasons that both matter to `drain()`: a fixed-width compact UTC
    # timestamp makes LEXICAL order == CHRONOLOGICAL order, so a sorted glob
    # is a time-ordered queue with no index and no state; and the random
    # suffix means two intents recorded in the same microsecond cannot
    # collide and silently overwrite one another.
    stamp = _parse_created_at(intent).strftime("%Y%m%dT%H%M%S%f")
    final = directory / f"{stamp}-{uuid.uuid4().hex[:8]}.json"

    # Atomic publish: write the whole file under a temporary name IN THE SAME
    # DIRECTORY, then rename. Never /tmp — os.replace() is only atomic within
    # one filesystem, and a cross-device rename would degrade into a copy that
    # a reader can observe half-written.
    tmp = directory / f".{final.name}.tmp"
    tmp.write_text(json.dumps(intent, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(tmp, 0o600)
    os.replace(tmp, final)
    return final


def _apply_approval_decision(conn: sqlite3.Connection, intent: dict[str, Any]) -> int:
    """One UPDATE, one transaction, owned by `drain()`.

    `AND decision IS NULL` is what makes a replayed spool file harmless: the
    file may be applied twice (a crash between COMMIT and unlink leaves it on
    disk), or an attacker may re-drop yesterday's file, and either way the
    second application changes nothing. It is also what stops a `deny` from
    overwriting an `approve` that a human already gave.

    Note what is NOT here: `expires_at` and `payload_hash` are untouched, and
    `spent_at` is untouched. The row keeps the binding it was created with.
    """
    cur = conn.execute(
        "UPDATE dispatch_approvals SET decision=?, decided_at=?, decided_by=?, signature=? "
        "WHERE nonce=? AND decision IS NULL",
        (
            intent["decision"],
            _now_iso(),
            intent["decided_by"],
            intent["signature"],
            intent["nonce"],
        ),
    )
    return cur.rowcount


_KIND_APPLIERS = {
    "approval_decision": _apply_approval_decision,
}


def _reject(path: Path, err: Exception) -> str:
    """Move a file that could not be applied into `rejected/` and drop a
    `.err` sibling beside it. NEVER unlink it — an intent that could not be
    applied is the one file worth keeping."""
    rejected_dir = _ensure_dir(INTENTS_DIR / _REJECTED_SUBDIR)
    dest = rejected_dir / path.name
    os.replace(path, dest)
    (rejected_dir / f"{path.name}.err").write_text(f"{type(err).__name__}: {err}\n", encoding="utf-8")
    return dest.name


def drain(conn: sqlite3.Connection) -> dict[str, Any]:
    """Apply every spooled intent, oldest first, and return
    `{"applied": int, "rejected": int, "rejected_files": [str, ...]}`.

    `conn` is an already-open WRITABLE connection and the caller owns it —
    `ledger.connect()` is the only thing in warden that makes one, and this
    module deliberately does not call it, so a `drain()` cannot quietly become
    a seventh way to open the database with its own pragmas.

    Each file is handled independently: parse, validate, apply in ONE
    transaction, unlink. One malformed file does not stop the ones behind it,
    and nothing is discarded — a failure lands in `rejected/` with its
    exception text and one line on stderr, because a rejection that reaches
    only a file is invisible (DESIGN.md, "deferral must be visible").
    """
    directory = INTENTS_DIR
    # Path.glob() on a directory that does not exist yields nothing rather
    # than raising — but say it out loud, because the first thing Wave 0
    # shipped was a glob that errored on its very first run against a home
    # that had not been created yet.
    files = sorted(p for p in directory.glob("*.json") if p.is_file())

    applied = 0
    rejected_files: list[str] = []

    for path in files:
        try:
            intent = validate(json.loads(path.read_text(encoding="utf-8")))
            rows = _KIND_APPLIERS[intent["kind"]](conn, intent)
            conn.commit()
        except Exception as err:  # noqa: BLE001 — one bad file may never stop the rest
            conn.rollback()
            print(f"intents: rejected {path.name}: {type(err).__name__}: {err}", file=sys.stderr)
            rejected_files.append(_reject(path, err))
            continue

        # rowcount == 0 is SUCCESS, not a rejection — do not "fix" this.
        # Zero rows means the nonce is unknown, or it was already decided.
        # Both are ordinary idempotency: the approval plugin's own code
        # already treats "already decided" as a normal branch, and an unknown
        # nonce is a request about a row that no longer exists, which nothing
        # here can or should resurrect. Turning either into a rejection would
        # fill `rejected/` with files whose only defect is that the world
        # moved on.
        _ = rows
        # Unlink AFTER the commit. If the process dies in between, the file is
        # drained again next pass and the `AND decision IS NULL` guard makes
        # that a no-op — the safe order is "apply twice", never "lose one".
        path.unlink(missing_ok=True)
        applied += 1

    return {"applied": applied, "rejected": len(rejected_files), "rejected_files": rejected_files}


# ── CLI ────────────────────────────────────────────────────────────────────────
#
# The same door, for the same reason, as `ledger.py`'s CLI (commit fe95e81, "a
# door for the one writer that is not warden"): the callers that need this most
# are a shell script and a Slack plugin, and neither can import a Python module
# by path. They can run one.
#
#   python3 intents.py --record            intent JSON on STDIN. Writes the spool
#                                          file, opens no database, prints the path.
#   python3 intents.py --drain [<db path>] open the ledger, drain, print a summary.
#
# `--record` reads STDIN and never argv, because argv crosses a `ps` boundary —
# the same refusal `_resolve_openai_api_key()` already makes in triage.py. An
# intent carries a signature, and a signature on the process table is a
# signature anyone logged into this host can copy.
#
# `--drain` passes `migrate=False`, so it ASSERTS the schema version and refuses
# on mismatch. Only the loop migrates.

def _main(argv: list[str]) -> int:
    if "--record" in argv:
        try:
            intent = json.loads(sys.stdin.read())
            path = record(intent)
        except Exception as err:  # noqa: BLE001 — the message IS the output
            print(f"intents: {err}", file=sys.stderr)
            return 1
        print(path)
        return 0

    if "--drain" in argv:
        i = argv.index("--drain")
        db = argv[i + 1] if i + 1 < len(argv) else None
        try:
            conn = ledger.connect(db, migrate=False)
        except Exception as err:  # noqa: BLE001 — the message IS the output
            print(f"intents: {err}", file=sys.stderr)
            return 1
        try:
            result = drain(conn)
        finally:
            conn.close()
        print(f"applied={result['applied']} rejected={result['rejected']}")
        # A rejection is not a crash — the drain did its job by isolating the
        # file — but the plugin drains SYNCHRONOUSLY right after a click, so
        # "your intent did not land" has to be visible in the exit status and
        # not only in a line of stderr nobody reads.
        return 1 if result["rejected"] else 0

    print(__doc__ or "", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
