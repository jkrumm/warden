"""ledger — the one module that owns the connection, the schema and the
migrations for `warden.db`. Everything else asks this module for a
connection; nothing else runs DDL.

WHY THIS EXISTS. Before this module, four processes each carried their own
copy of `db_connect()`: `executescript(SCHEMA)` plus a hand-rolled block of
`ALTER TABLE ... ADD COLUMN` guarded by `PRAGMA table_info`. `dispatches` was
declared in three places, `events` and `cursors` in two, with no version
table and no agreement on which copy was authoritative — the DDL itself was
the only source of truth, re-derived from `PRAGMA table_info` on every single
connect, by every single process, forever. That is not a migration system,
it is four independent guesses that happened to converge because nobody had
changed the schema yet. The day they diverge — one process updated, three
not — there is nothing here to notice, only a `sqlite3.OperationalError`
raised from whichever query touches the missing column first, in production,
possibly at 3am.

The fix is not "add a fifth copy that is more careful." It is one module that
is the schema: one `SCHEMA_VERSION`, one ordered `MIGRATIONS` map, one
`migrate()` that only the process that owns the loop's 10-minute boot is
allowed to run, and one `assert_schema_version()` that every other process
calls instead — so a poller or a sweeper that starts before the loop has
ever touched a fresh ledger fails loudly on a version mismatch rather than
quietly inventing its own tables.

The ledger this module was written against is not new — it is a live,
un-versioned SQLite file that four processes have been writing to for
months, on `journal_mode=delete`, with `user_version=0`. `migrate()` has to
treat *adopting* that file, unmodified, as its ordinary first case, not as a
one-off recovery path. See `migrate()`'s own docstring for why that case is
handled first and separately from every later version.
"""

import datetime as dt
import os
import sqlite3
import sys
from pathlib import Path

# Same env-var-first, documented-default-second shape triage.py already uses
# for HERMES_CC_BIN — an operator or a test overrides one variable and every
# reader of this module (this file, and anything that imports it) agrees
# without a second constant to keep in sync.
WARDEN_HOME = (
    Path(os.environ["WARDEN_HOME"]).expanduser() if os.environ.get("WARDEN_HOME") else Path.home() / ".warden"
)

# DB_PATH is a module-level global, not a value baked into a default
# argument, and every function below re-reads it at call time rather than
# capturing it at import — that is what lets a test (or a script's own `--db`
# override) monkeypatch `ledger.DB_PATH` and have `connect()` see the new
# value on its very next call, exactly like triage.py's own DB_PATH already
# works today.
DB_PATH = Path(os.environ["WARDEN_DB"]).expanduser() if os.environ.get("WARDEN_DB") else WARDEN_HOME / "warden.db"

# This IS the live ledger, since the cutover on 2026-09-09. Four LaunchAgents
# and hermes-cc.sh all resolve here. ~/.hermes/watchdog.db is still on disk,
# frozen at its pre-cutover state and written by nothing, kept as the rollback —
# so if you are reading this to work out which database is real, it is this one.

SCHEMA_VERSION = 2

# Single row, updated in place — never a history table. "Which version was
# this database at three migrations ago" is not a question anything here
# needs to answer; "is it at the version this process expects, right now" is
# the only one that matters, and one row answers it in one query with no
# ORDER BY / LIMIT to get wrong.
_SCHEMA_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS schema_version (
    version    INTEGER NOT NULL,
    applied_at TEXT    NOT NULL
)
"""

# Version 1 — every CREATE TABLE and CREATE INDEX currently spread across
# hermes-cc.sh, triage.py, watchdog-poll.py and dispatch-sweep.py, with the
# columns that today only exist because of a runtime `ALTER TABLE` declared
# inline, in the exact column order and position SQLite's own ALTER TABLE
# ADD COLUMN produces (new column text is spliced in after the last existing
# column, before any table-level constraint — which is why `dispatch_id`
# below sits before `UNIQUE(source, external_id)` rather than after it).
# Column lists, index definitions and column order were read directly off
# the then-live ~/.hermes/watchdog.db via a read-only connection — not
# reconstructed from memory of the four DDL blocks — so this is a faithful
# copy, not a redesign.
BASE_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    payload_json TEXT,
    first_seen TEXT NOT NULL,
    notified_at TEXT,
    last_reminder_at TEXT,
    reminder_count INTEGER NOT NULL DEFAULT 0,
    resolved_at TEXT,
    dispatch_id INTEGER,
    UNIQUE(source, external_id)
);
CREATE INDEX IF NOT EXISTS idx_events_open ON events(source) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_events_resolved_at ON events(resolved_at);

CREATE TABLE IF NOT EXISTS cursors (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dispatches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL UNIQUE,
    tier TEXT NOT NULL,
    repo TEXT NOT NULL,
    brief TEXT NOT NULL,
    why TEXT,
    origin_channel TEXT,
    origin_thread_ts TEXT,
    origin_event_id INTEGER,
    status TEXT NOT NULL,
    verdict_json TEXT,
    artifact_url TEXT,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    reported_at TEXT,
    merged_at TEXT,
    poll_misses INTEGER NOT NULL DEFAULT 0,
    validation_job_id TEXT,
    validation_status TEXT
);
CREATE INDEX IF NOT EXISTS idx_dispatches_open ON dispatches(status) WHERE reported_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_dispatches_created ON dispatches(created_at);

CREATE TABLE IF NOT EXISTS dispatch_approvals (
    nonce TEXT PRIMARY KEY,
    verb TEXT NOT NULL,
    repo TEXT NOT NULL,
    tier TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    channel TEXT,
    decision TEXT,
    decided_at TEXT,
    decided_by TEXT,
    signature TEXT,
    spent_at TEXT,
    argv_json TEXT,
    stdin_text TEXT,
    context_text TEXT
);
CREATE INDEX IF NOT EXISTS idx_approvals_hash ON dispatch_approvals(payload_hash);

CREATE TABLE IF NOT EXISTS triage_items (
  event_id      INTEGER PRIMARY KEY REFERENCES events(id),
  signature     TEXT NOT NULL,
  repo          TEXT,
  verb          TEXT,
  state         TEXT NOT NULL,
  card_channel  TEXT,
  card_ts       TEXT,
  card_hash     TEXT,
  dispatch_job  TEXT,
  artifact_url  TEXT,
  occurrences   INTEGER NOT NULL DEFAULT 0,
  first_seen    TEXT,
  last_seen     TEXT,
  snoozed_until TEXT,
  created_at    TEXT NOT NULL,
  updated_at    TEXT NOT NULL,
  note          TEXT,
  implement_job      TEXT,
  validation_job     TEXT,
  pr_url             TEXT,
  deploy_expect_json TEXT,
  liveness_deadline  TEXT,
  propose_unsure_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_triage_state ON triage_items(state);
"""

# Target version -> its DDL. Adding version 2 is appending one entry here —
# `migrate()` below already walks every version strictly greater than the
# database's current one, in order, so nothing else changes.
# Version 2 — `state_deadline`, one column, so that every non-terminal state can
# name the moment it stops being allowed to sit there. DESIGN.md § Deadlines:
# "Every non-terminal state gets a `state_deadline` and a named poller."
#
# ONE column, not one per state, and not a second table: the deadline is a
# property OF the row's current state, so it is rewritten by the same UPDATE that
# writes the state and is meaningless without it. A separate table would let the
# two disagree, and "the deadline says investigating, the row says merged" is a
# question nothing could answer.
#
# NULL means "no deadline applies" — a terminal state, or `new`, which is bounded
# by silence-resolve rather than by a clock (see triage.py's
# _SILENCE_RESOLVE_ELIGIBLE_STATES). It is NOT "the deadline was forgotten": the
# sweeper treats NULL on a non-terminal state as a finding, not as permission.
#
# `liveness_deadline` (version 1) is deliberately left alone rather than folded
# into this column. It is the deploy path's own probe window and predates this;
# merging them would be a data migration on a live ledger to save one column.
_MIGRATION_2 = """
ALTER TABLE triage_items ADD COLUMN state_deadline TEXT;
CREATE INDEX IF NOT EXISTS idx_triage_state_deadline ON triage_items(state_deadline);
"""

MIGRATIONS: dict[int, str] = {
    1: BASE_SCHEMA,
    2: _MIGRATION_2,
}

# The five tables BASE_SCHEMA declares, i.e. what "this is the live
# pre-versioned ledger" looks like from the outside. Used only by the
# adoption check in migrate() — never by connect()/assert_schema_version(),
# which only ever look at schema_version itself.
_ADOPTABLE_TABLES = ("events", "cursors", "dispatches", "dispatch_approvals", "triage_items")


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _stamp_version_sql(version: int, applied_at: str) -> str:
    """SQL text (no parameter binding — see migrate()'s docstring for why)
    that stamps schema_version's single row to `version`, inserting it if
    this is the very first stamp this database has ever received and
    updating it in place on every later one. `version` is our own int
    constant and `applied_at` is an isoformat() timestamp, neither ever
    attacker- or user-controlled, so string-formatting them into DDL/DML
    text here — the only way to fold this into the same executescript() as
    the surrounding CREATE statements, and therefore the same transaction —
    is safe."""
    return (
        f"UPDATE schema_version SET version = {version}, applied_at = '{applied_at}';\n"
        f"INSERT INTO schema_version (version, applied_at)\n"
        f"  SELECT {version}, '{applied_at}' WHERE NOT EXISTS (SELECT 1 FROM schema_version);\n"
    )


def _current_version(conn: sqlite3.Connection) -> int:
    """0 whenever schema_version does not exist yet — a fresh database and
    the live pre-versioned one look identical from here; migrate() is what
    tells them apart."""
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'").fetchone()
    if row is None:
        return 0
    row = conn.execute("SELECT version FROM schema_version").fetchone()
    return int(row[0]) if row is not None else 0


def _tables_exist(conn: sqlite3.Connection, names: tuple[str, ...]) -> bool:
    present = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    return all(name in present for name in names)


def _run_migrations(conn: sqlite3.Connection) -> None:
    """The single most dangerous function in this repo — read this before
    changing it.

    It has to serve two cases that look identical from `_current_version()`
    (both report 0) but must be handled in opposite ways:

    1. A genuinely fresh database — nothing exists yet. Run BASE_SCHEMA.
    2. THE LIVE LEDGER, adopted for the first time. `schema_version` has
       never existed because nothing before this module ever wrote one, but
       `events`, `cursors`, `dispatches`, `dispatch_approvals` and
       `triage_items` are already there, already carrying the columns that
       used to arrive via a runtime ALTER TABLE, already holding real rows
       going back months. This is the NORMAL case this module was written
       for, not an edge case: warden's ledger did not appear out of nowhere,
       it is being handed a file four other processes have been writing to
       since before this module existed.

    Case 2 must NOT run BASE_SCHEMA. Even though every statement in it is
    `IF NOT EXISTS` and would therefore be a no-op against a table whose
    columns already match, "no-op today" is not the guarantee this function
    exists to make — the guarantee is that adopting a live database is a
    pure read (three queries: does schema_version exist, do the five tables
    exist, what version do we stamp) plus one small, well-understood write
    (create schema_version, stamp it to 1). Nothing here drops, recreates or
    rewrites a single one of the five business tables. Case 2 is detected
    and handled BEFORE the migration loop below ever runs, and returns
    immediately.

    Refuses loudly, before touching anything, if the database is already
    past SCHEMA_VERSION — that can only mean an older warden was pointed at
    a ledger a newer warden already migrated, and the only safe move is to
    stop, not to guess which of its own migrations might still apply.
    """
    conn.execute(_SCHEMA_VERSION_TABLE)
    current = _current_version(conn)

    if current > SCHEMA_VERSION:
        raise RuntimeError(
            f"ledger schema_version={current} is newer than this warden's SCHEMA_VERSION={SCHEMA_VERSION} — "
            "refusing to migrate. This means an older warden was started against a ledger a newer warden "
            "has already migrated forward; running an old migrator against it would guess, and guessing is "
            "how data gets destroyed. Upgrade this warden (or point it at a different database) before "
            "running it against this ledger again."
        )

    if current == 0 and _tables_exist(conn, _ADOPTABLE_TABLES):
        # Adopting the live, pre-versioned ledger — see this function's own
        # docstring, case 2. Stamp only; BASE_SCHEMA never runs.
        conn.executescript(f"BEGIN;\n{_stamp_version_sql(1, _now_iso())}COMMIT;\n")
        # ...and then FALL THROUGH to the migration loop below rather than
        # returning. Adoption establishes that this file is at version 1's
        # shape; it does not establish that it is at SCHEMA_VERSION. This
        # `return` used to be unconditional, and was harmless for exactly as
        # long as SCHEMA_VERSION stayed 1 — the moment it became 2, adopting a
        # pre-versioned ledger would stamp it 1, skip migration 2, and then fail
        # `_verify_columns()` on a column the migration it skipped would have
        # added. The path that does that is the ROLLBACK
        # (~/.warden-cutover-backup/ holds a pre-versioned ledger), so it would
        # have failed at exactly the moment it was needed.
        current = 1

    for version in range(current + 1, SCHEMA_VERSION + 1):
        ddl = MIGRATIONS[version]
        # executescript() commits any pending transaction before it runs and
        # performs no other implicit transaction control of its own (stdlib
        # sqlite3 docs) — so the BEGIN/COMMIT bracketing the whole script
        # text below is what actually makes "this version's DDL plus its
        # schema_version stamp" one transaction: a crash mid-script leaves
        # the database at its pre-migration version, never half-migrated.
        conn.executescript(f"BEGIN;\n{ddl}\n{_stamp_version_sql(version, _now_iso())}COMMIT;\n")

    _verify_columns(conn)


def _expected_columns() -> dict[str, set[str]]:
    """What the schema AT `SCHEMA_VERSION` says each table has, derived by
    replaying BASE_SCHEMA and then every migration up to it against an
    in-memory database, rather than by keeping a second hand-written list
    beside it. A hand-written list is a copy, and a copy of a schema is a
    thing that drifts from the schema."""
    mem = sqlite3.connect(":memory:")
    try:
        # BASE_SCHEMA is version 1's shape, so replaying it alone would describe
        # a schema this module stopped having the moment MIGRATIONS gained a
        # second entry — and `_verify_columns()` would then silently stop
        # checking every column added after version 1. Replay the migrations in
        # order instead, which is the same "derive it, never copy it" property
        # one version further on.
        for version in sorted(MIGRATIONS):
            if version <= SCHEMA_VERSION:
                mem.executescript(MIGRATIONS[version])
        return {
            t: {r[1] for r in mem.execute(f"PRAGMA table_info('{t}')")}
            for t in _ADOPTABLE_TABLES
        }
    finally:
        mem.close()


def _verify_columns(conn: sqlite3.Connection) -> None:
    """Refuse a database that is stamped but structurally incomplete.

    Adoption (case 2 in `_run_migrations`) decides on TABLE PRESENCE alone —
    all five tables exist, so this is the live ledger, stamp it and touch
    nothing. That is the right call for the ledger this module was written to
    adopt, whose columns were verified to match exactly. It is the wrong call
    for a database where the five tables exist but one of them predates a
    column that arrived through a runtime ALTER TABLE on some other machine:
    adoption would stamp it version 1, every later query would be reading a
    schema that is not there, and the failure would surface as a bare
    `no such column` from somewhere deep in the loop, hours later, with
    nothing connecting it back to this decision.

    So the stamp is checked against the schema it claims. Missing columns are
    fatal and named. EXTRA columns are not — a newer warden may have added
    one, and refusing to read a database that has more than we expect would
    turn a forward-compatible situation into an outage."""
    expected = _expected_columns()
    problems: list[str] = []
    for table, want in expected.items():
        have = {r[1] for r in conn.execute(f"PRAGMA table_info('{table}')")}
        missing = want - have
        if missing:
            problems.append(f"  {table}: missing {sorted(missing)}")
    if problems:
        raise RuntimeError(
            f"ledger is stamped schema_version={SCHEMA_VERSION} but does not match that schema:\n"
            + "\n".join(problems)
            + "\n\nThis is a database that carried all the expected TABLES (so it was adopted rather "
            "than created) while missing columns the schema declares. Adopting it further would "
            "produce 'no such column' from inside the loop instead of here. Fix the database, or "
            "point warden at a different one."
        )


def migrate(conn: sqlite3.Connection) -> None:
    """Public entry point for `_run_migrations()` — kept as a thin wrapper
    so `connect(migrate=True)` can call the real logic under a different
    name. (`connect()`'s own `migrate` keyword argument shadows this
    function's name inside `connect()`'s body; the split avoids relying on
    Python's scoping rules to route around that.)"""
    _run_migrations(conn)


def assert_schema_version(conn: sqlite3.Connection) -> None:
    """What every non-migrating process calls instead of migrate(). Raises
    with both version numbers named, so the fix is obvious from the error
    alone: either the migrating process (triage.py, the loop) has not run
    yet against this file, or this process is stale and needs upgrading."""
    version = _current_version(conn)
    if version != SCHEMA_VERSION:
        # The connection's own idea of its file, not the module-global DB_PATH —
        # a caller that passed an explicit `path=` (a test, a --db override) would
        # otherwise get an error naming the wrong file.
        db_file = conn.execute("PRAGMA database_list").fetchone()["file"]
        raise RuntimeError(
            f"warden ledger at {db_file} is at schema_version={version}, this process expects "
            f"SCHEMA_VERSION={SCHEMA_VERSION}. Only the loop (triage.py, via `connect(migrate=True)`) "
            "is allowed to migrate this file — run it at least once, or if this ledger has already been "
            "migrated past this process's version, upgrade this process before pointing it here."
        )


def connect(
    path: Path | str | None = None,
    *,
    readonly: bool = False,
    migrate: bool = False,
) -> sqlite3.Connection:
    """The one place anything in warden opens `warden.db`.

    `path` defaults to the module-global DB_PATH, re-read at call time so a
    test or a script's own `--db` override can rebind `ledger.DB_PATH` (or
    pass `path=` directly) and have it take effect on the very next call —
    see the DB_PATH comment above.

    `readonly=True` opens `file:<path>?mode=ro` and sets no pragma that
    writes — a dashboard or a diagnostic reader must never be able to open a
    writable handle by accident, and must never bring a database file into
    existence just by trying to read it.

    On a writable connection: WAL, an explicit busy_timeout, and
    synchronous=NORMAL, in that order (see the inline comments below for
    why each one, and why NORMAL specifically is correct under WAL).

    `migrate=True` runs the migrator (see `_run_migrations()` — only the
    loop's boot path should ever pass this). `migrate=False` (the default)
    calls `assert_schema_version()` and raises on any mismatch, INCLUDING
    "the file doesn't have a schema at all yet" — an assert-only process
    that races the migrator on a brand-new mini must fail loudly, not
    silently create its own tables the way the four old `db_connect()`s did.
    """
    p = Path(path).expanduser() if path is not None else DB_PATH

    if readonly:
        conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(p)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA journal_mode=WAL")
    # sqlite3.connect(..., timeout=5.0) already sets busy_timeout=5000 as a
    # per-connection Python stdlib default — but that is true only by
    # accident of the driver, not by anything this file declares. A
    # non-Python writer of this database (a bare `sqlite3` CLI invocation, a
    # shell one-liner) gets no timeout at all under that accident. Setting
    # it explicitly here makes the 5s wait a property this module asserts on
    # every writable connection it hands out, not a side effect of which
    # client happened to open the file.
    conn.execute("PRAGMA busy_timeout=5000")
    # NORMAL, not FULL, and specifically because this connection is WAL:
    # under WAL, synchronous=NORMAL still guarantees the database can never
    # be corrupted by a crash or power loss — only that the last few
    # committed transactions might be lost, which then get re-derived on
    # this loop's very next 10-minute pass, not silently missed the way a
    # lost row in a one-shot system would be. FULL buys durability across
    # that crash window at the cost of an fsync on every single commit,
    # which is a guarantee a 10-minute polling loop does not need and a cost
    # every one of its six writers would otherwise pay on every write.
    conn.execute("PRAGMA synchronous=NORMAL")

    if migrate:
        _run_migrations(conn)
    else:
        assert_schema_version(conn)

    return conn


def snapshot(conn: sqlite3.Connection, dest: Path | str) -> Path:
    """`VACUUM INTO` a consistent, compacted copy of the currently-open
    database to `dest`. Refuses if `dest` already exists — SQLite's own
    behaviour, not ours, and deliberately not overridable here: a caller
    that wants to replace an old snapshot deletes it first, explicitly,
    rather than this function silently clobbering one."""
    dest_path = Path(dest).expanduser()
    conn.execute("VACUUM INTO ?", (str(dest_path),))
    return dest_path


# ── CLI ────────────────────────────────────────────────────────────────────────
#
# So a process that is NOT warden can prepare or check a ledger without carrying
# its own copy of the schema. That is not a convenience: `hermes-cc.sh` owns two
# of the five tables and still writes them, and the only alternative to this door
# is the one that was there before — a second, unversioned migrator running
# `executescript` on every connect, which is the exact defect this module exists
# to remove. A shell script cannot import a Python module; it can run one.
#
#   python3 ledger.py --migrate <path>   create or adopt, then stamp. The loop's job;
#                                        also what a test fixture needs.
#   python3 ledger.py --check <path>     assert the version and print it. Exit 1 on
#                                        mismatch, so a caller can fail closed.
#   python3 ledger.py --version          print SCHEMA_VERSION and exit.

def _main(argv: list[str]) -> int:
    if "--version" in argv:
        print(SCHEMA_VERSION)
        return 0
    for flag in ("--migrate", "--check"):
        if flag in argv:
            i = argv.index(flag)
            if i + 1 >= len(argv):
                print(f"ledger: {flag} needs a path", file=sys.stderr)
                return 2
            path = Path(argv[i + 1]).expanduser()
            try:
                conn = connect(path, migrate=(flag == "--migrate"))
            except Exception as err:  # noqa: BLE001 — the message IS the output
                print(f"ledger: {err}", file=sys.stderr)
                return 1
            version = _current_version(conn)
            conn.close()
            print(version)
            return 0
    print(__doc__ or "", file=sys.stderr)
    return 2


if __name__ == "__main__":
    import sys as _sys
    _sys.exit(_main(_sys.argv[1:]))
