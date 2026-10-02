#!/usr/bin/env python3
"""Regression suite for scripts/ledger.py — the one module that owns the
connection, the schema and the migrations for warden.db.

The case that matters most here is adoption: warden's ledger is not new, it
is the live, un-versioned ~/.hermes/watchdog.db four other processes have
been writing to for months. migrate() has to bring that file under version
control without touching a single row or column it did not itself add. See
test_migrate_adopts_pre_versioned_database below, and ledger.py's own
_run_migrations() docstring for why that case is handled first and
separately from every later schema version.

Run: .venv/bin/python3 tests/test_ledger.py
"""

import importlib.util
import sqlite3
import sys
import tempfile
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("ledger", REPO / "scripts" / "ledger.py")
ledger = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ledger)

# Every CREATE in this block is the pre-migration shape: base columns only,
# ALTER-added columns bolted on afterward via real ALTER TABLE statements
# (not declared inline), and deliberately no schema_version table — i.e.
# exactly what the live ledger looks like today.
_OLD_STYLE_BASE = """
CREATE TABLE events (
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
    UNIQUE(source, external_id)
);
CREATE TABLE cursors (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE dispatches (
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
    reported_at TEXT
);
CREATE TABLE dispatch_approvals (
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
    spent_at TEXT
);
CREATE TABLE triage_items (
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
  note          TEXT
);
"""

_OLD_STYLE_ALTERS = (
    "ALTER TABLE events ADD COLUMN dispatch_id INTEGER",
    "ALTER TABLE dispatches ADD COLUMN merged_at TEXT",
    "ALTER TABLE dispatches ADD COLUMN poll_misses INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE dispatches ADD COLUMN validation_job_id TEXT",
    "ALTER TABLE dispatches ADD COLUMN validation_status TEXT",
    "ALTER TABLE dispatch_approvals ADD COLUMN argv_json TEXT",
    "ALTER TABLE dispatch_approvals ADD COLUMN stdin_text TEXT",
    "ALTER TABLE dispatch_approvals ADD COLUMN context_text TEXT",
    "ALTER TABLE triage_items ADD COLUMN implement_job TEXT",
    "ALTER TABLE triage_items ADD COLUMN validation_job TEXT",
    "ALTER TABLE triage_items ADD COLUMN pr_url TEXT",
    "ALTER TABLE triage_items ADD COLUMN deploy_expect_json TEXT",
    "ALTER TABLE triage_items ADD COLUMN liveness_deadline TEXT",
    "ALTER TABLE triage_items ADD COLUMN propose_unsure_at TEXT",
)


def _tmp_path(name: str = "warden.db") -> Path:
    return Path(tempfile.mkdtemp(prefix="ledger-test-")) / name


def _build_old_style_db(path: Path) -> None:
    """A pre-migration ledger: base tables, ALTER-added columns, a couple of
    real rows, no schema_version — i.e. what the live database is today."""
    conn = sqlite3.connect(path)
    conn.executescript(_OLD_STYLE_BASE)
    for stmt in _OLD_STYLE_ALTERS:
        conn.execute(stmt)
    conn.execute(
        "INSERT INTO events (source, external_id, title, first_seen) VALUES ('s','e1','one','now')"
    )
    conn.execute(
        "INSERT INTO events (source, external_id, title, first_seen) VALUES ('s','e2','two','now')"
    )
    conn.execute("INSERT INTO cursors (key, value, updated_at) VALUES ('k','v','now')")
    conn.commit()
    conn.close()


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] if isinstance(r, sqlite3.Row) else r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


def _row_counts(conn: sqlite3.Connection, tables) -> dict[str, int]:
    return {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables}


def test_fresh_migrate_creates_all_tables_and_indexes():
    conn = ledger.connect(_tmp_path(), migrate=True)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for t in ("events", "cursors", "dispatches", "triage_items", "schema_version",
              "item_transitions", "operations"):
        assert t in tables, f"missing table {t}"
    assert "dispatch_approvals" not in tables, "the signed-approval table is gone from the fresh schema"
    indexes = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'")}
    expected = {
        "idx_events_open",
        "idx_events_resolved_at",
        "idx_dispatches_open",
        "idx_dispatches_created",
        "idx_triage_state",
        "idx_item_transitions_event",  # version 4
        "operations_open",             # version 5
        "operations_event",            # version 5
    }
    assert indexes == expected, f"got {indexes}"
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.LEDGER_SCHEMA_VERSION
    conn.close()


def test_connect_without_migrate_raises_naming_both_versions():
    path = _tmp_path()
    try:
        ledger.connect(path)
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        msg = str(e)
        assert "0" in msg, msg
        assert str(ledger.LEDGER_SCHEMA_VERSION) in msg, msg


def test_assert_schema_version_raises_ledger_behind_when_older():
    conn = ledger.connect(_tmp_path(), migrate=True)
    conn.execute("UPDATE schema_version SET version = ?", (ledger.LEDGER_SCHEMA_VERSION - 1,))
    conn.commit()
    try:
        ledger.assert_schema_version(conn)
        raise AssertionError("expected LedgerBehind")
    except ledger.LedgerBehind as e:
        assert isinstance(e, RuntimeError), "LedgerBehind must stay a RuntimeError"
    finally:
        conn.close()


def test_assert_schema_version_raises_plain_runtime_error_when_newer():
    conn = ledger.connect(_tmp_path(), migrate=True)
    conn.execute("UPDATE schema_version SET version = ?", (ledger.LEDGER_SCHEMA_VERSION + 1,))
    conn.commit()
    try:
        ledger.assert_schema_version(conn)
        raise AssertionError("expected RuntimeError")
    except ledger.LedgerBehind:
        raise AssertionError("a newer-than-us ledger must not raise LedgerBehind")
    except RuntimeError:
        pass
    finally:
        conn.close()


def test_wal_is_actually_on():
    conn = ledger.connect(_tmp_path(), migrate=True)
    assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    conn.close()


def test_busy_timeout_is_5000():
    conn = ledger.connect(_tmp_path(), migrate=True)
    assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
    conn.close()


def test_readonly_connection_cannot_write_and_does_not_create_file():
    # A migrated db exists at `path`; a readonly handle to it must refuse a write.
    path = _tmp_path()
    w = ledger.connect(path, migrate=True)
    w.commit()
    w.close()

    ro = ledger.connect(path, readonly=True)
    try:
        ro.execute("INSERT INTO cursors (key, value, updated_at) VALUES ('a','b','c')")
        raise AssertionError("expected an OperationalError on write")
    except sqlite3.OperationalError:
        pass
    finally:
        ro.close()

    # A readonly connection to a path that does not exist yet must never
    # bring the file into existence.
    missing = _tmp_path("does-not-exist.db")
    try:
        ledger.connect(missing, readonly=True)
        raise AssertionError("expected an OperationalError opening a missing db read-only")
    except sqlite3.OperationalError:
        pass
    finally:
        assert not missing.exists(), "readonly connect must not create the database file"


def test_migrate_adopts_pre_versioned_database():
    path = _tmp_path()
    _build_old_style_db(path)

    pre = sqlite3.connect(path)
    pre.row_factory = sqlite3.Row
    pre_has_schema_version = pre.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version'"
    ).fetchone()
    pre_cols = {t: _table_columns(pre, t) for t in ledger._ADOPTABLE_TABLES}
    pre_counts = _row_counts(pre, ledger._ADOPTABLE_TABLES)
    pre.close()

    assert pre_has_schema_version is None, "fixture must start without schema_version"

    conn = ledger.connect(path, migrate=True)
    # LEDGER_SCHEMA_VERSION, not 1. Adoption establishes that the file is at version
    # 1's SHAPE; it does not establish that it is at the current version, so it
    # stamps 1 and then falls through the migration loop like any other
    # database. Asserting `== 1` here is what hid that bug while LEDGER_SCHEMA_VERSION
    # happened to be 1.
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.LEDGER_SCHEMA_VERSION

    post_cols = {t: _table_columns(conn, t) for t in ledger._ADOPTABLE_TABLES}
    post_counts = _row_counts(conn, ledger._ADOPTABLE_TABLES)
    # Every column the fixture had is still there, except the two migration 13
    # drops from triage_items (dead since Wave 1); the only additions are the ones
    # the migrations past version 1 declare. Every other table stays additive.
    dropped_by_13 = {"snoozed_until", "propose_unsure_at"}
    for table, cols in pre_cols.items():
        lost = cols - post_cols[table]
        assert lost == (dropped_by_13 if table == "triage_items" else set()), f"{table} lost columns: {lost}"
    assert post_cols["triage_items"] - pre_cols["triage_items"] == {
        "occurrence_mark", "revert_pr", "origin", "max_tier", "brief",
        "origin_channel", "origin_thread_ts", "revision_count", "close_reason", "strikes", "retry_at",
    }, (
        f"unexpected column change on triage_items: "
        f"{post_cols['triage_items'] - pre_cols['triage_items']}")
    assert post_counts == pre_counts, f"rows lost: {post_counts} != {pre_counts}"

    # Functionally equivalent to a from-scratch database: same column sets
    # for every adopted table (position may legitimately differ — this
    # compares sets, not order, per the ALTER-vs-inline note in ledger.py).
    fresh = ledger.connect(_tmp_path(), migrate=True)
    fresh_cols = {t: _table_columns(fresh, t) for t in ledger._ADOPTABLE_TABLES}
    assert fresh_cols == post_cols, f"adopted schema diverges from a fresh one: {post_cols} != {fresh_cols}"
    fresh.close()
    conn.close()


def test_adopting_a_pre_versioned_ledger_runs_every_later_migration():
    """The bug this pins, found 2026-09-09 while reading ahead for Wave 1 item 3.

    `_run_migrations()`'s adoption branch used to stamp version 1 and `return`,
    skipping the migration loop entirely. That was invisible for exactly as long
    as LEDGER_SCHEMA_VERSION stayed 1. The moment it became 2, adopting a pre-versioned
    ledger would stamp it 1, skip migration 2, and then fail `_verify_columns()`
    on the column migration 2 would have added.

    The only thing that adopts a pre-versioned ledger is the ROLLBACK —
    ~/.warden-cutover-backup/ holds exactly such a file — so this would have
    failed at the one moment it was needed.

    The strongest assertion here is the last one: an adopted database and a
    freshly created one must end up with the SAME schema. That is the property
    the `return` broke."""
    path = _tmp_path()
    _build_old_style_db(path)

    conn = ledger.connect(path, migrate=True)
    version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
    assert version == ledger.LEDGER_SCHEMA_VERSION, (
        f"adoption stopped at version {version} instead of running on to "
        f"{ledger.LEDGER_SCHEMA_VERSION} — every migration after the adopted one was skipped")

    cols = _table_columns(conn, "triage_items")
    assert "occurrence_mark" in cols and "close_reason" in cols, "later migrations did not run against the adopted ledger"
    assert "state_deadline" not in cols, "migration 13 drops the generic deadline column"

    fresh = ledger.connect(_tmp_path(), migrate=True)
    for table in ledger._ADOPTABLE_TABLES:
        assert _table_columns(fresh, table) == _table_columns(conn, table), (
            f"{table}: an adopted ledger and a fresh one must have the same schema")
    fresh.close()
    conn.close()


def test_migrate_is_idempotent():
    path = _tmp_path()
    _build_old_style_db(path)

    conn = ledger.connect(path, migrate=True)
    conn.commit()
    first_counts = _row_counts(conn, ledger._ADOPTABLE_TABLES)
    first_cols = {t: _table_columns(conn, t) for t in ledger._ADOPTABLE_TABLES}
    conn.close()

    conn2 = ledger.connect(path, migrate=True)
    assert conn2.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.LEDGER_SCHEMA_VERSION
    assert conn2.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0] == 1, "must stay a single row"
    second_counts = _row_counts(conn2, ledger._ADOPTABLE_TABLES)
    second_cols = {t: _table_columns(conn2, t) for t in ledger._ADOPTABLE_TABLES}
    assert second_counts == first_counts
    assert second_cols == first_cols
    conn2.close()


def test_migrate_refuses_a_newer_database():
    conn = ledger.connect(_tmp_path(), migrate=True)
    conn.execute("UPDATE schema_version SET version = ?", (ledger.LEDGER_SCHEMA_VERSION + 1,))
    conn.commit()
    try:
        ledger.migrate(conn)
        raise AssertionError("expected RuntimeError on a newer-than-us database")
    except RuntimeError as e:
        msg = str(e)
        assert str(ledger.LEDGER_SCHEMA_VERSION + 1) in msg, msg
        assert str(ledger.LEDGER_SCHEMA_VERSION) in msg, msg
    conn.close()


def test_snapshot_copies_rows_and_refuses_to_overwrite():
    path = _tmp_path()
    conn = ledger.connect(path, migrate=True)
    conn.execute("INSERT INTO cursors (key, value, updated_at) VALUES ('k','v','now')")
    conn.commit()

    dest = _tmp_path("snapshot.db")
    dest.parent.mkdir(parents=True, exist_ok=True)
    out = ledger.snapshot(conn, dest)
    assert out == dest

    snap = sqlite3.connect(dest)
    assert snap.execute("SELECT COUNT(*) FROM cursors").fetchone()[0] == 1
    snap.close()

    try:
        ledger.snapshot(conn, dest)
        raise AssertionError("expected snapshot() to refuse an existing destination")
    except sqlite3.OperationalError:
        pass
    conn.close()


# --- runner ------------------------------------------------------------------

def test_migration_3_adds_occurrence_mark():
    """See _MIGRATION_3 / triage.py's _occurrence_mark() and reopen_if_needed()
    — the column that stops a quiet-resolved grouped item from being reopened
    and re-resolved every ten minutes."""
    conn = ledger.connect(_tmp_path(), migrate=True)
    cols = _table_columns(conn, "triage_items")
    assert "occurrence_mark" in cols
    conn.close()


def test_v2_database_migrates_to_v3_matching_a_fresh_one():
    """A real upgrade path, not adoption: a database already stamped at
    schema_version 2 (state_deadline present, occurrence_mark not yet) must
    reach LEDGER_SCHEMA_VERSION 3 with a schema identical to a fresh database's."""
    path = _tmp_path()
    conn = sqlite3.connect(path)
    conn.executescript(ledger.BASE_SCHEMA)
    conn.executescript(ledger._MIGRATION_2)
    conn.execute(ledger._SCHEMA_VERSION_TABLE)
    conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (2, 'test')")
    conn.commit()
    conn.close()

    conn = ledger.connect(path, migrate=True)
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.LEDGER_SCHEMA_VERSION
    cols = {t: _table_columns(conn, t) for t in ledger._ADOPTABLE_TABLES}
    assert "occurrence_mark" in cols["triage_items"]

    fresh = ledger.connect(_tmp_path(), migrate=True)
    fresh_cols = {t: _table_columns(fresh, t) for t in ledger._ADOPTABLE_TABLES}
    assert fresh_cols == cols, f"v2-upgraded schema diverges from a fresh one: {cols} != {fresh_cols}"
    fresh.close()
    conn.close()


def test_migration_4_converts_resolved_to_quiet_and_creates_item_transitions():
    """See _MIGRATION_4 — the literal 'resolved' string, not a symbolic
    constant: triage.STATE_RESOLVED no longer exists after this migration
    lands, and ledger.py must not import triage's constants to migrate
    triage's own data. The table starts empty: migration 4 records nothing
    retroactively."""
    path = _tmp_path()
    conn = sqlite3.connect(path)
    conn.executescript(ledger.BASE_SCHEMA)
    conn.executescript(ledger._MIGRATION_2)
    conn.executescript(ledger._MIGRATION_3)
    conn.execute(ledger._SCHEMA_VERSION_TABLE)
    conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (3, 'test')")
    conn.execute("INSERT INTO events (source, external_id, title, first_seen) VALUES ('s','e1','one','now')")
    conn.execute(
        "INSERT INTO triage_items (event_id, signature, state, created_at, updated_at) "
        "VALUES (1, 's:e1', 'resolved', 'now', 'now')"
    )
    conn.commit()
    conn.close()

    conn = ledger.connect(path, migrate=True)
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.LEDGER_SCHEMA_VERSION
    row = conn.execute("SELECT state FROM triage_items WHERE event_id=1").fetchone()
    assert row["state"] == "quiet", "migration 4 must convert every `resolved` row to `quiet`"
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "item_transitions" in tables
    count = conn.execute("SELECT COUNT(*) FROM item_transitions").fetchone()[0]
    assert count == 0, "the table starts empty — migration 4 records nothing retroactively"
    conn.close()


def test_v3_database_migrates_to_v4_matching_a_fresh_one():
    """A real upgrade path, not adoption: a database already stamped at
    schema_version 3 must reach LEDGER_SCHEMA_VERSION 4 with a schema identical to a
    fresh database's, including the new item_transitions table."""
    path = _tmp_path()
    conn = sqlite3.connect(path)
    conn.executescript(ledger.BASE_SCHEMA)
    conn.executescript(ledger._MIGRATION_2)
    conn.executescript(ledger._MIGRATION_3)
    conn.execute(ledger._SCHEMA_VERSION_TABLE)
    conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (3, 'test')")
    conn.commit()
    conn.close()

    conn = ledger.connect(path, migrate=True)
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.LEDGER_SCHEMA_VERSION

    fresh = ledger.connect(_tmp_path(), migrate=True)
    for table in (*ledger._ADOPTABLE_TABLES, "item_transitions"):
        assert _table_columns(fresh, table) == _table_columns(conn, table), (
            f"{table}: a v3-upgraded schema diverges from a fresh one")
    fresh.close()
    conn.close()


def test_adoption_refuses_a_structurally_incomplete_database() -> None:
    """The trap adoption sets for itself: it decides on TABLE presence alone,
    so a database carrying all five tables while missing a column that only
    ever arrived via a runtime ALTER TABLE would be stamped version 1 and then
    fail as a bare `no such column` from inside the loop hours later."""
    tmp = Path(tempfile.mkdtemp(prefix="ledger-incomplete-")) / "warden.db"
    conn = sqlite3.connect(tmp)
    # Every table present, so `_tables_exist` is satisfied and the adoption
    # path is taken — but `dispatches` is built WITHOUT the columns that
    # the old bash CLI used to add by ALTER TABLE at runtime.
    conn.executescript(ledger.BASE_SCHEMA)
    conn.execute("ALTER TABLE dispatches DROP COLUMN validation_status")
    conn.commit()
    conn.close()

    conn = sqlite3.connect(tmp)
    try:
        ledger.migrate(conn)
    except RuntimeError as err:
        assert "missing" in str(err), str(err)
        assert "validation_status" in str(err), str(err)
        assert "dispatches" in str(err), str(err)
    else:
        raise AssertionError("migrate() adopted a database missing a declared column")
    finally:
        conn.close()


def test_adoption_tolerates_an_unexpected_extra_column() -> None:
    """The other direction is NOT fatal: a newer warden may have added a
    column, and refusing to read a database that has more than we expect turns
    forward compatibility into an outage."""
    tmp = Path(tempfile.mkdtemp(prefix="ledger-extra-")) / "warden.db"
    conn = sqlite3.connect(tmp)
    conn.executescript(ledger.BASE_SCHEMA)
    conn.execute("ALTER TABLE dispatches ADD COLUMN something_newer TEXT")
    conn.commit()
    conn.close()

    conn = sqlite3.connect(tmp)
    ledger.migrate(conn)
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.LEDGER_SCHEMA_VERSION
    conn.close()


def test_migration_5_creates_operations_table_and_indexes():
    """See _MIGRATION_5 / triage.py's record_operation()/complete_operation()/
    reconcile_operations() — the crash-recovery unit DESIGN.md § Crash
    recovery asks for."""
    conn = ledger.connect(_tmp_path(), migrate=True)
    cols = _table_columns(conn, "operations")
    assert cols == {
        "op_id", "event_id", "kind", "repo", "authorized_by", "started_at",
        "outcome", "outcome_at", "receipt_json", "reconciled_at", "note",
    }, cols
    indexes = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='operations' AND name NOT LIKE 'sqlite_%'"
    )}
    assert indexes == {"operations_open", "operations_event"}, indexes
    assert conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0, (
        "the table starts empty — migration 5 records nothing retroactively, same as item_transitions")
    conn.close()


def test_v4_database_migrates_to_v5_matching_a_fresh_one():
    """A real upgrade path, not adoption: a database already stamped at
    schema_version 4 (item_transitions present, operations not yet) must
    reach LEDGER_SCHEMA_VERSION 5 with a schema identical to a fresh database's."""
    path = _tmp_path()
    conn = sqlite3.connect(path)
    conn.executescript(ledger.BASE_SCHEMA)
    conn.executescript(ledger._MIGRATION_2)
    conn.executescript(ledger._MIGRATION_3)
    conn.executescript(ledger._MIGRATION_4)
    conn.execute(ledger._SCHEMA_VERSION_TABLE)
    conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (4, 'test')")
    conn.commit()
    conn.close()

    conn = ledger.connect(path, migrate=True)
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.LEDGER_SCHEMA_VERSION
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "operations" in tables, "migration 5 did not run against the v4-stamped database"

    fresh = ledger.connect(_tmp_path(), migrate=True)
    for table in ledger._VERSIONED_TABLES:
        assert _table_columns(fresh, table) == _table_columns(conn, table), (
            f"{table}: a v4-upgraded schema diverges from a fresh one")
    fresh.close()
    conn.close()


def test_verify_columns_catches_missing_operations_column():
    """Closes state-log.md §41 known-open item 5: item_transitions' (and now
    operations') shape was created by a migration but never asserted by
    _verify_columns(), because it only ever walked _ADOPTABLE_TABLES —
    exactly the five original BASE_SCHEMA tables, never a table a LATER
    migration added. A database stamped at LEDGER_SCHEMA_VERSION with an
    `operations` table missing a declared column must be refused, the same
    way test_adoption_refuses_a_structurally_incomplete_database already
    proves for `dispatches`."""
    tmp = Path(tempfile.mkdtemp(prefix="ledger-ops-incomplete-")) / "warden.db"
    conn = ledger.connect(tmp, migrate=True)
    conn.close()

    conn = sqlite3.connect(tmp)
    conn.execute("ALTER TABLE operations DROP COLUMN note")
    conn.commit()
    conn.close()

    conn = sqlite3.connect(tmp)
    try:
        ledger.migrate(conn)
    except RuntimeError as err:
        assert "missing" in str(err), str(err)
        assert "note" in str(err), str(err)
        assert "operations" in str(err), str(err)
    else:
        raise AssertionError("migrate() accepted an `operations` table missing a declared column")
    finally:
        conn.close()


def test_migration_6_adds_delivery_status_column():
    """See _MIGRATION_6 — dispatches.delivery_status replaces the sentinel
    string UNDELIVERABLE_SENTINEL that used to live IN `reported_at` (a TEXT
    column supposed to hold a timestamp, not a status). Landed alongside
    dispatch-sweep.py's move off `hermes send` onto scripts/slack_client.py's
    plain HTTP client."""
    conn = ledger.connect(_tmp_path(), migrate=True)
    cols = _table_columns(conn, "dispatches")
    assert "delivery_status" in cols, cols
    conn.close()


def test_migration_6_backfills_delivery_status_from_reported_at():
    """A v5-stamped database carries the pre-migration overload: the sentinel
    string 'undeliverable:no-origin-channel' living IN `reported_at`, and a
    real timestamp meaning delivered. Migration 6 must move both facts into
    `delivery_status` without losing the "closed, out of the open index"
    property the sentinel used to buy by sitting directly in `reported_at` —
    see idx_dispatches_open (`WHERE reported_at IS NULL`)."""
    path = _tmp_path()
    conn = sqlite3.connect(path)
    conn.executescript(ledger.BASE_SCHEMA)
    conn.executescript(ledger._MIGRATION_2)
    conn.executescript(ledger._MIGRATION_3)
    conn.executescript(ledger._MIGRATION_4)
    conn.executescript(ledger._MIGRATION_5)
    conn.execute(ledger._SCHEMA_VERSION_TABLE)
    conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (5, 'test')")

    # sentinel, with a finished_at to fall back to
    conn.execute(
        "INSERT INTO dispatches(job_id,tier,repo,brief,status,created_at,finished_at,reported_at) "
        "VALUES('sentinel-with-finished','investigate','r','b','lost','2026-01-01T00:00:00+00:00',"
        "'2026-01-02T00:00:00+00:00','undeliverable:no-origin-channel')"
    )
    # sentinel, no finished_at — falls back to created_at
    conn.execute(
        "INSERT INTO dispatches(job_id,tier,repo,brief,status,created_at,reported_at) "
        "VALUES('sentinel-no-finished','investigate','r','b','lost','2026-01-03T00:00:00+00:00',"
        "'undeliverable:no-origin-channel')"
    )
    # a real, delivered timestamp
    conn.execute(
        "INSERT INTO dispatches(job_id,tier,repo,brief,status,created_at,reported_at) "
        "VALUES('delivered-row','investigate','r','b','done','2026-01-04T00:00:00+00:00',"
        "'2026-01-04T01:00:00+00:00')"
    )
    # still open — never attempted
    conn.execute(
        "INSERT INTO dispatches(job_id,tier,repo,brief,status,created_at) "
        "VALUES('open-row','investigate','r','b','queued','2026-01-05T00:00:00+00:00')"
    )
    conn.commit()
    conn.close()

    conn = ledger.connect(path, migrate=True)
    rows = {r["job_id"]: r for r in conn.execute("SELECT * FROM dispatches").fetchall()}

    sentinel_finished = rows["sentinel-with-finished"]
    assert sentinel_finished["delivery_status"] == "undeliverable:no-origin-channel"
    assert sentinel_finished["reported_at"] == "2026-01-02T00:00:00+00:00", sentinel_finished["reported_at"]

    sentinel_no_finished = rows["sentinel-no-finished"]
    assert sentinel_no_finished["delivery_status"] == "undeliverable:no-origin-channel"
    assert sentinel_no_finished["reported_at"] == "2026-01-03T00:00:00+00:00", sentinel_no_finished["reported_at"]

    delivered = rows["delivered-row"]
    assert delivered["delivery_status"] == "delivered"
    assert delivered["reported_at"] == "2026-01-04T01:00:00+00:00"

    open_row = rows["open-row"]
    assert open_row["delivery_status"] is None
    assert open_row["reported_at"] is None

    # both sentinel rows now carry a REAL timestamp in reported_at, so they must
    # be excluded from idx_dispatches_open (WHERE reported_at IS NULL) exactly as
    # the sentinel string used to exclude them by sitting there directly.
    open_ids = {r["job_id"] for r in conn.execute(
        "SELECT job_id FROM dispatches WHERE reported_at IS NULL"
    ).fetchall()}
    assert open_ids == {"open-row"}, open_ids
    conn.close()


def test_v5_database_migrates_to_v6_matching_a_fresh_one():
    """A real upgrade path, not adoption: a database already stamped at
    schema_version 5 must reach LEDGER_SCHEMA_VERSION 6 with a schema identical to a
    fresh database's."""
    path = _tmp_path()
    conn = sqlite3.connect(path)
    conn.executescript(ledger.BASE_SCHEMA)
    conn.executescript(ledger._MIGRATION_2)
    conn.executescript(ledger._MIGRATION_3)
    conn.executescript(ledger._MIGRATION_4)
    conn.executescript(ledger._MIGRATION_5)
    conn.execute(ledger._SCHEMA_VERSION_TABLE)
    conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (5, 'test')")
    conn.commit()
    conn.close()

    conn = ledger.connect(path, migrate=True)
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.LEDGER_SCHEMA_VERSION

    fresh = ledger.connect(_tmp_path(), migrate=True)
    for table in ledger._VERSIONED_TABLES:
        assert _table_columns(fresh, table) == _table_columns(conn, table), (
            f"{table}: a v5-upgraded schema diverges from a fresh one")
    fresh.close()
    conn.close()


def test_migration_13_drops_the_dead_triage_columns_and_leaves_events_alone():
    """The reminder, parked-recurrence, snooze and deadline columns are gone from
    triage_items; `events.reminder_count`/`last_reminder_at` belong to the pollers'
    own table and stay."""
    conn = ledger.connect(_tmp_path(), migrate=True)
    cols = _table_columns(conn, "triage_items")
    for gone in ("reminder_count", "last_reminder_at", "parked_mark", "parked_recurrences",
                 "recurrence_reminded_at", "propose_unsure_at", "snoozed_until", "state_deadline"):
        assert gone not in cols, gone
    for added in ("close_reason", "strikes", "retry_at"):
        assert added in cols, added
    event_cols = _table_columns(conn, "events")
    assert "reminder_count" in event_cols and "last_reminder_at" in event_cols
    indexes = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert "idx_triage_state_deadline" not in indexes
    conn.close()


def test_v8_database_migrates_to_current_matching_a_fresh_one():
    """A real upgrade path, not adoption: a database already stamped at
    schema_version 8 must reach LEDGER_SCHEMA_VERSION with a schema identical to a
    fresh database's, an existing triage_items row surviving with its old
    `needs_human` mapped to `failed`."""
    path = _tmp_path()
    conn = sqlite3.connect(path)
    conn.executescript(ledger.BASE_SCHEMA)
    conn.executescript(ledger._MIGRATION_2)
    conn.executescript(ledger._MIGRATION_3)
    conn.executescript(ledger._MIGRATION_4)
    conn.executescript(ledger._MIGRATION_5)
    conn.executescript(ledger._MIGRATION_6)
    conn.executescript(ledger._MIGRATION_7)
    conn.executescript(ledger._MIGRATION_8)
    conn.execute(ledger._SCHEMA_VERSION_TABLE)
    conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (8, 'test')")
    conn.execute(
        "INSERT INTO events(source, external_id, title, first_seen) VALUES ('uk', 'pre-v9', 't', 'test')"
    )
    conn.execute(
        "INSERT INTO triage_items(event_id, signature, state, occurrences, first_seen, last_seen, "
        "created_at, updated_at) VALUES (1, 'uk:pre-v9', 'needs_human', 1, 'test', 'test', 'test', 'test')"
    )
    conn.commit()
    conn.close()

    conn = ledger.connect(path, migrate=True)
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.LEDGER_SCHEMA_VERSION
    row = conn.execute("SELECT state, strikes, retry_at, close_reason FROM triage_items WHERE event_id=1").fetchone()
    assert (row["state"], row["strikes"], row["retry_at"], row["close_reason"]) == ("failed", 0, None, None), dict(row)

    fresh = ledger.connect(_tmp_path(), migrate=True)
    for table in ledger._VERSIONED_TABLES:
        assert _table_columns(fresh, table) == _table_columns(conn, table), (
            f"{table}: a v8-upgraded schema diverges from a fresh one")
    fresh.close()
    conn.close()


def test_migration_10_adds_error_column():
    """See _MIGRATION_10 — the 2026-09-12 defect (item 253): a terminal
    dispatch with no verdict must have somewhere to record why. NULL on a
    fresh database (no dispatch has ever failed yet), same no-backfill
    contract as every other additive migration in this file."""
    conn = ledger.connect(_tmp_path(), migrate=True)
    cols = _table_columns(conn, "dispatches")
    assert "error" in cols, cols
    conn.close()


def test_v9_database_migrates_to_v10_matching_a_fresh_one():
    """A real upgrade path, not adoption: a database already stamped at
    schema_version 9 must reach LEDGER_SCHEMA_VERSION 10 with a schema
    identical to a fresh database's, and a pre-existing dispatch row must
    read `error` as NULL — no backfill, a pre-migration failure genuinely has
    no recorded reason."""
    path = _tmp_path()
    conn = sqlite3.connect(path)
    conn.executescript(ledger.BASE_SCHEMA)
    conn.executescript(ledger._MIGRATION_2)
    conn.executescript(ledger._MIGRATION_3)
    conn.executescript(ledger._MIGRATION_4)
    conn.executescript(ledger._MIGRATION_5)
    conn.executescript(ledger._MIGRATION_6)
    conn.executescript(ledger._MIGRATION_7)
    conn.executescript(ledger._MIGRATION_8)
    conn.executescript(ledger._MIGRATION_9)
    conn.execute(ledger._SCHEMA_VERSION_TABLE)
    conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (9, 'test')")
    conn.execute(
        "INSERT INTO dispatches(job_id,tier,repo,brief,status,created_at) "
        "VALUES ('pre-v10-job','investigate','r','b','failed','2026-01-01T00:00:00+00:00')"
    )
    conn.commit()
    conn.close()

    conn = ledger.connect(path, migrate=True)
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.LEDGER_SCHEMA_VERSION
    row = conn.execute("SELECT error FROM dispatches WHERE job_id='pre-v10-job'").fetchone()
    assert row["error"] is None, row["error"]

    fresh = ledger.connect(_tmp_path(), migrate=True)
    for table in ledger._VERSIONED_TABLES:
        assert _table_columns(fresh, table) == _table_columns(conn, table), (
            f"{table}: a v9-upgraded schema diverges from a fresh one")
    fresh.close()
    conn.close()


def test_v11_database_migrates_to_v12_without_dispatch_approvals():
    """Migration 12 drops the signed-approval table and its index from a real
    v11 ledger (the one place it still exists), leaves every other table and
    row alone, and lands on a schema identical to a fresh database's."""
    path = _tmp_path()
    conn = sqlite3.connect(path)
    conn.executescript(ledger.BASE_SCHEMA)
    for version in range(2, 12):
        conn.executescript(ledger.MIGRATIONS[version])
    # What a live v11 ledger carries and a fresh one no longer creates.
    conn.executescript(
        "CREATE TABLE dispatch_approvals (nonce TEXT PRIMARY KEY, payload_hash TEXT NOT NULL);"
        "CREATE INDEX idx_approvals_hash ON dispatch_approvals(payload_hash);"
        "INSERT INTO dispatch_approvals(nonce, payload_hash) VALUES ('n1', 'h1');"
    )
    conn.execute(ledger._SCHEMA_VERSION_TABLE)
    conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (11, 'test')")
    conn.execute(
        "INSERT INTO dispatches(job_id,tier,repo,brief,status,created_at) "
        "VALUES ('pre-v12-job','investigate','r','b','done','2026-01-01T00:00:00+00:00')"
    )
    conn.commit()
    conn.close()

    conn = ledger.connect(path, migrate=True)
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.LEDGER_SCHEMA_VERSION
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master")}
    assert "dispatch_approvals" not in names and "idx_approvals_hash" not in names, names
    assert conn.execute("SELECT COUNT(*) FROM dispatches WHERE job_id='pre-v12-job'").fetchone()[0] == 1

    fresh = ledger.connect(_tmp_path(), migrate=True)
    for table in ledger._VERSIONED_TABLES:
        assert _table_columns(fresh, table) == _table_columns(conn, table), (
            f"{table}: a v11-upgraded schema diverges from a fresh one")
    fresh.close()
    conn.close()


_OLD_TO_NEW = {
    "new": ("new", None), "split": ("triaged", None),
    "investigating": ("working", None), "verdict": ("working", None),
    "implementing": ("working", None), "remediating": ("working", None),
    "validating": ("merging", None), "pr_open": ("merging", None),
    "merged": ("verifying", None), "liveness_pending": ("verifying", None),
    "needs_human": ("failed", None), "merge_blocked": ("failed", None), "reverted": ("failed", None),
    "fixed": ("fixed", None), "quiet": ("quiet", None),
    "closed": ("closed", "resolved"),
    "ignored": ("closed", "ignored"), "dismissed": ("closed", "ignored"), "note": ("closed", "ignored"),
    "snoozed": ("new", None),
}


def _v12_database(path, states):
    conn = sqlite3.connect(path)
    conn.executescript(ledger.BASE_SCHEMA)
    for version in range(2, 13):
        conn.executescript(ledger.MIGRATIONS[version])
    conn.execute(ledger._SCHEMA_VERSION_TABLE)
    conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (12, 'test')")
    for i, state in enumerate(states, start=1):
        conn.execute("INSERT INTO events(source, external_id, title, first_seen) VALUES ('s', ?, 't', 'now')",
                     (f"e{i}",))
        conn.execute(
            "INSERT INTO triage_items(event_id, signature, state, note, state_deadline, snoozed_until, "
            "reminder_count, parked_recurrences, created_at, updated_at, card_hash) "
            "VALUES (?, ?, ?, ?, '2030-01-01', '2030-01-01', 2, 5, 'now', 'now', 'sha-of-an-old-card')",
            (i, f"s:e{i}", state, f"note-{state}"))
        conn.execute("INSERT INTO item_transitions(event_id, from_state, to_state, at, note) VALUES (?,?,?,?,?)",
                     (i, "new", state, "now", None))
        conn.execute("INSERT INTO item_transitions(event_id, from_state, to_state, at, note) VALUES (?,?,?,?,?)",
                     (i, state, "new", "later", None))
    conn.commit()
    conn.close()


def test_migration_13_maps_every_old_state_and_close_reason_and_rewrites_history():
    """The spec's mapping table, on a synthetic v12 database holding one row per old
    state: state and close_reason land as documented, the note survives, the
    dead columns are gone, the new ones default to 0/NULL, and item_transitions
    (which live code reads back) speaks the new vocabulary in both columns."""
    path = _tmp_path()
    _v12_database(path, list(_OLD_TO_NEW))
    conn = ledger.connect(path, migrate=True)
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == 13 == ledger.LEDGER_SCHEMA_VERSION

    rows = {r["note"]: r for r in conn.execute("SELECT * FROM triage_items")}
    assert len(rows) == len(_OLD_TO_NEW)
    for old, (state, reason) in _OLD_TO_NEW.items():
        row = rows[f"note-{old}"]
        assert (row["state"], row["close_reason"]) == (state, reason), (old, dict(row))
        assert row["strikes"] == 0 and row["retry_at"] is None, (old, dict(row))
        # card_hash is now "the state a Slack line was posted for": an already-`fixed` item
        # is stamped so the first pass on the new schema does not announce the backlog.
        assert row["card_hash"] == ("fixed" if state == "fixed" else "sha-of-an-old-card"), (old, dict(row))
    assert {r["state"] for r in rows.values()} <= {
        "new", "triaged", "working", "merging", "verifying", "needs_decision", "failed",
        "fixed", "quiet", "closed"}
    cols = _table_columns(conn, "triage_items")
    assert not ({"state_deadline", "snoozed_until", "reminder_count", "parked_recurrences"} & cols)

    history = conn.execute("SELECT from_state, to_state FROM item_transitions").fetchall()
    seen = {s for r in history for s in (r["from_state"], r["to_state"])}
    assert seen <= {"new", "triaged", "working", "merging", "verifying", "needs_decision", "failed",
                    "fixed", "quiet", "closed"}, seen
    to_state = {r["note"]: r["to_state"] for r in conn.execute(
        "SELECT t.to_state, i.note FROM item_transitions t JOIN triage_items i ON i.event_id=t.event_id WHERE t.at='now'")}
    assert to_state["note-needs_human"] == "failed" and to_state["note-ignored"] == "closed", to_state
    assert conn.execute("SELECT COUNT(*) FROM item_transitions").fetchone()[0] == 2 * len(_OLD_TO_NEW), (
        "history rows survive")

    fresh = ledger.connect(_tmp_path(), migrate=True)
    for table in ledger._VERSIONED_TABLES:
        assert _table_columns(fresh, table) == _table_columns(conn, table), (
            f"{table}: a v12-upgraded schema diverges from a fresh one")
    fresh.close()
    conn.close()


def test_migration_13_is_one_transaction_and_a_second_connect_is_a_no_op():
    path = _tmp_path()
    _v12_database(path, ["needs_human", "ignored", "verdict"])
    ledger.connect(path, migrate=True).close()
    conn = ledger.connect(path, migrate=True)   # already at 13: nothing runs again
    states = sorted(r["state"] for r in conn.execute("SELECT state FROM triage_items"))
    assert states == ["closed", "failed", "working"], states
    conn.close()


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
