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
    for t in ("events", "cursors", "dispatches", "dispatch_approvals", "triage_items", "schema_version"):
        assert t in tables, f"missing table {t}"
    indexes = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'")}
    expected = {
        "idx_events_open",
        "idx_events_resolved_at",
        "idx_dispatches_open",
        "idx_dispatches_created",
        "idx_approvals_hash",
        "idx_triage_state",
    }
    assert indexes == expected, f"got {indexes}"
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.SCHEMA_VERSION
    conn.close()


def test_connect_without_migrate_raises_naming_both_versions():
    path = _tmp_path()
    try:
        ledger.connect(path)
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        msg = str(e)
        assert "0" in msg, msg
        assert str(ledger.SCHEMA_VERSION) in msg, msg


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
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == 1

    post_cols = {t: _table_columns(conn, t) for t in ledger._ADOPTABLE_TABLES}
    post_counts = _row_counts(conn, ledger._ADOPTABLE_TABLES)
    assert post_cols == pre_cols, f"columns changed: {post_cols} != {pre_cols}"
    assert post_counts == pre_counts, f"rows lost: {post_counts} != {pre_counts}"

    # Functionally equivalent to a from-scratch database: same column sets
    # for every adopted table (position may legitimately differ — this
    # compares sets, not order, per the ALTER-vs-inline note in ledger.py).
    fresh = ledger.connect(_tmp_path(), migrate=True)
    fresh_cols = {t: _table_columns(fresh, t) for t in ledger._ADOPTABLE_TABLES}
    assert fresh_cols == post_cols, f"adopted schema diverges from a fresh one: {post_cols} != {fresh_cols}"
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
    assert conn2.execute("SELECT version FROM schema_version").fetchone()[0] == 1
    assert conn2.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0] == 1, "must stay a single row"
    second_counts = _row_counts(conn2, ledger._ADOPTABLE_TABLES)
    second_cols = {t: _table_columns(conn2, t) for t in ledger._ADOPTABLE_TABLES}
    assert second_counts == first_counts
    assert second_cols == first_cols
    conn2.close()


def test_migrate_refuses_a_newer_database():
    conn = ledger.connect(_tmp_path(), migrate=True)
    conn.execute("UPDATE schema_version SET version = ?", (ledger.SCHEMA_VERSION + 1,))
    conn.commit()
    try:
        ledger.migrate(conn)
        raise AssertionError("expected RuntimeError on a newer-than-us database")
    except RuntimeError as e:
        msg = str(e)
        assert str(ledger.SCHEMA_VERSION + 1) in msg, msg
        assert str(ledger.SCHEMA_VERSION) in msg, msg
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

def test_adoption_refuses_a_structurally_incomplete_database() -> None:
    """The trap adoption sets for itself: it decides on TABLE presence alone,
    so a database carrying all five tables while missing a column that only
    ever arrived via a runtime ALTER TABLE would be stamped version 1 and then
    fail as a bare `no such column` from inside the loop hours later."""
    tmp = Path(tempfile.mkdtemp(prefix="ledger-incomplete-")) / "warden.db"
    conn = sqlite3.connect(tmp)
    # Every table present, so `_tables_exist` is satisfied and the adoption
    # path is taken — but `dispatches` is built WITHOUT the columns that
    # hermes-cc.sh used to add by ALTER TABLE at runtime.
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
    assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == ledger.SCHEMA_VERSION
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
