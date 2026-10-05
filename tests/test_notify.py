#!/usr/bin/env python3
"""Regression suite for scripts/loop/notify.py's owner-note-via-Argo idempotency.

The `[owner note via Argo #<id>, <date>]` tag is the only dedup a note action has, and
the note it lives in is capped to one short line (lifecycle/items.py NOTE_MAX) by
`core.set_state()`. A tag appended after a long note used to be cut off by that cap, so a
redelivered action found no tag and appended the text twice.

Run: .venv/bin/python3 tests/test_notify.py  (or: make test, from warden/)
"""

from __future__ import annotations

import datetime as dt
import sys
import tempfile
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import ledger  # noqa: E402
from lifecycle import items as _items  # noqa: E402
from loop import core, notify  # noqa: E402

NOW = dt.datetime(2030, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)


def _item_with_note(note: str):
    path = Path(tempfile.mkdtemp(prefix="notify-test-")) / "warden.db"
    conn = ledger.connect(path, migrate=True)
    conn.execute("INSERT INTO events(source, external_id, title, first_seen) VALUES ('s', 'e1', 't', 'now')")
    eid = conn.execute("SELECT id FROM events").fetchone()[0]
    conn.execute(
        "INSERT INTO triage_items(event_id, signature, repo, state, occurrences, first_seen, last_seen, "
        "created_at, updated_at) VALUES (?, 's:e1', 'demo', ?, 1, 'now', 'now', 'now', 'now')",
        (eid, core.STATE_NEEDS_DECISION))
    core.set_state(conn, eid, core.STATE_NEEDS_DECISION, NOW, note=note)
    conn.commit()
    return conn, eid


def _apply(conn, eid: int, action_id, text: str):
    item = core.get_item(conn, eid)
    return notify._apply_argo_note(conn, item, eid, NOW, {"text": text}, action_id)


def test_redelivered_note_is_not_appended_twice_after_a_long_note():
    conn, eid = _item_with_note("x" * 190)
    assert _apply(conn, eid, 7, "owner adds context")[0] == "applied"
    first = core.get_item(conn, eid)["note"]
    assert "[owner note via Argo #7, 2030-01-02]" in first, f"the cap cut the idempotency tag: {first!r}"
    assert "owner adds context" in first, first
    assert len(first) <= _items.NOTE_MAX, len(first)

    assert _apply(conn, eid, 7, "owner adds context")[0] == "applied"   # the redelivery
    assert core.get_item(conn, eid)["note"] == first, "a redelivered note action changed the note"


def test_a_long_owner_text_keeps_the_tag_and_still_dedupes():
    conn, eid = _item_with_note("short original")
    assert _apply(conn, eid, 8, "y" * 500)[0] == "applied"
    first = core.get_item(conn, eid)["note"]
    assert "[owner note via Argo #8, 2030-01-02]" in first and len(first) <= _items.NOTE_MAX, first
    _apply(conn, eid, 8, "y" * 500)
    assert core.get_item(conn, eid)["note"] == first


def test_a_short_note_still_reads_original_then_owner_text():
    conn, eid = _item_with_note("original note")
    _apply(conn, eid, 9, "owner adds context")
    note = core.get_item(conn, eid)["note"]
    assert note == "original note [owner note via Argo #9, 2030-01-02]: owner adds context", note


def test_append_to_note_keeps_the_tail_whole_and_gives_way_in_the_head():
    assert _items.append_to_note(None, "tail") == "tail"
    assert _items.append_to_note("head", "tail") == "head tail"
    tail = "t" * 150
    out = _items.append_to_note("h" * 300, tail)
    assert len(out) == _items.NOTE_MAX and out.endswith(tail) and "…" in out, out
    assert _items.append_to_note("h" * 300, "t" * 199) == "t" * 199
    assert _items.append_to_note("head", "z" * 400) == _items.cap_note("z" * 400)


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
