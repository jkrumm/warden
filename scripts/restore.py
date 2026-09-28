"""restore — the ledger's way back, and the drill that proves it (state-log §104).

A backup nobody has restored from is an assumption. This fetches a snapshot the
daily backup shipped (by default the OFF-BOX copy on homelab — if the mini is
gone, that is what is left), restores it into a fresh temporary directory, and
proves a running warden could come back from it:

  1. integrity   `PRAGMA integrity_check` == ok
  2. schema      stamped version <= this warden's, and the one migrator brings
                 the copy to the current version (a snapshot from before a schema
                 bump is the normal case, not an error)
  3. data        events and dispatches present; the newest write is no more than
                 MAX_WRITE_LAG before the snapshot's own timestamp, and not after it
  4. boots       the real loop runs one `--dry-run` pass against the copy
  5. repo        the repo bundle next to it passes `git bundle verify`

It can never write the live ledger: every path it writes is checked by
assert_safe_target() — resolved, symlinks followed — and anything inside
~/.warden/ or equal to the live WARDEN_DB is refused before a byte moves. The
result is one line on stdout, one JSON record in ~/.warden/restore-drill.json
(the self-audit's input: a failed or stale drill becomes a warden item), and the
exit code. The temporary directory is removed on every path.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import importlib.util  # noqa: E402

_spec = importlib.util.spec_from_file_location("ledger", REPO / "scripts" / "ledger.py")
_ledger = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ledger)

WARDEN_HOME = Path(os.environ.get("WARDEN_HOME", Path.home() / ".warden")).expanduser()
LIVE_DB = Path(os.environ["WARDEN_DB"]).expanduser() if os.environ.get("WARDEN_DB") else WARDEN_HOME / "warden.db"
RESULT_FILE = WARDEN_HOME / "restore-drill.json"
LOCAL_SNAPSHOTS = WARDEN_HOME / "backups"
REMOTE_HOST = "homelab"
REMOTE_SNAPSHOTS = "/mnt/hdd/backups/warden/backups"
SNAPSHOT_RE = re.compile(r"^warden-(\d{8}T\d{6}Z)\.db$")
MAX_WRITE_LAG = dt.timedelta(hours=2)   # the loop writes every 600 s; 2 h is twelve missed ticks
FUTURE_SLACK = dt.timedelta(minutes=5)
SSH_TIMEOUT = 120


class UnsafeTarget(Exception):
    """A restore target that is, or could become, the live ledger."""


def assert_safe_target(path: Path) -> Path:
    """The hard guard. Resolves `path` (symlinks followed, parents that do not
    exist yet resolved as far as they do) and refuses it if it is the live
    ledger, a sibling of it (`-wal`/`-shm`), or anywhere under WARDEN_HOME.
    Called on every path this module writes, not once at the start."""
    resolved = Path(os.path.realpath(path))
    live = Path(os.path.realpath(LIVE_DB))
    home = Path(os.path.realpath(WARDEN_HOME))
    if resolved == live or resolved.name.startswith(live.name) and resolved.parent == live.parent:
        raise UnsafeTarget(f"refusing to write {resolved}: that is the live ledger")
    if resolved == home or home in resolved.parents:
        raise UnsafeTarget(f"refusing to write {resolved}: it is inside {home}, the live ledger's home")
    return resolved


def _snapshot_time(name: str) -> dt.datetime | None:
    m = SNAPSHOT_RE.match(name)
    if not m:
        return None
    return dt.datetime.strptime(m.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=dt.timezone.utc)


def _latest_remote() -> str:
    proc = subprocess.run(["ssh", "-o", "BatchMode=yes", REMOTE_HOST, f"ls -1 {REMOTE_SNAPSHOTS}"],
                          capture_output=True, text=True, timeout=SSH_TIMEOUT)
    if proc.returncode != 0:
        raise RuntimeError(f"listing {REMOTE_HOST}:{REMOTE_SNAPSHOTS} failed: {proc.stderr.strip()[:200]}")
    names = sorted(n for n in proc.stdout.split() if SNAPSHOT_RE.match(n))
    if not names:
        raise RuntimeError(f"no snapshot in {REMOTE_HOST}:{REMOTE_SNAPSHOTS}")
    return names[-1]


def fetch(source: str, workdir: Path) -> tuple[Path, str, Path | None]:
    """Copy the chosen snapshot (and the repo bundle beside it) into workdir.
    `source` is `latest` (newest off-box snapshot), `local-latest`, or a path."""
    if source in ("latest", "remote-latest"):
        name = _latest_remote()
        dst = assert_safe_target(workdir / name)
        bundle = assert_safe_target(workdir / "warden-repo.bundle")
        for remote, local in ((f"{REMOTE_SNAPSHOTS}/{name}", dst),
                              (f"{REMOTE_SNAPSHOTS}/warden-repo.bundle", bundle)):
            proc = subprocess.run(["scp", "-q", "-o", "BatchMode=yes", f"{REMOTE_HOST}:{remote}", str(local)],
                                  capture_output=True, text=True, timeout=SSH_TIMEOUT)
            if proc.returncode != 0 and local == dst:
                raise RuntimeError(f"scp of {remote} failed: {proc.stderr.strip()[:200]}")
        return dst, f"{REMOTE_HOST}:{REMOTE_SNAPSHOTS}/{name}", bundle if bundle.exists() else None
    if source == "local-latest":
        names = sorted(p.name for p in LOCAL_SNAPSHOTS.glob("warden-*.db") if SNAPSHOT_RE.match(p.name))
        if not names:
            raise RuntimeError(f"no snapshot in {LOCAL_SNAPSHOTS}")
        src = LOCAL_SNAPSHOTS / names[-1]
    else:
        src = Path(source).expanduser()
    if not src.is_file():
        raise RuntimeError(f"no such snapshot: {src}")
    dst = assert_safe_target(workdir / src.name)
    shutil.copy2(src, dst)
    bundle_src = src.parent / "warden-repo.bundle"
    bundle = None
    if bundle_src.is_file():
        bundle = assert_safe_target(workdir / bundle_src.name)
        shutil.copy2(bundle_src, bundle)
    return dst, str(src), bundle


def verify(db: Path, snapshot_name: str, bundle: Path | None, *, boot: bool = True) -> dict[str, Any]:
    """Every check the module docstring names, on the restored copy only.
    Raises AssertionError with the failing check's own words."""
    assert_safe_target(db)
    facts: dict[str, Any] = {}
    raw = sqlite3.connect(db)
    try:
        integrity = raw.execute("PRAGMA integrity_check").fetchone()[0]
        assert integrity == "ok", f"integrity_check: {integrity}"
        stamped = raw.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
    finally:
        raw.close()
    facts["integrity"] = "ok"
    assert isinstance(stamped, int), "schema_version table empty"
    assert stamped <= _ledger.LEDGER_SCHEMA_VERSION, (
        f"snapshot is schema {stamped}, newer than this warden's {_ledger.LEDGER_SCHEMA_VERSION}")
    conn = _ledger.connect(db, migrate=True)
    try:
        migrated = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        assert migrated == _ledger.LEDGER_SCHEMA_VERSION, f"migrator left the copy at {migrated}"
        events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        dispatches = conn.execute("SELECT COUNT(*) FROM dispatches").fetchone()[0]
        newest = conn.execute(
            "SELECT MAX(t) FROM (SELECT MAX(updated_at) AS t FROM triage_items "
            "UNION ALL SELECT MAX(updated_at) FROM cursors)").fetchone()[0]
    finally:
        conn.close()
    facts.update(schema=f"{stamped}->{migrated}", events=events, dispatches=dispatches, newest_write=newest)
    assert events > 0 and dispatches > 0, f"implausibly empty: {events} events, {dispatches} dispatches"
    taken = _snapshot_time(snapshot_name)
    newest_dt = dt.datetime.fromisoformat(newest) if newest else None
    if taken is not None:
        assert newest_dt is not None, "no write timestamp in the copy at all"
        lag = taken - newest_dt
        facts["write_lag_min"] = round(lag.total_seconds() / 60, 1)
        assert lag <= MAX_WRITE_LAG, f"newest write {newest} is {lag} older than the snapshot ({taken.isoformat()})"
        assert lag >= -FUTURE_SLACK, f"newest write {newest} is after the snapshot ({taken.isoformat()}): wrong file"
    if boot:
        env = {k: v for k, v in os.environ.items()
               if k not in ("CLAUDECODE", "CLAUDE_CODE_SESSION", "CLAUDE_SESSION_ID")}
        proc = subprocess.run([sys.executable, str(REPO / "scripts" / "triage.py"), "--run", "--dry-run",
                               "--db", str(db)], capture_output=True, text=True, env=env, timeout=600)
        assert proc.returncode == 0, f"loop dry-run on the copy exited {proc.returncode}: {proc.stderr.strip()[-300:]}"
        facts["loop_dry_run"] = "ok"
    if bundle is not None:
        # Clone it, don't just `bundle verify` it: verify needs a repository as
        # its cwd (it failed under launchd, whose cwd is /, the first time the
        # agent ran), and a clone that yields the loop's own code is the actual
        # claim — warden can be rebuilt from what went off-box.
        clone = assert_safe_target(db.parent / "repo")
        proc = subprocess.run(["git", "clone", "-q", str(bundle), str(clone)], capture_output=True, text=True,
                              timeout=120, cwd=str(db.parent), env={**os.environ, "LC_ALL": "C"})
        assert proc.returncode == 0, f"git clone of the repo bundle failed: {proc.stderr.strip()[-200:]}"
        for needed in ("scripts/triage.py", "scripts/ledger.py"):
            assert (clone / needed).is_file(), f"repo bundle clone lacks {needed}"
        head = subprocess.run(["git", "-C", str(clone), "log", "-1", "--format=%h"], capture_output=True,
                              text=True, timeout=30).stdout.strip()
        facts["repo_bundle"] = f"ok@{head}"
    else:
        facts["repo_bundle"] = "missing"
    return facts


def drill(source: str, *, boot: bool = True, record: bool = True) -> int:
    started = time.time()
    workdir = Path(tempfile.mkdtemp(prefix="warden-restore-"))
    result: dict[str, Any] = {"ok": False, "source": source, "at": dt.datetime.now(dt.timezone.utc).isoformat()}
    try:
        assert_safe_target(workdir)
        db, origin, bundle = fetch(source, workdir)
        result["snapshot"] = origin
        result.update(verify(db, db.name, bundle, boot=boot))
        result["ok"] = True
    except (AssertionError, RuntimeError, UnsafeTarget, OSError, sqlite3.Error,
            subprocess.TimeoutExpired, ValueError) as e:
        result["error"] = f"{type(e).__name__}: {e}"
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
        result["cleaned_up"] = not workdir.exists()
        result["seconds"] = round(time.time() - started, 1)
    if record:
        WARDEN_HOME.mkdir(parents=True, exist_ok=True)
        tmp = RESULT_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(result))
        tmp.replace(RESULT_FILE)
    if result["ok"]:
        print(f"restore-drill: OK {result['snapshot']} integrity=ok schema={result['schema']} "
              f"events={result['events']} dispatches={result['dispatches']} "
              f"write_lag={result.get('write_lag_min')}m loop_dry_run={result.get('loop_dry_run', 'skipped')} "
              f"repo_bundle={result['repo_bundle']} cleaned_up={result['cleaned_up']} {result['seconds']}s")
        return 0
    print(f"restore-drill: FAIL {result.get('snapshot', source)}: {result['error']} "
          f"cleaned_up={result['cleaned_up']}")
    return 1


def main(argv: list[str]) -> int:
    source = argv[0] if argv else "latest"
    return drill(source)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
