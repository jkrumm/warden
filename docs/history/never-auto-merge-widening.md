# Landed: NEVER_AUTO_MERGE withdrawn (§107)

Prepared 2026-09-28 (§105) as a per-repo question; answered and landed
2026-09-29. The owner, in words: "mach es. Es soll effektiv sein, es soll
funktionieren." — everything that stops a fix at a draft PR goes, Makefiles,
`ops/`, `.github/`, plists, manifests, lockfiles, `pyproject.toml` included; the
one exception he did not withdraw is his own executor: `warden`, `sideclaw`,
`dotfiles`.

**What landed is broader than the diff below proposed**, and the reason is his
wording: the answer was not "weatherorb", it was "everything but the executor".
So instead of a `FULL_AUTONOMY_REPOS` exemption the tuple itself is gone from
`scripts/lifecycle/merge.py`, and the part he kept moved into code as
`EXECUTOR_REPOS = {"warden", "sideclaw", "dotfiles"}` — gated there even if
`merge_approval` in the dispatch policy were emptied, no scope ever, and the
CI-definition refusal (`.github/workflows`, `.github/actions`) now applies to
those three only, still on the owner's Argo click too. The four tests that
pinned the old rule were re-pinned to the new one, not weakened: the paths that
used to refuse now assert a merge, and a new test asserts the executor gate
holds with the policy file gating nothing.

`weatherorb-pull` grew a `make launchd-install` between the pull and the
kickstarts, because a merged `ops/*.plist` that nothing reloads is "merged, not
deployed" — and the repo's own target renders and bootstraps only a changed
plist.

The analysis below is kept as written on 2026-09-28; its "one question" is
answered above.

---

## Where the line is today

`config/triage-policy.json` now gives weatherorb `autoMergePaths: ["**"]` — the
default unattended scope. The only thing still stopping a weatherorb PR at a
draft is `NEVER_AUTO_MERGE` in `scripts/lifecycle/merge.py`: `Makefile`, `*.mk`,
`.github/*`, `scripts/*`, `launchd/*`, `*.plist`, Dockerfiles, compose,
`package.json`, lockfiles, `pyproject.toml`, `requirements*`, `.env*`, `*.tpl`.
It is code on purpose (DESIGN.md § Self-concealing change: warden must not write
the code it then runs) and the policy file may name and parameterise, never widen it.

The standing directive of 2026-09-28 was, in words: everything should be "automatisch reviewed, automatisch gemerged, deployed und verifiziert" — a human only where a human is genuinely needed, never "erfundene Friction". That directive landed weatherorb's `autoMergePaths: ["**"]`. It does **not** name Makefile, CI or plist paths, which is exactly why this widening is a diff to read and not a change to make: it moves the line on paths where a merged file executes with no human between (see the table below). One owner word lands it; nothing here is live.

What the invariant actually protects in weatherorb, checked against the checkout:

| Path class | Who runs it after a merge |
|-|-|
| `Makefile` | the owner by hand only — no LaunchAgent execs `make`, and `weatherorb-pull` never does |
| `ops/*.plist` | launchd, but only after `make launchd-install` (bootout + bootstrap); a pull and `kickstart -k` never re-read a plist |
| `.github/workflows/deploy-edge.yml` | GitHub Actions on the merge itself → RollHook → the VPS nginx edge. **The one path where a merged file executes with no human between.** |
| `package.json`, `bun.lock`, `uv.lock`, `pyproject.toml` | the next `uv run` / `bun install` — every periodic job installs from `pyproject`/`uv.lock` on its next tick. Supply chain, not self-modification. |
| `deploy/edge/Dockerfile` | the same Actions run |

Already outside the invariant, today: `ops/run-sync.sh` and `ops/run-blendfield.sh`
(launchd execs them, `scripts/*` does not match `ops/`), everything in `src/`.
So the line is not "warden never writes what launchd runs"; for weatherorb it
already does. The line is CI, service definitions and dependency manifests.

## The diff (do not apply without the owner's word)

Recommended shape: a per-repo exemption in code, not a narrower global tuple. A
narrower tuple drops the protection for every non-gated repo at once and breaks
`tests/test_merge.py::test_never_auto_merge_covers_variants_and_other_ecosystems`
(`.github/workflows/ci.yml`, `pnpm-lock.yaml`, `yarn.lock`, `package-lock.json`
are pinned there). An exemption keeps the tuple and its pin test intact.

```diff
--- a/scripts/lifecycle/merge.py
+++ b/scripts/lifecycle/merge.py
@@ NEVER_AUTO_MERGE
     ".env*", "*/.env*", "*.tpl",
 )

+# Repos the owner has taken out of NEVER_AUTO_MERGE entirely (owner, <date>):
+# private, only he files issues, and he wants Makefile/CI/manifest fixes landed
+# without a click. Still gated by the declared scope, CI where it exists, and
+# the confirmed step-7 review. Never a merge_approval repo — asserted below,
+# because that would open the loop's own executor.
+FULL_AUTONOMY_REPOS: frozenset[str] = frozenset({"weatherorb"})
+
@@ def merge_gate_check(
-        never = [fn for fn in filenames if any(fnmatch.fnmatch(fn, p) for p in NEVER_AUTO_MERGE)]
+        never = [] if repo in FULL_AUTONOMY_REPOS else [
+            fn for fn in filenames if any(fnmatch.fnmatch(fn, p) for p in NEVER_AUTO_MERGE)]
         if never:
```

Plus, in `effective_repo_entry()` (or at import, reading the dispatch policy):
refuse to start if any `FULL_AUTONOMY_REPOS` member is in `merge_approval`.

Tests to add in `tests/test_merge.py`: (1) a weatherorb PR touching `Makefile`
merges unattended; (2) the same PR on `homelab` still refuses with
`NEVER_AUTO_MERGE`; (3) `FULL_AUTONOMY_REPOS & merge_approval == ∅` is asserted.
The existing pin test at the bottom of `test_merge.py` stays byte-identical.

Not part of this diff, whatever the answer: `warden`, `sideclaw`, `dotfiles`
keep no `autoMergePaths`, stay in `merge_approval`, and land only on the Argo click.

## What changes if he says yes

- A merged `deploy-edge.yml` change runs on the VPS edge as part of the merge.
  The step-7 review is the only reader before it executes.
- A merged `bun.lock`/`uv.lock` bump installs on every periodic job's next tick.
  The release-age cooldown inside the episode still applies to what the worker
  installs; nothing re-checks it at merge time.
- A merged `ops/*.plist` change lands in git but stays inert until the owner
  runs `make launchd-install` — merged is not deployed for that class, and the
  item's liveness (the watchdog push) will not notice. Honest wording on the
  card would need a `deploy`-side note; not built.
- `Makefile` changes are inert until he types `make`.

## The one question

Weatherorb (only weatherorb) completely out of NEVER_AUTO_MERGE — Makefile,
`.github/workflows`, `ops/*.plist`, `package.json`/`bun.lock`/`uv.lock`/
`pyproject.toml` included — yes or no? "Yes" is the diff above verbatim;
"Makefile only" is `never = [fn for fn in ... if not (repo in FULL_AUTONOMY_REPOS and fnmatch(fn, "*Makefile"))]`
and the tests shrink to match.
