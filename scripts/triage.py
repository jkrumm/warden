"""Alert triage — the act-loop that turns deduplicated watchdog.db events into
one durable, updated-in-place Slack card per problem, with a real sideclaw
investigation attached once a signature repeats or stays open. THE ACT PATH
(ingest -> classify -> cluster -> escalate -> card -> resolve) MAKES NO LLM
CALL AT ALL (the dispatched sideclaw `investigate` episode itself runs Claude
Code, which is inherent to what "investigate" means — that is a property of
hermes-cc.sh, not of this script). The ONE exception in the whole file is
`propose_mappings()` — a bounded, once-a-day maintenance pass, batched, never
in the act path itself — see PROPOSE MAPPINGS below.

Runs every 10 min as a Hermes `no_agent` cron script (via triage-cron.py, the
thin loader — see that file's docstring for why it has to stay thin).

WHY THIS EXISTS. `scripts/watchdog-poll.py` already ingests `#alerts` and the
other sources into `events` (deduplicated by `normalize_title()`/UNIQUE(source,
external_id)), but nothing ever ACTED on that dedup — Hermes instead ran a full
`reasoning_effort: high` LLM turn on every inbound alert MESSAGE, discarding
most of the replies via a NO_REPLY marker. Because `slack.reply_in_thread: true`
makes every alert its own session, that turn had zero memory of the last time
the exact same signature fired: the same `research-gateway job.reaped >= 1
(15m)` alert was triaged 61 times in one day, reaching the same conclusion each
time, without ever noticing a fix already existed. See config.yaml's `slack:`
comment block and docs/triage.md for the full before/after.

THE LOOP, once per run — see docs/triage.md for the full state machine:
  1. Ingest      — upsert one triage_items row per open ingest-source event,
                   including op_refs_homelab/op_refs_vps (a dead 1Password
                   ref blocks every future secrets-cache reseal — this must
                   reach at least the digest, never silently drop).
  2. Reopen/unsnooze — undo a stale `quiet`/`fixed`/`closed`/`dismissed`/
                   `snoozed` state the underlying event has since moved past
                   (grouped sources reuse the same events.id across a
                   close -> recur cycle, so this is a state fix-up, never a
                   new row).
  3. Classify     — fnmatch MULTIPLE targets per event (see MATCH TARGETS
                   below) against config/triage-policy.json, in order: the
                   explicit `ignore` list (genuine recoveries/known-benign —
                   route to `ignored`, terminal, invisible), the structural
                   `ignoreUnstructuredSlackProse` fallback (route to
                   STATE_NOTE — terminal, but VISIBLE in the digest, see that
                   state's own docstring for why), then `rules` (resolve
                   EITHER `repo`, escalate to an episode, OR `verb`, run a
                   declared local command — see VERB OUTCOMES below). Only
                   ever touches a row still in state `new`.
  -1. Reconcile   — runs FIRST, before even Drain, over `operations` rows
                   with `outcome IS NULL` — an operation this process
                   recorded as STARTED (before the external call it covers:
                   `hermes-cc.sh dispatch --tier implement` or
                   `merge --confirm`) but never recorded the result of,
                   whether from a genuine crash or from a call site that
                   deliberately left it open on an ambiguous return (a
                   subprocess timeout, unparseable stdout — see
                   maybe_auto_implement()/poll_validation_jobs()). Asks the
                   external system (sideclaw, GitHub) what actually
                   happened; an operation that still cannot be resolved
                   moves its item to `needs_human` rather than being
                   retried (see reconcile_operations()). DESIGN.md §
                   Crash recovery: "unknown is an explicit outcome,
                   reconciled before any retry — never silently read as
                   failure." Must run before anything else in THE LOOP
                   could act on the item underneath an in-flight operation.
  0. Drain        — apply anything a surface spooled into ~/.warden/intents
                   since the last pass (see drain_intents() and
                   scripts/intents.py). The loop is the BACKSTOP drainer, not
                   the only one: the Slack approval plugin drains synchronously
                   because a click must land before `hermes-cc.sh --confirm`
                   runs. This is what stops an intent nobody else drained from
                   sitting in the spool forever.
  4. Resolve      — an event whose events.resolved_at is now set flips its
                   triage_items row to `quiet` — but ONLY a row still in
                   `new`. events.resolved_at is set by disappearance from
                   observation, and observation ending never discharges an
                   obligation (DESIGN.md principle 5; see
                   _SILENCE_RESOLVE_ELIGIBLE_STATES, which governs this step
                   and step 4b alike).
  4b. Quiet/paired resolve — the two GROUPED_TRIAGE_SOURCES (slack_alert,
                   hermes_log) never disappearance-resolve via step 4 at all
                   (watchdog-poll.py's own sweep_stale_grouped only clears
                   them after 7 idle days). resolve_recovery_paired() checks
                   a fresh #alerts fetch for a `✅`-prefixed message pairing
                   the same alert text (see that function's own docstring);
                   resolve_quiet_grouped() falls back to a quietResolveHours
                   silence timer. Both are silence paths, so both touch only
                   `new` rows too. Neither ever claims "fixed" — see
                   QUIET_RESOLVE_NOTE_PREFIX/RECOVERY_PAIRED_NOTE_PREFIX.
  5. Dissolve     — a cluster (see CLUSTERING below) whose folded verdict says
                   its members do not share a root cause splits into `split`
                   items — carrying that verdict in `note` — each waiting to
                   be re-evaluated individually (see STATE_SPLIT).
  6. Escalate     — every `new`+`repo`-mapped+eligible item, GROUPED BY REPO,
                   becomes at most one sideclaw `investigate` dispatch per
                   repo per run (a cluster), not one per item; a `split` item
                   escalates too, but always as a SINGLETON, ahead of that
                   repo's `new` clusters — see escalate()'s own comment.
  6b. Verbs       — every `new`+`verb`-mapped+eligible item runs its
                   allowlisted local command once (see VERB OUTCOMES) — never
                   an episode, never clustered with repo-mapped items.
  6c. Deadlines   — every non-terminal state names its poller and how long a
                   row may sit in it (STATE_DEADLINES); sweep_deadlines() is
                   what happens when the poller did not deliver. Runs after
                   every poller (so an item that can still advance does) and
                   before the card (so the expiry is visible in the same pass).
                   An expiry is never silent: it writes the reason into `note`
                   and prints it.
  7. Card         — one Slack card per cluster (or per verb outcome), posted
                   once state leaves `new` (an unescalated item, mapped or
                   not, is carried silently — see CARDED STATES below) and
                   updated in place after, no-op when the rendered content
                   hasn't changed.
  8. Propose      — at most once per 24h (see PROPOSE MAPPINGS below), one
                   batched LLM call over signatures that have stayed `new`
                   with no `repo`/`verb` for longer than
                   `proposeMappingsAgeDays`, proposing `map`/`ignore`/`unsure`
                   per signature. Applied outcomes land ONLY in
                   config/triage-policy.json (never triage_items directly),
                   committed (never pushed) in this repo's own checkout.
  9. Once a day, one digest message with up to three sections: signatures
     that matched no policy rule, STATE_NOTE rows (see step 3), and whatever
     step 8 just auto-added this run.

PROPOSE MAPPINGS — the one LLM call in this file. `config/triage-policy.json`
was designed to grow only by a human reading the daily unmapped digest and
hand-editing the file — measurably not happening: a signature that fired, got
hand-fixed once, then reappeared four months later matched nothing, because
the fix was never turned into a rule. `propose_mappings()` closes that loop
as cheaply as this problem allows: at most once per 24h (a cursor in
`cursors`, the same table the digest already uses), batched into ONE request
against the Hermes brain over the same OpenAI-compatible endpoint
config.yaml already configures (`OPENAI_BASE_URL`/`OPENAI_API_KEY`, model
`gpt-5.6-luna`), secrets resolved the same way every other secret in this
file is — never a plaintext key. At most `PROPOSE_MAPPINGS_MAX_SIGNATURES`
candidates per run: every `new` item with no `repo`/`verb` whose event has
been open longer than `proposeMappingsAgeDays` (policy knob, default 7 — a
signature younger than that may still be a one-off, and mapping it wastes a
whole investigate episode). The model returns strict JSON, one of three
shapes per signature: `ignore` (append to the ignore list — safe and cheap to
get wrong), `map` (append a rule — a wrong mapping costs at most one wasted
read-only `investigate` episode, bounded and visible), or `unsure` (leave
unmapped, and record the attempt so it is not re-billed on every run for
`PROPOSE_UNSURE_COOLDOWN_DAYS`). A `map` proposal's repo is re-validated
against the SAME discovery hermes-cc.sh's own `resolve_repo()` uses (must
resolve under `root`, must not be `deny`d) — never trusted from the model's
own claim alone, see BOUNDS THAT DO NOT MOVE. Every applied entry is stamped
`proposedAt`/`proposedBy: "triage-auto"` plus the model's one-line reason,
written back into config/triage-policy.json (preserving `_readme` and key
order), then `git add` + `git commit` — never `git push` — that ONE file, in
this repo's own checkout. Skipped outright, loudly, if that path already
carries a pending change, rather than sweeping an unrelated edit into an
auto-authored commit. A failed, timed-out, or unparseable model call is
logged to stderr and otherwise a no-op — this loop must never depend on it
succeeding, exactly like every other externally-visible call in this file.

`scripts/dispatch-sweep.py` closes the other half: when a dispatch tied to a
triage cluster (dispatches.origin_event_id) reaches a terminal status, it
calls `fold_dispatch_verdict()` below to fold the verdict onto every member's
row and the shared card — without waiting for this script's own next
10-minute pass.

MATCH TARGETS. `_match_targets()` builds TWO strings per event:
`f"{source}:{external_id}"` and `f"{source}:{normalize_title(title)}"`
(imported from watchdog-poll.py). A policy rule is tried against both, first
match wins. This matters most for state sources (`uk`) whose external_id is
an opaque UptimeKuma monitor id ("204") — unglobbable and unstable across a
monitor recreate — so `uk:macmini-dev-host-push` (the title-derived target) is
what makes that source mappable at all. For grouped sources (`slack_alert`,
`hermes_log`) the two targets are usually identical (their external_id already
IS normalize_title(title) — see aggregate_slack_batch()/poll_hermes_logs() in
watchdog-poll.py), so the second target is a harmless no-op there.

CLUSTERING. Multiple signatures can share one root cause — the shipped
example: `research-gateway job.reaped` and `audio-gateway podcast.failed` were
both `threshold: 0` in the same commit, fixed by the same two-line diff in the
same repo. `escalate()` groups every eligible `new` item BY RESOLVED REPO and
opens at most ONE sideclaw dispatch per repo per run (capped at 5 signatures
per brief; the rest wait for the next run), rather than one dispatch per item.
Cluster membership is DERIVED, never stored as its own column: every CARDED
triage_items row sharing a non-NULL `dispatch_job` value IS one cluster — a
dedicated `cluster_id` column would just duplicate that fact under a different
name. `_dissolve_cluster()` moves a split cluster's members to state `split`
(which drops them out of every cluster grouping — see CARDED STATES below —
while keeping the verdict that produced the split readable on each row, see
STATE_SPLIT), but deliberately leaves `dispatch_job` itself set on those rows
purely as a cooldown anchor, not a live cluster pointer — see that function's
own docstring for why clearing it outright would let the escalate() call in the
very same run instantly re-fuse the pair it just split. The clustering is a
HYPOTHESIS from deterministic co-occurrence, never an assertion: the brief
tells the episode so explicitly and asks it to confirm or split it (see
DISSOLVE_MARKER below).

THE TWO EDGES THIS FIXES. `events.dispatch_id` (Phase 3 projection, added by
watchdog-poll.py, never written) and `dispatches.origin_event_id` (accepted by
hermes-cc.sh's `--origin-event`, never passed) were both always NULL before
this file existed. `--origin-event` takes exactly one events.id (one
dispatches row = one episode), so `escalate_cluster()` passes the cluster's
PRIMARY member there — hermes-cc.sh's own INSERT writes origin_event_id from
it, no second writer races `dispatches` for that column — and then does the
`UPDATE events SET dispatch_id=?` for EVERY member itself, which is the one
edge nothing else can write.

CARDED STATES. A card exists only once an item has left `new` — an item that
is mapped but hasn't yet crossed minOccurrences/minOpenMinutes, or is simply
unmapped, is carried silently (visible only in the once-a-day daily digest —
see maybe_post_daily_digest()). This is what keeps an empty or partial policy
from turning into dozens of cards of noise on day one. STATE_NOTE is
deliberately excluded too — see that state's own docstring.

VERB OUTCOMES. A policy rule can name `verb` instead of `repo` — routes to a
declared, code-side ALLOWLISTED local command instead of a sideclaw episode.
The rule names a KEY (e.g. `"env-check"`), never a command — a policy file
must never be able to name an arbitrary argv, the same closed-verb-set
principle hermes-cc.sh's own dispatch/status/list/merge/cancel verbs use.
Seeded with exactly one: `env-check` (hermes-ops.sh's ssh-based 1Password
ref-health probe), which op_refs_homelab/op_refs_vps route to. Cheaper than a
dispatch AND safer: a bare 1Password item name in the output doesn't match
sideclaw's `op://vault/item/field` secret-scan pattern the way a dispatched
verdict would, so the useful answer survives onto the card instead of being
withheld. Terminal state is `needs_human`; run_verbs() runs the command at
most once per item (see that function's own docstring for why no cooldown
tracking is needed) and the card's note IS the whole verdict — the dangling
item name plus the exact remediation, so no further investigation is needed.

DRY-RUN CONTRACT. `--dry-run` never touches Slack (no chat.postMessage/
chat.update), never shells out to hermes-cc.sh, and never shells out to `gh`
— those three are the only externally-visible actions this script can take.
(`gh` is the newest of them and the only READ-ONLY one: reconcile_operations()
uses `gh pr view` to ask GitHub whether a merge it lost the answer to
actually landed. It is still a shell-out to a remote system, so it is named
here rather than quietly exempted for being harmless.) Every other step
(ingest, reopen/unsnooze, classify, resolve, dissolve bookkeeping) is local
bookkeeping against triage_items alone, idempotent and side-effect-free, so
it runs for real even under --dry-run: that is what lets a dry run against a
throwaway copy of watchdog.db print a meaningful "what would be carded and
dispatched" preview instead of nothing at all.

Three steps are carve-outs that do NOT run under --dry-run, each for its own
stated reason: drain_intents() (the spool is not part of the database),
sweep_deadlines() (it can move an item TERMINALLY, and a preview must not be
able to end an item), and reconcile_operations() (both of its branches
shell out — see the paragraph above; a preview that cannot ask sideclaw or
GitHub what happened has nothing to reconcile with, and guessing is the one
thing that function exists not to do).

Source of truth: ~/SourceRoot/warden/scripts/triage.py
~/.hermes/scripts/ is a symlink to hermes-agent/scripts, NOT to this
directory — this code left that repo on 2026-09-09 and is reached by its
own path now.
"""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import fnmatch
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import subprocess
import sys
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, NamedTuple

HERMES_HOME = Path.home() / ".hermes"
# The ledger. `~/.warden/warden.db` since the extraction — the same file
# scripts/ledger.py resolves, and the same two env vars, so a `--db` override, a
# test fixture and the module default cannot disagree about which database this
# is. It moved out of ~/.hermes because the control plane cannot keep living
# inside the thing it supervises; ~/.hermes/watchdog.db is left in place,
# untouched, as the rollback.
WARDEN_HOME = (Path(os.environ["WARDEN_HOME"]).expanduser()
               if os.environ.get("WARDEN_HOME") else Path.home() / ".warden")
DB_PATH = (Path(os.environ["WARDEN_DB"]).expanduser()
           if os.environ.get("WARDEN_DB") else WARDEN_HOME / "warden.db")

_env_cc_bin = os.environ.get("HERMES_CC_BIN")
# hermes-cc.sh moved wholesale into this repo (2026-09-10) — resolved relative to
# this file rather than hardcoded, so a moved checkout is a working directory
# change, not a grep-and-replace.
HERMES_CC_BIN = Path(_env_cc_bin).expanduser() if _env_cc_bin else (Path(__file__).resolve().parent / "hermes-cc.sh")

# Same env-var-first, documented-absolute-default-second shape as HERMES_CC_BIN
# above, and for the same reason: reconcile_operations() shells out to `gh`
# under a LaunchAgent, and launchd hands a job a minimal PATH with no
# guarantee `gh` is on it — a bare `gh` would work fine in an interactive
# shell and fail silently under the agent, which is exactly the class of
# defect this project keeps finding (see docs/triage.md and STATE.md).
_env_gh_bin = os.environ.get("GH_BIN")
GH_BIN = Path(_env_gh_bin).expanduser() if _env_gh_bin else Path("/opt/homebrew/bin/gh")

# Same env var name hermes-cc.sh itself honors for this file (HERMES_CC_REPOS_JSON)
# — one override reaches both the real dispatch and this script's own pre-check.
# Moved into this repo's own config/ with hermes-cc.sh (2026-09-10), resolved
# relative to this file for the same reason HERMES_CC_BIN above is.
_env_repos_json = os.environ.get("HERMES_CC_REPOS_JSON")
DISPATCH_REPOS_JSON = (
    Path(_env_repos_json).expanduser() if _env_repos_json
    else (Path(__file__).resolve().parent.parent / "config" / "dispatch-repos.json")
)

# This repo's own config/, not ~/.hermes/config/, since the extraction. That is
# not cosmetic: propose_mappings() writes this file and then `git commit`s it
# inside TRIAGE_REPO_DIR, and while the file lived outside this checkout that
# whole path returned early — the signature map could not extend itself at all.
# hermes-cc.sh still reads the same file for the merge/deploy half of it, via its
# own HERMES_CC_TRIAGE_POLICY_JSON default pointing here. One file, two readers,
# as it always was.
# This repo's root: the `git -C` target for propose_mappings()'s policy
# auto-commit, and the anchor POLICY_PATH resolves against. Defined here,
# above its first use, rather than twice in one file.
TRIAGE_REPO_DIR = Path(__file__).resolve().parent.parent
_env_policy = os.environ.get("WARDEN_TRIAGE_POLICY") or os.environ.get("HERMES_TRIAGE_POLICY")
POLICY_PATH = (Path(_env_policy).expanduser() if _env_policy
               else TRIAGE_REPO_DIR / "config" / "triage-policy.json")

# Sources watchdog-poll.py already dedups that this loop acts on. github_*,
# hermes_cron, stray_skill are deliberately excluded — they are either
# already self-describing (a GitHub issue/PR IS the durable card) or
# governance-cadence, not the reactive-alert-channel firehose this exists to
# stop. op_refs_homelab/op_refs_vps ARE included: a dead 1Password ref blocks
# every future reseal of the mini's offline secrets cache (dotfiles' own
# CLAUDE.md §Secrets) — it must reach at least the unmapped digest, and
# routes to the `env-check` VERB (see VERB_ALLOWLIST below), never an
# episode. See docs/triage.md.
INGEST_SOURCES = ("slack_alert", "uk", "docker_homelab", "docker_vps", "hermes_log",
                   "op_refs_homelab", "op_refs_vps")

# The two INGEST_SOURCES that are grouped (upsert_grouped()-based, append-only)
# rather than state (reconcile()-based, disappearance-resolved) — mirrors
# watchdog-poll.py's own GROUPED_SOURCES, minus slack_update, which this loop
# never ingests. See resolve_quiet_grouped()/resolve_recovery_paired().
GROUPED_TRIAGE_SOURCES = ("slack_alert", "hermes_log")

STATE_NEW = "new"
STATE_INVESTIGATING = "investigating"
STATE_VERDICT = "verdict"
# A cluster member whose SHARED investigation returned DISSOLVE_MARKER
# ("UNRELATED SIGNATURES" — see _dissolve_cluster()): it HAS been evaluated,
# it CARRIES that verdict, and it is waiting to be re-evaluated on its own.
# Reached only from `verdict`, via maybe_dissolve_clusters(), which is why it
# sits here rather than next to STATE_NEW.
#
# Deliberately NOT `new`. `new` is the one state a silence path may discharge
# (_SILENCE_RESOLVE_ELIGIBLE_STATES is an inclusion list of exactly that one
# state) precisely because a `new` row carries no obligation yet — and a
# dissolved member does. Measured live, 2026-09-09 21:09-21:39Z (STATE.md
# §43): two members carrying a real, correct verdict about an active
# hermes-agent watchdog race were dissolved to `new`, missed re-escalation
# inside cooldownHours (correctly — see _dissolve_cluster()'s docstring for
# why `dispatch_job` stays the cooldown anchor), and were silently
# quiet-resolved by apply_resolutions() before anyone ever saw the verdict.
# The verdict survived only in dispatches.verdict_json, which nothing reads.
# That is the defect this state exists to close, and it closes it by putting
# the row somewhere silence structurally cannot reach.
#
# Deliberately NOT in CARDED_STATES — see that tuple's own comment for why: a
# dissolved member still shares its (deliberately-retained) `dispatch_job`
# with its former cluster-mates, and _cluster_groups() groups CARDED rows by
# that column, so carding `split` would re-render, as one cluster card, the
# very cluster this state exists to take apart.
#
# escalate() is what advances it — as a SINGLETON, never grouped, see that
# function's own comment — and STATE_DEADLINES is what bounds it.
STATE_SPLIT = "split"
STATE_NEEDS_HUMAN = "needs_human"
STATE_PR_OPEN = "pr_open"
# Wave 2's `resolved` split — three honest terminal outcomes instead of one
# number that improves the more questions go unanswered (DESIGN.md § the
# state machine). Each is produced by exactly the paths its own comment names;
# see the module docstring's producer table for the full mapping.
#
# A change landed AND a positive signal confirmed it. The only genuinely
# verified-fix state in the file — produced solely by maybe_check_liveness()'s
# positive branch (a live probe, never silence). See STATE_QUIET for why the
# two silence-adjacent paths (quiet timer, recovery pairing) do NOT produce
# this even when a recovery message looks like good news.
STATE_FIXED = "fixed"
# The signal stopped and NOTHING SHIPPED — never claims a fix. Produced by all
# three silence/observation paths that predate the split: apply_resolutions()
# (disappearance from observation), resolve_quiet_grouped() (a quiet timer),
# and resolve_recovery_paired() (an explicit ✅ recovery message is still only
# an OBSERVATION that the alert cleared — DESIGN.md § What must not be lost,
# item 4: "neither [grouped resolve path] ever claims a fix"). A service that
# is fully down also stops emitting, so silence alone — however it arrives —
# can never be proof of a fix.
STATE_QUIET = "quiet"
# Done WITHOUT a verified positive signal: a human said so (cmd_close), or it
# landed and there was nothing left to verify (a `merged` item whose repo has
# no deploy target — sweep_deadlines() expires it here after 1h, per
# DESIGN.md's own deadline table).
STATE_CLOSED = "closed"
STATE_SNOOZED = "snoozed"
STATE_IGNORED = "ignored"
# --- the auto-implement chain (verdict -> implement -> validate -> merge ->
# deploy -> verify), all downstream of a STATE_VERDICT item whose folded
# investigate verdict already said nextAction=implement at confidence=high.
# See maybe_auto_implement()/poll_implement_jobs()/poll_validation_jobs()/
# maybe_check_liveness() and their own docstrings for the state machine.
STATE_IMPLEMENTING = "implementing"      # implement episode dispatched, awaiting a PR
STATE_VALIDATING = "validating"          # a SECOND, different-model episode reviewing that PR's diff
STATE_MERGE_BLOCKED = "merge_blocked"    # implement failed, validation disagreed/errored, or merge itself refused
STATE_MERGED = "merged"                  # landed; no deploy configured/enabled for this repo
STATE_LIVENESS_PENDING = "liveness_pending"  # deployed; waiting on a positive liveness signal
# Terminal, like `ignored` — never escalates, never gets its own card — but
# UNLIKE `ignored`, it is NOT a recovery/known-benign match: it's a
# `slack_alert` row that doesn't look like a structured bot alert
# (ignoreUnstructuredSlackProse — see classify()) and therefore MIGHT be a
# human diagnosis that never got actioned (the shipped example: a Slack
# message naming the exact 1Password rate-limit root cause and its two-line
# fix, sitting unactioned). Silently dropping that into `ignored` would
# recreate the exact failure this whole redesign exists to kill. A `note`
# row surfaces once, under its own heading, in the daily digest — see
# maybe_post_daily_digest() — then is never touched again.
STATE_NOTE = "note"
# Terminal, and the only state a DEADLINE is allowed to expire an item into
# (see STATE_DEADLINES below). Always carries its reason in `note` —
# _set_state() raises rather than let a reasonless dismissal exist, because
# the reason is the whole content of the state: "nobody answered in 7 days"
# and "the merge stayed blocked for 7 days" are different facts and the row
# is the only place either survives.
#
# Deliberately neither of the two terminal states it superficially resembles.
# NOT `resolved`: nothing was fixed, and `resolved` is the number DESIGN.md
# optimizes for — a metric that improves the more questions go unanswered is
# precisely the failure this loop exists to remove. NOT `ignored`: that is a
# human calling a signature benign, and an expiry is the absence of a human
# rather than a judgement by one. Conflating either would corrupt the only
# two numbers this system reports.
STATE_DISMISSED = "dismissed"

# A card exists only for a row that has actually left `new` — see the module
# docstring's CARDED STATES paragraph. `snoozed` and `note` are deliberately
# excluded too: a human just silenced a snoozed row, and a `note` row is
# visible via the digest, not a card (see STATE_NOTE above).
#
# THE INVARIANT (2026-09-08 correction — a 13-card burst reopened this exact
# failure mode through the resolve path): a card is a conversation with the
# human about an item they were told about; a state change on an item they
# were never told about is not news. `STATE_QUIET`/`STATE_FIXED` sit in this
# tuple because a CARDED item's close is real news (the human saw the problem,
# now sees it close) — but both are also reachable straight from `new`
# (apply_resolutions()/resolve_quiet_grouped()/resolve_recovery_paired() all
# flip `new` straight to `quiet` on a silence/observation signal that was
# never escalated, and `new` is the ONLY state they may flip — see
# _SILENCE_RESOLVE_ELIGIBLE_STATES), and an item whose whole life was
# `new -> quiet` was never told about in the first place. Being in
# CARDED_STATES only makes a row ELIGIBLE for a card — sync_card() below is
# where "already had one" is actually enforced (via `card_ts`), and that is the
# one place this rule is checked, rather than every caller having to
# re-derive it. `STATE_CLOSED` is here for the same reason: cmd_close()
# addresses an item a human is looking at (already carded), and the `merged`
# deadline's expiry to `closed` updates a card that already exists.
# STATE_DISMISSED is in this tuple for the same reason: an item that HAD a
# card and then ran out of time is real news to the human who was asked and
# did not answer — silently dropping it is what a deadline must never look
# like. Every state that can expire to `dismissed` (needs_human,
# merge_blocked, pr_open) is itself carded, so in practice this only ever
# updates a card that already exists.
# STATE_SPLIT is deliberately ABSENT, and it looks like an omission rather
# than a decision, so: `_cluster_groups()` groups CARDED rows BY
# `dispatch_job`, and a dissolved member still shares its former cluster's
# `dispatch_job` (kept on purpose, as the cooldown anchor — see
# _dissolve_cluster()). Carding `split` would therefore re-render, as one
# cluster card, the very cluster the dissolve just took apart — the row
# would look carded-and-grouped exactly like the `investigating` cluster it
# used to be. Its own dissolve-notice update (posted by _dissolve_cluster()
# itself, directly) IS its card history; nothing further renders for it
# until it reaches `needs_human` on expiry (see STATE_DEADLINES), which IS
# carded.
CARDED_STATES = (STATE_INVESTIGATING, STATE_VERDICT, STATE_NEEDS_HUMAN, STATE_PR_OPEN,
                  STATE_FIXED, STATE_QUIET, STATE_CLOSED,
                  STATE_IMPLEMENTING, STATE_VALIDATING, STATE_MERGE_BLOCKED, STATE_MERGED,
                  STATE_LIVENESS_PENDING, STATE_DISMISSED)

# `triage_items.note` prefixes for the grouped-source resolve paths (see
# resolve_quiet_grouped()/resolve_recovery_paired()) plus the liveness path
# (see maybe_check_liveness()) — render_card_blocks() only surfaces `note` on
# a STATE_QUIET/STATE_FIXED card when it starts with one of these,
# specifically so an ordinary event-driven resolve (apply_resolutions, which
# now clears `note` outright) never accidentally inherits stale text from an
# earlier phase. Deliberately NOT "fixed"/"resolved" wording for the first
# two — a service that is fully down also stops emitting, so silence alone is
# never proof of a fix; see both functions' own docstrings, and STATE_QUIET's.
# LIVENESS_CONFIRMED_NOTE_PREFIX is the one genuine "this is actually fixed"
# claim in the file, because it is backed by a POSITIVE probe
# (maybe_check_liveness()'s own gatherer), not silence — it is the only prefix
# of the three that ever lands on a STATE_FIXED row rather than STATE_QUIET.
QUIET_RESOLVE_NOTE_PREFIX = "signal quiet since "
RECOVERY_PAIRED_NOTE_PREFIX = "recovery message observed: "
LIVENESS_CONFIRMED_NOTE_PREFIX = "liveness confirmed: "
# _dissolve_cluster()'s own note prefix — the dissolve verdict's text
# (summary + verdict + recommendation, the same text_blob DISSOLVE_MARKER is
# matched against), so a `split` row's obligation is readable on its own row,
# not only inside dispatches.verdict_json where STATE.md §43 found nobody
# ever reads it. Safe to write here where a note was not safe on `new`
# (apply_resolutions() clears note=NULL, but a `split` row is never a
# candidate for that function — see _SILENCE_RESOLVE_ELIGIBLE_STATES): this
# is the ledger RECORDING the obligation (DESIGN.md principle 1), not
# forgetting it. sweep_deadlines() preserves it, rather than overwriting it,
# on the one expiry path that can reach a `split` row — see that function.
SPLIT_VERDICT_NOTE_PREFIX = "cluster split — the investigation's verdict, pending individual re-evaluation: "

STATE_EMOJI = {
    STATE_NEW: ":large_blue_circle:",
    STATE_INVESTIGATING: ":mag:",
    STATE_VERDICT: ":memo:",
    STATE_NEEDS_HUMAN: ":raising_hand:",
    STATE_PR_OPEN: ":twisted_rightwards_arrows:",
    # `fixed` inherits the old `resolved` emoji — it is the only one of the
    # three that earns it (a verified positive signal, not silence).
    STATE_FIXED: ":white_check_mark:",
    STATE_QUIET: ":mute:",
    STATE_CLOSED: ":ballot_box_with_check:",
    STATE_SNOOZED: ":zzz:",
    STATE_IMPLEMENTING: ":hammer_and_wrench:",
    STATE_VALIDATING: ":test_tube:",
    STATE_MERGE_BLOCKED: ":no_entry:",
    STATE_MERGED: ":rocket:",
    STATE_LIVENESS_PENDING: ":hourglass_flowing_sand:",
    STATE_DISMISSED: ":wastebasket:",
    # Unreachable on a rendered card today — `split` is deliberately absent
    # from CARDED_STATES (see that state's own comment) — but STATE_EMOJI is
    # a total map over the vocabulary, and a gap here would silently render
    # `:question:` the day this state is ever carded.
    STATE_SPLIT: ":scissors:",
}

# Terminal means: no poller, no deadline, no exit. Named once, as a constant,
# so STATE_DEADLINES below can be checked against it by a test rather than by
# a reader — DESIGN.md principle 6 is "checked against the diagram, not
# assumed", and a hand-maintained second list of terminal states is exactly
# how the two drift apart.
TERMINAL_STATES = (STATE_FIXED, STATE_QUIET, STATE_CLOSED, STATE_IGNORED, STATE_NOTE, STATE_DISMISSED)


class _DeadlineRule(NamedTuple):
    """What advances a state, how long a row may sit in it, and where it goes
    when nothing advanced it in time.

    `poller` names the function or the human — principle 6 asks every
    non-terminal state to name the thing that polls it, and a name in a table
    the code reads is a claim a test can check, where a name in a comment is
    not. `hours` is None for a state bounded by something other than a clock.
    `deadline_column` names WHICH column carries the moment, because two
    states own their own and sweep_deadlines() must not touch a window it
    does not own."""

    poller: str
    hours: float | None
    on_expiry: str | None
    reason: str | None
    deadline_column: str | None


# The generic column, owned by _set_state() (writes it) and sweep_deadlines()
# (acts on it). `liveness_pending` and `snoozed` name their own instead.
# Underscore-private, and that is load-bearing: it is not a state, and
# test_every_non_terminal_state_names_a_poller_and_a_deadline enumerates every
# public STATE_* string in this module to find the states it must check.
_STATE_DEADLINE_COLUMN = "state_deadline"

# THE table. Every non-terminal state appears here exactly once; a state that
# does not is a state an item can sit in forever, which is the failure
# DESIGN.md § Deadlines was written about
# (test_every_non_terminal_state_names_a_poller_and_a_deadline enforces it).
# The numbers are DESIGN.md's own table, not re-derived here.
#
# Two entries deviate from that table, deliberately, because a reader will
# check:
#
#   * `verdict` is not in DESIGN.md's table at all, and it is non-terminal.
#     maybe_auto_implement() only advances it when the repo has auto-implement
#     enabled AND the verdict reads nextAction=implement at confidence=high;
#     every other verdict sits in `verdict` with nothing scheduled to touch it
#     again. 24h -> needs_human. An ADDITION to the design's table, not a
#     contradiction of it.
#   * `needs_human`'s "reminder at 1d" is NOT built here. A reminder is a
#     notification feature, not a deadline — it changes nothing about when the
#     row may stop existing. Scoped out on purpose, not missed.
#   * `split` is the same third addition as `verdict` above, for the same
#     reason: post-verdict, nothing scheduled to touch it unless a specific
#     condition is met (here: escalate() finding it a free per-repo slot —
#     see that function). Poller is named as "escalate" — the real function
#     that advances it, per principle 6 — not a comment. 24h, matching
#     `verdict`'s own rule: `split` is the same KIND of state, and the
#     deadline has to clear DEFAULT_COOLDOWN_HOURS=6 by a wide margin so
#     escalate() gets several real chances (several 10-minute passes across
#     multiple cooldown windows) before the clock takes over — this is the
#     backstop for "the repo is denied/unmapped, or budgets stayed
#     saturated", not the normal path. Expires to `needs_human`, not
#     `dismissed`: the row holds an unactioned verdict, so the honest expiry
#     is to put it in front of a human — which also makes it visible again,
#     since `needs_human` IS carded (see CARDED_STATES).
STATE_DEADLINES: dict[str, _DeadlineRule] = {
    # Bounded by silence, not by a clock: the three silence paths resolve a
    # `new` row after `quietResolveHours` (see _SILENCE_RESOLVE_ELIGIBLE_STATES,
    # which is an inclusion list of exactly this one state). A clock here would
    # be a second, competing bound on the one state that already has one.
    STATE_NEW: _DeadlineRule("resolve_quiet_grouped / apply_resolutions", None, None, None, None),
    STATE_INVESTIGATING: _DeadlineRule("dispatch-sweep.py", 2, STATE_NEEDS_HUMAN, None,
                                        _STATE_DEADLINE_COLUMN),
    STATE_VERDICT: _DeadlineRule("maybe_auto_implement", 24, STATE_NEEDS_HUMAN, None,
                                  _STATE_DEADLINE_COLUMN),
    STATE_SPLIT: _DeadlineRule("escalate", 24, STATE_NEEDS_HUMAN, "unre-evaluated", _STATE_DEADLINE_COLUMN),
    STATE_IMPLEMENTING: _DeadlineRule("poll_implement_jobs", 2, STATE_MERGE_BLOCKED, None,
                                       _STATE_DEADLINE_COLUMN),
    STATE_VALIDATING: _DeadlineRule("poll_validation_jobs", 1, STATE_MERGE_BLOCKED, None,
                                     _STATE_DEADLINE_COLUMN),
    STATE_MERGE_BLOCKED: _DeadlineRule("operator", 168, STATE_DISMISSED, "unresolved",
                                        _STATE_DEADLINE_COLUMN),
    # Nothing polls a `merged` row — the deploy path already declined it (see
    # poll_validation_jobs()), so the clock IS its only exit, and this sweeper
    # is therefore honestly named as the poller rather than left blank. Expires
    # to `closed`, per DESIGN.md's own deadline table and FLOWS.md flow 2:
    # landed, no deploy target, closed after 1h with no deploy — done without a
    # verified positive signal, which is exactly STATE_CLOSED's definition.
    STATE_MERGED: _DeadlineRule("sweep_deadlines", 1, STATE_CLOSED, None, _STATE_DEADLINE_COLUMN),
    STATE_NEEDS_HUMAN: _DeadlineRule("operator", 168, STATE_DISMISSED, "expired",
                                      _STATE_DEADLINE_COLUMN),
    STATE_PR_OPEN: _DeadlineRule("operator", 336, STATE_DISMISSED, "expired", _STATE_DEADLINE_COLUMN),
    # Owns `liveness_deadline` (schema version 1, the deploy path's own probe
    # window) — maybe_check_liveness() both sets and acts on it, including the
    # reopen-to-`new` that a generic expiry could never express. The generic
    # sweeper skips it on this column name alone.
    STATE_LIVENESS_PENDING: _DeadlineRule("maybe_check_liveness", None, None, None, "liveness_deadline"),
    # Owns `snoozed_until`: a human chose that moment, and unsnooze_if_expired()
    # is the poller that honours it.
    STATE_SNOOZED: _DeadlineRule("unsnooze_if_expired", None, None, None, "snoozed_until"),
}

# Written into `note` by sweep_deadlines() and by nothing else. A human
# reading a card must be able to tell "the clock ran out" from "something
# decided this" without knowing the state machine, so the note says so in
# words, first.
DEADLINE_EXPIRED_NOTE_PREFIX = "deadline expired: "

DEFAULT_CARD_CHANNEL = "C0BVDE5R562"  # #agents — see config/triage-policy.json
DEFAULT_MIN_OCCURRENCES = 3
DEFAULT_MIN_OPEN_MINUTES = 30
DEFAULT_COOLDOWN_HOURS = 6
# Grouped sources (slack_alert, hermes_log) never disappearance-resolve on
# their own — watchdog-poll.py's sweep_stale_grouped() only clears them
# after 7 idle DAYS (GROUPED_TTL_DAYS), which is deliberately housekeeping,
# not signal (its own docstring: silent specifically so a months-old row
# doesn't trigger a notification burst). 2h is the triage-side fix's
# default: comfortably past the 30-min watchdog-poll cadence (4+ consecutive
# misses before a false resolve) and short of either grouped source's own
# reminder window (6h/24h in REM_HOURS), so a signature that is GENUINELY
# still flapping gets re-noticed well before this would ever fire — while a
# fixed-and-deployed alert still closes the same day instead of sitting
# open for a week. See resolve_quiet_grouped().
DEFAULT_QUIET_RESOLVE_HOURS = 2.0

# Concurrency ceiling: simultaneously-open CLUSTERS (distinct dispatch_job
# values in state=investigating) this loop is allowed to have outstanding at
# once. Independent of the daily budget below — this bounds how many run AT
# THE SAME TIME, which matters because sideclaw itself has its own
# concurrency limits shared with every other dispatch source.
MAX_OPEN_INVESTIGATIONS = int(os.environ.get("TRIAGE_MAX_OPEN_INVESTIGATIONS", "3"))
# Dispatches opened per rolling UTC day BY THIS LOOP ONLY (counted via
# dispatches.origin_event_id IS NOT NULL, the same marker escalate_cluster()
# writes). Deliberately well under hermes-cc.sh's own 20/day so a triage storm
# can never starve interactive dispatch of its own budget. Counts CLUSTERS
# (one dispatch row), not member items.
DAILY_INVESTIGATE_BUDGET = int(os.environ.get("TRIAGE_DAILY_INVESTIGATE_BUDGET", "8"))
# A bare GET/POST against sideclaw (via hermes-cc.sh, which itself bounds its
# own HTTP calls) or Slack should never hang a 10-minute cron indefinitely.
SUBPROCESS_TIMEOUT = int(os.environ.get("TRIAGE_SUBPROCESS_TIMEOUT", "60"))
# How many signatures ride in one cluster's brief. The rest stay in `new` and
# wait for a later run — never dropped, never silently merged in anyway.
MAX_CLUSTER_SIGNATURES = 5

# hermes-cc.sh refuses a brief over this anyway (MAX_BRIEF_CHARS in that
# script) — capped here too, in Python, before the brief ever reaches a shell
# invocation, per CLAUDE.md's shell-conventions trap: this script pipes the
# brief to hermes-cc.sh on stdin rather than argv, so there is no `head -c`
# SIGPIPE hazard here, but the cap still belongs at the point the text is
# assembled, not left to the downstream script to enforce alone.
MAX_BRIEF_CHARS = 8000

# A cluster is a hypothesis, not an assertion (see module docstring). The
# brief asks the episode to say so, in these exact words, if it determines
# the grouped signatures do NOT share a root cause — a plain, case-sensitive
# substring check on the folded verdict's own text, never an LLM call here.
DISSOLVE_MARKER = "UNRELATED SIGNATURES"

SLACK_POST_URL = "https://slack.com/api/chat.postMessage"
SLACK_UPDATE_URL = "https://slack.com/api/chat.update"
SECTION_TEXT_MAX = 3000  # Block Kit section text hard limit

DAILY_DIGEST_CURSOR_KEY = "triage_unmapped_digest_date"

# A policy rule names a KEY, never a command — a policy file must never be
# able to name an arbitrary argv. This is the closed allowlist code maps a
# `"verb"` rule outcome to; same closed-verb-set principle as hermes-cc.sh's
# own dispatch/status/list/merge/cancel verbs, applied to a single-purpose
# LOCAL health probe instead of a sideclaw episode — cheaper than a dispatch
# and safer: `env-check`'s bare 1Password item names don't match sideclaw's
# `op://vault/item/field` secret pattern the way a dispatched verdict would,
# so the useful answer survives onto the card instead of being withheld.
# hermes-ops.sh (62 KB) deliberately stayed behind in hermes-agent — this is
# a live cross-repo argv, not an oversight. Same env-override shape as
# HERMES_CC_BIN above: env var first, documented default second.
_env_ops_bin = os.environ.get("WARDEN_HERMES_OPS_BIN")
_HERMES_OPS_BIN = Path(_env_ops_bin).expanduser() if _env_ops_bin else (HERMES_HOME / "scripts" / "hermes-ops.sh")
VERB_ALLOWLIST: dict[str, list[str]] = {
    "env-check": [str(_HERMES_OPS_BIN), "env-check", "--json"],
}
# env-check runs TWO sequential ssh_run() probes (homelab, then vps), each
# individually bounded by hermes-ops.sh's own SSH_TIMEOUT=120 — so the outer
# bound here has to clear 240s, not just one call's worth, or this would kill
# a legitimately slow-but-healthy probe before hermes-ops.sh's own timeout
# ever got a chance to fire.
VERB_TIMEOUT = 260

# --- evidence commands — declared runtime-state probes, fenced into the brief -
#
# The episode this loop dispatches runs in a read-only sideclaw WORKTREE — it
# can see the repo, never the machine. All three real investigations this
# loop has run so far came back `nextAction: human` citing exactly that: no
# runtime state (var/health.json, watchdog-alerts.log, live OTel) was
# reachable from inside the checkout. A policy rule can now declare an
# `evidence` list — but, same closed-set principle as VERB_ALLOWLIST just
# above, ONLY a key from EVIDENCE_ALLOWLIST, never an arbitrary command: a
# policy file must never be able to name an arbitrary argv (or, here, an
# arbitrary probe). Unlike VERB_ALLOWLIST these four run IN-PROCESS rather
# than via subprocess.run on a fixed argv: three are bounded local file
# reads, and the fourth (kuma-push-last) needs both a secret
# (HOMELAB_API_KEY, which must never cross an argv/`ps` boundary) and
# per-cluster context (which UptimeKuma monitor actually fired) that a fixed
# argv has no way to carry. The closed-key-set contract itself — a policy
# file can only ever select one of these four, never invent a fifth — is
# still enforced the same way, at load_policy() time (see _valid_rule()).
EVIDENCE_ALLOWLIST: tuple[str, ...] = ("meteo-health", "gateway-starts", "hermes-log-tail", "kuma-push-last")

METEO_HEALTH_PATH = Path.home() / "SourceRoot" / "meteo" / "var" / "health.json"
GATEWAY_STARTS_LOG = HERMES_HOME / "gateway-starts.log"
HERMES_ERROR_LOG = HERMES_HOME / "logs" / "errors.log"
ALERTS_CHANNEL = "C0AS1LAUQ3C"  # #alerts — same channel watchdog-poll.py's slack_alert source reads

# Every evidence probe is read-only and bounded: a hard wall-clock timeout
# (a hung file read on a stale network mount, or a slow argo API call, must
# never stall a 10-minute cron) and a hard output cap. EVIDENCE_TOTAL_CAP_CHARS
# is well under MAX_BRIEF_CHARS so a 5-signature cluster (each pulling its own
# evidence) still leaves room for the rest of the brief — _build_evidence_block()
# enforces the remaining-budget cut described in the module docstring, never
# eating into the brief's own structure.
EVIDENCE_TIMEOUT = int(os.environ.get("TRIAGE_EVIDENCE_TIMEOUT", "20"))
EVIDENCE_CAP_CHARS = 1200
EVIDENCE_TOTAL_CAP_CHARS = 3200

# --- the auto-implement chain (steps 6-10: verdict -> implement -> validate ->
# merge -> deploy -> verify) ---------------------------------------------------
#
# STEP 7's whole point is a genuinely DIFFERENT model reviewing the implement
# episode's own diff — probed live against sideclaw before being hardcoded
# here: a throwaway `investigate` job at `model: "gpt-5.6-terra"` failed
# outright inside the Claude Code session with
# `[claude-code:unrecognized_model]` (session exit 1, not the usual harmless
# stderr telemetry line CLAUDE.md documents for that string elsewhere — this
# one actually crashed the session). `claude-opus-5[1m]` — still a different
# model from the claude-sonnet-5 default that writes the implement episode —
# ran a real Claude Code session and returned a structured verdict. Re-probe
# if this ever needs to change; do not guess a model id.
VALIDATION_MODEL = os.environ.get("TRIAGE_VALIDATION_MODEL", "claude-opus-5[1m]")

# Deterministic, not an LLM judgement by THIS file (see the module docstring's
# "no LLM call anywhere in this file" — the episode itself runs one, which is
# inherent to what "investigate" means). The validation brief instructs the
# episode to end with exactly one of these two phrases; poll_validation_jobs()
# does a plain substring check, the same technique DISSOLVE_MARKER already
# uses for the clustering hypothesis.
VALIDATION_CONFIRM_MARKER = "VALIDATION: CONFIRMED"
VALIDATION_DISAGREE_MARKER = "VALIDATION: DISAGREE"

# How long a deployed-but-unverified item waits for a positive liveness signal
# before this loop gives up and REOPENS it (see maybe_check_liveness()) rather
# than letting it sit "deployed" forever on a probe that never resolves either
# way. Comfortably longer than one 10-minute cron cycle so a slow-to-propagate
# change (HyperDX's own config apply, an alert re-evaluation window) isn't
# mistaken for a failure.
LIVENESS_WINDOW_HOURS = float(os.environ.get("TRIAGE_LIVENESS_WINDOW_HOURS", "2"))

HYPERDX_BASE = os.environ.get("TRIAGE_HYPERDX_BASE", "https://hyperdx.jkrumm.com")


def _gather_hyperdx_alert_state(expected: list[dict[str, Any]]) -> tuple[bool, str]:
    """Re-reads every expected alert's LIVE `threshold`/`thresholdType` from
    HyperDX's own `GET /api/api/v2/alerts` — the SAME REST endpoint
    vps/scripts/hyperdx-sync.sh's own `export`/`apply` already use (verified
    by reading that script, never a fabricated endpoint) — and asserts it
    matches what the merged diff set (captured at deploy time by
    hermes-cc.sh's `collect_expected_alerts()`, carried on
    triage_items.deploy_expect_json). Returns (ok, detail); `ok` is a genuine
    POSITIVE confirmation, never inferred from silence — see the module this
    is called from for why that distinction matters here specifically."""
    if not expected:
        return False, "no expected alert definitions were captured at deploy time"
    wp = _wp_module()
    if wp is None:
        return False, "watchdog-poll.py sibling module did not load — cannot reach HyperDX"
    token = wp.resolve_secret("HYPERDX_AGENT_ACCESS_KEY")
    if not token:
        return False, "HYPERDX_AGENT_ACCESS_KEY unresolved"
    req = urllib.request.Request(
        f"{HYPERDX_BASE}/api/api/v2/alerts", headers={"Authorization": f"Bearer {token}"}
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, TimeoutError, OSError) as e:
        return False, f"HyperDX alerts fetch failed: {e}"
    live_by_name = {a.get("name"): a for a in (data.get("data") or []) if isinstance(a, dict)}
    mismatches = []
    for exp in expected:
        name = exp.get("name")
        live = live_by_name.get(name)
        if live is None:
            mismatches.append(f"{name}: not found live")
            continue
        if live.get("threshold") != exp.get("threshold") or live.get("thresholdType") != exp.get("thresholdType"):
            mismatches.append(f"{name}: live threshold={live.get('threshold')}/{live.get('thresholdType')} "
                              f"!= expected {exp.get('threshold')}/{exp.get('thresholdType')}")
    if mismatches:
        return False, "; ".join(mismatches)
    return True, f"{len(expected)} alert definition(s) verified live"


ARGO_HEALTH_URL = os.environ.get("TRIAGE_ARGO_HEALTH_URL", "https://argo.jkrumm.com/api/health")


def _gather_argo_commit_live(expected: list[dict[str, Any]]) -> tuple[bool, str]:
    """The deployOnMerge liveness probe for `argo` (item 1b, STATE.md §47/§48).
    Re-reads argo's own `GET /api/health` — tailnet-reachable, no auth, no
    secret, verified directly against a real merge (jkrumm/argo#16, 56s
    merge-to-served) — and asserts its `commit` field matches the merge
    commit sha captured at merge time (poll_validation_jobs()'s deployOnMerge
    branch, carried on triage_items.deploy_expect_json as the SAME
    list-of-dicts shape `_gather_hyperdx_alert_state()` above already uses —
    `[{"commit": sha}]` — see maybe_check_liveness()'s own docstring for why
    that shape is fixed, not reinvented per gatherer).

    `ok` is a genuine exact-sha-match POSITIVE confirmation, never inferred
    from the service merely being reachable or having recently restarted. A
    restart-time-only probe cannot distinguish a landed deploy from a
    container that bounced for an unrelated reason — STATE.md §47's own
    research-gateway/meteo reconnaissance hit exactly this ambiguity, which
    is why it stopped short of using `lastRestartAt` here. `fixed` is the
    one state in this file that claims a change actually worked
    (LIVENESS_CONFIRMED_NOTE_PREFIX is "the one genuine 'this is actually
    fixed' claim in the file" — see its own comment), and REVIEW.md C3 is
    about exactly how that claim gets gamed by a weaker probe. `"unknown"`
    (argo's own placeholder for a build outside CI), a missing `commit`
    field, a differing sha, a non-200, and unparseable JSON all fall through
    to the same `!= want` comparison below and read as a genuine mismatch,
    never as "not sure".

    THE URL LIVES HERE, NOT IN THE POLICY FILE. DESIGN.md principle 4 (the
    four closed allowlists — see this repo's own CLAUDE.md): a policy file
    may name and parameterise a behaviour, never express one.
    LIVENESS_ALLOWLIST names a KEY ("argo-commit-live"); this function owns
    argo's endpoint the same way _gather_hyperdx_alert_state() above owns
    HYPERDX_BASE — one gatherer per repo is the correct shape here, not a
    generic URL-fetcher driven by config. This will read as duplication next
    to that gatherer; it is not — a config-driven URL would let a policy
    edit alone decide what this loop asks an arbitrary host to confirm,
    which is exactly what the closed-allowlist principle exists to prevent."""
    if not expected:
        return False, "no expected commit was captured at merge time"
    want = expected[0].get("commit")
    if not want:
        return False, "expected deploy record carried no commit sha"
    req = urllib.request.Request(ARGO_HEALTH_URL)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, TimeoutError, OSError) as e:
        return False, f"argo health fetch failed: {e}"
    live = data.get("commit") if isinstance(data, dict) else None
    if not isinstance(live, str) or live != want:
        return False, f"live commit {str(live)[:12]!r} != expected {str(want)[:12]!r}"
    return True, f"commit {want[:12]} live"


# Same closed-set principle as VERB_ALLOWLIST/EVIDENCE_ALLOWLIST above: a
# repo's `config/triage-policy.json` entry names a `liveness` KEY, never a
# probe. Seeded with the original `deploy` key plus `argo-commit-live` (item
# 1b) for the merge-is-deploy path.
LIVENESS_ALLOWLIST = {
    "hyperdx-alert-state": _gather_hyperdx_alert_state,
    "argo-commit-live": _gather_argo_commit_live,
}

# --- propose_mappings() — the one LLM call in this file (see module docstring
# PROPOSE MAPPINGS) --------------------------------------------------------

# Same table/pattern DAILY_DIGEST_CURSOR_KEY already uses, but this one stores
# a full timestamp (not a bare date) so the gate is a genuine rolling 24h,
# checked before the model is ever called — a run that fires at 00:05 today
# must not fire again at 00:05 tomorrow just because the calendar date rolled.
PROPOSE_MAPPINGS_CURSOR_KEY = "triage_propose_mappings_last_run"

# How many candidates ride in ONE batched request. The rest simply wait for a
# later day's run — never dropped, never silently expanded into a second call
# (this loop makes at most one model call per run, full stop).
# A signature this many occurrences deep has proven it is not a one-off,
# whatever its age — see _propose_mapping_candidates() for why age alone
# is insufficient.
PROPOSE_MAPPINGS_MIN_OCCURRENCES = int(os.environ.get("TRIAGE_PROPOSE_MIN_OCCURRENCES", "5"))
PROPOSE_MAPPINGS_MAX_SIGNATURES = 25

# A batch of at most 25 short JSON decisions (one action + one one-line reason
# each) comfortably fits well under this; the cap exists so a misbehaving
# endpoint can't turn one daily maintenance call into an open-ended generation.
PROPOSE_MAPPINGS_MAX_OUTPUT_TOKENS = 2000

# The call itself, separate from SUBPROCESS_TIMEOUT (which bounds a hermes-cc.sh
# subprocess, not an HTTP request this file makes directly).
PROPOSE_MAPPINGS_TIMEOUT = int(os.environ.get("TRIAGE_PROPOSE_TIMEOUT", "90"))

# A signature younger than this may still be a one-off (a transient blip that
# resolves on its own before anyone would ever hand-map it) — mapping it this
# early wastes a whole investigate episode on something that might never
# recur. Policy knob: config/triage-policy.json's `proposeMappingsAgeDays`.
DEFAULT_PROPOSE_MAPPINGS_AGE_DAYS = 7.0

# How long an `unsure` verdict suppresses re-proposing THE SAME signature —
# long enough that a genuinely ambiguous signature is not re-billed into the
# model on every single day's run, short enough that it is reconsidered
# occasionally rather than permanently stuck.
PROPOSE_UNSURE_COOLDOWN_DAYS = 7.0

# Cheapest reasonable use of a model this file makes: chat_completions, no
# tools, temperature 0, over the SAME OpenAI-compatible endpoint config.yaml
# already points gpt-5.6-luna at (api_mode: chat_completions, e.g. the
# auxiliary blocks around OPENAI_BASE_URL/OPENAI_API_KEY) — never the
# Responses-API leg the main agent uses (codex_responses), which this file
# has no reason to touch.
PROPOSE_MAPPINGS_MODEL = os.environ.get("TRIAGE_PROPOSE_MODEL", "gpt-5.6-luna")

# Mirrors .env.tpl's own OPENAI_API_KEY ref exactly — never a plaintext key.
_OPENAI_API_KEY_REF = "op://common/anthropic/API_KEY"


# scripts/ledger.py — loaded by path, the same mechanism used elsewhere in
# this file (see the watchdog-poll.py borrow below) because the sibling
# filenames here are not importable. ledger.py now owns the schema and the
# migrations that used to live inline as this file's own SCHEMA constant
# plus a hand-rolled ALTER TABLE block — see its module docstring for why
# four independent copies of that block was the actual defect.
_LEDGER_PATH = Path(__file__).resolve().parent / "ledger.py"
_ledger_spec = importlib.util.spec_from_file_location("ledger", _LEDGER_PATH)
assert _ledger_spec and _ledger_spec.loader, "Failed to load scripts/ledger.py"
_ledger = importlib.util.module_from_spec(_ledger_spec)
_ledger_spec.loader.exec_module(_ledger)

# scripts/intents.py — same by-path load, same reason. The loop is the backstop
# drainer: a surface that spools an intent may be unable to drain it (Argo has
# no ledger access at all by design), and the one that CAN — the Slack approval
# plugin, which drains synchronously because a click must take effect before
# `hermes-cc.sh --confirm` runs — can still fail at it. Without a drain here an
# intent nobody drained sits in the spool forever, which is precisely the silent
# discard this control plane exists to remove. DESIGN.md § The ledger: "intents
# go through the loop's queue."
_INTENTS_PATH = Path(__file__).resolve().parent / "intents.py"
_intents_spec = importlib.util.spec_from_file_location("intents", _INTENTS_PATH)
assert _intents_spec and _intents_spec.loader, "Failed to load scripts/intents.py"
_intents = importlib.util.module_from_spec(_intents_spec)
_intents_spec.loader.exec_module(_intents)


def drain_intents(conn: sqlite3.Connection, *, dry_run: bool) -> None:
    """Step 0 — apply anything a surface spooled since the last pass.

    Runs FIRST, before ingest(), so a decision recorded between two passes is
    already on the row by the time anything in this file reads state.

    Under --dry-run this reports and does nothing, which is a DEPARTURE from
    this file's usual "local bookkeeping runs for real" rule (apply_resolutions,
    classify). The reason is that the spool is not part of the database: a
    dry-run is pointed at a COPY of the ledger, but there is only one
    ~/.warden/intents, so draining here would consume — permanently — intents
    the live loop still needs, and apply them to a database nobody reads. The
    dry-run contract is "never touches Slack, never shells out"; eating the
    live system's queue is worse than either.
    """
    if dry_run:
        pending = sorted(_intents.INTENTS_DIR.glob("*.json")) if _intents.INTENTS_DIR.exists() else []
        if pending:
            print(f"[dry-run] would drain {len(pending)} spooled intent(s) (skipped under --dry-run)")
        return
    result = _intents.drain(conn)
    if result["applied"] or result["rejected"]:
        print(f"triage: drained {result['applied']} intent(s), "
              f"rejected {result['rejected']}", file=sys.stderr)


def db_connect() -> sqlite3.Connection:
    """triage.py is the LaunchAgent that runs the loop every 10 minutes —
    the one process DESIGN.md's § The ledger names as running at boot — so
    it is the only caller in warden allowed to pass migrate=True. Every
    other reader/writer of this database (watchdog-poll.py,
    dispatch-sweep.py) calls ledger.connect() without it and asserts the
    version instead, so a process that starts before the loop has ever
    touched a fresh ledger fails loudly rather than inventing its own
    tables."""
    return _ledger.connect(DB_PATH, migrate=True)


def _apply_db_override(argv: list[str]) -> None:
    """--db PATH, or the HERMES_CC_DB env var hermes-cc.sh/dispatch-sweep.py
    already honor (same table) — lets a test or a --dry-run inspection point
    this at a throwaway copy of the DB without touching the real
    ~/.hermes/watchdog.db."""
    global DB_PATH
    if "--db" in argv:
        idx = argv.index("--db")
        if idx + 1 < len(argv):
            DB_PATH = Path(argv[idx + 1]).expanduser()
            return
    env_override = os.environ.get("HERMES_CC_DB")
    if env_override:
        DB_PATH = Path(env_override).expanduser()


# --- resolve_slack_token() -----------------------------------------------
#
# Used to be loaded by path from agents-overview.py, the same mechanism the
# cron entry-point wrappers use (the filenames are not importable), with this
# body as its hand-mirrored fallback so the file stayed independently
# runnable if the sibling could not be loaded. agents-overview.py stayed
# behind in hermes-agent when this file moved to warden, so the borrow is
# gone and the former fallback is now the sole, plain definition.
_SECRETS_RUN = Path.home() / ".local" / "bin" / "secrets-run"
_SLACK_TOKEN_REF = "op://hermes/slack/bot-token"


def resolve_slack_token() -> str:
    """Mirrors agents-overview.py's resolve_slack_token() by hand — verbatim
    equivalent of that sibling's implementation, now the sole definition
    since agents-overview.py itself stayed behind in hermes-agent."""
    val = os.environ.get("SLACK_BOT_TOKEN", "")
    if val:
        return val
    env = os.environ.copy()
    env["PATH"] = "/opt/homebrew/bin:/usr/local/bin:" + env.get("PATH", "/usr/bin:/bin")
    try:
        r = subprocess.run(
            [str(_SECRETS_RUN), "read", _SLACK_TOKEN_REF],
            capture_output=True, text=True, timeout=15, env=env,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return r.stdout.strip() if r.returncode == 0 else ""


# --- Reused, not reimplemented: normalize_title() -------------------------
#
# Loaded by path from its owning sibling script — the same mechanism the cron
# entry-point wrappers use (the filenames are not importable) — so a change
# to it is picked up here automatically rather than silently drifting out of
# sync. Has a hand-mirrored fallback that only runs if the sibling script
# could not be loaded at all, so this file stays independently runnable.
# watchdog-poll.py moves to warden alongside this file, so this mechanism
# still works.
_WATCHDOG_POLL_PATH = Path(__file__).resolve().parent / ("watchdog" + "-poll.py")
try:
    _wp_spec = importlib.util.spec_from_file_location("watchdog_poll_for_triage", _WATCHDOG_POLL_PATH)
    assert _wp_spec and _wp_spec.loader
    _watchdog_poll = importlib.util.module_from_spec(_wp_spec)
    _wp_spec.loader.exec_module(_watchdog_poll)
    normalize_title = _watchdog_poll.normalize_title
except Exception:  # pragma: no cover - defensive: keep triage.py independently runnable
    import re as _re

    _DEDUP_NORMALIZE = _re.compile(r"[^a-z0-9]+")

    def normalize_title(text: str) -> str:  # type: ignore[no-redef]
        """Mirrors watchdog-poll.py's normalize_title() by hand — this branch
        only runs if that sibling script could not be loaded at all."""
        return _DEDUP_NORMALIZE.sub("-", text.lower()).strip("-")[:120]


def _slack_call(url: str, payload: dict[str, Any], token: str) -> tuple[bool, str | None]:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, TimeoutError, OSError):
        return False, None
    ok = bool(data.get("ok"))
    if not ok:
        print(f"triage: slack call failed: {data.get('error', 'unknown')}", file=sys.stderr)
    return ok, data.get("ts")


def post_blocks(channel: str, blocks: list[dict[str, Any]], text_fallback: str, token: str) -> tuple[bool, str | None]:
    return _slack_call(
        SLACK_POST_URL,
        {"channel": channel, "blocks": blocks, "text": text_fallback, "unfurl_links": False},
        token,
    )


def update_blocks(channel: str, ts: str, blocks: list[dict[str, Any]], text_fallback: str,
                   token: str) -> tuple[bool, str | None]:
    return _slack_call(
        SLACK_UPDATE_URL,
        {"channel": channel, "ts": ts, "blocks": blocks, "text": text_fallback},
        token,
    )


# --- policy + repo-deny loading -----------------------------------------------

def _valid_rule(r: Any) -> bool:
    """A rule needs a `match` and EITHER `repo` (escalate to an episode) OR
    `verb` (run a declared local command — see VERB_ALLOWLIST). A `verb` not
    in the allowlist is rejected here, loudly, rather than silently matching
    nothing at classify() time — a policy file names a KEY, never a command,
    and a typo'd key is a policy bug worth surfacing immediately. An optional
    `evidence` list is validated the same way, against EVIDENCE_ALLOWLIST —
    see that constant's own comment."""
    if not (isinstance(r, dict) and r.get("match")):
        return False
    has_target = False
    if r.get("repo"):
        has_target = True
    else:
        verb = r.get("verb")
        if verb:
            if verb in VERB_ALLOWLIST:
                has_target = True
            else:
                print(f"triage: policy rule {r.get('match')!r} names verb {verb!r}, not in VERB_ALLOWLIST "
                      f"{sorted(VERB_ALLOWLIST)} — dropping this rule", file=sys.stderr)
    if not has_target:
        return False
    evidence = r.get("evidence")
    if evidence is not None:
        if not (isinstance(evidence, list) and all(isinstance(e, str) for e in evidence)):
            print(f"triage: policy rule {r.get('match')!r} has a malformed 'evidence' field (must be a "
                  f"list of strings) — dropping this rule", file=sys.stderr)
            return False
        unknown = [e for e in evidence if e not in EVIDENCE_ALLOWLIST]
        if unknown:
            print(f"triage: policy rule {r.get('match')!r} names evidence key(s) {unknown} not in "
                  f"EVIDENCE_ALLOWLIST {sorted(EVIDENCE_ALLOWLIST)} — dropping this rule", file=sys.stderr)
            return False
    return True


def load_policy() -> dict[str, Any]:
    try:
        data = json.loads(POLICY_PATH.read_text())
    except (OSError, json.JSONDecodeError) as e:
        print(f"triage: could not read policy at {POLICY_PATH}: {e} — nothing will map or escalate this run",
              file=sys.stderr)
        data = {}
    return {
        "cardChannel": data.get("cardChannel") or DEFAULT_CARD_CHANNEL,
        "minOccurrences": int(data.get("minOccurrences") or DEFAULT_MIN_OCCURRENCES),
        "minOpenMinutes": int(data.get("minOpenMinutes") or DEFAULT_MIN_OPEN_MINUTES),
        "cooldownHours": int(data.get("cooldownHours") or DEFAULT_COOLDOWN_HOURS),
        "quietResolveHours": float(data.get("quietResolveHours") or DEFAULT_QUIET_RESOLVE_HOURS),
        "rules": [r for r in (data.get("rules") or []) if _valid_rule(r)],
        # `ignore` entries are usually a bare pattern string (hand-authored).
        # propose_mappings() instead appends a stamped object
        # ({"match", "proposedAt", "proposedBy", "reason"}) so an auto-added
        # entry carries its own provenance in the file itself — only the
        # `match` string is ever used for fnmatch, so both shapes classify()
        # identically.
        "ignore": [
            p if isinstance(p, str) else p["match"]
            for p in (data.get("ignore") or [])
            if isinstance(p, str) or (isinstance(p, dict) and isinstance(p.get("match"), str))
        ],
        # See CLAUDE.md/docs/triage.md — filters Hermes's OWN pre-silencing
        # conversational replies that watchdog-poll.py ingested from #alerts
        # as if they were alerts (297 signatures, ~30 permanently open) —
        # routed to STATE_NOTE, never STATE_IGNORED (see classify()).
        "ignoreUnstructuredSlackProse": bool(data.get("ignoreUnstructuredSlackProse")),
        # Per-repo merge/deploy/liveness policy (autoMergePaths, noCiRequired,
        # deploy, autoDeploy, liveness) — hermes-cc.sh's `merge` reads its own
        # half straight from this same file; this file only reads `liveness`
        # (maybe_check_liveness()). Malformed entries are left as-is here and
        # validated at the point each key is actually used, matching `verb`/
        # `evidence`'s own load_policy()-time-vs-use-time split above.
        "repos": data.get("repos") if isinstance(data.get("repos"), dict) else {},
        # propose_mappings()'s own age gate — see DEFAULT_PROPOSE_MAPPINGS_AGE_DAYS's
        # own comment for why a signature younger than this is left alone.
        "proposeMappingsAgeDays": float(data.get("proposeMappingsAgeDays") or DEFAULT_PROPOSE_MAPPINGS_AGE_DAYS),
    }


def _card_channel(policy: dict[str, Any]) -> str:
    return policy["cardChannel"]


def _denied_repos() -> set[str]:
    """The dispatch bridge's own deny list (config/dispatch-repos.json) —
    checked here BEFORE ever shelling out to hermes-cc.sh, so a stale or
    mistaken policy rule naming a denied repo produces a loud stderr line and
    zero dispatches, never a dispatch that hermes-cc.sh then refuses anyway."""
    try:
        data = json.loads(DISPATCH_REPOS_JSON.read_text())
    except (OSError, json.JSONDecodeError):
        return set()
    deny = data.get("deny")
    return set(deny) if isinstance(deny, list) else set()


def _match_targets(event_row: sqlite3.Row) -> list[str]:
    """Two candidate strings a policy rule can match against, in order: the
    raw `source:external_id` (works for grouped/self-describing sources), and
    `source:normalize_title(title)` (works for a state source like `uk`,
    whose external_id is an opaque, unglobbable monitor id — see the module
    docstring's MATCH TARGETS paragraph)."""
    source = event_row["source"]
    external_id = event_row["external_id"] or ""
    targets = [f"{source}:{external_id}"]
    norm = normalize_title(event_row["title"] or "")
    if norm:
        alt = f"{source}:{norm}"
        if alt not in targets:
            targets.append(alt)
    return targets


def _fnmatch_any(targets: list[str], patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(t, p) for t in targets for p in patterns)


def _match_rule(targets: list[str], rules: list[dict[str, Any]]) -> dict[str, Any] | None:
    """First rule (already validated by _valid_rule — `repo` XOR a
    VERB_ALLOWLIST-known `verb`) whose `match` fnmatches any target. The
    caller reads whichever of `repo`/`verb` is present to decide the
    outcome — see classify()."""
    for rule in rules:
        for t in targets:
            if fnmatch.fnmatch(t, rule["match"]):
                return rule
    return None


# `[`  — UptimeKuma's own bracketed monitor-name format: "[X] [:red_circle: Down] ..."
# emoji — HyperDX/argo-alert style: "🚨 ...", "✅ ...", "⚠️ ...", "*⚠️ ..." (bold mrkdwn)
_BOT_ALERT_PREFIXES = ("[", "\U0001F6A8", "✅", "⚠️", "*⚠️")


def _looks_like_bot_alert(title: str) -> bool:
    return (title or "").lstrip().startswith(_BOT_ALERT_PREFIXES)


# --- small helpers -------------------------------------------------------------

def _now_iso(now: dt.datetime) -> str:
    return now.isoformat()


def _safe_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _parse_ts(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def _fmt_ts(value: str | None) -> str:
    parsed = _parse_ts(value)
    if parsed is None:
        return value or "?"
    return parsed.astimezone(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _age_minutes(first_seen: str | None, now: dt.datetime) -> float:
    parsed = _parse_ts(first_seen)
    if parsed is None:
        return 0.0
    return (now - parsed).total_seconds() / 60.0


def _signature(event_row: sqlite3.Row) -> str:
    return f"{event_row['source']}:{event_row['external_id']}"


def _get_event(conn: sqlite3.Connection, event_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()


def _get_item(conn: sqlite3.Connection, event_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM triage_items WHERE event_id=?", (event_id,)).fetchone()


def _occurrence_mark(event: sqlite3.Row | dict[str, Any] | None) -> str | None:
    """An opaque fingerprint of which occurrences `event` has produced so far,
    for reopen_if_needed() to compare with `!=` — never with `>` or `MAX()`.

    Five `|`-separated slots, each the raw string value (empty for None/missing),
    in fixed position:

        payload_json->$.ts_last | last_reminder_at | notified_at | first_seen | reminder_count

    Slot 1 (grouped sources only, via _safe_json() — never SQL json_extract, so
    a malformed payload degrades to "absent" instead of raising) is a Slack
    `ts` float-string ("1788850795.862159"). Slots 2-4 are ISO-8601
    ("2026-09-08T07:00:20..."). These are TWO DIFFERENT CLOCKS IN TWO DIFFERENT
    FORMATS — a `MAX()` or `>` across them is a lexical compare of "1788…"
    against "2026…" that reads as correct and is not. Fixed slots plus whole-
    string equality never compares one clock against the other.

    All five slots are required, not redundant with each other:
    watchdog-poll.py's upsert_grouped() re-stamps last_reminder_at/notified_at
    ONLY when it emits (cooldown-gated) — a suppressed occurrence moves only
    payload_json.ts_last. A state source has no ts_last at all; its reopen
    signal is watchdog-poll.py:878 resetting resolved_at=NULL, first_seen=<now>,
    notified_at=NULL, last_reminder_at=NULL, reminder_count=0 on that same
    UPDATE — which moves the ISO slots. Every slot moves only on a genuine
    occurrence, or on that reopen reset (itself a genuine occurrence). Title
    and URL churn is deliberately NOT in the mark.

    Returns None if `event` is None — reopen_if_needed() reads this as "no
    event row", which cannot happen for a joined query but keeps the function
    total rather than partial."""
    if event is None:
        return None
    payload = _safe_json(event["payload_json"])
    ts_last = payload.get("ts_last")
    slots = (
        ts_last if isinstance(ts_last, str) else "",
        event["last_reminder_at"] or "",
        event["notified_at"] or "",
        event["first_seen"] or "",
        str(event["reminder_count"] if event["reminder_count"] is not None else ""),
    )
    return "|".join(slots)


# --- the one state transition ------------------------------------------------

# Every column a state transition in this file is allowed to write alongside
# `state`. A closed allowlist, for the same reason the verb/evidence/liveness/
# repo lists are closed: these names are interpolated into SQL, and the rule
# that a policy file (or any caller) may name and parameterise but never
# express holds here too. `state`, `state_deadline` and `updated_at` are not in
# it — those three are the helper's own, written on every transition, never by
# a caller.
_SET_STATE_COLUMNS = (
    "note", "dispatch_job", "card_channel", "card_ts", "card_hash", "artifact_url",
    "pr_url", "implement_job", "validation_job", "liveness_deadline", "deploy_expect_json",
    "snoozed_until",
)


class _Coalesce(NamedTuple):
    """`_set_state(..., artifact_url=_Coalesce(url))` writes
    `artifact_url=COALESCE(?, artifact_url)` — keep the existing value when the
    new one is NULL. One call site needs it (fold_dispatch_verdict(), where a
    verdict with no artifact must not erase the pull request an earlier fold
    recorded), and it is a sentinel rather than a second helper so that site
    does not have to be the one raw UPDATE left in the file."""

    value: Any


def _set_state(conn: sqlite3.Connection, event_id: int, state: str, now: dt.datetime, *,
                expect_state: str | None = None, expect_null: tuple[str, ...] = (),
                **columns: Any) -> int:
    """The ONLY place this file writes triage_items.state. Returns rowcount.

    It exists because `state` and `state_deadline` are one fact written in two
    columns: the deadline is a property OF the state (scripts/ledger.py's
    migration 2 says the same from the schema side), so a transition that
    writes one without the other produces a row that either sits forever with
    no clock or carries the previous state's clock. Twenty-six call sites each
    remembering to compute a deadline is twenty-six chances to forget one;
    computing it HERE, from STATE_DEADLINES, is the entire reason for the
    helper. test_no_raw_state_transition_remains is what keeps it the only one.

    The deadline is NULL — deliberately, not by omission — for a terminal
    state, for `new` (bounded by silence, see _SILENCE_RESOLVE_ELIGIBLE_STATES)
    and for the two states carrying their own column. Everything else gets
    `now + hours`.

    Raises on a state STATE_DEADLINES has never heard of and that is not
    terminal: a state added later must fail loudly at its first transition
    rather than quietly acquire "no deadline, forever". Raises on a `dismissed`
    with no reason, because a dismissal without one is indistinguishable from
    a bug (see STATE_DISMISSED).

    `expect_state`/`expect_null` turn the UPDATE into a compare-and-swap —
    maybe_auto_implement() claims an item that way, and the returned rowcount
    is how it learns whether it won.

    Also writes `occurrence_mark` (see _occurrence_mark()), on EVERY
    transition, not only into terminal states — same reasoning as
    `state_deadline`: a closed list of "states that need a mark" is one more
    list to forget to update, so stamping unconditionally means the column is
    never stale. It is computed here, from the event row (_get_event()), and
    is not caller-settable — like `state_deadline`, it is not in
    _SET_STATE_COLUMNS. The mark exists because `updated_at` cannot serve the
    same purpose: ingest() rewrites `updated_at` on every open row on every
    pass regardless of state, so "when did this item last actually change
    state" needs its own column. reopen_if_needed() is the only reader — it
    compares the mark stored here against the event's CURRENT mark to tell a
    resolved/dismissed row that is still quiet from one a fresh occurrence
    reopened underneath.

    Also appends exactly one `item_transitions` row on a REAL state change —
    rowcount>0 from the UPDATE AND the prior state differs from `state` — and
    nothing otherwise. This is also the only writer of that table, for the
    same reason this is the only writer of `triage_items.state`: a second
    writer of history is a second source of truth about it. The guard matters
    because this same UPDATE also serves CALLERS THAT DO NOT CHANGE STATE
    (sync_card() and friends write card_ts/card_hash/etc. through here with
    `state` unchanged) — recording those as transitions would fill the table
    with noise and corrupt every duration /metrics computes from it. `note`
    rides along verbatim when the caller passed one (a dismissal's reason, a
    quiet-resolve's timestamp) so history stays readable after the item moves
    on again; a caller that passed none leaves it NULL rather than guessing."""
    rule = STATE_DEADLINES.get(state)
    if rule is None and state not in TERMINAL_STATES:
        raise ValueError(
            f"{state!r} is in neither STATE_DEADLINES nor TERMINAL_STATES. A non-terminal state "
            f"with no deadline rule is an item that sits in it forever with nothing polling it out "
            f"— add it to STATE_DEADLINES (poller, hours, on_expiry) or to TERMINAL_STATES.")
    if state == STATE_DISMISSED and not str(columns.get("note") or "").strip():
        raise ValueError(
            "a `dismissed` transition must carry its reason in note= — the reason IS the state's "
            "content, and a reasonless dismissal cannot be told apart from a bug (see STATE_DISMISSED)")
    unknown = tuple(c for c in (*columns, *expect_null) if c not in _SET_STATE_COLUMNS)
    if unknown:
        raise ValueError(f"{unknown} not in _SET_STATE_COLUMNS — column names reach SQL here, so the "
                         f"list is closed on purpose")

    deadline = None
    if rule is not None and rule.hours is not None and rule.deadline_column == _STATE_DEADLINE_COLUMN:
        deadline = (now + dt.timedelta(hours=rule.hours)).isoformat()

    mark = _occurrence_mark(_get_event(conn, event_id))

    # The row's state BEFORE this write — the only way to tell a real
    # transition from a column-only write below (see this function's own
    # docstring, item_transitions paragraph).
    prev_row = conn.execute("SELECT state FROM triage_items WHERE event_id=?", (event_id,)).fetchone()
    prev_state = prev_row["state"] if prev_row is not None else None

    sql = "UPDATE triage_items SET state=?, state_deadline=?, occurrence_mark=?, updated_at=?"
    params: list[Any] = [state, deadline, mark, _now_iso(now)]
    for col, value in columns.items():
        if isinstance(value, _Coalesce):
            sql += f", {col}=COALESCE(?, {col})"
            params.append(value.value)
        else:
            sql += f", {col}=?"
            params.append(value)
    sql += " WHERE event_id=?"
    params.append(event_id)
    if expect_state is not None:
        sql += " AND state=?"
        params.append(expect_state)
    for col in expect_null:
        sql += f" AND {col} IS NULL"
    rowcount = conn.execute(sql, params).rowcount

    if rowcount > 0 and prev_state != state:
        note_value = columns.get("note")
        if isinstance(note_value, _Coalesce):
            note_value = note_value.value
        conn.execute(
            "INSERT INTO item_transitions(event_id, from_state, to_state, at, note) VALUES (?,?,?,?,?)",
            (event_id, prev_state, state, _now_iso(now), note_value),
        )
    return rowcount


# --- 1. ingest -------------------------------------------------------------

def ingest(conn: sqlite3.Connection, now: dt.datetime) -> None:
    """Upsert one triage_items row per open ingest-source event. Occurrences
    and last_seen refresh every run; repo/state are left alone on an existing
    row — classify() owns those, so a re-run never clobbers an escalation in
    progress."""
    placeholders = ",".join("?" * len(INGEST_SOURCES))
    rows = conn.execute(
        f"SELECT * FROM events WHERE resolved_at IS NULL AND source IN ({placeholders})",
        INGEST_SOURCES,
    ).fetchall()
    now_iso = _now_iso(now)
    for ev in rows:
        event_id = ev["id"]
        payload = _safe_json(ev["payload_json"])
        occurrences = payload.get("batch_count")
        if not isinstance(occurrences, int):
            occurrences = int(ev["reminder_count"] or 0) + 1
        last_seen = ev["last_reminder_at"] or ev["notified_at"] or ev["first_seen"] or now_iso
        existing = _get_item(conn, event_id)
        if existing is None:
            conn.execute(
                "INSERT INTO triage_items(event_id, signature, repo, state, occurrences, "
                "first_seen, last_seen, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (event_id, _signature(ev), None, STATE_NEW, occurrences,
                 ev["first_seen"], last_seen, now_iso, now_iso),
            )
        else:
            conn.execute(
                "UPDATE triage_items SET occurrences=?, last_seen=?, updated_at=? WHERE event_id=?",
                (occurrences, last_seen, now_iso, event_id),
            )
    conn.commit()


def reopen_if_needed(conn: sqlite3.Connection, now: dt.datetime) -> None:
    """A grouped or state source reuses the SAME events.id across a
    resolve -> recur cycle (UNIQUE(source, external_id)) — so a triage_items
    row stuck in `resolved` for an event that has since produced a NEW
    occurrence would otherwise sit invisible forever. Reopening to `new`
    (never clearing artifact_url/dispatch_job) is exactly what lets the next
    escalation's brief say "a PR already exists for this signature" instead
    of re-discovering it from scratch.

    The predicate is "the event's _occurrence_mark() has changed since this
    row's last transition", NOT `events.resolved_at IS NULL`. For a GROUPED
    source (slack_alert, hermes_log), resolved_at stays NULL for up to 7 idle
    days by design (sweep_stale_grouped()) — so `resolved_at IS NULL` is true
    on every pass for months after a quiet-resolve, and used to reopen a row
    this function had itself just re-closed one pass earlier, forever, every
    10 minutes, silently (the re-rendered card is byte-identical so
    card_hash short-circuits the API call — see docs/triage.md). Comparing
    marks instead of asking "is resolved_at NULL" ties reopening to an actual
    new occurrence, whichever of the two clocks produced it (see
    _occurrence_mark()'s own docstring for why both are needed).

    Three-way outcome per resolved/dismissed row, comparing the event's
    CURRENT mark against the one `_set_state()` stamped at this row's last
    transition:

    - differs -> a genuine new occurrence arrived since this row closed.
      Reopen via _set_state(), exactly as before.
    - same -> still quiet. Leave it alone. This is the whole fix: on a
      quiet-resolved grouped item, every subsequent pass takes this branch
      instead of re-opening and re-resolving the row every 10 minutes.
    - stored mark IS NULL -> this row closed before occurrence_mark existed
      (or, in principle, missed a stamp). Do not reopen it and do not guess a
      history it does not have — write the current mark as a baseline via a
      plain UPDATE (not _set_state(): it writes no `state`, so it is not a
      state transition) and leave the state alone. It then reopens on the
      next genuine occurrence like any other row. This is the adoption path;
      it is what removes the need for a data backfill in ledger.py's
      migration 3.

    `dismissed` reopens too, and the distinction matters. `ignored` and `note`
    do NOT, because a human looked at those and said benign — a recurrence
    tells us nothing new. `dismissed` is the opposite: it means NOBODY
    answered before the deadline (see STATE_DEADLINES / sweep_deadlines), so a
    fresh occurrence is new information about a question that was never
    actually decided. Without this, item 1's whole point would be handed back
    by item 3's clock — a `needs_human` row protected from silence-resolve
    would instead go terminal on a 7-day fuse and never be seen again however
    often its monitor fired. Terminal means "this item is closed", not "this
    signature may never open another".

    `fixed`/`quiet`/`closed` (the Wave 2 `resolved` split) inherit exactly the
    behaviour `resolved` had here — none of the three is a human judgement
    that a signature is benign, so a genuine recurrence is new information for
    all of them, same as it was for the one state they replaced."""
    rows = conn.execute(
        "SELECT ti.event_id AS event_id, ti.occurrence_mark AS stored_mark, e.* "
        "FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        "WHERE ti.state IN (?, ?, ?, ?)",
        (STATE_FIXED, STATE_QUIET, STATE_CLOSED, STATE_DISMISSED),
    ).fetchall()
    for row in rows:
        stored_mark = row["stored_mark"]
        current_mark = _occurrence_mark(row)
        if stored_mark is None:
            conn.execute(
                "UPDATE triage_items SET occurrence_mark=? WHERE event_id=?",
                (current_mark, row["event_id"]),
            )
        elif current_mark != stored_mark:
            _set_state(conn, row["event_id"], STATE_NEW, now)
    conn.commit()


def unsnooze_if_expired(conn: sqlite3.Connection, now: dt.datetime) -> None:
    # Row at a time rather than one set-based UPDATE, so this goes through
    # _set_state() like every other transition in the file: `snoozed` carries
    # its deadline in its own column (STATE_DEADLINES), and clearing that
    # column in the same write that changes the state is exactly what the
    # helper exists to keep together.
    now_iso = _now_iso(now)
    rows = conn.execute(
        "SELECT event_id FROM triage_items "
        "WHERE state=? AND snoozed_until IS NOT NULL AND snoozed_until<=?",
        (STATE_SNOOZED, now_iso),
    ).fetchall()
    for row in rows:
        _set_state(conn, row["event_id"], STATE_NEW, now, snoozed_until=None)
    conn.commit()


def apply_resolutions(conn: sqlite3.Connection, now: dt.datetime) -> None:
    # `events.resolved_at` is set only by disappearance-from-observation
    # (watchdog-poll.py's own ingest sweep) or its 7-idle-day housekeeping —
    # never by a human decision. So this is a SILENCE path, and only `new` is
    # eligible: see _SILENCE_RESOLVE_ELIGIBLE_STATES. STATE_NOTE, IGNORED and
    # SNOOZED are covered by the same allowlist rather than by being named
    # here — a `note` row is terminal by design (see that state's docstring)
    # and must never flip to QUIET, which IS a carded state.
    #
    # -> STATE_QUIET, never STATE_FIXED: disappearance from observation is
    # pure silence, and nothing here confirms a change actually shipped (see
    # STATE_QUIET's own comment).
    placeholders = ",".join("?" * len(_SILENCE_RESOLVE_ELIGIBLE_STATES))
    rows = conn.execute(
        f"SELECT ti.event_id FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        f"WHERE e.resolved_at IS NOT NULL AND ti.state IN ({placeholders})",
        _SILENCE_RESOLVE_ELIGIBLE_STATES,
    ).fetchall()
    for row in rows:
        # note=NULL is safe BECAUSE the row is `new`: a `new` row carries no
        # obligation and therefore no prior-phase text worth keeping (a
        # needs_human blocker, an env-check remediation, ...) — those states
        # are not reachable from here at all any more. What clearing it does
        # do is stop a stale QUIET_RESOLVE_NOTE_PREFIX/
        # RECOVERY_PAIRED_NOTE_PREFIX note from a much earlier quiet-resolve
        # surviving a reopen -> genuine fix -> resolve cycle and rendering
        # under render_card_blocks()'s STATE_QUIET branch as if it were
        # still current.
        _set_state(conn, row["event_id"], STATE_QUIET, now, note=None)
    conn.commit()


def _quiet_resolve_hours(policy: dict[str, Any]) -> float:
    return float(policy.get("quietResolveHours") or DEFAULT_QUIET_RESOLVE_HOURS)


# The only state a SILENCE path may resolve — DESIGN.md § "The quiet rule,
# corrected" and principle 5, "Observation status and remediation obligation are
# different facts". All three silence paths (apply_resolutions(),
# resolve_recovery_paired(), resolve_quiet_grouped()) resolve an item because its
# signal STOPPED BEING OBSERVED, and observation ending is not a discharge: `new`
# is the one state carrying no obligation yet, which is exactly why silence may
# cancel it.
#
# The concrete failure this closes, which was live: an intermittent fault alerts,
# an investigation writes a correct fix, the item reaches `needs_human`, the
# fault clears on its own, the item goes terminal and the written fix is
# abandoned. The old exclusion list let a grouped `needs_human` item quiet-resolve
# after 2h — 90 minutes before DESIGN.md's own 4h SLA for answering one.
#
# It is an INCLUSION list of one, and that is the load-bearing part: Wave 2 adds
# `implementing`, `validating`, `deploying` and `verifying` to the chain. An
# exclusion list silently ADMITS every state added after it was written; an
# inclusion list silently EXCLUDES them. This must fail closed.
_SILENCE_RESOLVE_ELIGIBLE_STATES = (STATE_NEW,)


def resolve_recovery_paired(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime,
                             *, dry_run: bool) -> None:
    """The stronger of the two grouped-source resolve paths (see
    resolve_quiet_grouped() for the fallback): a `✅ <same alert text>`
    recovery message is itself a positive signal that the paired `🚨` alert
    cleared, strictly better than waiting out a quiet window — a service
    that is fully DOWN also stops emitting, so silence alone can never tell
    the two apart, but an explicit recovery message can.

    This is cheap specifically because HyperDX's own webhook posts the
    IDENTICAL alert text with only the leading glyph flipped (see the brief
    that shipped this: "the titles differ only by the leading glyph"), and
    normalize_title() already strips every non-alnum character — including
    both glyphs — so a `✅ research-gateway job.reaped >= 1 (15m)` message
    normalizes to the SAME string as `🚨 research-gateway job.reaped >= 1
    (15m)`. For a grouped source that string already IS the event's own
    stable `external_id` (see docs/triage.md's MATCH TARGETS — computed once
    at first-batch time and never re-derived), so no new text-matching
    scheme is needed: one fresh #alerts fetch, one dict lookup per
    candidate.

    This CANNOT be read back out of `events`/`payload_json` — upsert_grouped()
    only retains one title (whichever batch's group needed to emit) and never
    a per-occurrence history (see docs/triage.md's own note on that), so a ✅
    that lands in the same 30-min batch as its 🚨 counterpart is silently
    folded into a bumped `batch_count` with no trace of which glyph it was.
    Reading live #alerts instead of `events` is what makes this possible at
    all — and it stays cheap because it is ONE fetch per triage run
    (poll_slack_messages's own single call), shared across every open
    slack_alert candidate via one dict, not one call per item.

    A recovery message is still only an OBSERVATION that the alert cleared, so
    like every other silence path this one only ever touches a row still in
    `new` (see _SILENCE_RESOLVE_ELIGIBLE_STATES). A human's pending decision
    about a written fix, an open dispatch, an in-flight merge — none of those
    are discharged by the thing that raised them going away.

    Resolves to STATE_QUIET, NOT STATE_FIXED — read this paragraph before
    "fixing" it. A ✅ is a POSITIVE signal, so `fixed` looks like the obviously
    correct bucket here; it is not. DESIGN.md § What must not be lost, item 4:
    "Recovery-pairing is the strong path, the 2h timer the fallback, and
    NEITHER EVER CLAIMS A FIX." Nothing shipped by this function's own
    knowledge — the service recovered, by our hand or its own, and a ✅
    message cannot tell the two apart any more than silence can. `fixed` is
    reserved for maybe_check_liveness()'s positive branch, the one place a
    POSITIVE PROBE (not an inbound message, not silence) confirms a change
    that actually shipped — see STATE_QUIET's own comment."""
    if dry_run:
        # Mirrors escalate_cluster()/run_verbs(): --dry-run makes NO outbound
        # call, Slack reads included, so a preview against a throwaway DB
        # copy never depends on live credentials or network.
        placeholders = ",".join("?" * len(_SILENCE_RESOLVE_ELIGIBLE_STATES))
        candidates = conn.execute(
            f"SELECT ti.event_id FROM triage_items ti JOIN events e ON e.id = ti.event_id "
            f"WHERE e.source='slack_alert' AND e.resolved_at IS NULL AND ti.state IN ({placeholders})",
            _SILENCE_RESOLVE_ELIGIBLE_STATES,
        ).fetchall()
        if candidates:
            print(f"[dry-run] would check {len(candidates)} open slack_alert item(s) against live "
                  f"#alerts for a ✅ recovery pairing (skipped under --dry-run)")
        return

    wp = _wp_module()
    if wp is None:
        return
    token = wp.resolve_secret("HOMELAB_API_KEY")
    if not token:
        return
    msgs, _latest, ok = wp.poll_slack_messages({"HOMELAB_API_KEY": token}, ALERTS_CHANNEL, None,
                                                skip_uk_push=True)
    if not ok or not msgs:
        return
    latest_by_key: dict[str, tuple[str, str]] = {}
    for m in msgs:
        text = (m.get("payload") or {}).get("text") or m.get("title") or ""
        key = normalize_title(text)
        if not key:
            continue
        ts = m.get("external_id") or "0"
        prev = latest_by_key.get(key)
        if prev is None or ts > prev[0]:
            latest_by_key[key] = (ts, text)

    placeholders = ",".join("?" * len(_SILENCE_RESOLVE_ELIGIBLE_STATES))
    rows = conn.execute(
        f"SELECT ti.event_id, e.external_id FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        f"WHERE e.source='slack_alert' AND e.resolved_at IS NULL AND ti.state IN ({placeholders})",
        _SILENCE_RESOLVE_ELIGIBLE_STATES,
    ).fetchall()
    for row in rows:
        match = latest_by_key.get(row["external_id"])
        if match is None:
            continue
        _ts, text = match
        if not text.lstrip().startswith("✅"):
            continue
        note = f"{RECOVERY_PAIRED_NOTE_PREFIX}{text.strip()[:200]}"
        # STATE_QUIET, never STATE_FIXED — see this function's own docstring,
        # last paragraph, before changing this line.
        _set_state(conn, row["event_id"], STATE_QUIET, now, note=note)
    conn.commit()


def resolve_quiet_grouped(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime) -> None:
    """The fallback half of grouped-source resolution (see
    resolve_recovery_paired() for the stronger positive-signal path):
    GROUPED_TRIAGE_SOURCES (slack_alert, hermes_log) never disappearance-
    resolve on their own event lifecycle — watchdog-poll.py's own
    sweep_stale_grouped() only clears them after 7 idle DAYS, deliberately
    housekeeping rather than signal (its own docstring: silent specifically
    so a months-old row doesn't trigger a notification burst). This is the
    triage-SIDE fix: a row whose underlying event has produced no new
    occurrence in `quietResolveHours` (DEFAULT_QUIET_RESOLVE_HOURS's own
    comment justifies the default) flips to `quiet` here — a purely
    local state transition that never touches `events.resolved_at` (that
    column stays owned end to end by watchdog-poll.py, per this file's own
    brief: "Do not change watchdog-poll.py's own sweep_stale_grouped").

    No new column is needed to track "quiet since": `events.last_reminder_at`
    / `notified_at` / `first_seen` (the same idle anchor sweep_stale_grouped()
    itself uses) only ADVANCES when watchdog-poll.py re-stamps the row on a
    fresh occurrence (see upsert_grouped()) — so a value that has stopped
    changing already IS the quiet duration.

    Only a row still in `new` is eligible (see
    _SILENCE_RESOLVE_ELIGIBLE_STATES). `investigating` is excluded here not as
    a special case about racing dispatch-sweep.py's fold_dispatch_verdict(),
    but as one instance of the general rule: every state past `new` carries an
    obligation, and a quiet timer is an observation about the signal, never a
    discharge of that obligation. It exits through its own transition or its
    deadline, not through silence.

    Deliberately never claims a fix: render_card_blocks() only ever shows
    this as "signal quiet since <time>" (QUIET_RESOLVE_NOTE_PREFIX) — a
    service that is fully down also stops emitting, so silence alone is
    never proof of anything beyond silence. Pure local bookkeeping (no
    Slack, no dispatch), so — like apply_resolutions()/classify() — this
    runs for real even under --dry-run; only the eventual card sync
    respects `dry_run` (see run()'s own sync_card() call)."""
    quiet_hours = _quiet_resolve_hours(policy)
    placeholders_sources = ",".join("?" * len(GROUPED_TRIAGE_SOURCES))
    placeholders_states = ",".join("?" * len(_SILENCE_RESOLVE_ELIGIBLE_STATES))
    rows = conn.execute(
        f"SELECT ti.event_id, e.last_reminder_at, e.notified_at, e.first_seen "
        f"FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        f"WHERE e.source IN ({placeholders_sources}) AND e.resolved_at IS NULL "
        f"AND ti.state IN ({placeholders_states})",
        (*GROUPED_TRIAGE_SOURCES, *_SILENCE_RESOLVE_ELIGIBLE_STATES),
    ).fetchall()
    for row in rows:
        anchor_raw = row["last_reminder_at"] or row["notified_at"] or row["first_seen"]
        quiet_since = _parse_ts(anchor_raw)
        if quiet_since is None:
            continue
        if (now - quiet_since).total_seconds() < quiet_hours * 3600:
            continue
        note = (f"{QUIET_RESOLVE_NOTE_PREFIX}{_fmt_ts(anchor_raw)} — no new occurrence for "
                f"{quiet_hours:g}h. This closes the item on silence alone; it is NOT a confirmed "
                f"fix, and the signature reopens automatically the moment it recurs.")
        _set_state(conn, row["event_id"], STATE_QUIET, now, note=note)
    conn.commit()


def classify(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime) -> set[str]:
    """Resolve `repo`/`verb` (fnmatch against BOTH match targets — see
    _match_targets()) and apply, in order: the explicit `ignore` list (same
    targets — a deliberate human call that THIS signature is a genuine
    recovery or known-benign pattern, checked first so it always wins), then
    the structural `ignoreUnstructuredSlackProse` fallback (routes to
    STATE_NOTE, never STATE_IGNORED — see that state's own docstring for why
    silently dropping unstructured #alerts prose would recreate the exact
    bug this file exists to kill), then rule matching. Only ever touches a
    row still in state `new`. Returns every signature that matched no rule
    this run, for the once-a-day digest."""
    rows = conn.execute(
        "SELECT event_id, signature, repo, verb FROM triage_items WHERE state=?", (STATE_NEW,)
    ).fetchall()
    unmapped: set[str] = set()
    now_iso = _now_iso(now)
    for row in rows:
        event_row = _get_event(conn, row["event_id"])
        if event_row is None:
            continue
        targets = _match_targets(event_row)

        if _fnmatch_any(targets, policy["ignore"]):
            _set_state(conn, row["event_id"], STATE_IGNORED, now)
            continue

        if policy["ignoreUnstructuredSlackProse"] and event_row["source"] == "slack_alert" \
                and not _looks_like_bot_alert(event_row["title"]):
            _set_state(conn, row["event_id"], STATE_NOTE, now)
            continue

        if row["repo"] is None and row["verb"] is None:
            rule = _match_rule(targets, policy["rules"])
            if rule is not None and rule.get("repo"):
                conn.execute(
                    "UPDATE triage_items SET repo=?, updated_at=? WHERE event_id=?",
                    (rule["repo"], now_iso, row["event_id"]),
                )
            elif rule is not None and rule.get("verb"):
                conn.execute(
                    "UPDATE triage_items SET verb=?, updated_at=? WHERE event_id=?",
                    (rule["verb"], now_iso, row["event_id"]),
                )
            else:
                unmapped.add(row["signature"])
    conn.commit()
    return unmapped


# --- clustering ----------------------------------------------------------------

def _cluster_groups(conn: sqlite3.Connection) -> dict[str, list[sqlite3.Row]]:
    """Every carded (state in CARDED_STATES) triage_items row, grouped by
    `dispatch_job` — the derived cluster key (see module docstring). A row
    with no dispatch_job (e.g. `quiet` without ever having escalated) is
    its own singleton group keyed by its own event_id."""
    placeholders = ",".join("?" * len(CARDED_STATES))
    rows = conn.execute(
        f"SELECT * FROM triage_items WHERE state IN ({placeholders}) ORDER BY event_id",
        CARDED_STATES,
    ).fetchall()
    groups: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        key = row["dispatch_job"] or f"solo:{row['event_id']}"
        groups.setdefault(key, []).append(row)
    return groups


def _count_open_investigation_clusters(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT count(DISTINCT dispatch_job) c FROM triage_items WHERE state=? AND dispatch_job IS NOT NULL",
        (STATE_INVESTIGATING,),
    ).fetchone()
    return int(row["c"]) if row else 0


def _investigate_dispatches_today(conn: sqlite3.Connection, now: dt.datetime) -> int:
    start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    row = conn.execute(
        "SELECT count(*) c FROM dispatches WHERE origin_event_id IS NOT NULL AND created_at >= ?",
        (start,),
    ).fetchone()
    return int(row["c"]) if row else 0


def _sibling_open_items(conn: sqlite3.Connection, repo: str, exclude_event_ids: list[int],
                         limit: int = 5) -> list[dict[str, str]]:
    # STATE_RESOLVED's three successors plus STATE_IGNORED — same exclusion,
    # same reasoning, unchanged behaviour: `note`/`dismissed` still count as
    # "open" siblings worth mentioning in a brief, exactly as before the split.
    placeholders = ",".join("?" * len(exclude_event_ids)) if exclude_event_ids else "-1"
    rows = conn.execute(
        f"SELECT ti.signature, e.title FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        f"WHERE ti.repo=? AND ti.event_id NOT IN ({placeholders}) AND ti.state NOT IN (?, ?, ?, ?) "
        f"ORDER BY ti.updated_at DESC LIMIT ?",
        (repo, *exclude_event_ids, STATE_FIXED, STATE_QUIET, STATE_CLOSED, STATE_IGNORED, limit),
    ).fetchall()
    return [{"signature": r["signature"], "title": r["title"]} for r in rows]


def _recent_raw_texts(event_row: sqlite3.Row) -> list[str]:
    """Best-effort distinct raw text for the brief, capped at 3. The
    watchdog schema does not retain a history of individual occurrences
    beyond a grouped signature's `first_text`/`first_line` — upsert_grouped
    rewrites payload_json in place on every poll (see docs/triage.md) — so
    this surfaces what is actually available (the current title, plus the
    grouped payload's first_text/first_line if distinct) rather than
    fabricating a 3-item history that doesn't exist in the DB."""
    out: list[str] = []
    title = (event_row["title"] or "").strip()
    if title:
        out.append(title)
    payload = _safe_json(event_row["payload_json"])
    for key in ("first_text", "first_line"):
        val = payload.get(key)
        val = val.strip() if isinstance(val, str) else ""
        if val and val not in out:
            out.append(val)
    return out[:3]


def _cap_brief(text: str) -> str:
    if len(text) <= MAX_BRIEF_CHARS:
        return text
    return text[: MAX_BRIEF_CHARS - 1].rstrip() + "…"


# --- evidence gathering — declared runtime-state probes (see EVIDENCE_ALLOWLIST) --

def _cap_evidence(text: str, cap: int) -> str:
    text = text or ""
    if len(text) <= cap:
        return text
    return text[: max(cap - 1, 0)].rstrip() + "…"


def _wp_module() -> Any | None:
    """The sibling watchdog-poll.py module, if it loaded (see the module-level
    try/except above `normalize_title` for why it might not have) — used only
    by evidence gatherers that need its already-proven resolve_secret()/
    poll_slack_messages(), never re-implemented here. None (never raises) if
    that sibling load failed, so a broken import degrades one evidence key,
    not the whole run."""
    return globals().get("_watchdog_poll")


def _run_bounded(fn: Any, *args: Any, timeout: int = EVIDENCE_TIMEOUT) -> tuple[bool, str]:
    """Runs fn(*args) with a hard wall-clock timeout. Every evidence gatherer
    below is read-only and side-effect-free, so this is the in-process
    equivalent of the `timeout=` subprocess.run() already gives VERB_ALLOWLIST
    commands — a hang (a stuck network mount, a slow argo API call) can't
    stall a 10-minute cron. A timeout or ANY exception folds into a returned
    error string rather than raising — a failing evidence command must never
    abort the run (see the module docstring's DRY-RUN/loop contract)."""
    # NOT `with ThreadPoolExecutor(...)`: its __exit__ calls shutdown(wait=True),
    # which blocks until the worker thread finishes — so a gatherer that hung
    # would sail straight past `timeout` and stall the loop anyway, defeating the
    # whole point of this function. shutdown(wait=False) lets a stuck thread keep
    # running (it is read-only and side-effect-free by contract, and the process
    # is a short-lived cron invocation that will exit) while the caller moves on.
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        fut = ex.submit(fn, *args)
        try:
            return True, fut.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            return False, f"timed out after {timeout}s"
        except Exception as e:  # noqa: BLE001 - must never raise into the caller
            return False, f"{type(e).__name__}: {e}"
    finally:
        ex.shutdown(wait=False)


def _gather_meteo_health(_event_rows: list[sqlite3.Row]) -> str:
    """meteo's own health probe (var/health.json, written by its own
    heartbeat) — the exact gap the meteo episode named: a repo checkout has
    no runtime state at all. Summarized (ok/heartbeat/timestamp + failing
    checks only), not dumped raw — the file runs ~40 checks and dumping all
    of them would blow the per-key cap on a mostly-healthy day for no
    benefit."""
    try:
        data = json.loads(METEO_HEALTH_PATH.read_text())
    except (OSError, json.JSONDecodeError) as e:
        return f"could not read {METEO_HEALTH_PATH}: {e}"
    if not isinstance(data, dict):
        return f"{METEO_HEALTH_PATH} did not contain a JSON object"
    checks = data.get("checks")
    checks = checks if isinstance(checks, list) else []
    bad = [c for c in checks if isinstance(c, dict) and not c.get("ok")]
    lines = [f"ok={data.get('ok')} heartbeat={data.get('heartbeat')!r} as of {data.get('timestamp')}",
             f"{len(bad)}/{len(checks)} checks failing"]
    for c in bad[:10]:
        lines.append(f"- {c.get('name')}: {c.get('detail')}")
    return "\n".join(lines)


def _gather_gateway_starts(_event_rows: list[sqlite3.Row]) -> str:
    """The last few recorded Hermes gateway process starts — gateway-starts.log
    is an append-only ledger of one UTC epoch per start (gateway/status.py's
    record_start_and_check_storm()), which happens to sit outside every repo
    worktree (~/.hermes/, not ~/SourceRoot/hermes-agent/) — the one source
    the hermes-agent episode could already see, by accident, per the brief
    that shipped this. This makes it reliable rather than incidental."""
    try:
        lines = GATEWAY_STARTS_LOG.read_text().splitlines()
    except OSError as e:
        return f"could not read {GATEWAY_STARTS_LOG}: {e}"
    out = []
    for raw in lines[-5:]:
        raw = raw.strip()
        if not raw:
            continue
        try:
            ts = dt.datetime.fromtimestamp(float(raw), dt.timezone.utc)
        except ValueError:
            continue
        out.append(ts.strftime("%Y-%m-%d %H:%M UTC"))
    if not out:
        return f"{GATEWAY_STARTS_LOG} has no parseable start timestamps"
    return f"last {len(out)} gateway start(s) (UTC): " + "; ".join(out)


def _current_gateway_start_local() -> dt.datetime | None:
    """The latest recorded gateway start, in LOCAL system time — errors.log's
    own lines are unqualified local timestamps (Python logging's default), so
    the slice boundary below has to be computed in the same zone or the
    string comparison in _gather_hermes_log_tail() is silently off by the
    system's UTC offset."""
    try:
        lines = GATEWAY_STARTS_LOG.read_text().splitlines()
    except OSError:
        return None
    for raw in reversed(lines):
        raw = raw.strip()
        if not raw:
            continue
        try:
            return dt.datetime.fromtimestamp(float(raw))
        except ValueError:
            continue
    return None


def _gather_hermes_log_tail(_event_rows: list[sqlite3.Row]) -> str:
    """Tail of Hermes's own error log, SLICED AT the current gateway process
    start — skills/hermes-gateway/SKILL.md Rule 0 exists because this was
    gotten wrong once already: agent.log/errors.log are not rotated per
    process, so an unsliced tail mixes a dead incarnation's errors with the
    live one, and a 48-hour-old burst got reported as a live fault. That
    skill establishes the boundary via a live PID + `ps -o lstart=`; this
    script isn't the gateway process and has no business inspecting
    launchd/pgrep, so gateway-starts.log's own append-only ledger (one epoch
    per start) stands in for the same boundary."""
    start = _current_gateway_start_local()
    try:
        raw_lines = HERMES_ERROR_LOG.read_text(errors="replace").splitlines()
    except OSError as e:
        return f"could not read {HERMES_ERROR_LOG}: {e}"
    ts_re = getattr(_wp_module(), "LOG_TS_RE", None) or re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
    if start is None:
        sliced = raw_lines
        note = "no gateway-starts.log boundary found — showing the unsliced tail"
    else:
        boundary = start.strftime("%Y-%m-%d %H:%M:%S")
        idx = None
        for i, ln in enumerate(raw_lines):
            m = ts_re.match(ln)
            if m and m.group(1) >= boundary:
                idx = i
                break
        sliced = raw_lines[idx:] if idx is not None else []
        note = f"sliced at current gateway start {boundary} (local)"
    tail = sliced[-40:]
    if not tail:
        return f"{note}; zero lines since the current process started — a real finding, not a gap"
    return f"{note}; last {len(tail)} line(s):\n" + "\n".join(tail)


_BRACKET_PREFIX_RE = re.compile(r"^\[([^\]]+)\]")


def _gather_kuma_push_last(event_rows: list[sqlite3.Row]) -> str:
    """The most recent `[<monitor name>] ...` message text from #alerts for
    whichever member of this cluster is a `uk` (UptimeKuma) signal. The gap
    this fixes: watchdog-poll.py's own `uk` event payload is literally
    `{"type", "status"}` (see poll_uk()), and the 16-component heartbeat line
    (e.g. `FAIL: disk 90% used (max 90%)`) exists ONLY in the raw Slack
    text — poll_slack_messages() deliberately DROPS it via `skip_uk_push`
    before it ever reaches `events`, and argo's own monitor endpoint doesn't
    carry it either. Reuses watchdog-poll.py's own proven Slack-fetch path
    (same #alerts channel, same HOMELAB_API_KEY) rather than a second HTTP
    mechanism against the raw Slack API, whose read-scope on this bot token
    is unverified."""
    uk_event = next((e for e in event_rows if e is not None and e["source"] == "uk"), None)
    if uk_event is None:
        return "no uk (UptimeKuma) member in this cluster — nothing to match a push line against"
    wp = _wp_module()
    if wp is None:
        return "watchdog-poll.py sibling module did not load — cannot fetch #alerts"
    token = wp.resolve_secret("HOMELAB_API_KEY")
    if not token:
        return "HOMELAB_API_KEY unresolved — cannot fetch #alerts history"
    monitor_key = normalize_title(uk_event["title"] or "")
    msgs, _latest, ok = wp.poll_slack_messages({"HOMELAB_API_KEY": token}, ALERTS_CHANNEL, None,
                                                skip_uk_push=False)
    if not ok:
        return "#alerts fetch failed (see watchdog-poll.py's own poll_slack_messages)"
    matches = []
    for m in msgs:
        text = (m.get("payload") or {}).get("text") or m.get("title") or ""
        bm = _BRACKET_PREFIX_RE.match(text.strip())
        if bm and normalize_title(bm.group(1)) == monitor_key:
            matches.append((m.get("external_id") or "0", text))
    if not matches:
        return f"no `[{uk_event['title']}] ...` message found in the last {len(msgs)} #alerts messages"
    matches.sort(key=lambda t: t[0])
    return matches[-1][1]


_EVIDENCE_GATHERERS = {
    "meteo-health": _gather_meteo_health,
    "gateway-starts": _gather_gateway_starts,
    "hermes-log-tail": _gather_hermes_log_tail,
    "kuma-push-last": _gather_kuma_push_last,
}
assert set(_EVIDENCE_GATHERERS) == set(EVIDENCE_ALLOWLIST), "EVIDENCE_ALLOWLIST and its gatherers drifted"

EVIDENCE_BLOCK_HEADER = (
    "CAPTURED RUNTIME STATE — gathered by the triage loop from the live machine just "
    "before this brief was built. This is NOT visible from the repo checkout the episode "
    "runs in, and the episode CANNOT re-run these commands itself; treat it as "
    "authoritative for what it reports, but it may be incomplete (see any per-key error "
    "below) or already stale by the time it's read."
)


def _gather_evidence(key: str, event_rows: list[sqlite3.Row]) -> str:
    fn = _EVIDENCE_GATHERERS.get(key)
    if fn is None:
        return f"unknown evidence key {key!r} (policy/code drifted after load_policy() validated it)"
    ok, result = _run_bounded(fn, event_rows)
    return result if ok else f"evidence command {key!r} failed: {result}"


def _evidence_keys_for_members(members: list[sqlite3.Row], event_rows_by_id: dict[int, sqlite3.Row],
                                policy: dict[str, Any]) -> list[str]:
    """Re-derives which policy rule matched each member — triage_items only
    stores the resolved `repo`, never which rule produced it — and unions
    their declared `evidence` lists, in first-member-seen order. A cluster's
    evidence set is whatever its own member signatures' rules ask for."""
    keys: list[str] = []
    for m in members:
        er = event_rows_by_id.get(m["event_id"])
        if er is None:
            continue
        rule = _match_rule(_match_targets(er), policy["rules"])
        if rule is None:
            continue
        for key in rule.get("evidence") or []:
            if key not in keys:
                keys.append(key)
    return keys


def _build_evidence_block(keys: list[str], event_rows_by_id: dict[int, sqlite3.Row], max_chars: int) -> str:
    """Renders every requested key's output into one labelled block, each
    capped at EVIDENCE_CAP_CHARS, the whole block additionally capped at
    `max_chars` — the REMAINING budget in the brief the caller computed, so
    evidence is what gets truncated when a brief is tight, never the brief's
    own structure (the alert list, the sibling items, the closing
    instructions) around it."""
    if not keys or max_chars <= 0:
        return ""
    event_rows = list(event_rows_by_id.values())
    parts = []
    for key in keys:
        text = _cap_evidence(_gather_evidence(key, event_rows), EVIDENCE_CAP_CHARS)
        parts.append(f"--- {key} ---\n{text}")
    block = EVIDENCE_BLOCK_HEADER + "\n" + "\n".join(parts)
    cap = min(max_chars, EVIDENCE_TOTAL_CAP_CHARS)
    if len(block) > cap:
        truncation_note = "\n…(evidence truncated to fit the brief cap)"
        keep = max(cap - len(truncation_note), 0)
        block = block[:keep].rstrip() + truncation_note
    return block


def _build_cluster_brief(*, repo: str, members: list[sqlite3.Row], event_rows_by_id: dict[int, sqlite3.Row],
                          sibling_events: list[dict[str, str]], evidence_keys: list[str] | None = None) -> str:
    lines = [f"Repo: {repo}"]
    if len(members) == 1:
        lines.append("Alert:")
    else:
        lines.append(f"{len(members)} alert signatures fired together and MAY share one root cause:")
    for m in members:
        er = event_rows_by_id[m["event_id"]]
        lines.append(f"- `{m['signature']}` — {m['occurrences']}x since {_fmt_ts(m['first_seen'])} "
                      f"(last {_fmt_ts(m['last_seen'])}) — {er['title']}")
        for t in _recent_raw_texts(er)[:2]:
            lines.append(f"    raw: {t}")
        if m["artifact_url"]:
            lines.append(f"    already-linked artifact from a prior investigation of this EXACT "
                          f"signature: {m['artifact_url']} — check whether it already fixes this "
                          f"(including whether it simply hasn't been merged yet) before proposing "
                          f"something new")
    if sibling_events:
        lines.append(f"Other open triage items in {repo}:")
        lines.extend(f"- {s['signature']}: {s['title']}" for s in sibling_events)

    closing_lines = [""]
    if len(members) > 1:
        closing_lines.append(
            "Determine whether these signatures share a single root cause before proposing separate "
            "fixes. This grouping is a HYPOTHESIS from deterministic co-occurrence, never an "
            "assertion — confirm or split it. If they do NOT share a root cause, say so explicitly "
            f"in your summary or recommendation using the exact phrase '{DISSOLVE_MARKER}' so they "
            "can be re-triaged individually."
        )
    closing_lines.append(
        "This alert reached the auto-triage escalation threshold (repeat occurrences or stayed open "
        "long enough). Investigate the root cause and report a verdict. No LLM was involved in "
        "reaching this point — deduplication, clustering and escalation are all deterministic."
    )

    # Evidence is built LAST, sized against whatever budget the rest of the
    # brief (which must never itself be truncated) leaves behind — see
    # _build_evidence_block()'s own docstring and the module docstring's
    # DRY-RUN/brief-cap contract.
    rest = "\n".join(lines + closing_lines)
    evidence_block = ""
    if evidence_keys:
        remaining = MAX_BRIEF_CHARS - len(rest) - 2  # 2 for the blank-line join below
        evidence_block = _build_evidence_block(evidence_keys, event_rows_by_id, max(remaining, 0))

    if evidence_block:
        full = "\n".join(lines) + "\n\n" + evidence_block + "\n" + "\n".join(closing_lines)
    else:
        full = rest
    return _cap_brief(full)


def _run_hermes_cc_dispatch(*, repo: str, brief: str, event_id: int, channel: str | None,
                             thread_ts: str | None, timeout: int) -> dict[str, Any] | None:
    """Shell out to hermes-cc.sh's `dispatch` verb. THE BRIEF NEVER TOUCHES
    ARGV — it goes on stdin, exactly the invariant hermes-cc.sh's own header
    documents and enforces. `event_id` is the cluster's PRIMARY member —
    `--origin-event` takes exactly one events.id (one dispatches row = one
    episode); the caller writes events.dispatch_id on every OTHER member
    itself afterward (see escalate_cluster()). Returns the parsed --json
    object on a clean `ok: true` response, None on anything else (never
    raises)."""
    argv = [str(HERMES_CC_BIN), "dispatch", repo, "--tier", "investigate", "--json",
            "--origin-event", str(event_id)]
    if channel:
        argv += ["--origin-channel", channel]
        if thread_ts:
            argv += ["--origin-thread", thread_ts]
    try:
        r = subprocess.run(argv, input=brief, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError) as e:
        print(f"triage: hermes-cc.sh dispatch failed to run for {repo}: {e}", file=sys.stderr)
        return None
    if r.returncode != 0:
        print(f"triage: hermes-cc.sh dispatch exited {r.returncode} for {repo}: "
              f"{r.stderr.strip()[:500]}", file=sys.stderr)
        return None
    try:
        obj = json.loads(r.stdout)
    except json.JSONDecodeError:
        print(f"triage: hermes-cc.sh dispatch returned non-JSON for {repo}: {r.stdout[:300]}", file=sys.stderr)
        return None
    if not obj.get("ok") or not obj.get("jobId"):
        print(f"triage: hermes-cc.sh dispatch not ok for {repo}: {obj}", file=sys.stderr)
        return None
    return obj


def _is_escalation_eligible(item: sqlite3.Row, policy: dict[str, Any], now: dt.datetime) -> bool:
    if item["occurrences"] >= policy["minOccurrences"]:
        return True
    return _age_minutes(item["first_seen"], now) >= policy["minOpenMinutes"]


def _cooldown_ok(conn: sqlite3.Connection, item: sqlite3.Row, policy: dict[str, Any],
                  now: dt.datetime) -> bool:
    """No prior dispatch for this signature -> always ok. Otherwise wait out
    cooldownHours from the PRIOR dispatch's created_at before re-escalating a
    signature that recurred — a flapping alert should not open a fresh
    investigate episode every 10 minutes."""
    if not item["dispatch_job"]:
        return True
    row = conn.execute("SELECT created_at FROM dispatches WHERE job_id=?", (item["dispatch_job"],)).fetchone()
    created = _parse_ts(row["created_at"]) if row else None
    if created is None:
        return True
    return (now - created).total_seconds() >= policy["cooldownHours"] * 3600


def escalate_cluster(conn: sqlite3.Connection, repo: str, members: list[sqlite3.Row], now: dt.datetime,
                      policy: dict[str, Any], *, dry_run: bool) -> str | None:
    sigs = [m["signature"] for m in members]
    if dry_run:
        print(f"[dry-run] would dispatch investigate for cluster in {repo}: {sigs}")
        card_channel = _card_channel(policy)
        print(f"[dry-run] would post card for cluster in {repo} ({len(members)} signature"
              f"{'s' if len(members) != 1 else ''}: {sigs}) in {card_channel}")
        return None

    event_rows_by_id = {m["event_id"]: _get_event(conn, m["event_id"]) for m in members}
    exclude_ids = [m["event_id"] for m in members]
    sibling_events = _sibling_open_items(conn, repo, exclude_ids)
    evidence_keys = _evidence_keys_for_members(members, event_rows_by_id, policy)
    brief = _build_cluster_brief(repo=repo, members=members, event_rows_by_id=event_rows_by_id,
                                  sibling_events=sibling_events, evidence_keys=evidence_keys)
    primary = members[0]
    channel = _card_channel(policy)
    result = _run_hermes_cc_dispatch(
        repo=repo, brief=brief, event_id=primary["event_id"], channel=channel, thread_ts=None,
        timeout=SUBPROCESS_TIMEOUT,
    )
    if result is None:
        return None
    job_id = result["jobId"]
    for m in members:
        _set_state(conn, m["event_id"], STATE_INVESTIGATING, now, dispatch_job=job_id)
    dispatch_row = conn.execute("SELECT id FROM dispatches WHERE job_id=?", (job_id,)).fetchone()
    if dispatch_row is not None:
        for m in members:
            conn.execute("UPDATE events SET dispatch_id=? WHERE id=?", (dispatch_row["id"], m["event_id"]))
    else:
        print(f"triage: hermes-cc.sh reported job {job_id} but no matching dispatches row was found "
              f"(events.dispatch_id left unset for {sigs})", file=sys.stderr)
    conn.commit()

    # The card only starts existing now (state just became `investigating`,
    # the first CARDED_STATES member of this cluster) — post it immediately,
    # then retro-fill origin_thread_ts on the dispatches row so
    # dispatch-sweep.py's actionable-dispatch nudge lands on the card's own
    # thread. Same "small follow-up write to a column dispatch-sweep.py
    # doesn't own" pattern that file already uses for status/verdict_json/
    # artifact_url/merged_at/poll_misses/reported_at on this same table.
    fresh_members = [_get_item(conn, m["event_id"]) for m in members]
    fresh_members = [m for m in fresh_members if m is not None]
    fresh_events = [event_rows_by_id[m["event_id"]] for m in fresh_members]
    fresh_members = sync_card(conn, fresh_members, fresh_events, policy, dry_run=False)
    card_ts = fresh_members[0]["card_ts"] if fresh_members else None
    if card_ts:
        conn.execute("UPDATE dispatches SET origin_thread_ts=? WHERE job_id=?", (card_ts, job_id))
        conn.commit()
    return job_id


def escalate(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime, *, dry_run: bool) -> None:
    """Groups every eligible `new`+mapped item BY REPO and opens at most one
    sideclaw dispatch per repo per run (a cluster — see module docstring),
    capped at MAX_CLUSTER_SIGNATURES members per brief. A `split` item (see
    STATE_SPLIT) is ALSO an escalation candidate, gated by the exact same
    checks (snoozed_until, denied repo, _is_escalation_eligible(),
    _cooldown_ok()) — but it escalates as a SINGLETON, never grouped with
    another `split` item or with `new` items: grouping it would re-fuse the
    very cluster _dissolve_cluster() just took apart, which its own Slack
    notice promises will not happen ("Each will be re-evaluated
    individually").

    `split` candidates are considered BEFORE `new` clusters — an item
    carrying an obligation and a deadline outranks work that has not
    started — and the "at most one dispatch per repo per run" property holds
    across both kinds: if a repo has an eligible `split` item, THAT repo's
    slot for this run is spent on it, and every `new` item (and any
    additional `split` item) in that same repo waits for a later run,
    reported exactly like the existing cluster-cap overflow is — a
    deferral that only reaches a `.err` file is indistinguishable from a
    broken loop.

    Concurrency and daily budget are checked once per run, decremented as
    clusters are opened, so later repos (and later, `new`, attempts) in the
    same run correctly see an exhausted cap."""
    denied = _denied_repos()
    open_investigations = _count_open_investigation_clusters(conn)
    budget_used_today = _investigate_dispatches_today(conn, now)

    candidates = conn.execute(
        "SELECT * FROM triage_items WHERE state IN (?, ?) AND repo IS NOT NULL ORDER BY event_id",
        (STATE_NEW, STATE_SPLIT),
    ).fetchall()
    split_by_repo: dict[str, list[sqlite3.Row]] = {}
    new_by_repo: dict[str, list[sqlite3.Row]] = {}
    for item in candidates:
        if item["snoozed_until"]:
            continue
        repo = item["repo"]
        if repo in denied:
            print(f"triage: repo {repo!r} for {item['signature']} is denied in dispatch-repos.json "
                  f"— fix the policy rule, this will never escalate", file=sys.stderr)
            continue
        if not _is_escalation_eligible(item, policy, now):
            continue
        if not _cooldown_ok(conn, item, policy, now):
            print(f"triage: {item['signature']} recurred inside cooldownHours, not re-escalating yet",
                  file=sys.stderr)
            continue
        bucket = split_by_repo if item["state"] == STATE_SPLIT else new_by_repo
        bucket.setdefault(repo, []).append(item)

    # One ordered list of (repo, members, deferrals) attempts — `split`
    # singletons first (see this function's own docstring), each repo
    # appearing at most once. `claimed_repos` is what makes "one dispatch per
    # repo per run" hold ACROSS the two kinds, not just within `new_by_repo`
    # as it used to.
    #
    # `deferrals` are the lines saying who this attempt pushed to a later run,
    # and they are CARRIED rather than printed here on purpose: an attempt
    # that never gets past the two caps below did not take anyone's slot, and
    # announcing "N more wait for next run" for a cluster that was itself
    # deferred describes a dispatch that did not happen. That is the shape the
    # cluster-cap message had before `split` existed — the overflow print sat
    # after both `continue`s — and it is preserved rather than reinvented.
    attempts: list[tuple[str, list[sqlite3.Row], list[str]]] = []
    claimed_repos: set[str] = set()
    for repo, items in split_by_repo.items():
        primary, overflow = items[0], items[1:]
        deferrals = []
        if overflow:
            deferrals.append(
                f"triage: {len(overflow)} more split item(s) in {repo} wait for next run "
                f"(a split item escalates as a singleton, never grouped): "
                f"{[m['signature'] for m in overflow]}")
        held_new = new_by_repo.get(repo) or []
        if held_new:
            deferrals.append(
                f"triage: {repo}'s slot this run went to a split item — {len(held_new)} new item(s) "
                f"wait for next run: {[m['signature'] for m in held_new]}")
        attempts.append((repo, [primary], deferrals))
        claimed_repos.add(repo)

    for repo, members in new_by_repo.items():
        if repo in claimed_repos:
            continue
        group = members[:MAX_CLUSTER_SIGNATURES]
        overflow = members[MAX_CLUSTER_SIGNATURES:]
        deferrals = []
        if overflow:
            deferrals.append(
                f"triage: {len(overflow)} more eligible {repo} items wait for next run "
                f"(cluster cap {MAX_CLUSTER_SIGNATURES}/brief): {[m['signature'] for m in overflow]}")
        attempts.append((repo, group, deferrals))

    for repo, members, deferrals in attempts:
        if open_investigations >= MAX_OPEN_INVESTIGATIONS:
            print(f"triage: at MAX_OPEN_INVESTIGATIONS={MAX_OPEN_INVESTIGATIONS}, deferring cluster in "
                  f"{repo} ({[m['signature'] for m in members]})", file=sys.stderr)
            continue
        if budget_used_today >= DAILY_INVESTIGATE_BUDGET:
            print(f"triage: at DAILY_INVESTIGATE_BUDGET={DAILY_INVESTIGATE_BUDGET}, deferring cluster "
                  f"in {repo}", file=sys.stderr)
            continue
        for line in deferrals:
            print(line, file=sys.stderr)
        job_id = escalate_cluster(conn, repo, members, now, policy, dry_run=dry_run)
        # escalate_cluster() always returns None under --dry-run (it never
        # calls hermes-cc.sh) — `or dry_run` keeps the two caps' PREVIEW
        # meaningful across multiple repos in one dry-run pass (a later repo
        # in the same run correctly sees an exhausted cap), without ever
        # persisting anything. A REAL run only counts an actual success, so a
        # failed dispatch is retried next run rather than burning budget.
        if job_id or dry_run:
            open_investigations += 1
            budget_used_today += 1


# --- verb outcomes — a deterministic local probe, never an episode -----------

def _run_verb(argv: list[str], *, timeout: int) -> dict[str, Any] | None:
    """Run one VERB_ALLOWLIST-resolved argv, parse its --json stdout. Never
    raises — a spawn failure, timeout, non-zero exit, or unparseable stdout
    all fold into a small dict with an `_error` key so the caller can render
    something on the card either way, rather than the row silently getting
    stuck."""
    try:
        r = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError) as e:
        return {"_error": str(e)}
    try:
        obj = json.loads(r.stdout)
    except json.JSONDecodeError:
        return {"_error": f"non-JSON output (rc={r.returncode}): {r.stdout.strip()[:300] or '(empty)'}"}
    if r.returncode not in (0, 3):
        # hermes-ops.sh's own convention: env-check exits 3 for "ok: false",
        # which is a normal, expected outcome here (that IS the dangling ref
        # this verb exists to surface) — only something else is a real error.
        return {"_error": f"unexpected exit {r.returncode}: {json.dumps(obj)[:300]}"}
    return obj if isinstance(obj, dict) else {"_error": f"non-object JSON: {r.stdout[:300]}"}


def _render_env_check_note(output: dict[str, Any] | None) -> str:
    """Deterministic prose for the needs_human card — no LLM, straight from
    hermes-ops.sh's own --json shape: {"ok", "homelab": {"danglingItems": [...],
    ...}, "vps": {...}}. The dangling item name and the exact remediation are
    inlined so the card is the whole answer — no further investigation should
    be needed."""
    if output is None or "_error" in output:
        err = (output or {}).get("_error", "no output")
        return f"env-check probe failed to run: {err}. Retry manually: `hermes-ops.sh env-check`."
    dangling: list[str] = []
    for host_key in ("homelab", "vps"):
        host = output.get(host_key)
        if isinstance(host, dict):
            for item in host.get("danglingItems") or []:
                dangling.append(f"{host_key}: `{item}`")
    if not dangling:
        return ("env-check ran and found no dangling item on this pass — likely transient; the "
                "underlying event will disappearance-resolve on its own if it clears.")
    items_text = "; ".join(dangling)
    return (
        f"Dangling 1Password item(s) — {items_text}. `op run` fails WHOLESALE on the shared "
        f".env.tpl until this is fixed, taking every cron sharing that template down at once. "
        f"Fix: restore/rename the item in 1Password, then run `make secrets-seed` (biometric "
        f"1Password prompt — MacBook only, this cannot be done headlessly on the mini)."
    )


def run_verbs(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime, *, dry_run: bool) -> None:
    """Every `new` item routed to a `verb` (never a `repo` — see classify())
    runs its allowlisted local command once eligibility is met, same
    minOccurrences/minOpenMinutes gate as an episode escalation. No
    concurrency/daily-budget cap: a verb is a bounded local probe, not a
    sideclaw episode, and doesn't compete for that budget. No cooldown
    tracking either — a verb-routed item runs at most ONCE, because its
    terminal state (`needs_human`) falls out of STATE_NEW candidates
    permanently; if the underlying condition later clears, the normal
    resolve path (events.resolved_at) closes it out without needing a
    re-run, and if it recurs after a reopen, running the probe again is
    exactly correct."""
    candidates = conn.execute(
        "SELECT * FROM triage_items WHERE state=? AND verb IS NOT NULL AND repo IS NULL ORDER BY event_id",
        (STATE_NEW,),
    ).fetchall()
    for item in candidates:
        if item["snoozed_until"]:
            continue
        if not _is_escalation_eligible(item, policy, now):
            continue
        verb = item["verb"]
        argv = VERB_ALLOWLIST.get(verb)
        if argv is None:
            print(f"triage: verb {verb!r} for {item['signature']} is not in VERB_ALLOWLIST — "
                  f"skipping (policy/code drifted after load_policy() validated it)", file=sys.stderr)
            continue
        if dry_run:
            print(f"[dry-run] would run verb {verb!r} for {item['signature']}")
            continue
        output = _run_verb(argv, timeout=VERB_TIMEOUT)
        note = _render_env_check_note(output) if verb == "env-check" else json.dumps(output)[:SECTION_TEXT_MAX]
        _set_state(conn, item["event_id"], STATE_NEEDS_HUMAN, now, note=note)
        conn.commit()
        fresh_item = _get_item(conn, item["event_id"])
        event_row = _get_event(conn, item["event_id"])
        if fresh_item is not None and event_row is not None:
            sync_card(conn, [fresh_item], [event_row], policy, dry_run=False)


# --- dissolve — a cluster the episode itself says is unrelated ----------------

def _dissolve_cluster(conn: sqlite3.Connection, members: list[sqlite3.Row], now: dt.datetime,
                       verdict_text: str, *, dry_run: bool) -> None:
    """Move every member to `split` so each is re-evaluated individually.
    `dispatch_job` is deliberately LEFT SET (only card_channel/card_ts/
    card_hash are cleared) — a `split` row is never grouped by
    `_cluster_groups()` (which only looks at CARDED_STATES, and `split` is
    deliberately not in it — see that tuple's own comment), so the cluster
    is functionally gone for card/escalation purposes, but keeping the
    pointer means `_cooldown_ok()` still finds the dissolved dispatch's
    created_at and enforces a real cooldownHours wait. Without this, the very
    same `run()` that dissolves a cluster would see both members freshly
    eligible with no cooldown at all and instantly re-fuse them into an
    identical cluster in the escalate() call that follows — dissolve would be
    a no-op in practice. The tradeoff: a dissolved pair COULD re-cluster again
    after cooldownHours if both are still open — accepted, since a hard
    permanent split needs a negative-relationship table this schema doesn't
    have.

    `verdict_text` (the SAME `summary`/`verdict`/`recommendation` text_blob
    maybe_dissolve_clusters() already built to check DISSOLVE_MARKER against —
    passed down rather than re-read from `dispatches` here, which would be a
    second source of truth for the same string) is written into every
    member's `note` under SPLIT_VERDICT_NOTE_PREFIX. This is the actual fix
    for STATE.md §43: the split verdict used to survive only in
    `dispatches.verdict_json`, which nothing reads — now it survives on the
    row itself, in the one state silence can never touch.

    Runs its bookkeeping for real even under `dry_run` — only the Slack
    `update_blocks()` call is skipped, per the module's own DRY-RUN CONTRACT
    ("dissolve bookkeeping ... runs for real even under --dry-run"). A
    dissolve moves a row between two WORKING states (verdict -> split), the
    same class of move classify()/apply_resolutions()/resolve_quiet_grouped()
    already perform for real under --dry-run; it is not the TERMINAL,
    non-re-derivable move sweep_deadlines() carves an exception for."""
    sigs = [m["signature"] for m in members]
    job_id = members[0]["dispatch_job"]
    card_channel = members[0]["card_channel"]
    card_ts = members[0]["card_ts"]
    print(f"{'[dry-run] would dissolve' if dry_run else 'triage: dissolving'} cluster {job_id}: {sigs}")
    if card_channel and card_ts:
        if dry_run:
            print(f"[dry-run] would post cluster-split update to {card_channel}/{card_ts}")
        else:
            token = resolve_slack_token()
            if token:
                sig_list = ", ".join(f"`{s}`" for s in sigs)
                blocks = [
                    {"type": "header", "text": {"type": "plain_text", "text": ":arrows_counterclockwise: Cluster split"}},
                    {"type": "section", "text": {"type": "mrkdwn", "text":
                        f"The investigation found these did not share a root cause: {sig_list}. "
                        f"Each will be re-evaluated individually."}},
                ]
                update_blocks(card_channel, card_ts, blocks, "Cluster split — re-evaluating individually", token)
    note = f"{SPLIT_VERDICT_NOTE_PREFIX}{_cap_brief(verdict_text)}" if verdict_text.strip() else None
    for m in members:
        _set_state(conn, m["event_id"], STATE_SPLIT, now,
                   card_channel=None, card_ts=None, card_hash=None, note=note)
    conn.commit()


def maybe_dissolve_clusters(conn: sqlite3.Connection, now: dt.datetime, *, dry_run: bool) -> None:
    """A cluster (>1 member sharing one dispatch_job) that landed in plain
    `verdict` (not needs_human/pr_open — those found something actionable,
    splitting doesn't apply) whose folded verdict text contains
    DISSOLVE_MARKER gets unwound: every member moves to `split`, its
    dispatch_job/card pointers cleared (dispatch_job itself retained — see
    _dissolve_cluster()), carrying the verdict that produced the split in its
    own `note` so each is re-evaluated independently on a later run without
    losing it. Runs once per pass, before escalate() — "the cluster is
    dissolved on the next run" per the design this implements."""
    rows = conn.execute(
        "SELECT dispatch_job, count(*) c FROM triage_items WHERE dispatch_job IS NOT NULL AND state=? "
        "GROUP BY dispatch_job HAVING c > 1",
        (STATE_VERDICT,),
    ).fetchall()
    for row in rows:
        job_id = row["dispatch_job"]
        d = conn.execute("SELECT verdict_json FROM dispatches WHERE job_id=?", (job_id,)).fetchone()
        if d is None:
            continue
        result = _safe_json(d["verdict_json"])
        text_blob = " ".join(str(result.get(k) or "") for k in ("summary", "verdict", "recommendation"))
        if DISSOLVE_MARKER not in text_blob:
            continue
        members = conn.execute(
            "SELECT * FROM triage_items WHERE dispatch_job=? ORDER BY event_id", (job_id,)
        ).fetchall()
        _dissolve_cluster(conn, list(members), now, text_blob, dry_run=dry_run)


# --- card rendering ------------------------------------------------------------

def _escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def render_card_blocks(members: list[sqlite3.Row], event_rows: list[sqlite3.Row], conn: sqlite3.Connection
                        ) -> list[dict[str, Any]]:
    primary = members[0]
    state = primary["state"]
    emoji = STATE_EMOJI.get(state, ":question:")
    if len(members) > 1:
        title = f"{len(members)} related alerts in `{primary['repo']}`"
    else:
        title = _escape(event_rows[0]["title"] or primary["signature"])[:140]
    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": f"{emoji} {title}"[:150]}},
    ]

    member_lines = [
        f"• `{m['signature']}` — {m['occurrences']}× since {_fmt_ts(m['first_seen'])} · "
        f"last {_fmt_ts(m['last_seen'])}"
        for m in members
    ]
    ctx_text = "\n".join(member_lines)
    if primary["repo"]:
        ctx_text += f"\nrepo `{primary['repo']}`"
    elif primary["verb"]:
        ctx_text += f"\nverb `{primary['verb']}`"
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": ctx_text[:SECTION_TEXT_MAX]}})

    if state == STATE_INVESTIGATING and primary["dispatch_job"]:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"Investigation running — job `{primary['dispatch_job'][:8]}`"}],
        })
    elif state in (STATE_VERDICT, STATE_NEEDS_HUMAN, STATE_PR_OPEN) and primary["dispatch_job"]:
        d = conn.execute("SELECT verdict_json FROM dispatches WHERE job_id=?", (primary["dispatch_job"],)).fetchone()
        result = _safe_json(d["verdict_json"]) if d and d["verdict_json"] else {}
        summary = (result.get("summary") or "").strip()
        confidence = result.get("confidence") or "?"
        text_lines = []
        if summary:
            text_lines.append(_escape(summary))
        text_lines.append(f"_confidence {confidence}_")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(text_lines)[:SECTION_TEXT_MAX]}})
        evidence = result.get("evidence")
        if isinstance(evidence, list) and evidence:
            ev_lines = []
            for e in evidence[:3]:
                if isinstance(e, dict):
                    ev_lines.append(f"- `{_escape(str(e.get('file') or '?'))}` — {_escape(str(e.get('detail') or ''))}")
                else:
                    ev_lines.append(f"- {_escape(str(e))}")
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(ev_lines)}})
        artifact_url = primary["artifact_url"]
        if artifact_url:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Artifact:* <{artifact_url}>"}})
        if state == STATE_NEEDS_HUMAN and primary["note"]:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"↳ _{_escape(primary['note'])}_"}]})
    elif state == STATE_NEEDS_HUMAN and primary["note"]:
        # A verb outcome (run_verbs()) — no dispatch_job at all, since no
        # sideclaw episode was ever opened. The note IS the whole verdict: a
        # deterministic local probe's output, not an episode's.
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": _escape(primary["note"])[:SECTION_TEXT_MAX]}})
    elif state == STATE_SNOOZED:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"Snoozed until {_fmt_ts(primary['snoozed_until'])}"}],
        })
    elif state in (STATE_QUIET, STATE_FIXED) and (primary["note"] or "").startswith(
            (QUIET_RESOLVE_NOTE_PREFIX, RECOVERY_PAIRED_NOTE_PREFIX, LIVENESS_CONFIRMED_NOTE_PREFIX)):
        # Only ever rendered for the grouped-source resolve paths (see
        # resolve_quiet_grouped()/resolve_recovery_paired(), both STATE_QUIET)
        # and the liveness confirm path (maybe_check_liveness(), STATE_FIXED)
        # — an ordinary event-driven resolve clears `note` outright
        # (apply_resolutions()), so this never fires for a genuine
        # state-source (uk/docker/op_refs) recovery, which needs no caveat.
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"↳ _{_escape(primary['note'])}_"}]})
    elif state == STATE_CLOSED:
        # `closed`'s reason is the whole point of the state (a human said
        # done, or a `merged` item ran out its 1h clock with nothing left to
        # verify) — same principle as STATE_DISMISSED just below.
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text":
            f"↳ _{_escape(primary['note'] or 'closed, no reason recorded')}_"[:SECTION_TEXT_MAX]}]})
    elif state == STATE_DISMISSED:
        # The reason is the whole point of the state (see STATE_DISMISSED), and
        # the human reading this card is the one who was asked and did not
        # answer — so the card must say what expired rather than just going
        # quiet. _set_state() guarantees the note exists; the fallback is for a
        # row edited by hand outside this file.
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text":
            f"↳ _{_escape(primary['note'] or 'dismissed, no reason recorded')}_"[:SECTION_TEXT_MAX]}]})
    elif state == STATE_IMPLEMENTING and primary["implement_job"]:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"Auto-implement running — job `{primary['implement_job'][:8]}`"}],
        })
    elif state == STATE_VALIDATING:
        text = "Independent validation running"
        if primary["validation_job"]:
            text += f" — job `{primary['validation_job'][:8]}`"
        if primary["pr_url"]:
            text += f"\nPull request: <{primary['pr_url']}>"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text[:SECTION_TEXT_MAX]}})
    elif state == STATE_MERGE_BLOCKED:
        blocks.append({"type": "section", "text": {"type": "mrkdwn",
                        "text": _escape(primary["note"] or "blocked, no further detail")[:SECTION_TEXT_MAX]}})
    elif state == STATE_MERGED:
        text = f"Merged: <{primary['pr_url']}>" if primary["pr_url"] else "Merged"
        if primary["note"]:
            text += f"\n_{_escape(primary['note'])}_"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text[:SECTION_TEXT_MAX]}})
    elif state == STATE_LIVENESS_PENDING:
        text = f"Deployed — confirming liveness by {_fmt_ts(primary['liveness_deadline'])}"
        if primary["pr_url"]:
            text += f"\nPull request: <{primary['pr_url']}>"
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": text}]})

    footer_sig = primary["signature"] if len(members) == 1 else f"<signature> ({len(members)} in this cluster)"
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"Snooze one member: `triage.py --snooze {footer_sig} --hours 24`"}],
    })
    return blocks


def _card_hash(blocks: list[dict[str, Any]]) -> str:
    return hashlib.sha256(json.dumps(blocks, sort_keys=True).encode()).hexdigest()


# States a row can reach directly from `new` (or, for `dismissed`, from a
# carded state whose deadline nobody answered) that must never receive a
# FIRST card — see sync_card()'s own docstring for the incident this guards.
# `dismissed` was already missing here before this slice (a known-open item);
# it is in scope now purely because this line is being edited anyway for the
# `resolved` split. Named once, as a tuple, rather than four inline literals
# repeated at the one call site that checks them.
_NEVER_CARDED_FIRST_STATES = (STATE_FIXED, STATE_QUIET, STATE_CLOSED, STATE_DISMISSED)


def sync_card(conn: sqlite3.Connection, members: list[sqlite3.Row], event_rows: list[sqlite3.Row],
              policy: dict[str, Any], *, dry_run: bool) -> list[sqlite3.Row]:
    """Render this cluster's card and post/update Slack ONLY if the rendered
    content changed since the last sync (the card_hash short-circuit — the
    property that keeps the channel from becoming a firehose again). Every
    member row carries an identical copy of card_channel/card_ts/card_hash
    (rather than one "owning" row) so cluster membership stays self-
    describing even after a process restart. Returns the (possibly reloaded)
    member rows.

    A cluster in one of _NEVER_CARDED_FIRST_STATES with no `card_ts` was
    never carded in the first place — every one of the resolve paths
    (apply_resolutions(), resolve_quiet_grouped(), resolve_recovery_paired())
    can flip an unescalated `new` row straight to `quiet` on a
    quiet/disappeared signal, and a card announcing the resolution of a
    problem nobody was told about is exactly the noise this loop replaced
    (see CARDED_STATES's own comment for the incident). The fix is a
    `chat.postMessage` this branch must never make — an already-carded item
    still gets its final `chat.update` below, unchanged."""
    if not members:
        return members
    primary = members[0]
    if primary["state"] in _NEVER_CARDED_FIRST_STATES and not primary["card_ts"]:
        return members
    blocks = render_card_blocks(members, event_rows, conn)
    new_hash = _card_hash(blocks)
    if new_hash == primary["card_hash"]:
        return members
    channel = primary["card_channel"] or _card_channel(policy)
    if len(members) > 1:
        fallback = f"{len(members)} related alerts in {primary['repo']}"
    else:
        fallback = (event_rows[0]["title"] or primary["signature"])[:150]
    if dry_run:
        action = "update" if primary["card_ts"] else "post"
        sigs = [m["signature"] for m in members]
        print(f"[dry-run] would {action} card for {sigs} (state={primary['state']}) in {channel}")
        return members
    token = resolve_slack_token()
    if not token:
        print(f"triage: no Slack token, cannot sync card for cluster "
              f"{[m['signature'] for m in members]}", file=sys.stderr)
        return members
    if primary["card_ts"]:
        ok, ts = update_blocks(channel, primary["card_ts"], blocks, fallback, token)
    else:
        ok, ts = post_blocks(channel, blocks, fallback, token)
    if not ok:
        print(f"triage: slack card sync failed for cluster {[m['signature'] for m in members]}", file=sys.stderr)
        return members
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    ts_value = ts or primary["card_ts"]
    for m in members:
        conn.execute(
            "UPDATE triage_items SET card_channel=?, card_ts=?, card_hash=?, updated_at=? WHERE event_id=?",
            (channel, ts_value, new_hash, now_iso, m["event_id"]),
        )
    conn.commit()
    return [r for r in (_get_item(conn, m["event_id"]) for m in members) if r is not None]


def fold_dispatch_verdict(conn: sqlite3.Connection, *, origin_event_id: int, job_id: str,
                           now: dt.datetime, dry_run: bool) -> None:
    """Called by dispatch-sweep.py once a dispatch tied to a triage cluster
    (dispatches.origin_event_id) reaches a terminal status. Looks up EVERY
    triage_items row sharing this `dispatch_job` (not just origin_event_id's
    own row — a cluster can have several), folds the verdict onto all of
    them, and syncs the shared card immediately rather than waiting for this
    file's own next 10-minute pass. Idempotent and safe to call on every
    sweep for a row already folded — the card_hash short-circuit makes a
    repeat call a no-op. `origin_event_id` is used only as a sanity check
    (the primary member should be among the rows found by job_id); job_id is
    authoritative for cluster membership."""
    members = conn.execute(
        "SELECT * FROM triage_items WHERE dispatch_job=? ORDER BY event_id", (job_id,)
    ).fetchall()
    if not members:
        return
    if origin_event_id not in {m["event_id"] for m in members}:
        print(f"triage: fold_dispatch_verdict: origin_event_id {origin_event_id} not among the "
              f"{len(members)} rows sharing dispatch_job {job_id} — proceeding on job_id anyway",
              file=sys.stderr)
    d = conn.execute(
        "SELECT status, verdict_json, artifact_url FROM dispatches WHERE job_id=?", (job_id,)
    ).fetchone()
    if d is None:
        return
    result = _safe_json(d["verdict_json"]) if d["verdict_json"] else {}
    next_action = (result.get("nextAction") or "").strip().lower()
    if d["artifact_url"]:
        new_state = STATE_PR_OPEN
    elif next_action == "human":
        new_state = STATE_NEEDS_HUMAN
    else:
        new_state = STATE_VERDICT
    blocker = ""
    if new_state == STATE_NEEDS_HUMAN:
        blocker = (result.get("recommendation") or result.get("summary") or "").strip()

    if dry_run:
        print(f"[dry-run] would fold dispatch {job_id} onto {len(members)} triage item(s): state={new_state}")
        return

    for m in members:
        _set_state(conn, m["event_id"], new_state, now,
                   artifact_url=_Coalesce(d["artifact_url"]), note=blocker or None)
    conn.commit()

    fresh_members = [r for r in (_get_item(conn, m["event_id"]) for m in members) if r is not None]
    fresh_events = [e for e in (_get_event(conn, m["event_id"]) for m in fresh_members) if e is not None]
    if fresh_members and len(fresh_members) == len(fresh_events):
        policy = load_policy()
        sync_card(conn, fresh_members, fresh_events, policy, dry_run=False)


# --- the auto-implement chain: verdict -> implement -> validate -> merge ----
# --- -> deploy -> verify (steps 6-10) -----------------------------------------
#
# Everything below is downstream of a STATE_VERDICT item whose folded
# investigate verdict already said nextAction=implement at confidence=high.
# Every step shells out to hermes-cc.sh (never sideclaw directly — that
# script is "the ONLY way the Hermes agent opens a Claude Code episode", per
# its own header) and re-derives what it needs from watchdog.db every run,
# same as the rest of this file. No LLM call happens IN THIS FILE at any of
# these steps either — the dispatched episodes run one each, which is
# inherent to what "investigate"/"implement" mean, not something this loop
# does itself.

# --- operations: the crash-recovery unit (schema 5, DESIGN.md § Crash
# recovery) --------------------------------------------------------------
#
# ONLY the two mutating verbs in this chain get an operation: `implement`
# (opens a branch + draft PR) and `merge` (merges, deletes the branch,
# deploys). `investigate`/validation-`investigate` episodes are deliberately
# excluded — they run read-only, in their own worktree, mutate nothing
# outside sideclaw, and dispatch-sweep.py's own poll_misses/`lost` path
# already covers a forgotten job. Recording one for a read-only tier would
# be bookkeeping with nothing to reconcile against.
#
# `merge` is recorded as ONE operation, not two, even though
# `hermes-cc.sh merge --confirm` is a compound external call — GraphQL
# ready-for-review, `PUT /merge`, a branch delete, then `ssh <host> make
# <target>` — behind ONE subprocess boundary. There is no way to write a row
# between the merge and the deploy without a change in hermes-agent, which is
# out of scope here; the one operation's receipt carries both results
# instead (see poll_validation_jobs() below).
_OPERATION_KINDS = ("implement", "merge")
_OPERATION_OUTCOMES = ("done", "failed", "unknown")


def record_operation(conn: sqlite3.Connection, *, event_id: int, kind: str, repo: str,
                      authorized_by: str, note: str | None = None) -> str:
    """Mint and durably record an operation BEFORE the external call it
    covers is made. This is the unit DESIGN.md § Crash recovery asks for:
    "an operation id recorded before dispatch; the approval binds to it."

    `conn.commit()`s before returning, and that commit IS the entire
    contract: the row must be on disk before the caller goes on to shell out
    to hermes-cc.sh, because a crash inside that external call is exactly
    the case this table exists to survive. If this function returned
    without committing, a crash between the INSERT and the external call
    would lose the operation id along with the process, and
    reconcile_operations() would have nothing to reconcile against — the row
    simply would not exist.

    `op_id` is a fresh `uuid.uuid4()`, chosen once and never recomputed. It
    does NOT need to be reproducible from the request's own arguments — the
    reproducibility constraint STATE.md §46 describes (an operation id
    folded into `payload_hash` so `require_signed_approval()` can recompute
    it at verify time) applies only to binding an id to a SIGNED approval,
    which is a later slice (3.1b), not this one. Nothing recorded by this
    file's own callers is signed today: `authorized_by` is always the
    literal `"auto-from-item"` on this chain (maybe_auto_implement() dispatches
    via `--auto-from-item`, and `merge --confirm` is ungated by owner
    decision, hermes-cc.sh:2029-2035) — see api.py's `"signed:"` prefix for
    the value a future signed-approval caller would pass instead.

    `kind` reaches SQL and is checked against the closed `_OPERATION_KINDS`
    allowlist for the same reason `_SET_STATE_COLUMNS` is closed: a caller
    that passes a typo'd kind must fail loudly here, not silently write a row
    reconcile_operations() will never recognize."""
    if kind not in _OPERATION_KINDS:
        raise ValueError(f"{kind!r} not in _OPERATION_KINDS={_OPERATION_KINDS} — kind reaches SQL, closed on purpose")
    op_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO operations(op_id, event_id, kind, repo, authorized_by, started_at, note) "
        "VALUES (?,?,?,?,?,?,?)",
        (op_id, event_id, kind, repo, authorized_by, dt.datetime.now(dt.timezone.utc).isoformat(), note),
    )
    conn.commit()
    return op_id


def complete_operation(conn: sqlite3.Connection, op_id: str, *, outcome: str,
                        receipt: str | None = None, note: str | None = None) -> None:
    """Called AFTER the external call record_operation() preceded returns —
    never before, and never for an ambiguous return (a subprocess timeout,
    unparseable stdout): those call sites deliberately leave the row's
    `outcome` NULL rather than guess, and reconcile_operations() — which
    runs first on every later pass, before anything could retry — is the
    only thing that ever resolves one of those. See that function's own
    docstring.

    `outcome` is checked against the closed `_OPERATION_OUTCOMES` allowlist,
    same reasoning as `kind` above. `receipt` is caller-supplied JSON text
    (already `json.dumps()`'d) rather than a dict, so this function never
    has an opinion about a receipt's shape — implement's and merge's
    receipts are structurally different (a bare job id vs. pull request +
    merge sha + deploy result) and this is the one write path both share."""
    if outcome not in _OPERATION_OUTCOMES:
        raise ValueError(
            f"{outcome!r} not in _OPERATION_OUTCOMES={_OPERATION_OUTCOMES} — outcome reaches SQL, closed on purpose")
    conn.execute(
        "UPDATE operations SET outcome=?, outcome_at=?, receipt_json=COALESCE(?, receipt_json), "
        "note=COALESCE(?, note) WHERE op_id=?",
        (outcome, dt.datetime.now(dt.timezone.utc).isoformat(), receipt, note, op_id),
    )
    conn.commit()


def _hermes_cc_status(job_id: str) -> dict[str, Any] | None:
    """`hermes-cc.sh status <job-id> --json` — a single poll, never --wait
    (an implement episode can run 30 minutes; this loop is a 10-minute cron
    and must never block inside it). Returns the parsed --json object, or
    None on anything that could not even be read (never raises)."""
    argv = [str(HERMES_CC_BIN), "status", job_id, "--json"]
    try:
        r = subprocess.run(argv, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
    except (OSError, subprocess.SubprocessError) as e:
        print(f"triage: hermes-cc.sh status failed for {job_id}: {e}", file=sys.stderr)
        return None
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        print(f"triage: hermes-cc.sh status returned non-JSON for {job_id}: {r.stdout[:300]}", file=sys.stderr)
        return None


# Same shape as hermes-cc.sh's own `url_re` inside cmd_merge (hermes-cc.sh
# ~line 1927) — reused rather than reinvented, per STATE.md §46's explicit
# instruction not to write a second parser for the same URL.
_PR_URL_RE = re.compile(r"^https://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/pull/([0-9]+)$")

# A full git commit sha, lowercase hex, exactly 40 chars — what hermes-cc.sh's
# own `merge_sha` and `gh`'s `mergeCommit.oid` both produce (STATE.md §47's
# jkrumm/argo#16 verification). Used by the deployOnMerge branches below (item
# 1b) to refuse a probe with nothing real to compare against, rather than
# entering `liveness_pending` on a short/garbled/empty value that could never
# match a live commit and would just sit until liveness_deadline and reopen.
_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def _parse_pr_url(url: str | None) -> tuple[str, str, int] | None:
    """(owner, repo, pr_number) parsed from a GitHub pull request URL, or
    None for anything that isn't one — including a NULL/empty artifact_url,
    which a dispatch that never reached `implement` completion legitimately
    has."""
    if not url:
        return None
    m = _PR_URL_RE.match(url)
    if not m:
        return None
    return m.group(1), m.group(2), int(m.group(3))


def _run_gh_pr_view(owner: str, repo: str, pr: int) -> dict[str, Any] | None:
    """`gh pr view <n> --repo <owner>/<repo> --json state,mergedAt,mergeCommit`
    — read-only, used only by reconcile_operations() to ask GitHub whether a
    `merge` operation this process never recorded the outcome of actually
    landed. `gh` holds its own credential; this file never reads a GitHub
    token, the same discipline hermes-cc.sh's own `gh_token()` uses. Same
    single-bounded-poll, never-raises shape as _hermes_cc_status(): None on
    anything that could not even be read.

    `mergeCommit` in `gh`'s own JSON output is a nested `{"oid": "<sha>"}`
    object, not a plain string (verified directly against this host's `gh
    2.100.0` on a real merged PR, jkrumm/vps#8 — STATE.md §46's own example);
    the caller unwraps it, this function returns the raw object verbatim."""
    argv = [str(GH_BIN), "pr", "view", str(pr), "--repo", f"{owner}/{repo}",
            "--json", "state,mergedAt,mergeCommit"]
    try:
        r = subprocess.run(argv, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
    except (OSError, subprocess.SubprocessError) as e:
        print(f"triage: gh pr view failed for {owner}/{repo}#{pr}: {e}", file=sys.stderr)
        return None
    if r.returncode != 0:
        print(f"triage: gh pr view exited {r.returncode} for {owner}/{repo}#{pr}: "
              f"{r.stderr.strip()[:300]}", file=sys.stderr)
        return None
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        print(f"triage: gh pr view returned non-JSON for {owner}/{repo}#{pr}: {r.stdout[:300]}", file=sys.stderr)
        return None


def _run_gh_run_list(owner: str, repo: str, sha: str) -> list[dict[str, Any]] | None:
    """`gh run list --repo <owner>/<repo> --commit <sha> --json databaseId,name,status,conclusion,createdAt`
    — read-only, used only by the deployOnMerge path (item 1b, STATE.md §47)
    to attach the GitHub Actions run as the merge's deploy RECEIPT. This is
    the thing DESIGN.md § Crash recovery asks for and the ssh deploy path
    structurally cannot provide: `ssh <host> make <target>` returns only an
    exit code to the (now dead) process that ran it, while an Actions run has
    an id and is queryable after the fact, by anyone, at any later time.

    Same single-bounded-poll, never-raises shape as _run_gh_pr_view()/
    _hermes_cc_status(): `None` means the read itself failed (network,
    non-zero exit, unparseable stdout) — never guessed at. An EMPTY list is a
    different, legitimate answer: the workflow is queued by the push and can
    take a moment to appear, so "no run visible yet" is normal right after a
    merge and must not be conflated with "the read failed". Callers record
    whichever of the two they got; neither retries in a loop."""
    argv = [str(GH_BIN), "run", "list", "--repo", f"{owner}/{repo}", "--commit", sha,
            "--json", "databaseId,name,status,conclusion,createdAt"]
    try:
        r = subprocess.run(argv, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
    except (OSError, subprocess.SubprocessError) as e:
        print(f"triage: gh run list failed for {owner}/{repo}@{sha}: {e}", file=sys.stderr)
        return None
    if r.returncode != 0:
        print(f"triage: gh run list exited {r.returncode} for {owner}/{repo}@{sha}: "
              f"{r.stderr.strip()[:300]}", file=sys.stderr)
        return None
    try:
        data = json.loads(r.stdout)
    except json.JSONDecodeError:
        print(f"triage: gh run list returned non-JSON for {owner}/{repo}@{sha}: {r.stdout[:300]}", file=sys.stderr)
        return None
    return data if isinstance(data, list) else None


def reconcile_operations(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime,
                          *, dry_run: bool) -> None:
    """Step -1 (see the module docstring) — runs FIRST in run(), before even
    drain_intents(). Every `operations` row with `outcome IS NULL` is an
    operation this process recorded as STARTED (record_operation(), before
    the external call it covers) but never recorded the result of — either a
    genuine process crash, or a call site (maybe_auto_implement(),
    poll_validation_jobs()) that deliberately left it open on an ambiguous
    return (a subprocess timeout, unparseable stdout) rather than guess. Both
    look identical from here, and both get the SAME treatment: ask the
    external system what actually happened, using only what the row itself
    already carries — never the item's own current state, which is exactly
    what might be stale.

    `outcome`, once resolved, is always one of `_OPERATION_OUTCOMES`. An
    operation that resolves to `unknown` moves its item to STATE_NEEDS_HUMAN
    rather than being left for something else to retry: "we do not know
    whether the world changed" is precisely the case DESIGN.md § Crash
    recovery routes to a human, and `needs_human` is carded, so the operator
    sees it. It must never silently become `failed` — that would retry a
    possibly-already-running implement episode or re-attempt a merge that
    may already have landed, which is the whole defect this slice exists to
    close."""
    if dry_run:
        # Both branches below shell out (hermes-cc.sh status, `gh pr view`) —
        # the dry-run contract is "never shells out", the same reason
        # poll_implement_jobs()/poll_validation_jobs() return outright below.
        return
    rows = conn.execute("SELECT * FROM operations WHERE outcome IS NULL ORDER BY op_id").fetchall()
    for row in rows:
        receipt = _safe_json(row["receipt_json"])
        new_receipt: dict[str, Any] | None = None
        note: str | None = None

        if row["kind"] == "implement":
            job_id = receipt.get("jobId")
            if not job_id:
                # No job id was ever recorded — this process crashed (or the
                # call site left the row open on a timeout/non-JSON stdout)
                # before it even learned whether sideclaw accepted the
                # submission. There is nothing left here to ask.
                outcome = "unknown"
                note = "no sideclaw job id was ever recorded for this implement dispatch"
            else:
                resp = _hermes_cc_status(job_id)
                if resp is None:
                    # A pruned job returns 404, byte-identical to a job id
                    # that never existed (STATE.md §46, verified against
                    # sideclaw directly) — absence proves nothing, so this is
                    # unknown, never failed.
                    outcome = "unknown"
                    note = f"sideclaw has no record of job {job_id} (pruned, unreachable, or never accepted)"
                else:
                    status = resp.get("status")
                    if status == "done":
                        outcome, new_receipt = "done", {**receipt, "status": status}
                    elif status in ("failed", "interrupted", "lost"):
                        outcome, new_receipt = "failed", {**receipt, "status": status}
                    else:
                        # queued/running — genuinely still in flight, not yet
                        # resolvable either way; try again next pass.
                        outcome = "unknown"
                        note = f"sideclaw reports job {job_id} still {status!r}"
        elif row["kind"] == "merge":
            d = conn.execute(
                "SELECT artifact_url FROM dispatches WHERE job_id = "
                "(SELECT implement_job FROM triage_items WHERE event_id=?)",
                (row["event_id"],),
            ).fetchone()
            parsed = _parse_pr_url(d["artifact_url"] if d else None)
            if parsed is None:
                outcome = "unknown"
                note = "could not derive a pull request from this item's implement dispatch artifact_url"
            else:
                owner, repo_name, pr = parsed
                gh_resp = _run_gh_pr_view(owner, repo_name, pr)
                if gh_resp is None:
                    outcome = "unknown"
                    note = f"gh pr view could not be read for {owner}/{repo_name}#{pr}"
                elif gh_resp.get("state") == "MERGED":
                    merge_commit = gh_resp.get("mergeCommit")
                    sha = merge_commit.get("oid") if isinstance(merge_commit, dict) else merge_commit
                    outcome = "done"
                    repo_policy = (policy.get("repos") or {}).get(row["repo"] or "") or {}
                    if repo_policy.get("deployOnMerge") and isinstance(sha, str) and _FULL_SHA_RE.match(sha):
                        # Unlike the ssh path, this repo's deploy DOES have a
                        # remote receipt to ask for — the Actions run against
                        # this exact sha. Read what is there; a run that has
                        # not appeared yet (_run_gh_run_list returns []) is
                        # recorded as such, not retried in a loop.
                        runs = _run_gh_run_list(owner, repo_name, sha)
                        deploy_value: Any = runs if runs is not None else "unknown"
                    else:
                        # The ssh deploy half has no remote answer — `ssh
                        # <host> make <target>` returns only an exit code to
                        # the (now dead) process that ran it. Recorded
                        # honestly as "unknown" rather than guessed, per
                        # DESIGN.md § Crash recovery.
                        deploy_value = "unknown"
                    new_receipt = {
                        "pullRequest": pr, "mergeCommit": sha, "reconciled": True, "deploy": deploy_value,
                    }
                else:
                    outcome = "failed"
                    new_receipt = {"pullRequest": pr, "state": gh_resp.get("state"), "reconciled": True}
        else:
            outcome = "unknown"
            note = f"reconcile_operations: unrecognized operation kind {row['kind']!r}"

        complete_operation(conn, row["op_id"], outcome=outcome,
                            receipt=json.dumps(new_receipt) if new_receipt is not None else None, note=note)
        conn.execute("UPDATE operations SET reconciled_at=? WHERE op_id=?", (_now_iso(now), row["op_id"]))
        conn.commit()

        if row["event_id"] is None:
            continue

        if outcome == "unknown":
            _set_state(conn, row["event_id"], STATE_NEEDS_HUMAN, now,
                       note=f"operation {row['op_id']} ({row['kind']}) could not be reconciled: {note}")
            conn.commit()
            continue

        # A RESOLVED operation still has to move the item, and forgetting that
        # reproduces the very defect this slice closes. Concretely: a `merge`
        # operation left open by a timeout, then reconciled to `done` because
        # GitHub says MERGED, leaves the item sitting in `validating` — whose
        # STATE_DEADLINES rule expires it to `merge_blocked` after 1h. The
        # operations table would correctly read "merged, here is the sha"
        # while the item read "blocked", which is STATE.md §46's
        # merged-but-recorded-as-failure bug wearing a different hat.
        #
        # Only `merge` needs this. An `implement` operation cannot reach
        # `done` here in practice: the only way its receipt carries a jobId is
        # complete_operation() having already been called with one, in the
        # same call+commit that sets the outcome, so an orphaned implement
        # always lands in the no-jobId branch above and resolves `unknown`.
        if row["kind"] != "merge":
            continue
        if outcome == "failed":
            # GitHub is authoritative and says it did not merge. Same
            # destination the live path uses for a definite refusal.
            detail = note or f"see operation {row['op_id']}"
            _set_state(conn, row["event_id"], STATE_MERGE_BLOCKED, now,
                       note=f"reconciled from GitHub: the pull request is not merged ({detail})")
            conn.commit()
            continue
        sha = (new_receipt or {}).get("mergeCommit")
        repo_entry = (policy.get("repos") or {}).get(row["repo"] or "") or {}
        if repo_entry.get("deployOnMerge") and isinstance(sha, str) and _FULL_SHA_RE.match(sha):
            # RECONCILIATION CONVERGES ON THE LIVE PATH, and that is the whole
            # point of this branch. A deployOnMerge repo's deploy is driven by
            # GitHub Actions off the push, NOT by the subprocess warden lost —
            # so a crash here says nothing about whether the deploy ran, and
            # the probe can still answer. Sending this item to `merged` (as
            # the `else` below would) or to `needs_human` (as `autoDeploy`
            # does) would strand a deploy that very likely succeeded, with a
            # note saying "no deploy configured for this repo" that is simply
            # false for this repo class.
            #
            # So it lands exactly where poll_validation_jobs() would have put
            # it: `liveness_pending` on the same `[{"commit": sha}]` shape,
            # with a fresh window measured from NOW rather than from the lost
            # merge — the probe is idempotent and the deadline is a bound on
            # how long we wait for it, not a claim about when the deploy
            # happened.
            deadline = (now + dt.timedelta(hours=LIVENESS_WINDOW_HOURS)).isoformat()
            _set_state(conn, row["event_id"], STATE_LIVENESS_PENDING, now,
                       liveness_deadline=deadline,
                       deploy_expect_json=json.dumps([{"commit": sha}]),
                       note=None)
        elif repo_entry.get("autoDeploy"):
            # Merged, and the deploy rode along inside the same lost
            # subprocess. `ssh <host> make <target>` leaves no remote handle
            # to ask (STATE.md §46), so whether production changed is
            # genuinely unknown — and `merged` would quietly expire to
            # `closed`, claiming a clean landing nobody verified.
            _set_state(conn, row["event_id"], STATE_NEEDS_HUMAN, now,
                       note=(f"reconciled from GitHub: merged as {sha}, but this repo auto-deploys and the "
                             f"deploy ran inside the same lost call — whether it completed cannot be "
                             f"determined remotely. Verify the deployment before acting on this item."))
        else:
            # No deploy was configured, so nothing else was supposed to
            # happen — this is exactly where the live path puts a merge with
            # no deploy target, and `merged`'s own 1h rule takes it to
            # `closed` from here.
            _set_state(conn, row["event_id"], STATE_MERGED, now,
                       note=f"reconciled from GitHub: merged as {sha}; no deploy configured for this repo")
        conn.commit()


class _AutoImplementResult(NamedTuple):
    """Return shape for _run_hermes_cc_auto_implement(), replacing a bare
    `str | None`. A bare None used to conflate every failure mode into one
    value, and the caller (maybe_auto_implement()) always rolled its claim
    back on it — which is safe ONLY for the one case where the subprocess
    never ran at all. `subprocess.TimeoutExpired` and a non-JSON stdout are
    both reachable AFTER sideclaw has already accepted the job (STATE.md §46's
    write-ordering map): a timeout can fire while hermes-cc.sh is sitting
    inside its own bounded curl to sideclaw, well after the POST that matters
    has already been sent, and a non-JSON stdout can follow a submission that
    hermes-cc.sh itself started emitting a response for. Rolling either of
    those back re-opens the item to a SECOND implement dispatch next tick —
    two branches, two draft PRs, the exact bug STATE.md §46 names. The rule:
    a timeout is not a refusal.

    `job_id` is set only on a genuine, complete success (hermes-cc.sh
    returned `{"ok": true, "jobId": ...}`). `outcome` is one of
    `_OPERATION_OUTCOMES` whenever `job_id` is None — "failed" only for the
    subprocess never having run at all (OSError) or hermes-cc.sh completing
    and plainly saying no (non-zero exit, or a parsed `{"ok": false, ...}`);
    "unknown" for TimeoutExpired and for stdout that never parsed as JSON —
    never read when `job_id` is set."""
    job_id: str | None
    outcome: str | None


def _run_hermes_cc_auto_implement(*, repo: str, event_id: int) -> _AutoImplementResult:
    """Step 6 — `dispatch <repo> --tier implement --auto-from-item <event_id>`.
    hermes-cc.sh re-checks every precondition itself from watchdog.db (see
    its own require_auto_from_item()); this function only decides WHICH item
    is a candidate (maybe_auto_implement()) and shells out. The brief is
    deliberately terse — the analysis already happened in the linked
    investigate episode, which the implement episode can and should re-read
    itself inside the repo (CLAUDE.md, the actual code) rather than trusting
    a second-hand summary here.

    See _AutoImplementResult above for why this returns a NamedTuple instead
    of a bare `str | None`, and specifically why OSError (process never ran)
    and TimeoutExpired (process may have already reached sideclaw) are no
    longer caught by the same `except` clause — they used to be, both being
    `subprocess.SubprocessError` subclasses, which is exactly how the two got
    conflated in the first place."""
    brief = (
        "A prior read-only investigation of this repo (dispatched by the alert triage loop) "
        "already concluded, at high confidence, that the fix should be implemented — re-read "
        "that investigation's own verdict and evidence yourself (it ran against this exact "
        "repo) before writing anything, then implement the fix it described. If what you find "
        "on re-reading no longer supports that conclusion, say so in your own verdict and stop "
        "rather than forcing a change."
    )
    argv = [str(HERMES_CC_BIN), "dispatch", repo, "--tier", "implement",
            "--auto-from-item", str(event_id), "--origin-event", str(event_id),
            "--why", "triage auto-implement: investigation concluded implement at high confidence",
            "--json"]
    try:
        r = subprocess.run(argv, input=brief, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
    except subprocess.TimeoutExpired as e:
        # hermes-cc.sh may already have POSTed to sideclaw and be waiting on
        # the response when the outer timeout fires — see _AutoImplementResult.
        print(f"triage: hermes-cc.sh auto-implement timed out for {repo}: {e}", file=sys.stderr)
        return _AutoImplementResult(None, "unknown")
    except (OSError, subprocess.SubprocessError) as e:
        # The subprocess never ran at all (e.g. HERMES_CC_BIN not executable)
        # — sideclaw was never called, so this is a genuine, safe-to-retry failure.
        print(f"triage: hermes-cc.sh auto-implement failed to run for {repo}: {e}", file=sys.stderr)
        return _AutoImplementResult(None, "failed")
    if r.returncode != 0:
        print(f"triage: hermes-cc.sh auto-implement exited {r.returncode} for {repo}: "
              f"{r.stderr.strip()[:500]}", file=sys.stderr)
        return _AutoImplementResult(None, "failed")
    try:
        obj = json.loads(r.stdout)
    except json.JSONDecodeError:
        # hermes-cc.sh completed but stdout never parsed — cannot tell "crashed
        # before submitting" from "crashed after sideclaw accepted the job".
        print(f"triage: hermes-cc.sh auto-implement returned non-JSON for {repo}: {r.stdout[:300]}",
              file=sys.stderr)
        return _AutoImplementResult(None, "unknown")
    if not obj.get("ok") or not obj.get("jobId"):
        print(f"triage: hermes-cc.sh auto-implement not ok for {repo}: {obj}", file=sys.stderr)
        return _AutoImplementResult(None, "failed")
    return _AutoImplementResult(obj["jobId"], None)


def maybe_auto_implement(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime,
                          *, dry_run: bool) -> None:
    """Step 6. A STATE_VERDICT item is eligible once, the moment its folded
    investigate verdict (dispatches.verdict_json, keyed by its own
    dispatch_job) reads nextAction=implement at confidence=high AND it has
    not already been auto-implemented (implement_job IS NULL) — the same
    "runs at most once" shape run_verbs() already uses, for the same reason:
    the outcome falls out of the state machine (a re-triggered item is no
    longer in STATE_VERDICT once this fires)."""
    candidates = conn.execute(
        "SELECT * FROM triage_items WHERE state=? AND dispatch_job IS NOT NULL AND implement_job IS NULL "
        "ORDER BY event_id", (STATE_VERDICT,)
    ).fetchall()
    for item in candidates:
        if item["repo"] is None:
            continue
        d = conn.execute("SELECT verdict_json FROM dispatches WHERE job_id=?", (item["dispatch_job"],)).fetchone()
        if d is None:
            continue
        verdict = _safe_json(d["verdict_json"])
        if (verdict.get("nextAction") or "").strip().lower() != "implement":
            continue
        if (verdict.get("confidence") or "").strip().lower() != "high":
            continue
        if dry_run:
            print(f"[dry-run] would auto-implement {item['signature']} in {item['repo']} "
                  f"(event {item['event_id']})")
            continue
        # CLAIM BEFORE DISPATCH, not after. The eligibility query above is
        # `state='verdict' AND implement_job IS NULL`, so recording the claim only
        # after hermes-cc.sh returns leaves a window: if this process dies between
        # the dispatch and the UPDATE, the item is still eligible on the next tick
        # and a SECOND implement episode opens for the same verdict — duplicate
        # branches and duplicate draft PRs, bounded only by the 5/day budget. The
        # conditional UPDATE is the claim: `AND state=?` makes it a compare-and-set,
        # so a concurrent run that already claimed this item changes 0 rows and this
        # one skips instead of racing it.
        claimed = _set_state(conn, item["event_id"], STATE_IMPLEMENTING, now,
                             expect_state=STATE_VERDICT, expect_null=("implement_job",))
        conn.commit()
        if not claimed:
            continue
        # Recorded and committed BEFORE the external call — see
        # record_operation()'s own docstring for why the commit is the whole
        # contract. This is the "operation id recorded before dispatch"
        # DESIGN.md § Crash recovery asks for.
        op_id = record_operation(conn, event_id=item["event_id"], kind="implement",
                                  repo=item["repo"], authorized_by="auto-from-item")
        result = _run_hermes_cc_auto_implement(repo=item["repo"], event_id=item["event_id"])
        if result.job_id is None:
            if result.outcome == "unknown":
                # A timeout or unparseable stdout — sideclaw MAY have accepted
                # the job. Do NOT roll back: that would re-implement next tick
                # against a job that could already be running (the exact
                # duplication bug this slice exists to fix). Leave both the
                # item in `implementing` (no implement_job to poll yet) and
                # the operation's outcome NULL — reconcile_operations() runs
                # first on the very next pass and resolves it before
                # anything here gets a chance to retry.
                continue
            # "failed" — the subprocess never ran, or hermes-cc.sh completed and
            # plainly refused. Sideclaw was never called either way, so it is
            # safe to hand the claim back rather than stranding the item.
            complete_operation(conn, op_id, outcome="failed")
            _set_state(conn, item["event_id"], STATE_VERDICT, now,
                       expect_state=STATE_IMPLEMENTING)
            conn.commit()
            continue
        complete_operation(conn, op_id, outcome="done", receipt=json.dumps({"jobId": result.job_id}))
        conn.execute(
            "UPDATE triage_items SET implement_job=?, updated_at=? WHERE event_id=?",
            (result.job_id, _now_iso(now), item["event_id"]),
        )
        conn.commit()
        fresh_item, fresh_event = _get_item(conn, item["event_id"]), _get_event(conn, item["event_id"])
        if fresh_item is not None and fresh_event is not None:
            sync_card(conn, [fresh_item], [fresh_event], policy, dry_run=False)


def _run_hermes_cc_validation(conn: sqlite3.Connection, *, repo: str, event_id: int,
                               implement_job: str, pr_url: str) -> str | None:
    """Step 7 — a SECOND investigate episode, on VALIDATION_MODEL (a
    deliberately DIFFERENT model from the one that wrote the implement
    episode), reviewing that pull request's actual diff against the repo:
    is the change correct, and does the PR body's own claims match the
    diff? This is not ceremony — in the incident that shipped this file, the
    implement episode asserted the wrong comparator semantics in its own PR
    body while the diff itself was right, and only a second, independent
    read caught it. Binds the validation job onto the IMPLEMENT dispatch's
    own row (dispatches.validation_job_id) the moment it opens — the
    'extra writer touching a column it doesn't own' pattern dispatch-sweep.py
    and escalate_cluster() already use on this same table — so cmd_merge's
    gate has something to read even before this validation finishes (NULL
    still correctly blocks a merge attempted too early)."""
    brief = (
        f"Review this pull request: {pr_url}\n\n"
        "Fetch its actual diff (e.g. `gh pr diff <number>`, or the branch's commits against "
        "the default branch — this worktree has the repo, use it) and read it against the "
        "repo's own code and CLAUDE.md. Answer two questions: (1) Is the change correct — "
        "does it do what it claims, with no obvious bug, and does it match this repo's own "
        "conventions? (2) Does the pull request's own title/body accurately describe what the "
        "diff actually does, or does it overstate/misstate it?\n\n"
        f"End your summary or recommendation with the EXACT phrase '{VALIDATION_CONFIRM_MARKER}' "
        f"if both answers are yes, or '{VALIDATION_DISAGREE_MARKER}' if either is not — always "
        "exactly one of the two, verbatim, on its own — this is read by a script, not a human."
    )
    argv = [str(HERMES_CC_BIN), "dispatch", repo, "--tier", "investigate",
            "--model", VALIDATION_MODEL, "--origin-event", str(event_id), "--json"]
    try:
        r = subprocess.run(argv, input=brief, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
    except (OSError, subprocess.SubprocessError) as e:
        print(f"triage: hermes-cc.sh validation dispatch failed for {repo}: {e}", file=sys.stderr)
        return None
    if r.returncode != 0:
        print(f"triage: hermes-cc.sh validation dispatch exited {r.returncode} for {repo}: "
              f"{r.stderr.strip()[:500]}", file=sys.stderr)
        return None
    try:
        obj = json.loads(r.stdout)
    except json.JSONDecodeError:
        print(f"triage: hermes-cc.sh validation dispatch returned non-JSON for {repo}: {r.stdout[:300]}",
              file=sys.stderr)
        return None
    if not obj.get("ok") or not obj.get("jobId"):
        print(f"triage: hermes-cc.sh validation dispatch not ok for {repo}: {obj}", file=sys.stderr)
        return None
    job_id = obj["jobId"]
    conn.execute("UPDATE dispatches SET validation_job_id=? WHERE job_id=?", (job_id, implement_job))
    conn.commit()
    return job_id


# hermes-cc.sh `merge --confirm` covers GraphQL ready-for-review, `PUT
# /pulls/:pr/merge`, a branch delete AND an `ssh <host> make <target>` deploy
# behind ONE subprocess boundary (STATE.md §46's write-ordering map) — capped
# here, in Python, before `deploy.output` ever reaches a receipt JSON blob,
# same "cap at the point the text is assembled" reasoning MAX_BRIEF_CHARS
# already documents for the brief itself.
_DEPLOY_OUTPUT_RECEIPT_CAP_CHARS = 2000

# hermes-cc.sh's own exit taxonomy (`_err`, hermes-cc.sh:355-368): 2
# precondition, 3 remote, 4 policy, 64 usage — and its `--json` error object
# carries the code back as `exitCode`, so this is read, never guessed from a
# message string.
#
# Only 3 can fire AFTER an external mutation has already been attempted:
# `remote_err` is reached once hermes-cc.sh is already talking to something,
# and it covers both a transport failure and the literal
# `remote_err "sideclaw accepted the job but returned no id"` — a case where
# the job IS running. 2, 4 and 64 are all decided BEFORE anything is
# attempted (a missing tool, a policy gate, a bad flag), so they are definite
# refusals and stay safe to roll back.
_HERMES_CC_EX_REMOTE = 3


class _MergeCallResult(NamedTuple):
    """Return shape for _run_hermes_cc_merge() — same reasoning as
    _AutoImplementResult above: a bare `dict | None` conflated "the merge
    call never ran" with "it may already have landed (and deployed)".
    `hermes-cc.sh merge --confirm` covers GraphQL ready-for-review, `PUT
    /pulls/:pr/merge`, a branch delete and an `ssh <host> make <target>`
    deploy behind ONE subprocess boundary — a timeout after ANY of those
    steps means the PR may already be merged, so treating that the same as a
    refusal is DESIGN.md § Crash recovery's "silently read as failure",
    verbatim (STATE.md §46's `cmd_merge` write-ordering row is exactly this).

    `result` is the parsed `merge --json` object on a genuine completion —
    whether the merge itself succeeded or hermes-cc.sh's own gate refused it,
    either way it returned a definite, parseable answer. `outcome` is set
    only when `result` is None: "failed" for a subprocess that never ran at
    all (OSError), "unknown" for a timeout or unparseable stdout."""
    result: dict[str, Any] | None
    outcome: str | None


def _run_hermes_cc_merge(job_id: str) -> _MergeCallResult:
    """Step 8 — `merge <job-id> --confirm`. `merge`'s own `--confirm` is an
    ungated, instruction-level flag (unlike `dispatch --tier implement`,
    which needs the signed-approval OR --auto-from-item gate) — owner
    decision, see hermes-cc.sh's own header: confirming the implement WAS
    the approval, landing it is finishing the thing already said yes to.
    Every real bound (declared path scope, CI reality, this exact
    validation) is enforced INSIDE cmd_merge, re-checked against the
    current head — this call is not itself a trust boundary.

    See _MergeCallResult above for why this returns a NamedTuple: a timeout
    here must never be read the same as a definite refusal."""
    argv = [str(HERMES_CC_BIN), "merge", job_id, "--why",
            "triage auto-merge: step-7 validation confirmed", "--confirm", "--json"]
    try:
        r = subprocess.run(argv, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
    except subprocess.TimeoutExpired as e:
        print(f"triage: hermes-cc.sh merge timed out for {job_id}: {e}", file=sys.stderr)
        return _MergeCallResult(None, "unknown")
    except (OSError, subprocess.SubprocessError) as e:
        print(f"triage: hermes-cc.sh merge failed to run for {job_id}: {e}", file=sys.stderr)
        return _MergeCallResult(None, "failed")
    try:
        return _MergeCallResult(json.loads(r.stdout), None)
    except json.JSONDecodeError:
        print(f"triage: hermes-cc.sh merge returned non-JSON for {job_id}: {r.stdout[:300]}", file=sys.stderr)
        return _MergeCallResult(None, "unknown")


def poll_implement_jobs(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime,
                         *, dry_run: bool) -> None:
    """Step 6 -> 7. Polls every STATE_IMPLEMENTING item once. A terminal
    result carrying a pull request opens the step-7 validation episode; a
    terminal result with no artifact (failed, interrupted, or done with
    nothing to show) blocks the chain outright — STATE_MERGE_BLOCKED, never
    a silent drop, so the card says why nothing landed."""
    if dry_run:
        return
    items = conn.execute(
        "SELECT * FROM triage_items WHERE state=? AND implement_job IS NOT NULL", (STATE_IMPLEMENTING,)
    ).fetchall()
    for item in items:
        resp = _hermes_cc_status(item["implement_job"])
        if resp is None:
            # sideclaw no longer knows this job: it prunes terminal jobs at 24h
            # OR at 200 terminal rows, a cap shared with every interactive
            # /check, so 200 can arrive in an afternoon. Nothing here can move
            # the item — which is why `implementing` has a 2h deadline in
            # STATE_DEADLINES that fires long before either bound and takes it
            # to `merge_blocked`. No miss counter, no extra column: the clock
            # already covers the case (see sweep_deadlines()).
            continue
        status = resp.get("status")
        if status not in ("done", "failed", "interrupted", "lost"):
            continue
        artifact_url = resp.get("artifactUrl")
        if status != "done" or not artifact_url:
            reason = resp.get("error") or ((resp.get("verdict") or {}).get("summary")) or "no further detail"
            note = f"implement episode {item['implement_job']} finished '{status}' with no pull request: {reason}"
            _set_state(conn, item["event_id"], STATE_MERGE_BLOCKED, now, note=note)
        else:
            val_job = _run_hermes_cc_validation(conn, repo=item["repo"], event_id=item["event_id"],
                                                 implement_job=item["implement_job"], pr_url=artifact_url)
            if val_job is None:
                _set_state(conn, item["event_id"], STATE_MERGE_BLOCKED, now,
                           note="could not open the step-7 validation episode", pr_url=artifact_url)
            else:
                _set_state(conn, item["event_id"], STATE_VALIDATING, now,
                           validation_job=val_job, pr_url=artifact_url)
        conn.commit()
        fresh_item, fresh_event = _get_item(conn, item["event_id"]), _get_event(conn, item["event_id"])
        if fresh_item is not None and fresh_event is not None:
            sync_card(conn, [fresh_item], [fresh_event], policy, dry_run=False)


def poll_validation_jobs(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime,
                          *, dry_run: bool) -> None:
    """Step 7 -> 8. Polls every STATE_VALIDATING item once. A DISAGREEING,
    FAILED, or ERRORED validation blocks the merge outright — never read as
    a pass (the brief's own words). Only an explicit CONFIRMED marker with
    no DISAGREE marker in the same text calls `merge`; its own outcome
    (landed, or refused by cmd_merge's gate) decides the next state."""
    if dry_run:
        return
    items = conn.execute(
        "SELECT * FROM triage_items WHERE state=? AND validation_job IS NOT NULL", (STATE_VALIDATING,)
    ).fetchall()
    for item in items:
        resp = _hermes_cc_status(item["validation_job"])
        if resp is None:
            # Same pruned-job case as poll_implement_jobs() above, same answer:
            # `validating` carries a 1h deadline in STATE_DEADLINES, so an item
            # whose validation job sideclaw has forgotten exits to
            # `merge_blocked` on the clock instead of being polled forever.
            continue
        status = resp.get("status")
        if status not in ("done", "failed", "interrupted", "lost"):
            continue
        verdict = resp.get("verdict") or {}
        text_blob = " ".join(str(verdict.get(k) or "") for k in ("summary", "verdict", "recommendation"))
        confirmed = (status == "done" and VALIDATION_CONFIRM_MARKER in text_blob
                     and VALIDATION_DISAGREE_MARKER not in text_blob)
        outcome = "confirmed" if confirmed else ("disagreed" if status == "done" else "error")
        conn.execute("UPDATE dispatches SET validation_status=? WHERE job_id=?",
                     (outcome, item["implement_job"]))
        conn.commit()
        if outcome != "confirmed":
            note = (f"step-7 validation ({outcome}): "
                    f"{verdict.get('summary') or resp.get('error') or 'no further detail'}")
            _set_state(conn, item["event_id"], STATE_MERGE_BLOCKED, now, note=note)
            conn.commit()
        else:
            # Recorded and committed BEFORE the external call — same
            # contract as maybe_auto_implement()'s implement operation.
            op_id = record_operation(conn, event_id=item["event_id"], kind="merge",
                                      repo=item["repo"], authorized_by="auto-from-item")
            merge_call = _run_hermes_cc_merge(item["implement_job"])
            if merge_call.result is None:
                if merge_call.outcome == "unknown":
                    # The merge (and its bundled deploy) may already have
                    # happened — see _MergeCallResult's own docstring. Leave
                    # the operation open (outcome still NULL) and the item in
                    # `validating`: reconcile_operations() asks GitHub
                    # directly on the very next pass, BEFORE this function
                    # gets another chance to re-attempt the merge. Setting
                    # STATE_MERGE_BLOCKED here would be exactly DESIGN.md §
                    # Crash recovery's "silently read as failure".
                    conn.commit()
                else:
                    complete_operation(conn, op_id, outcome="failed")
                    _set_state(conn, item["event_id"], STATE_MERGE_BLOCKED, now,
                               note="validation confirmed but the merge call itself failed to run")
                    conn.commit()
            elif merge_call.result.get("ok") and merge_call.result.get("merged"):
                deploy = merge_call.result.get("deploy") or {}
                merge_sha = merge_call.result.get("mergeCommit")
                repo_entry = (policy.get("repos") or {}).get(item["repo"] or "") or {}
                # mergeCommit is the one genuine remote receipt this system
                # already obtains (hermes-cc.sh's own merge_sha) and used to
                # throw away — STATE.md §46's write-ordering map names this
                # exactly. Persisted here, plus a `deploy` object — either the
                # ssh-deploy half hermes-cc.sh already ran (output capped, see
                # _DEPLOY_OUTPUT_RECEIPT_CAP_CHARS), or, for a deployOnMerge
                # repo, the Actions run identity below.
                receipt_deploy: dict[str, Any] = {
                    **deploy,
                    "output": _cap_evidence(deploy.get("output") or "", _DEPLOY_OUTPUT_RECEIPT_CAP_CHARS),
                }
                deadline: str | None = None
                expect_json: str | None = None
                if deploy.get("attempted") and deploy.get("ok"):
                    deadline = (now + dt.timedelta(hours=LIVENESS_WINDOW_HOURS)).isoformat()
                    expect_json = json.dumps(deploy.get("expectedAlerts") or [])
                elif repo_entry.get("deployOnMerge") and isinstance(merge_sha, str) and _FULL_SHA_RE.match(merge_sha):
                    # item 1b (STATE.md §47/§48): this repo's CI/CD IS the
                    # deploy (GitHub Actions -> RollHook, DESIGN.md § Deploy)
                    # — there is no ssh half to have set `deploy.attempted`,
                    # so this branch only runs once the ssh-deploy case above
                    # has already said no (hermes-cc.sh's own
                    # run_deploy_if_enabled() returns attempted:false for a
                    # repo with no `autoDeploy` key, which a deployOnMerge
                    # repo deliberately never declares — see the policy
                    # entry's own comment). The Actions run is the deploy
                    # RECEIPT the ssh path can never supply (STATE.md §47);
                    # owner/repo parsed from the PR url the same way
                    # reconcile_operations() already does, never a second URL
                    # parser (_parse_pr_url()).
                    parsed = _parse_pr_url(item["pr_url"])
                    runs = _run_gh_run_list(parsed[0], parsed[1], merge_sha) if parsed else None
                    receipt_deploy = {"mechanism": "deploy-on-merge", "commit": merge_sha,
                                       "runs": runs if runs is not None else "unknown"}
                    deadline = (now + dt.timedelta(hours=LIVENESS_WINDOW_HOURS)).isoformat()
                    expect_json = json.dumps([{"commit": merge_sha}])
                receipt = json.dumps({
                    "pullRequest": merge_call.result.get("pullRequest"),
                    "mergeCommit": merge_sha,
                    "branch": merge_call.result.get("branch"),
                    "mergeMethod": merge_call.result.get("mergeMethod"),
                    "deploy": receipt_deploy,
                })
                complete_operation(conn, op_id, outcome="done", receipt=receipt)
                if deadline is not None:
                    _set_state(conn, item["event_id"], STATE_LIVENESS_PENDING, now,
                               liveness_deadline=deadline, deploy_expect_json=expect_json, note=None)
                elif repo_entry.get("deployOnMerge"):
                    # deployOnMerge is set but the merge carried no usable
                    # sha — a probe with nothing real to compare against
                    # would sit until liveness_deadline and then reopen the
                    # item to `new`, a worse outcome than an honest `merged`.
                    reason = (f"deployOnMerge is set for this repo but the merge result carried no "
                              f"usable mergeCommit ({merge_sha!r})")
                    _set_state(conn, item["event_id"], STATE_MERGED, now, note=reason)
                else:
                    reason = deploy.get("reason") or "merged; no deploy configured for this repo"
                    _set_state(conn, item["event_id"], STATE_MERGED, now, note=reason)
                conn.commit()
            elif merge_call.result.get("exitCode") == _HERMES_CC_EX_REMOTE:
                # A REMOTE failure, not a refusal. hermes-cc.sh reaches
                # `remote_err` only once it is already talking to something —
                # so this is reachable AFTER `PUT /pulls/:pr/merge` has been
                # sent and even after it succeeded (a lost response, a
                # timeout inside the branch-delete or the ssh deploy that
                # follows it). Reading it as `merge_blocked` is DESIGN.md §
                # Crash recovery's "silently read as failure" arriving through
                # a parsed error object instead of a lost process. Leave the
                # operation open and the item in `validating`;
                # reconcile_operations() asks GitHub — which is authoritative
                # about whether the PR merged — before this function can run
                # again. See _HERMES_CC_EX_REMOTE for why 2/4/64 are not here.
                print(f"triage: merge for {item['signature']} returned a REMOTE error "
                      f"({merge_call.result.get('error')}) — left unresolved for reconcile_operations()",
                      file=sys.stderr)
                conn.commit()
            else:
                # hermes-cc.sh completed and gave a definite refusal (its own
                # gate, or a GitHub 409/405) — a real answer, not an ambiguity.
                complete_operation(conn, op_id, outcome="failed",
                                   receipt=json.dumps({"error": merge_call.result.get("error")}))
                note = f"merge refused: {merge_call.result.get('error') or 'unknown reason'}"
                _set_state(conn, item["event_id"], STATE_MERGE_BLOCKED, now, note=note)
                conn.commit()
        fresh_item, fresh_event = _get_item(conn, item["event_id"]), _get_event(conn, item["event_id"])
        if fresh_item is not None and fresh_event is not None:
            sync_card(conn, [fresh_item], [fresh_event], policy, dry_run=False)


def maybe_check_liveness(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime,
                          *, dry_run: bool) -> None:
    """Step 10. The item must not close because the alert went quiet — a
    fully-down service is also quiet, same principle as
    resolve_quiet_grouped()/resolve_recovery_paired() above. Runs the
    repo's declared `liveness` probe (config/triage-policy.json's
    `repos.<repo>.liveness`, same closed-allowlist shape as `verb`/
    `evidence`) against the alert definition(s) captured at deploy time.
    Only a genuine POSITIVE match resolves the item. Past
    `liveness_deadline` with no positive match, the item REOPENS to `new`,
    carrying the full history (the pull request, the last liveness check) —
    the exact context that was missing when the same alert was
    re-diagnosed 61 times before this file existed."""
    items = conn.execute("SELECT * FROM triage_items WHERE state=?", (STATE_LIVENESS_PENDING,)).fetchall()
    for item in items:
        repo_entry = (policy.get("repos") or {}).get(item["repo"] or "") or {}
        key = repo_entry.get("liveness")
        gatherer = LIVENESS_ALLOWLIST.get(key) if key else None
        try:
            expected = json.loads(item["deploy_expect_json"] or "[]")
        except json.JSONDecodeError:
            expected = []

        if gatherer is None:
            live_ok, detail = False, f"no liveness key declared for repo {item['repo']!r}"
        else:
            ran_ok, result = _run_bounded(gatherer, expected, timeout=EVIDENCE_TIMEOUT)
            live_ok, detail = result if ran_ok else (False, f"liveness probe error: {result}")

        now_iso = _now_iso(now)
        if live_ok:
            if dry_run:
                print(f"[dry-run] would resolve {item['signature']} on confirmed liveness: {detail}")
                continue
            note = f"{LIVENESS_CONFIRMED_NOTE_PREFIX}{detail}"
            # -> STATE_FIXED, the only producer of it in this file: a change
            # landed (merged + deployed) AND a live probe confirmed it, the
            # one place a POSITIVE PROBE — not silence, not an inbound
            # message — backs the claim.
            _set_state(conn, item["event_id"], STATE_FIXED, now, note=note)
            conn.commit()
            fresh_item, fresh_event = _get_item(conn, item["event_id"]), _get_event(conn, item["event_id"])
            if fresh_item is not None and fresh_event is not None:
                sync_card(conn, [fresh_item], [fresh_event], policy, dry_run=False)
            continue

        deadline = _parse_ts(item["liveness_deadline"])
        if deadline is not None and now < deadline:
            continue  # still inside the window — try again next run
        if dry_run:
            print(f"[dry-run] would REOPEN {item['signature']} — liveness never confirmed: {detail}")
            continue

        # Reopen carries history on the card itself (mirrors _dissolve_cluster()'s
        # own final-update-then-reset shape) — never through sync_card()/
        # render_card_blocks(), because the row is about to land back in
        # `new`, which must never be carded (see CARDED_STATES's own comment).
        history = (f"PR: {item['pr_url'] or '(none)'} — reopened, liveness never confirmed "
                   f"within the window. Last check: {detail}. Still failing as of "
                   f"{_fmt_ts(now_iso)}.")
        if item["card_channel"] and item["card_ts"]:
            token = resolve_slack_token()
            if token:
                blocks = [
                    {"type": "header", "text": {"type": "plain_text",
                     "text": ":recycle: Reopened — liveness never confirmed"}},
                    {"type": "section", "text": {"type": "mrkdwn", "text": _escape(history)[:SECTION_TEXT_MAX]}},
                ]
                update_blocks(item["card_channel"], item["card_ts"], blocks,
                              "Reopened — liveness never confirmed", token)
        _set_state(conn, item["event_id"], STATE_NEW, now, note=history)
        conn.commit()


# --- the clock ----------------------------------------------------------------

def sweep_deadlines(conn: sqlite3.Connection, now: dt.datetime, *, dry_run: bool) -> None:
    """The only thing that acts on `state_deadline`. Every non-terminal state
    names a poller and a deadline (STATE_DEADLINES); this is what happens when
    the poller did not deliver in time.

    Runs LAST of the pollers and BEFORE the card sync, and both halves of that
    matter. A poller that can still advance an item this pass must get its
    chance before the clock takes it away — an item that reached its 2h mark
    thirty seconds before its episode finished should finish, not expire. And
    an expiry the human never sees is the same as no expiry at all, so the
    state change has to land before sync_card() renders this pass's cards.

    UNDER --dry-run THIS REPORTS AND DOES NOT WRITE, which is a departure from
    apply_resolutions()/classify()/resolve_quiet_grouped() and is deliberate.
    Those three move an item between working states; this one can move it
    TERMINALLY, to `dismissed`, and `--dry-run` defaults to the live ledger.
    "Never touches Slack, never shells out, everything else real" was written
    around writes that the next real pass would re-derive anyway. A terminal
    dismissal is not re-derivable — `reopen_if_needed()` is the only way back
    and it needs a fresh occurrence. A preview must not be able to end an item.
    It prints every expiry it WOULD apply, because a deadline that fires
    silently is indistinguishable from a loop that is not firing them at all.

    A NON-TERMINAL row with a NULL `state_deadline` is stamped `now + hours`
    and reported. Two anchors were possible and only one is safe: `updated_at`
    is rewritten by ingest() on every recurrence, so a deadline derived from it
    would move further away every pass and never fire while looking
    authoritative. `now` cannot drift that way, and it is deliberately
    conservative — a row already six days into `needs_human` gets a fresh 168h
    rather than expiring at once.

    Reporting WITHOUT stamping was the first version and it was wrong in the
    only case that occurs: every NULL-deadline row on the live ledger is
    `needs_human`, and only a human transitions one, so "it gets a deadline
    when it next transitions" meant never. It stays a FINDING because a NULL
    deadline is either a pre-column legacy row or a bug in a transition site,
    and silently fixing the second is how it stays a bug.

    NARROW exception to "write a fresh generic note": a `split` row's `note`
    carries the dissolve verdict under SPLIT_VERDICT_NOTE_PREFIX (see
    _dissolve_cluster()) — the one unactioned obligation this sweeper can
    expire. Overwriting it at the exact moment the row finally becomes
    visible again (split -> needs_human, which IS carded) would lose the
    verdict a second time, on top of the loss STATE.md §43 already recorded
    once. So THIS ONE CASE appends the expiry note to the existing one
    instead of replacing it. Deliberately NOT generalised to every state's
    prior note — the other five expiry paths' prior notes are HISTORICAL
    (an old written fix, a stale env-check remediation), not a pending
    obligation being handed to the deadline for the first time, and
    preserving them too is a different, unasked-for change."""
    now_iso = _now_iso(now)
    # The string comparison is a prefilter over the state_deadline index (the
    # same shape unsnooze_if_expired() uses on snoozed_until); _parse_ts()
    # below makes the actual decision, so a differently-formatted timestamp
    # can never expire an item on a lexical accident.
    rows = conn.execute(
        "SELECT event_id, signature, state, state_deadline, note FROM triage_items "
        "WHERE state_deadline IS NOT NULL AND state_deadline<=? ORDER BY event_id",
        (now_iso,),
    ).fetchall()
    for row in rows:
        rule = STATE_DEADLINES.get(row["state"])
        if rule is None or rule.on_expiry is None:
            continue
        if rule.deadline_column != _STATE_DEADLINE_COLUMN:
            # `liveness_pending` and `snoozed` own their own window and their
            # own poller. A stale value in this column on one of them is not
            # this sweeper's to act on.
            continue
        deadline = _parse_ts(row["state_deadline"])
        if deadline is None or now < deadline:
            continue
        note = (f"{DEADLINE_EXPIRED_NOTE_PREFIX}sat in `{row['state']}` for its full "
                f"{rule.hours:g}h without {rule.poller} advancing it (deadline "
                f"{_fmt_ts(row['state_deadline'])}). Moved to `{rule.on_expiry}` by the clock, "
                f"not by a decision")
        if rule.reason:
            note += f" — reason `{rule.reason}`"
        note += "."
        prior_note = row["note"] or ""
        if row["state"] == STATE_SPLIT and prior_note.startswith(SPLIT_VERDICT_NOTE_PREFIX):
            # See this function's own docstring, "NARROW exception" paragraph
            # — this is the one prior note that is itself an unactioned
            # obligation, not history, so it is appended rather than lost.
            note = f"{note}\n\n{prior_note}"
        if dry_run:
            print(f"[dry-run] would expire {row['signature']} (event {row['event_id']}): "
                  f"{row['state']} -> {rule.on_expiry} after {rule.hours:g}h with no advance "
                  f"from {rule.poller}")
            continue
        _set_state(conn, row["event_id"], rule.on_expiry, now, note=note)
        print(f"triage: deadline expired for {row['signature']} (event {row['event_id']}): "
              f"{row['state']} -> {rule.on_expiry} after {rule.hours:g}h with no advance "
              f"from {rule.poller}")
    conn.commit()

    expected = [s for s, r in STATE_DEADLINES.items() if r.deadline_column == _STATE_DEADLINE_COLUMN]
    placeholders = ",".join("?" * len(expected))
    for row in conn.execute(
        f"SELECT event_id, signature, state FROM triage_items "
        f"WHERE state_deadline IS NULL AND state IN ({placeholders}) ORDER BY event_id",
        expected,
    ).fetchall():
        # Stamp it from NOW, and say so. Two anchors were possible and only one
        # is safe: `updated_at` is rewritten by ingest() on every recurrence, so
        # a deadline derived from it would move further away every pass and
        # never fire — which is the bug this whole slice closes, rebuilt. `now`
        # cannot do that. It is deliberately CONSERVATIVE: a row that has
        # already sat in `needs_human` for six days gets a fresh 168h rather
        # than expiring immediately, and erring toward not-discarding is the
        # same call item 1 made.
        #
        # Leaving these as a report only was the first version of this, and it
        # was wrong in the one case that matters: the rows carrying a NULL
        # deadline on the live ledger are all `needs_human`, and the only thing
        # that transitions a `needs_human` row is a human — so "it gets a real
        # one when it next transitions" means "never", for exactly the
        # population that must not sit forever. It would have printed the same
        # four lines every 600s and bounded nothing.
        #
        # Still printed as a FINDING, once, at stamp time: a NULL deadline on a
        # non-terminal row can only be a pre-column legacy row or a bug in a
        # transition site, and silently fixing the second is how it stays a bug.
        rule = STATE_DEADLINES[row["state"]]
        stamped = now + dt.timedelta(hours=rule.hours)
        if dry_run:
            print(f"[dry-run] would stamp {row['signature']} (event {row['event_id']}) in "
                  f"{row['state']!r} with a {rule.hours:g}h deadline", file=sys.stderr)
            continue
        conn.execute(
            "UPDATE triage_items SET state_deadline=? WHERE event_id=? AND state_deadline IS NULL",
            (_now_iso(stamped), row["event_id"]),
        )
        print(f"triage: FINDING — {row['signature']} (event {row['event_id']}) was in non-terminal "
              f"state {row['state']!r} with a NULL state_deadline, so nothing would ever have timed "
              f"it out. Stamped {rule.hours:g}h from now ({_fmt_ts(_now_iso(stamped))}), not from "
              f"updated_at, which ingest() rewrites every pass. If this row is not a pre-column "
              f"legacy row, a transition site is failing to write a deadline.", file=sys.stderr)
    conn.commit()


# --- propose_mappings() — step 8, the one LLM call in this file --------------
#
# See the module docstring's PROPOSE MAPPINGS paragraph for the full contract.

def _resolve_openai_base_url() -> str:
    """OPENAI_BASE_URL is a plain literal in .env.tpl (not a secret — it is
    already committed to this repo), so this only ever needs the inherited
    process env or that same template file, never secrets-run."""
    val = os.environ.get("OPENAI_BASE_URL", "")
    if val:
        return val
    try:
        text = (HERMES_HOME / ".env.tpl").read_text()
    except OSError:
        return ""
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("OPENAI_BASE_URL="):
            return line.split("=", 1)[1].split("#", 1)[0].strip()
    return ""


def _resolve_openai_api_key() -> str:
    """Mirrors resolve_slack_token()'s own hand-fallback shape exactly (env
    var first, else the secrets-run shim against the SAME op:// ref .env.tpl
    declares for OPENAI_API_KEY) — never a plaintext key, and never crosses
    an argv/`ps` boundary."""
    val = os.environ.get("OPENAI_API_KEY", "")
    if val:
        return val
    env = os.environ.copy()
    env["PATH"] = "/opt/homebrew/bin:/usr/local/bin:" + env.get("PATH", "/usr/bin:/bin")
    secrets_run = Path.home() / ".local" / "bin" / "secrets-run"
    try:
        r = subprocess.run(
            [str(secrets_run), "read", _OPENAI_API_KEY_REF],
            capture_output=True, text=True, timeout=15, env=env,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return r.stdout.strip() if r.returncode == 0 else ""


def _discoverable_repos() -> set[str]:
    """Mirrors hermes-cc.sh's own resolve_repo()/discoverable() exactly: every
    top-level entry directly under `root` that is not dotted, not `deny`d,
    is a directory, and carries a `.git` subdirectory. A `map` proposal
    naming anything outside this set is dropped at apply time — never
    trusted from the model's own claim, or from the prompt's own list, alone
    (see BOUNDS THAT DO NOT MOVE in the module docstring)."""
    try:
        data = json.loads(DISPATCH_REPOS_JSON.read_text())
    except (OSError, json.JSONDecodeError):
        return set()
    root = Path(os.path.realpath(os.path.expanduser(data.get("root", "~/SourceRoot"))))
    deny = set(data.get("deny") or [])
    try:
        entries = os.listdir(root)
    except OSError:
        return set()
    out: set[str] = set()
    for name in entries:
        if name.startswith(".") or name in deny:
            continue
        p = root / name
        if p.is_dir() and (p / ".git").exists():
            out.add(name)
    return out


def _signature_first_seen(row: sqlite3.Row) -> dt.datetime | None:
    """When this SIGNATURE was first seen, not when its current open period began.

    A grouped source's payload carries `ts_first` — a unix timestamp set the very
    first time the dedup key appeared and never reset — while `events.first_seen`
    is rewritten every time reconcile() reopens a resolved row. Any question of
    the form "has this been going on a while" must read the former; the latter
    answers a different question and understates it badly."""
    payload = _safe_json(row["payload_json"] if "payload_json" in row.keys() else None)
    raw = payload.get("ts_first")
    if raw is None:
        return None
    try:
        return dt.datetime.fromtimestamp(float(raw), dt.timezone.utc)
    except (TypeError, ValueError):
        return None


def _propose_mapping_candidates(conn: sqlite3.Connection, policy: dict[str, Any],
                                 now: dt.datetime) -> list[sqlite3.Row]:
    """Every item classify() found no rule for, whose event is at least
    `proposeMappingsAgeDays` old, excluding anything the model already said
    `unsure` about within PROPOSE_UNSURE_COOLDOWN_DAYS — oldest first, capped
    at PROPOSE_MAPPINGS_MAX_SIGNATURES.

    Deliberately NOT restricted to `state = new`. Keying the pool on current
    state made this whole pass inert: resolve_quiet_grouped() runs earlier in
    the same cycle, so a grouped `slack_alert` signature flips to `resolved` on
    the 2h quiet timer long before this query sees it. Measured against the
    live DB, 16 of 19 unmapped signatures vanished that way and the other 3
    were younger than the age floor — zero candidates, permanently.

    The question this pass answers is "has this signature ever been mapped",
    which is a property of the POLICY FILE, not of an item's lifecycle. A
    signature that resolved quietly is still unmapped and will fire again; that
    is precisely the case worth mapping. `ignored` is the one state excluded —
    a human or a rule already decided it deliberately, and re-proposing it
    would relitigate a settled call. Items are collapsed per signature, since
    the same signature can own several rows over time."""
    age_days = policy["proposeMappingsAgeDays"]
    # Age alone is the wrong test on its own. The floor exists to avoid spending a
    # proposal on a one-off, but a signature that has already fired many times is
    # demonstrably not one — `homelab-temperature-above-threshold` had 25
    # occurrences in 5 days and would have sat under a 7-day floor while paging
    # the whole time. Either signal qualifies it: old enough to have proven
    # persistent, OR frequent enough to have proven the same thing faster.
    min_occurrences = PROPOSE_MAPPINGS_MIN_OCCURRENCES
    unsure_cutoff = now - dt.timedelta(days=PROPOSE_UNSURE_COOLDOWN_DAYS)
    rows = conn.execute(
        "SELECT ti.event_id, ti.signature, ti.occurrences, ti.first_seen, ti.last_seen, "
        "ti.propose_unsure_at, e.title, e.payload_json FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        "WHERE ti.state != ? AND ti.repo IS NULL AND ti.verb IS NULL "
        "GROUP BY ti.signature ORDER BY MIN(ti.first_seen) ASC",
        (STATE_IGNORED,),
    ).fetchall()
    candidates: list[sqlite3.Row] = []
    for row in rows:
        # Age must be measured from when the SIGNATURE was first seen, not from
        # when this row's current open period began. For a grouped source those
        # differ by months: `homelab-temperature-above-threshold` carries
        # ts_first = 2026-04-30 in its payload while events.first_seen reads
        # 2026-09-04, because reconcile() reopens a resolved row with a fresh
        # first_seen. Reading the row's own column made a 132-day-old recurring
        # signature look five days old and kept it under every floor.
        first_seen = _signature_first_seen(row) or _parse_ts(row["first_seen"])
        old_enough = first_seen is not None and now - first_seen >= dt.timedelta(days=age_days)
        # occurrences is the LAST poll's batch count, not a cumulative total, so
        # it cannot stand in for persistence on its own — it is a fast path for a
        # signature that fired hard in one window, nothing more.
        frequent_enough = (row["occurrences"] or 0) >= min_occurrences
        if not (old_enough or frequent_enough):
            continue
        unsure_at = _parse_ts(row["propose_unsure_at"])
        if unsure_at is not None and unsure_at > unsure_cutoff:
            continue
        candidates.append(row)
        if len(candidates) >= PROPOSE_MAPPINGS_MAX_SIGNATURES:
            break
    return candidates


def _build_propose_mappings_prompt(candidates: list[sqlite3.Row], repo_names: list[str]) -> str:
    lines = [
        "You maintain a signature -> repo mapping table for an infrastructure alert "
        "triage system. Each signature below has fired repeatedly for at least a week "
        "with no owning repo, so it never escalates and never gets a card.",
        "",
        "For EACH signature, decide exactly ONE of:",
        '  {"action": "ignore", "reason": "<one line>"}  — a known-benign pattern or a '
        "genuine recovery, safe to silence forever",
        '  {"action": "map", "repo": "<repo name>", "reason": "<one line>"}  — future '
        "occurrences should open a read-only investigation of this repo",
        '  {"action": "unsure"}  — you cannot confidently decide either way',
        "",
        "Valid repo names — a name outside this list is dropped, never applied:",
        ", ".join(repo_names) if repo_names else "(none discoverable)",
        "",
        "Respond with STRICT JSON ONLY: a single JSON object keyed by the EXACT signature "
        "string, each value one of the three shapes above. No prose, no markdown fences, "
        "no extra keys, no signatures other than the ones listed below.",
        "",
        "Signatures:",
    ]
    for c in candidates:
        lines.append(
            f"- signature: {c['signature']}\n"
            f"  title: {c['title'] or ''}\n"
            f"  occurrences: {c['occurrences']}\n"
            f"  first_seen: {c['first_seen']}\n"
            f"  last_seen: {c['last_seen']}"
        )
    return "\n".join(lines)


def _call_propose_mappings_model(prompt: str) -> dict[str, Any] | None:
    """The ONLY LLM call in this file. One request, strict JSON, hard-bounded
    on every axis this loop can bound (timeout, output tokens, and the
    caller's own cap on how many signatures went into the prompt). ANY
    failure — unresolved secrets, network, timeout, non-2xx, unexpected
    response shape, or unparseable JSON — is caught here and returns None;
    propose_mappings() logs to stderr and moves on. This loop must never
    depend on this call succeeding."""
    base_url = _resolve_openai_base_url()
    api_key = _resolve_openai_api_key()
    if not base_url or not api_key:
        print("triage: propose_mappings — OPENAI_BASE_URL/OPENAI_API_KEY unresolved, skipping",
              file=sys.stderr)
        return None
    body = json.dumps({
        "model": PROPOSE_MAPPINGS_MODEL,
        "messages": [
            {"role": "system", "content": "You output strict JSON only — no prose, no markdown fences."},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": PROPOSE_MAPPINGS_MAX_OUTPUT_TOKENS,
        "temperature": 0,
    }).encode()
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/chat/completions",
        data=body,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=PROPOSE_MAPPINGS_TIMEOUT) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, TimeoutError, OSError) as e:
        print(f"triage: propose_mappings — model call failed: {e}", file=sys.stderr)
        return None
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        print(f"triage: propose_mappings — unexpected response shape: {str(data)[:300]}", file=sys.stderr)
        return None
    content = (content or "").strip()
    if content.startswith("```"):
        content = content.strip("`")
        if content[:4].lower() == "json":
            content = content[4:]
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as e:
        print(f"triage: propose_mappings — response was not valid JSON: {e}", file=sys.stderr)
        return None
    if not isinstance(parsed, dict):
        print("triage: propose_mappings — response JSON was not an object, skipping", file=sys.stderr)
        return None
    return parsed


def _apply_propose_mappings(conn: sqlite3.Connection, now: dt.datetime, response: dict[str, Any],
                             candidates_by_sig: dict[str, sqlite3.Row],
                             valid_repos: set[str]) -> list[dict[str, str]]:
    """Applies each decision for a signature actually in THIS batch (a
    signature the model invents is ignored — it was never asked about). A
    `map` repo is re-checked against `valid_repos` (the SAME discovery
    hermes-cc.sh's own resolve_repo() uses) AND the deny list — never
    trusted from the model alone. `unsure` writes a cooldown marker directly
    to triage_items; `map`/`ignore` write NOTHING to triage_items here —
    they only ever become policy entries, picked up by classify() on a
    LATER run, exactly like a hand-written rule would be. Returns the
    applied `map`/`ignore` decisions only (for the policy file + digest —
    `unsure` is not "applied", it is deferred)."""
    applied: list[dict[str, str]] = []
    now_iso = _now_iso(now)
    denied = _denied_repos()
    for sig, decision in response.items():
        row = candidates_by_sig.get(sig)
        if row is None:
            continue
        if not isinstance(decision, dict):
            print(f"triage: propose_mappings — malformed decision for {sig!r}, dropping", file=sys.stderr)
            continue
        action = decision.get("action")
        reason = decision.get("reason") if isinstance(decision.get("reason"), str) else ""
        if action == "ignore":
            applied.append({"signature": sig, "action": "ignore", "reason": reason})
        elif action == "map":
            repo = decision.get("repo")
            if not isinstance(repo, str) or repo not in valid_repos or repo in denied:
                print(f"triage: propose_mappings — dropping map proposal for {sig!r}: repo "
                      f"{repo!r} does not resolve under the dispatch root or is denied", file=sys.stderr)
                continue
            applied.append({"signature": sig, "action": "map", "repo": repo, "reason": reason})
        elif action == "unsure":
            conn.execute(
                "UPDATE triage_items SET propose_unsure_at=?, updated_at=? WHERE event_id=?",
                (now_iso, now_iso, row["event_id"]),
            )
        else:
            print(f"triage: propose_mappings — unknown action {action!r} for {sig!r}, dropping", file=sys.stderr)
    conn.commit()
    return applied


def _dump_policy_json(data: dict[str, Any]) -> str:
    """Serializes the policy file preserving top-level key order, rendering
    `_readme`/`rules`/`ignore` one array element per line (the file's own
    hand-authored style) rather than json.dump's default fully-expanded
    nesting, which would rewrite every untouched rule and turn one new line
    into a whole-file diff."""
    parts = []
    for key, value in data.items():
        if isinstance(value, list):
            if not value:
                rendered = "[]"
            else:
                items = ",\n    ".join(json.dumps(v) for v in value)
                rendered = "[\n    " + items + "\n  ]"
        else:
            rendered = json.dumps(value, indent=2).replace("\n", "\n  ")
        parts.append(f"  {json.dumps(key)}: {rendered}")
    return "{\n" + ",\n".join(parts) + "\n}\n"


def _write_policy_additions(applied: list[dict[str, str]], now: dt.datetime) -> bool:
    """Reads the RAW policy JSON (never load_policy()'s normalized/defaulted
    view — that would drop unknown keys and reorder nothing back), appends
    one stamped rules/ignore entry per applied proposal, and writes it back
    preserving `_readme` and key order. False (no-op) on any read/parse
    failure or an empty `applied` list."""
    if not applied:
        return False
    try:
        data = json.loads(POLICY_PATH.read_text())
    except (OSError, json.JSONDecodeError) as e:
        print(f"triage: propose_mappings — cannot read policy file to apply proposals: {e}", file=sys.stderr)
        return False
    stamp = _now_iso(now)
    rules = list(data.get("rules") or [])
    ignore = list(data.get("ignore") or [])
    for item in applied:
        if item["action"] == "map":
            rules.append({
                "match": item["signature"],
                "repo": item["repo"],
                "proposedAt": stamp,
                "proposedBy": "triage-auto",
                "reason": item["reason"],
            })
        elif item["action"] == "ignore":
            ignore.append({
                "match": item["signature"],
                "proposedAt": stamp,
                "proposedBy": "triage-auto",
                "reason": item["reason"],
            })
    data["rules"] = rules
    data["ignore"] = ignore
    POLICY_PATH.write_text(_dump_policy_json(data))
    return True


def _policy_git_rel_path() -> Path | None:
    try:
        return POLICY_PATH.resolve().relative_to(TRIAGE_REPO_DIR.resolve())
    except (OSError, ValueError):
        return None


def _policy_path_is_dirty(rel: Path) -> bool:
    """True if config/triage-policy.json ALREADY carries a pending
    staged-or-unstaged change before this run's own write — checked before
    _write_policy_additions() ever touches the file, so a human's own
    in-progress edit is never swept into an auto-authored commit. Also true
    (fail closed) if `git status` itself cannot be run at all."""
    try:
        res = subprocess.run(
            ["git", "-C", str(TRIAGE_REPO_DIR), "status", "--porcelain", "--", str(rel)],
            capture_output=True, text=True, timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return True
    if res.returncode != 0:
        return True
    return bool(res.stdout.strip())


def _git_commit_policy_file(rel: Path, applied: list[dict[str, str]]) -> None:
    """`git add` + `git commit` ONLY config/triage-policy.json, in this
    repo's own checkout — never `git add -A`, never `git push` (a human
    sends anything further). The file is symlinked live into
    ~/.hermes/config/, so the change already took effect the moment
    _write_policy_additions() returned; committing turns that into a
    reviewable diff instead of the dirty-working-tree drift this repo has
    been bitten by before (see CLAUDE.md "After any edit: commit here"). No
    lock is taken — this is the only writer of this file — the dirtiness
    check the caller already did before writing is what keeps this from
    sweeping an unrelated pending edit into an auto-authored commit."""
    repo_dir = str(TRIAGE_REPO_DIR)
    summary = "; ".join(
        f"{a['signature']} -> {a['repo']}" if a["action"] == "map" else f"{a['signature']} -> ignore"
        for a in applied
    )
    msg = f"chore(triage-policy): auto-propose {len(applied)} mapping(s)\n\n{summary}"
    add = subprocess.run(["git", "-C", repo_dir, "add", "--", str(rel)],
                          capture_output=True, text=True, timeout=30)
    if add.returncode != 0:
        print(f"triage: propose_mappings — git add failed: {add.stderr.strip()}", file=sys.stderr)
        return
    commit = subprocess.run(["git", "-C", repo_dir, "commit", "-m", msg, "--", str(rel)],
                             capture_output=True, text=True, timeout=30)
    if commit.returncode != 0:
        print(f"triage: propose_mappings — git commit failed: {commit.stderr.strip()}", file=sys.stderr)


def propose_mappings(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime,
                      *, dry_run: bool) -> list[dict[str, str]]:
    """Step 8 — see the module docstring's PROPOSE MAPPINGS paragraph for the
    full contract. Returns the applied map/ignore proposals (possibly
    empty), so run() can hand them to maybe_post_daily_digest() for
    announcement the same day. Under --dry-run this makes zero calls of any
    kind (model, git) and returns []  — matching every other externally
    visible action in this file's DRY-RUN CONTRACT."""
    if dry_run:
        return []

    row = conn.execute("SELECT value FROM cursors WHERE key=?", (PROPOSE_MAPPINGS_CURSOR_KEY,)).fetchone()
    if row is not None:
        last_run = _parse_ts(row["value"])
        if last_run is not None and now - last_run < dt.timedelta(hours=24):
            return []

    candidates = _propose_mapping_candidates(conn, policy, now)
    # The cursor is a once-per-24h BUDGET, not a "keep retrying until it
    # succeeds" loop — stamped here, before the call, so a failed call still
    # counts against today's attempt rather than hammering the endpoint on
    # every 10-minute cycle until one happens to succeed.
    now_iso = _now_iso(now)
    conn.execute(
        "INSERT INTO cursors(key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (PROPOSE_MAPPINGS_CURSOR_KEY, now_iso, now_iso),
    )
    conn.commit()
    if not candidates:
        return []

    valid_repos = _discoverable_repos()
    prompt = _build_propose_mappings_prompt(candidates, sorted(valid_repos))
    response = _call_propose_mappings_model(prompt)
    if response is None:
        return []  # already logged by the call itself

    candidates_by_sig = {c["signature"]: c for c in candidates}
    applied = _apply_propose_mappings(conn, now, response, candidates_by_sig, valid_repos)
    if not applied:
        return []

    rel = _policy_git_rel_path()
    if rel is None:
        print(f"triage: propose_mappings — {POLICY_PATH} is outside the repo checkout at "
              f"{TRIAGE_REPO_DIR}, skipping this run's proposals entirely (not written, not "
              "committed)", file=sys.stderr)
        return []
    if _policy_path_is_dirty(rel):
        print(f"triage: propose_mappings — {rel} already has a pending change, skipping this "
              "run's proposals entirely rather than sweeping it into an auto-authored commit",
              file=sys.stderr)
        return []
    if not _write_policy_additions(applied, now):
        return []
    _git_commit_policy_file(rel, applied)
    return applied


# --- unmapped-signature digest -------------------------------------------------

def _fetch_note_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT ti.signature, e.title FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        "WHERE ti.state=? ORDER BY ti.updated_at DESC",
        (STATE_NOTE,),
    ).fetchall()


def maybe_post_daily_digest(conn: sqlite3.Connection, policy: dict[str, Any], unmapped: set[str],
                             now: dt.datetime, *, dry_run: bool,
                             auto_mapped: list[dict[str, str]] | None = None) -> None:
    """One Slack message, at most once per UTC day, with up to three
    sections: unmapped signatures (no policy rule matched — see classify()),
    STATE_NOTE rows (unstructured #alerts prose that might be an unactioned
    root cause — see that state's own docstring), and whatever
    propose_mappings() just auto-added THIS run (see PROPOSE MAPPINGS) — the
    latter is how an auto-authored policy commit gets ANNOUNCED rather than
    only discovered later in `git log`. All three are silent by default;
    this is the only place any of them becomes visible."""
    auto_mapped = auto_mapped or []
    notes = _fetch_note_rows(conn)
    if not unmapped and not notes and not auto_mapped:
        return
    today = now.date().isoformat()
    row = conn.execute("SELECT value FROM cursors WHERE key=?", (DAILY_DIGEST_CURSOR_KEY,)).fetchone()
    if row and row["value"] == today:
        return

    lines: list[str] = []
    if auto_mapped:
        lines.append("*Auto-proposed triage-policy changes* — added to `triage-policy.json` and "
                      "committed this run (`proposedBy: triage-auto`):")
        for a in auto_mapped:
            target = f"repo `{a['repo']}`" if a["action"] == "map" else "`ignore`"
            reason = a.get("reason") or "(no reason given)"
            lines.append(f"- `{a['signature']}` -> {target} — {reason}")
    if unmapped:
        if lines:
            lines.append("")
        sigs = sorted(unmapped)
        lines.append("*Unmapped triage signatures* — no rule in `triage-policy.json`, so these never escalate:")
        lines.extend(f"- `{s}`" for s in sigs[:20])
        if len(sigs) > 20:
            lines.append(f"… and {len(sigs) - 20} more")
    if notes:
        if lines:
            lines.append("")
        lines.append("*Unstructured notes in #alerts* — possible root causes nobody actioned:")
        for r in notes[:20]:
            title = (r["title"] or "").strip()
            truncated = title[:140] + ("…" if len(title) > 140 else "")
            lines.append(f"- `{r['signature']}` — {truncated}")
        if len(notes) > 20:
            lines.append(f"… and {len(notes) - 20} more")

    text = "\n".join(lines)
    channel = _card_channel(policy)
    if dry_run:
        print(f"[dry-run] would post daily digest ({len(unmapped)} unmapped, {len(notes)} notes, "
              f"{len(auto_mapped)} auto-mapped) to {channel}")
        return
    token = resolve_slack_token()
    if not token:
        print("triage: no Slack token, cannot post daily digest", file=sys.stderr)
        return
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": text[:SECTION_TEXT_MAX]}}]
    ok, _ts = post_blocks(channel, blocks, text, token)
    if not ok:
        print("triage: daily digest post failed", file=sys.stderr)
        return
    conn.execute(
        "INSERT INTO cursors(key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (DAILY_DIGEST_CURSOR_KEY, today, _now_iso(now)),
    )
    conn.commit()


# --- the loop --------------------------------------------------------------

def run(conn: sqlite3.Connection, *, dry_run: bool) -> int:
    now = dt.datetime.now(dt.timezone.utc)
    policy = load_policy()

    # Step -1 — MUST run before anything else in this pass, including
    # drain_intents(): an item sitting under an in-flight operation (a
    # process that crashed, or an ambiguous hermes-cc.sh return the call
    # site deliberately left unresolved) must be reconciled before any
    # poller gets a chance to retry the same external call. See
    # reconcile_operations()'s own docstring and the module docstring's
    # step -1.
    reconcile_operations(conn, policy, now, dry_run=dry_run)

    drain_intents(conn, dry_run=dry_run)
    ingest(conn, now)
    reopen_if_needed(conn, now)
    unsnooze_if_expired(conn, now)
    unmapped = classify(conn, policy, now)
    apply_resolutions(conn, now)
    resolve_recovery_paired(conn, policy, now, dry_run=dry_run)
    resolve_quiet_grouped(conn, policy, now)
    maybe_dissolve_clusters(conn, now, dry_run=dry_run)

    escalate(conn, policy, now, dry_run=dry_run)
    run_verbs(conn, policy, now, dry_run=dry_run)

    # Steps 6-10 — verdict -> implement -> validate -> merge -> deploy ->
    # verify. Each is a poll-once-per-run step over its own state, so this
    # ordering (implement before validation before liveness) lets an item
    # that crossed a stage earlier THIS SAME RUN also be picked up by the
    # next stage rather than waiting a full 10 minutes — never required for
    # correctness (each stage re-derives its own eligibility from the DB
    # every run regardless), just fewer idle cycles.
    maybe_auto_implement(conn, policy, now, dry_run=dry_run)
    poll_implement_jobs(conn, policy, now, dry_run=dry_run)
    poll_validation_jobs(conn, policy, now, dry_run=dry_run)
    maybe_check_liveness(conn, policy, now, dry_run=dry_run)

    # After every poller above, before the cards below — see sweep_deadlines()'s
    # own docstring: a poller that can still advance an item this pass gets its
    # chance before the clock takes it away, and an expiry has to be on the card
    # in the same pass it happens.
    sweep_deadlines(conn, now, dry_run=dry_run)

    for _key, members in _cluster_groups(conn).items():
        members = sorted(members, key=lambda r: r["event_id"])
        event_rows = [_get_event(conn, m["event_id"]) for m in members]
        if any(er is None for er in event_rows):
            continue
        sync_card(conn, members, event_rows, policy, dry_run=dry_run)

    # Step 8 — the one LLM call in this file, at most once per 24h. Runs
    # AFTER classify() so `unmapped` above already reflects this run's own
    # rule matching, and its result feeds directly into today's digest below
    # rather than waiting for a separate delivery mechanism.
    auto_mapped = propose_mappings(conn, policy, now, dry_run=dry_run)

    maybe_post_daily_digest(conn, policy, unmapped, now, dry_run=dry_run, auto_mapped=auto_mapped)

    # No timestamp argument, deliberately — see record_heartbeat().
    record_heartbeat(conn, dry_run=dry_run)
    return 0


# --- Heartbeat ----------------------------------------------------------------

HEARTBEAT_CURSOR_KEY = "triage_last_run"


def record_heartbeat(conn: sqlite3.Connection, *, dry_run: bool) -> None:
    """Write one `cursors` row per completed pass, unconditionally.

    Every other write in this file is conditional on something having CHANGED,
    so a pass that finds nothing eligible leaves no trace at all. That makes
    "the loop ran and had nothing to do" and "the loop did not run" literally
    indistinguishable in the database — measured 2026-09-09, when
    `triage_items.updated_at` showed 5.5h and 6h gaps against this agent's
    600s `StartInterval` and nothing on disk could say which it was.

    `launchctl print`'s `runs` counter cannot settle it either: it counts
    process spawns, not completed work, and it is monotonic across a
    reload — the plist comment already warns against trusting it. This row
    can: `updated_at` is the wall-clock end of the last COMPLETED pass, and
    `value` carries that pass's state census, so an idle run is visible,
    a stalled loop is obvious from a stale `updated_at`, and reading it costs
    one indexed lookup. Skipped under --dry-run, which by definition did not
    complete a real pass.
    """
    if dry_run:
        return
    census = {
        row["state"]: row["n"]
        for row in conn.execute(
            "SELECT state, COUNT(*) AS n FROM triage_items GROUP BY state"
        )
    }
    open_clusters = conn.execute(
        "SELECT COUNT(DISTINCT dispatch_job) AS n FROM triage_items "
        "WHERE state=? AND dispatch_job IS NOT NULL",
        (STATE_INVESTIGATING,),
    ).fetchone()["n"]
    value = json.dumps({"states": census, "open_clusters": open_clusters}, sort_keys=True)
    conn.execute(
        "INSERT INTO cursors(key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        # Stamped HERE, from a clock read at write time — the function takes no
        # timestamp at all, and that is the point. `updated_at` answers "when
        # did this pass COMPLETE" (api.py reads it as exactly that, against a
        # 3x-StartInterval staleness threshold), while every caller has a `now`
        # in hand that is the pass's START. Three scripts each remembering to
        # pass a fresh clock instead of the one already in scope is three
        # chances to stamp the loop as fresh at the moment it began; two of the
        # three got it wrong until 2026-09-09. Removing the parameter makes the
        # mistake unexpressible rather than merely tested — the same argument
        # _set_state() makes for owning `state_deadline`.
        (HEARTBEAT_CURSOR_KEY, value, _now_iso(dt.datetime.now(dt.timezone.utc))),
    )
    conn.commit()


# --- CLI verbs: --snooze / --ignore / --reopen / --close / --list --------------

def _items_for_signature(conn: sqlite3.Connection, signature: str) -> list[sqlite3.Row]:
    """The four CLI verbs address a SIGNATURE, not an event, so they resolve
    it to rows first and transition each through _set_state() — same reason
    unsnooze_if_expired() stopped being one set-based UPDATE: the deadline is
    written with the state, by one function, or it drifts."""
    return conn.execute("SELECT event_id FROM triage_items WHERE signature=?", (signature,)).fetchall()


def _arg_value(argv: list[str], flag: str) -> str | None:
    if flag not in argv:
        return None
    idx = argv.index(flag)
    return argv[idx + 1] if idx + 1 < len(argv) else None


def cmd_snooze(conn: sqlite3.Connection, argv: list[str], now: dt.datetime) -> int:
    signature = _arg_value(argv, "--snooze")
    hours_s = _arg_value(argv, "--hours")
    if not signature:
        print("triage: --snooze needs a signature", file=sys.stderr)
        return 2
    try:
        hours = float(hours_s) if hours_s else 24.0
    except ValueError:
        print(f"triage: --hours must be a number, got {hours_s!r}", file=sys.stderr)
        return 2
    until = (now + dt.timedelta(hours=hours)).isoformat()
    rows = _items_for_signature(conn, signature)
    for row in rows:
        _set_state(conn, row["event_id"], STATE_SNOOZED, now, snoozed_until=until)
    conn.commit()
    if not rows:
        print(f"triage: no triage item for signature {signature!r}", file=sys.stderr)
        return 1
    print(f"snoozed {signature} until {until}")
    return 0


def cmd_ignore(conn: sqlite3.Connection, argv: list[str], now: dt.datetime) -> int:
    signature = _arg_value(argv, "--ignore")
    if not signature:
        print("triage: --ignore needs a signature", file=sys.stderr)
        return 2
    rows = _items_for_signature(conn, signature)
    for row in rows:
        _set_state(conn, row["event_id"], STATE_IGNORED, now, snoozed_until=None)
    conn.commit()
    if not rows:
        print(f"triage: no triage item for signature {signature!r}", file=sys.stderr)
        return 1
    print(f"ignored {signature}")
    return 0


def cmd_reopen(conn: sqlite3.Connection, argv: list[str], now: dt.datetime) -> int:
    signature = _arg_value(argv, "--reopen")
    if not signature:
        print("triage: --reopen needs a signature", file=sys.stderr)
        return 2
    rows = _items_for_signature(conn, signature)
    for row in rows:
        _set_state(conn, row["event_id"], STATE_NEW, now, snoozed_until=None)
    conn.commit()
    if not rows:
        print(f"triage: no triage item for signature {signature!r}", file=sys.stderr)
        return 1
    print(f"reopened {signature}")
    return 0


def cmd_close(conn: sqlite3.Connection, argv: list[str], now: dt.datetime) -> int:
    """`closed`'s only HUMAN producer (its other producer, the `merged`
    deadline's expiry, is a clock — see STATE_CLOSED and STATE_DEADLINES).
    Addressed by signature like every other CLI verb (see
    _items_for_signature()). A reason is required for the same reason
    _set_state() already requires one for `dismissed`: a close with no reason
    is indistinguishable from a bug, and the reason is the whole content of
    the state."""
    signature = _arg_value(argv, "--close")
    reason = _arg_value(argv, "--reason")
    if not signature:
        print("triage: --close needs a signature", file=sys.stderr)
        return 2
    if not reason or not reason.strip():
        print("triage: --close needs --reason <text>", file=sys.stderr)
        return 2
    rows = _items_for_signature(conn, signature)
    for row in rows:
        _set_state(conn, row["event_id"], STATE_CLOSED, now, note=reason.strip())
    conn.commit()
    if not rows:
        print(f"triage: no triage item for signature {signature!r}", file=sys.stderr)
        return 1
    print(f"closed {signature}: {reason.strip()}")
    return 0


def cmd_list(conn: sqlite3.Connection) -> int:
    # STATE_RESOLVED's three successors, and nothing else — `note`/`dismissed`/
    # `ignored` stay listed, exactly as before the split.
    rows = conn.execute(
        "SELECT signature, state, repo, verb, occurrences, first_seen FROM triage_items "
        "WHERE state NOT IN (?, ?, ?) ORDER BY updated_at DESC",
        (STATE_FIXED, STATE_QUIET, STATE_CLOSED),
    ).fetchall()
    if not rows:
        print("no open triage items")
        return 0
    for r in rows:
        print(f"{r['state']:<13} {r['signature']:<70} repo={r['repo'] or r['verb'] or '-'} "
              f"occurrences={r['occurrences']} since={r['first_seen']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(argv if argv is not None else sys.argv[1:])
    _apply_db_override(argv)
    now = dt.datetime.now(dt.timezone.utc)
    conn = db_connect()
    try:
        if "--snooze" in argv:
            return cmd_snooze(conn, argv, now)
        if "--ignore" in argv:
            return cmd_ignore(conn, argv, now)
        if "--reopen" in argv:
            return cmd_reopen(conn, argv, now)
        if "--close" in argv:
            return cmd_close(conn, argv, now)
        if "--list" in argv:
            return cmd_list(conn)
        return run(conn, dry_run="--dry-run" in argv)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
