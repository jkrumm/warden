# Alert triage — the act-loop over watchdog.db

`scripts/triage.py` (its own LaunchAgent, `com.jkrumm.warden-loop`, every 10 min —
see *Why a LaunchAgent, not `hermes cron`* below) closes the loop
`scripts/watchdog-poll.py` opened but never acted on: deduplicated `events`
rows become one durable, updated-in-place Slack card per problem, with a real
sideclaw investigation attached once a signature repeats or stays open.
**The act path (ingest → classify → cluster → escalate → card → resolve)
makes no LLM call at all.** The one exception is `propose_mappings()` — a
bounded, once-a-day maintenance pass that proposes new policy entries for
signatures that have sat unmapped too long; see *Propose mappings* below.

## Why this exists

Before this file, `#alerts` ran a full `reasoning_effort: high` LLM turn on
every inbound alert *message*, then post-hoc suppressed the reply with a
`NO_REPLY` marker for anything that wasn't new (`config.yaml`'s old
`channel_prompts.C0AS1LAUQ3C` block). Because `slack.reply_in_thread: true`
makes every alert its own session, that turn had zero memory of the last time
the exact same signature fired — the same `research-gateway job.reaped >= 1
(15m)` alert was triaged 61 times in one day, reaching the same conclusion
every time, without ever noticing a fix already existed. `watchdog-poll.py`
already deduplicates `#alerts` (and four other sources) into `events`
(`UNIQUE(source, external_id)`, grouped signatures via `normalize_title()`)
30 minutes at a time — nothing downstream ever consumed that dedup to *act*.
`events.dispatch_id` (a Phase 3 projection column) and `dispatches.origin_event_id`
existed for this purpose and were always NULL. This file is the missing act-loop
and the two edges it writes.

## Signals -> items -> episodes

| Layer | What | Table |
|-|-|-|
| Signal | One deduplicated alert row, owned by `watchdog-poll.py` | `events` |
| Item | One triage problem, 1:1 with an `events` row, owned by `triage.py` | `triage_items` |
| Episode | One sideclaw `investigate` dispatch, owned by `scripts/lifecycle/dispatch.py` | `dispatches` |

`triage_items.event_id` is both the primary key and the stable identity across
a resolve -> recur cycle: a grouped or state source reuses the *same*
`events.id` when a signature reopens (`UNIQUE(source, external_id)`,
`resolved_at` reset to `NULL`), so a `triage_items` row's `artifact_url` and
`dispatch_job` from a PRIOR investigation survive a reopen — see
*Reopen preserves history* below, which is precisely the property that stops a
future re-triage from rediscovering a fix that already shipped.

Multiple items can share ONE episode: a **cluster** is every `triage_items`
row that shares a non-NULL `dispatch_job` value. There is no separate cluster
table or `cluster_id` column — membership is derived, and a cluster's
lifetime already equals its dispatch's lifetime. See *Escalation — the two
edges* and *Clustering* below.

## Origins (Wave 6.1)

Every `triage_items` row now carries `origin` (`alert | human | github_issue`,
DESIGN.md:177) and `max_tier` (`implement | investigate` — the ceiling this
item may reach on its own; `brief` (the human's text or the issue body, NULL
for `alert`); and, since Wave 6.2, `origin_channel`/`origin_thread_ts` — the
Slack thread this item's own verdict must answer into (`human` origin, from
`warden run --origin-channel --origin-thread`; NULL for `github_issue`, which
has no thread of its own yet). `_dispatch_investigate_and_advance()` reads
these off the item and, when set, builds the investigate episode's `Origin`
from them instead of the shared triage card's channel — the card is a
projection, the asker's own thread is the origin, and `dispatch-sweep.py`
must land the answer THERE. When unset (a plain `warden run`, or any alert
cluster, which never carries these columns), behaviour is unchanged from
before Wave 6.2: the card's own channel, retro-filled onto the `dispatches`
row's `origin_thread_ts` once the card posts.

- **`alert`** — the seven `INGEST_SOURCES` above, via `ingest()`.
  `max_tier='implement'`: policy + `autoMergePaths` decide, as always.
- **`human`** — `warden run <repo>` (`scripts/warden.py`'s `cmd_run`), brief on
  stdin. Default `--tier investigate`; `--tier implement` requires `--why` and
  sets `max_tier='implement'`. `--origin-channel`/`--origin-thread` (Hermes
  answering in its own thread, no `--wait`) set the two columns above.
- **`github_issue`** — a GitHub issue in one of `_github.GH_OWNER`'s repos
  carrying the label `warden:go`, polled once per loop tick
  (`ingest_github_go()`). The owner's own issues (author == `GH_OWNER`) get
  `max_tier='implement'`; every third-party issue gets `max_tier='investigate'`
  **always**, regardless of the label — every repo here is public, so anyone
  can apply it. A third-party issue body is wrapped as untrusted,
  attacker-influenceable text before it ever reaches a brief.

Both non-alert origins insert their `triage_items` row through
`open_origin_item()` (dedup on `(origin, repo, external_id)`: a non-terminal
match reuses it, a terminal match opens nothing — a stale label after the
work finished is not a new handover) and escalate as their OWN cluster of one
through `escalate_origin_items()` — no `minOccurrences`/`minOpenMinutes`/
`cooldownHours` gate (a human or a trusted label already decided), no
`DAILY_INVESTIGATE_BUDGET` (that bounds the loop's own autonomous escalation
of alert noise), but still bounded by `MAX_OPEN_INVESTIGATIONS` (overflow
waits in `new`, never drops) and the same dispatch budget `warden dispatch`
itself is bound by.

`max_tier='investigate'` is a hard ceiling, enforced twice: `maybe_auto_implement()`'s
own eligibility query excludes it, and `lifecycle/policy.py`'s
`require_auto_from_item()` refuses it by name (defence in depth — the loop and
the CLI's `--auto-from-item` both pass through the same function). When the
investigate verdict lands for an `investigate`-ceiling origin item, it goes
straight to `closed` with `answered: <summary>` instead of `verdict` — a
question was asked and answered, and the `verdict -> needs_human` 24h deadline
would only manufacture noise for something nobody is going to act on further.

## The loop, per run

1. **Ingest** — upsert one `triage_items` row per open event in
   `slack_alert`, `uk`, `docker_homelab`, `docker_vps`, `hermes_log`,
   `op_refs_homelab`, `op_refs_vps`. `occurrences` comes from
   `payload_json.batch_count` (grouped sources) or falls back to
   `reminder_count + 1` (state sources, which don't batch). Never writes to
   `events.payload_json` — `upsert_grouped()` rewrites that blob wholesale on
   every poll (verified by reading it), so a second writer there would
   silently lose data. `triage_items` is a fully separate table for exactly
   this reason.
2. **Reopen / unsnooze** — a `triage_items` row stuck in `quiet`, `fixed`,
   `closed` or `dismissed` whose underlying event has since reopened flips
   back to `new` (never clearing `artifact_url`/`dispatch_job`); a `snoozed`
   row whose `snoozed_until` has passed flips back to `new`.
3. **Classify** — fnmatch TWO match targets per event (`source:external_id`
   and `source:normalize_title(title)` — see *Match targets* below) against
   `config/triage-policy.json`, in this order:
   1. `ignore` (checked first, both targets) — a deliberate human call that
      THIS signature is a genuine recovery or known-benign pattern. Routes
      to `ignored`: terminal, invisible, never in the digest.
   2. `ignoreUnstructuredSlackProse` (structural, `slack_alert` only) — a
      title that doesn't start with a recognized bot-alert shape. Routes to
      `STATE_NOTE`: terminal, but VISIBLE in the daily digest — see *Notes vs
      ignored* below.
   3. `rules` (first match wins) — resolves EITHER `repo` (escalate to a
      sideclaw episode) OR `verb` (run a declared local command — see *Verb
      outcomes*).
   Only ever touches a row still in state `new` — an escalated, snoozed, or
   manually ignored/noted item is never reclassified out from under itself.
   A signature matching no rule stays `new` with `repo`/`verb` unset and is
   named in the daily digest so the map can grow deliberately.
4. **Resolve** — an event whose `resolved_at` is now set flips its
   `triage_items` row to `quiet` — but ONLY a row still in `new`
   (`_SILENCE_RESOLVE_ELIGIBLE_STATES`). `resolved_at` is set by
   disappearance from observation, and observation ending never discharges an
   obligation (DESIGN.md principle 5), so every other state — `ignored`,
   `snoozed`, `note`, and every chain state from `investigating` onward — stays
   put and exits through its own transition or its deadline. `note` is also
   cleared here, which is safe precisely because the row was `new` and had no
   prior-phase text to lose: it stops a stale quiet/recovery note (below) from
   surviving into a later, unrelated resolve.
4b. **Quiet / recovery-paired resolve** — the two GROUPED sources
   (`slack_alert`, `hermes_log`) never disappearance-resolve via step 4 at
   all: `watchdog-poll.py`'s own `sweep_stale_grouped()` only clears them
   after 7 idle DAYS, deliberate housekeeping, not signal. Two triage-side
   fixes instead, checked in this order and never touching
   `events.resolved_at`: `resolve_recovery_paired()` looks for a fresh `✅`
   HyperDX recovery message pairing the same alert text, and
   `resolve_quiet_grouped()` falls back to a `quietResolveHours` silence
   timer. See *Evidence commands and grouped-source resolution* below.
5. **Dissolve** — a cluster whose folded verdict says its members don't
   share a root cause (`DISSOLVE_MARKER`, see *Clustering*) splits: every
   member moves to `split` — carrying that verdict in `note` — and waits to
   be re-evaluated on its own. See *The `split` state* below.
6. **Escalate** — every eligible `new`+`repo`-mapped item, GROUPED BY REPO,
   becomes at most ONE sideclaw dispatch per repo per run (a cluster, capped
   at `MAX_CLUSTER_SIGNATURES` = 5 members; the rest wait for a later run) —
   not one dispatch per item. A `split` item is ALSO a candidate, considered
   BEFORE `new` clusters, but escalates as a SINGLETON, never grouped — and
   still counts against that repo's one-dispatch-per-run slot, so a repo with
   an eligible `split` item defers its `new` items to the next run. See
   *Clustering* and *The `split` state*.
6b. **Verbs** — every eligible `new`+`verb`-mapped item runs its allowlisted
   local command once. See *Verb outcomes*.
6c. **Deadlines** — every non-terminal state names its poller and how long a
   row may sit in it (`STATE_DEADLINES`); `sweep_deadlines()` is what happens
   when that poller did not deliver. See *Deadlines* below. Runs after every
   poller (an item that can still advance gets its chance first) and before
   the card (an expiry the human never sees is the same as no expiry).
7. **Card** — one Slack card per cluster (`_cluster_groups()`, keyed by
   `dispatch_job`, a verb outcome is its own singleton "cluster"), posted
   once state leaves `new` (see *Carded states* below) and updated in place
   after, no-op when the rendered content hasn't changed.
8. **Propose mappings** — at most once per 24h (tracked in the `cursors`
   table, same pattern the digest already uses), the ONE LLM call in this
   file: batches every `new`+unmapped item whose event has stayed open at
   least `proposeMappingsAgeDays`, and proposes `map`/`ignore`/`unsure` per
   signature. Applied outcomes land only in `config/triage-policy.json`
   (never `triage_items` directly), committed — never pushed — in this
   repo's own checkout. See *Propose mappings* below.
9. **Daily digest** — once per UTC day (tracked in the `cursors` table,
   shared with `watchdog-poll.py`), a single Slack message with up to three
   sections: signatures that matched no rule, `STATE_NOTE` rows, and
   whatever step 8 just auto-added this run.

`scripts/dispatch-sweep.py` closes the other half: when a dispatch tied to a
triage cluster (`dispatches.origin_event_id` set) reaches a terminal status,
it calls `triage.fold_dispatch_verdict()`, which now looks up EVERY
`triage_items` row sharing that `dispatch_job` (not just the primary member)
and folds the verdict onto all of them and their one shared card immediately,
rather than waiting up to 10 minutes for this file's own next pass. The
`#watchdog` delivery path for dispatches with no `origin_event_id` (every
non-triage dispatch) is unchanged.

## Match targets

A policy `rules`/`ignore` pattern is tried against TWO strings per event, in
order, first match across either wins:

1. `f"{source}:{external_id}"` — the raw form.
2. `f"{source}:{normalize_title(title)}"` — the human-readable form,
   `normalize_title()` imported from `scripts/watchdog-poll.py`.

For a grouped source (`slack_alert`, `hermes_log`) these are USUALLY the same
string — that source's `external_id` already IS `normalize_title(title)` (see
`aggregate_slack_batch()`/`poll_hermes_logs()`), so the second target is a
harmless no-op there. For a state source they differ, and this is what makes
`uk` mappable at all: its `external_id` is an opaque, unglobbable UptimeKuma
monitor id (`"204"`), unstable across a monitor recreate — a rule can only be
written against the title-derived target, e.g. `uk:macmini-dev-host-push`.

## Notes vs ignored

Two different terminal, uncarded outcomes for a `slack_alert` item that
never becomes an episode — deliberately NOT the same state:

- **`ignored`** — the explicit `ignore` list. A human decided THIS
  signature is a genuine recovery or known-benign pattern. Invisible:
  never carded, never in the digest.
- **`STATE_NOTE`** — the structural `ignoreUnstructuredSlackProse` fallback.
  A title that doesn't start with a recognized bot-alert shape (`[`, siren,
  checkmark, warning). Visible: named (signature + a truncated title) under
  its own heading in the daily digest, though never carded or escalated.

The split exists because `watchdog.db` genuinely contains rows like a human
Slack message diagnosing the exact 1Password rate-limit root cause with a
concrete two-line fix — never shipped. Before `#alerts` was silenced, Hermes
replied to every message in that channel, and `watchdog-poll.py`'s
`slack_alert` poller ingested those replies right alongside real bot alerts;
routing all of that unstructured backlog into `ignored` would make a real,
unactioned diagnosis permanently invisible — precisely the failure mode this
whole redesign exists to kill. Routing it to `ignored` was the original
(wrong) design of `ignoreUnstructuredSlackProse`; `STATE_NOTE` fixed it.

## Verb outcomes

A policy rule can carry `verb` instead of `repo` — routes to a declared,
CODE-SIDE ALLOWLISTED local command (`VERB_ALLOWLIST` in `scripts/triage.py`)
rather than a sideclaw episode. The policy file names a KEY (`"env-check"`),
never a command — a policy file must never be able to name an arbitrary
argv, the same closed-verb-set principle the `warden` CLI's own
`dispatch|status|list|merge|abort|revert` verbs use, applied to a bounded
local probe instead of an episode.

Seeded with exactly one: `env-check` (`hermes-ops.sh env-check --json`, two
sequential ssh probes of homelab/vps's shared `.env.tpl`), which
`op_refs_homelab`/`op_refs_vps` route to. A dead 1Password ref blocks every
future reseal of the mini's offline secrets cache (`dotfiles/CLAUDE.md`
§Secrets) — it is a deterministic, already-diagnosed condition the moment
`env-check` runs, so dispatching a sideclaw episode to "investigate" it would
be both slower and actively worse: a bare 1Password item name in a
DISPATCHED verdict can collide with sideclaw's own `op://vault/item/field`
secret-scan pattern and get withheld from the card, whereas `env-check`'s
output reaching the card directly does not have that problem.

`run_verbs()` applies the same `minOccurrences`/`minOpenMinutes` eligibility
gate as an episode escalation, but NO concurrency/daily-budget cap (a verb is
a bounded local probe, not a sideclaw episode, and doesn't compete for that
budget) and NO cooldown tracking — a verb-routed item runs AT MOST ONCE,
because its terminal state (`needs_human`) permanently falls out of the
`state=new` candidate query. If the underlying condition later clears, the
normal resolve path (`events.resolved_at`) closes the row out without
needing a re-run; if it recurs after a reopen, running the probe again is
exactly correct. `VERB_TIMEOUT` = 260s (comfortably over `env-check`'s two
sequential 120s-bounded ssh probes) — long for a 10-minute cron, but the
probe runs at most once per dangling-ref episode, not every cycle.

The card's `note` (rendered by `_render_env_check_note()`) IS the whole
verdict: every dangling item name plus the exact remediation
(`make secrets-seed`, biometric, MacBook only) inlined, so no further
investigation should be needed.

**A prerequisite fix in `watchdog-poll.py` makes this trustworthy at all.**
`poll_op_refs()`'s `raw:` fallback (when `op run`'s error can't be parsed
into a specific dangling item name) used to build its dedup key from the
raw stderr text UNCHANGED — and that text embeds a timestamp (`[ERROR]
2026/09/01 15:00:34 (504) Unknown: ...`). Because `normalize_title()` does
not strip timestamps, every 30-minute poll minted a brand-new `external_id`,
so `reconcile()`'s disappearance logic resolved the "old" row and inserted a
"new" one every cycle — the DB reported the dangling ref clearing every 30
minutes while it stayed dead indefinitely, and this loop would have kept
escalating an ever-fresh signature into `run_verbs()` instead of running the
probe once and landing on a stable `needs_human` row. `_strip_op_refs_timestamps()`
strips ISO-8601 and slash/dash-separated date-time shapes plus any bare
6+-digit run from the text BEFORE it reaches `normalize_title()` — on this
one fallback path only; `normalize_title()` itself is unchanged, every other
source depends on its current behavior.

## Evidence commands

Every real investigation this loop has run so far (meteo, vps, hermes-agent)
came back `nextAction: human` citing the SAME reason: the dispatched sideclaw
episode runs in a read-only repo WORKTREE, which has the repo but never the
live machine — `var/health.json` and `watchdog-alerts.log` are gitignored/
empty there, live OTel data isn't in the checkout at all, and the one episode
that got anywhere only did because `~/.hermes/gateway-starts.log` happens to
sit outside the worktree by accident. A policy `rules` entry can now carry an
optional `evidence` list, fencing DECLARED, read-only, bounded probes into the
escalation brief — same closed-set principle as `verb` (see *Verb outcomes*):
this file names KEYS from `EVIDENCE_ALLOWLIST` in `scripts/triage.py`, never a
command or argv. An unknown key drops the WHOLE rule at `load_policy()` time,
loudly, exactly like an unknown `verb`.

Seeded with four:

| Key | What | Why it can't come from the repo checkout |
|-|-|-|
| `meteo-health` | meteo's own `var/health.json`, summarized (ok/heartbeat/timestamp + failing checks only, never dumped raw) | gitignored, empty in the worktree |
| `gateway-starts` | The last 5 lines of `~/.hermes/gateway-starts.log` (an append-only ledger of every gateway process start) | Outside every repo worktree by construction |
| `hermes-log-tail` | The tail of Hermes's `errors.log`, SLICED at the current gateway process start | Same file, same boundary requirement as `skills/hermes-gateway/SKILL.md` Rule 0 — an unsliced tail mixes a dead incarnation's errors with the live one |
| `kuma-push-last` | The live `[<monitor name>] ...` UptimeKuma push text for a `uk`-sourced cluster member | `events.payload_json` for a `uk` row is literally `{"type", "status"}` (see `poll_uk()`) — the actual heartbeat line (e.g. `FAIL: disk 90% used`) exists ONLY in raw #alerts Slack text, which `poll_slack_messages()`'s `skip_uk_push` deliberately drops before it ever reaches `events`; argo's own monitor endpoint doesn't carry it either |

Three of the four run as bounded, in-process Python (local file reads); the
fourth (`kuma-push-last`) additionally needs a secret (`HOMELAB_API_KEY`,
which must never cross an argv/`ps` boundary) and per-cluster context (which
monitor actually fired) that a fixed subprocess argv has no way to carry — see
`EVIDENCE_ALLOWLIST`'s own comment for why this deliberately diverges from
`VERB_ALLOWLIST`'s literal-argv shape while keeping the identical closed-key
contract. Every gatherer runs under `_run_bounded()` (a hard wall-clock
timeout, `TRIAGE_EVIDENCE_TIMEOUT`, default 20s) and any exception folds into
a returned error string — a failing evidence command is non-fatal and never
aborts the run; the brief still gets built with whatever evidence succeeded.

Evidence is rendered into one clearly-labelled block inside the brief,
explicitly told to the episode as CAPTURED runtime state gathered by the loop
— authoritative for what it reports, but not re-runnable by the episode
itself and possibly already stale by the time it's read. It is capped twice:
each key at `EVIDENCE_CAP_CHARS` (1200), the whole block at
`EVIDENCE_TOTAL_CAP_CHARS` (3200) or whatever budget the REST of the brief
(the alert list, sibling items, closing instructions) leaves behind inside
`MAX_BRIEF_CHARS` — whichever is smaller. When evidence has to be cut, only
evidence is cut, never the brief's own structure, and the block says so
("…truncated to fit the brief cap"). `config/triage-policy.json` seeds
`meteo-health` on the meteo rules, `kuma-push-last` on every `uk:*` rule, and
`gateway-starts`/`hermes-log-tail` on the hermes-agent-related `uk:hermes-*`
and `hermes_log:*` rules.

## Grouped-source resolution (quiet timer + recovery pairing)

`slack_alert` and `hermes_log` (`GROUPED_TRIAGE_SOURCES`) are the two
INGEST_SOURCES that are grouped (`upsert_grouped()`-based, append-only) rather
than state (`reconcile()`-based, disappearance-resolved) — the concrete gap
this closes: `research-gateway job.reaped`/`audio-gateway podcast.failed`
were fixed and deployed to prod HyperDX at ~18:37, the alert itself posted
`✅ resolved` at 18:45, and the `triage_items` row stayed open regardless,
because step 4 above only ever fires from `events.resolved_at`, and
`watchdog-poll.py`'s own `sweep_stale_grouped()` — which this file
deliberately never touches; that column and that sweep stay owned end to end
by `watchdog-poll.py` — only clears a grouped row after 7 idle DAYS.

Two triage-SIDE fixes (`triage_items` state only, never `events.resolved_at`),
checked in this order, both eligible only for a row still in `new`
(`_SILENCE_RESOLVE_ELIGIBLE_STATES` — the same allowlist step 4 uses). An item
mid-investigation is excluded not as a special case about racing an open
dispatch, but as one instance of the general rule: every state past `new`
carries an obligation, and silence is an observation about the signal, never a
discharge of that obligation.

- **`resolve_recovery_paired()`** — a `✅ <same alert text>` HyperDX recovery
  message is a strictly better signal than silence: a service that is fully
  DOWN also stops emitting, so absence of new occurrences alone can never
  tell the two apart. This is cheap because HyperDX's webhook posts the
  IDENTICAL alert text with only the leading glyph flipped, and
  `normalize_title()` already strips every non-alnum character — including
  both glyphs — so `✅ research-gateway job.reaped >= 1 (15m)` and
  `🚨 research-gateway job.reaped >= 1 (15m)` normalize to the exact same
  string, which for a grouped source already IS the event's own stable
  `external_id` (see *Match targets*). One fresh #alerts fetch per run (not
  per item), one dict lookup per open `slack_alert` candidate. This CANNOT be
  read back out of `events`/`payload_json` — `upsert_grouped()` retains only
  one title per batch, never a per-occurrence history (see *The brief*) — so
  reading live #alerts is the only way to see it at all. Skipped entirely
  under `--dry-run`, same as every other outbound call this file makes.
- **`resolve_quiet_grouped()`** — the fallback: a row whose underlying event
  has produced no new occurrence in `quietResolveHours` (policy knob, default
  2h — comfortably past the 30-min watchdog-poll cadence and short of either
  source's own 6h/24h reminder window, so one missed poll can never trip a
  false resolve) flips to `quiet`. No new column needed:
  `last_reminder_at`/`notified_at`/`first_seen` (the same idle anchor
  `sweep_stale_grouped()` itself uses) only advances when `watchdog-poll.py`
  re-stamps the row on a fresh occurrence, so a value that has stopped
  changing already IS the quiet duration. Pure local bookkeeping (no Slack,
  no dispatch), so it runs for real even under `--dry-run`, like
  `apply_resolutions()`.

**Neither path ever means "fixed."** A service that is fully down also stops
emitting, so silence is never proof of anything beyond silence, and a ✅
message is HyperDX's own claim, not this loop's. Both land in `quiet`
(see *The `resolved` split* below), never `fixed`. The quiet card always
reads "signal quiet since \<time\>" (`QUIET_RESOLVE_NOTE_PREFIX`) or "recovery
message observed: …" (`RECOVERY_PAIRED_NOTE_PREFIX`) — `render_card_blocks()`
only surfaces `note` on a `quiet`/`fixed` card when it starts with one of
those prefixes, specifically so an ordinary event-driven resolve (which
clears `note` outright) never inherits stale text from an earlier phase.

## The `resolved` split

Wave 2 splits the old single `resolved` state into three honest outcomes,
because one state answering "closed how" for all three collapsed the
headline metric *"closes that are verified fixes vs. silence"* to 2/28 —
`resolved` was the number the loop reported, and a metric that improves the
more questions go unanswered is precisely the failure this loop exists to
remove.

| State | Means |
|-|-|
| `fixed` | a change landed **and a positive signal confirmed it** |
| `quiet` | the signal stopped and **nothing shipped** — never claims a fix |
| `closed` | done without a verified positive signal — a human said so, or it landed and there was nothing left to verify |

Five producers, three buckets:

| Producer | State | Why |
|-|-|-|
| `apply_resolutions()` | `quiet` | `events.resolved_at` is set only by disappearance from observation — pure silence. |
| `resolve_quiet_grouped()` | `quiet` | The 2h no-new-occurrence timer; its own note already says "NOT a confirmed fix". |
| `resolve_recovery_paired()` | `quiet` | **The non-obvious one.** A `✅` recovery message is a positive OBSERVATION, but DESIGN.md § What must not be lost, item 4, is explicit: "Recovery-pairing is the strong path, the 2h timer the fallback, and neither ever claims a fix." Nothing shipped by this loop's own knowledge — the service recovered, by our hand or its own, and the loop cannot tell which. |
| `maybe_check_liveness()` positive branch | `fixed` | **The only producer of `fixed` in the file.** A change landed (merged + deployed) AND a live probe confirmed it — a positive PROBE, not an inbound message and not silence. |
| `merged` deadline expiry (`STATE_DEADLINES[STATE_MERGED]`) | `closed` | Landed, no deploy target configured for the repo — done, with nothing left to verify. |

`cmd_close` (`--close <signature> --reason <text>`) is `closed`'s only HUMAN
producer, mirroring `--snooze`/`--ignore`/`--reopen` exactly: it resolves a
signature to its `triage_items` row(s) and transitions each through
`_set_state()`. A reason is required, refused if empty, for the same reason
`_set_state()` already requires one for `dismissed` — a close with no reason
is indistinguishable from a bug.

**`item_transitions`** — an append-only history table, written by
`_set_state()` and by nothing else (the same reason `_set_state()` is the only
writer of `triage_items.state`). One row per REAL state change (`from_state`,
`to_state`, `at`, `note`); a column-only write with the state unchanged
(`sync_card()` and friends writing `card_ts`/`dispatch_job`/etc. through
`_set_state()`) is not recorded — recording it would fill the table with
noise and corrupt every duration `/metrics` computes from it. It exists
because three of `/metrics`'s six funnel numbers (median `needs_human` →
decision, verified unattended fixes per week, reopen-after-`fixed`) are not
derivable from the ledger without it: `triage_items.updated_at` is rewritten
on every open row on every `ingest()` pass regardless of state, so it cannot
answer "when did this item enter/leave a state" at all. The table starts
**empty** — migration 4 records nothing retroactively, so a `/metrics` window
that predates it reads as empty by construction, not as zero; do not mistake
the two.

## Carded states

A card exists ONLY once a cluster has left `new` — state in `investigating`,
`verdict`, `needs_human`, `pr_open`, `fixed`, `quiet`, `closed`, `dismissed`,
or one of the auto-implement chain's own states (`implementing`, `validating`,
`merge_blocked`, `merged`, `liveness_pending` — see *Closing the loop* below).
`dismissed` is carded for the same reason `fixed`/`quiet`/`closed` are: an
item that HAD a card and then ran out of time is real news to the human who
was asked and did not answer. An item that is mapped
but hasn't yet crossed `minOccurrences`/`minOpenMinutes` — or is simply
unmapped — is carried silently; it appears only in the daily digest (if
unmapped) or not at all (if mapped but not yet eligible). `ignored` and
`STATE_NOTE` are excluded too — see *Notes vs ignored*. `split` is excluded
for its own, different reason — see *The `split` state*. This is deliberate:
an empty or partial policy must never turn into dozens of cards of noise on
day one, which is exactly what carding every non-`ignored` row (regardless of
state) used to do.

**Being in this list only makes a row ELIGIBLE for a card — `sync_card()` is
where "was this item actually told to the human before" is enforced** (2026-09-08
correction, the sibling failure the list above exists to prevent): `quiet`/`fixed`
is reachable from `new` itself (`apply_resolutions()`,
`resolve_quiet_grouped()` and `resolve_recovery_paired()` all flip an
unescalated `new` row straight to `quiet` on a quiet/disappeared signal
that was never carded — and `new` is the only state they may flip), and a card announcing the resolution of a problem the
human was never told about is exactly the noise this whole file replaced —
caught live from a 13-card burst where every card was a `new -> resolved`
transition (today, a `new -> quiet` transition). `sync_card()` now refuses
outright (zero Slack calls, neither `chat.postMessage` nor `chat.update`)
whenever `state` is one of `fixed`/`quiet`/`closed`/`dismissed`
(`_NEVER_CARDED_FIRST_STATES`) and `card_ts` is still `NULL`; an item that had
a card already still gets its final `chat.update`, unchanged. The invariant,
stated once so every future state addition can be checked against it: **a
card is a conversation with the human about an item they were told about; a
state change on an item they were never told about is not news.**

## State machine

```
new ──(escalate, clustered by repo)──> investigating ──(sweeper folds verdict)──┬──> verdict ──(DISSOLVE_MARKER)──> split (cluster splits)
 │                                                                                │        │
 │                                                                                │        └──(nextAction=implement, confidence=high)──> implementing (step 6)
 │                                                                                ├──> needs_human
 │                                                                                └──> pr_open
 ├──(verb outcome — run_verbs())──> needs_human  (terminal-ish: no dispatch_job, runs at most once)
 ├──(ignore rule / --ignore)──> ignored  (terminal, never carded, never digested)
 ├──(ignoreUnstructuredSlackProse)──> note  (terminal, never carded, but IN the daily digest)
 ├──(--snooze)──> snoozed ──(snoozed_until passes)──> new
 └──(event resolves / quiet timer / recovery ✅)──> quiet ──(event reopens)──> new

split ──(escalate, SINGLETON — never grouped)──> investigating  (re-evaluated on its own)
split ──(24h, no escalate slot won)──> needs_human  (verdict from `note` carried into the expiry note)

investigating ──(2h)──> needs_human      merge_blocked ──(7d)──> dismissed
verdict ──────(24h)──> needs_human      needs_human ──(7d)──> dismissed
implementing ─(2h)──> merge_blocked     pr_open ─────(14d)──> dismissed
validating ───(1h)──> merge_blocked     merged ──────(1h)──> closed

implementing ──(outcome=pr_opened)────────────────────> validating (step 7, sideclaw's own `review` job)
             └──(checks_failed / salvaged / unexpected)─> needs_human
             └──(no_changes / diff_refused / etc.)──────> merge_blocked

validating ──(review outcome=clean, or actionable+no blocking; merge lands)──> merged | liveness_pending (step 8/9)
           └──(blocking findings / merge refused)────────────────────────────> merge_blocked
           └──(review outcome=needs-human)─────────────────────────────────────> needs_human

liveness_pending ──(positive liveness match)──────────> fixed (step 10)
                 └──(window elapses, still not live)──> new  (REOPENED, full history on the card)

(any signature, human call)──(--close <sig> --reason <text>)──> closed
```

A row in `new` — and ONLY a row in `new` — also goes to `quiet` the
moment the underlying event's `resolved_at` is set (see step 4). Once quiet,
`fixed` or `closed`, the row's rendered content stops changing, so the
card-hash short-circuit (below) means it is genuinely never touched again —
"stop touching it" falls out of the state machine, it isn't a separate rule.

## Deadlines

DESIGN.md principle 6: every non-terminal state names the thing that polls it
and its deadline, *checked against the diagram, not assumed*.
`STATE_DEADLINES` in `scripts/triage.py` is that table — poller, hours,
on-expiry state, reason — and
`test_every_non_terminal_state_names_a_poller_and_a_deadline` enumerates the
module's own `STATE_*` constants against it, so a state added without a
deadline fails at the moment it is added rather than months later as one row
nobody looked at again.

| State | Poller | Deadline | On expiry |
|-|-|-|-|
| `new` | `resolve_quiet_grouped` / `apply_resolutions` | — | bounded by `quietResolveHours`, not by a clock |
| `investigating` | `dispatch-sweep.py` | 2h | `needs_human` |
| `verdict` | `maybe_auto_implement` | 24h | `needs_human` |
| `implementing` | `poll_implement_jobs` | 2h | `merge_blocked` |
| `validating` | `poll_validation_jobs` | 1h | `merge_blocked` |
| `merge_blocked` | operator | 7d | `dismissed`, reason `unresolved` |
| `merged` | the clock itself | 1h | `closed` |
| `needs_human` | operator | 7d | `dismissed`, reason `expired` |
| `pr_open` | operator | 14d | `dismissed`, reason `expired` |
| `liveness_pending` | `maybe_check_liveness` | own column `liveness_deadline` | reopens to `new` with history, or `fixed` on a positive match |
| `snoozed` | `unsnooze_if_expired` | own column `snoozed_until` | `new` |
| `split` | `escalate` | 24h | `needs_human`, reason `unre-evaluated`, verdict carried in `note` |

Three deviations from DESIGN.md's own table remain, all deliberate. `verdict`
is not in it at all and is non-terminal — `maybe_auto_implement()` only
advances a verdict that reads `nextAction=implement` at `confidence=high`, so
every other one sits there with nothing scheduled to touch it. `split` is the
same kind of addition, for the same reason — nothing scheduled to touch it
unless `escalate()` finds it a free per-repo slot this run — at the same 24h,
matching `verdict`'s own rule, well clear of `cooldownHours` so `escalate()`
gets several real chances first. `needs_human`'s "reminder at 1d" is NOT
built: a reminder is a notification, not a deadline. (A fourth deviation —
`merged` expiring to `resolved` instead of `closed` — was here until the
`resolved` split landed; `merged` now expires to `closed`, matching
DESIGN.md's own table exactly.)

`dismissed` is terminal and always carries its reason in `note` —
`_set_state()` raises rather than let a reasonless dismissal exist. It is
deliberately neither `quiet`/`fixed`/`closed` (nothing was necessarily fixed,
and conflating "nobody answered" with a genuine close would corrupt the exact
metric this split exists to make honest) nor `ignored` (that
is a human calling a signature benign; an expiry is the *absence* of a human).

Two mechanical properties hold this together. **Every transition goes through
`_set_state()`**, which writes `state`, `state_deadline` and `updated_at` in
one statement — state and deadline are one fact, and a call site that could
forget the deadline is a row that sits with no clock or with the previous
state's clock (`test_no_raw_state_transition_remains` keeps it the only such
statement in the file). And **a non-terminal row with a NULL `state_deadline`
is reported to stderr, never backfilled**: `updated_at` is the only anchor
available and `ingest()` rewrites it every pass for every open event, so a
deadline derived from it would move further away every run and never fire. The
rows that predate schema version 2 print on every pass until they next
transition.

This also closes the pruned-job gap: `sideclaw` prunes terminal jobs at 24h
*or* 200 terminal rows (a cap shared with every interactive `/check`), after
which `poll_implement_jobs()`/`poll_validation_jobs()` poll a job that no
longer exists and nothing moves the item. The 2h/1h deadlines fire long before
either bound, so no miss counter and no extra column are needed — the item
exits to `merge_blocked` on the clock.

## Clustering

Multiple signatures can share one root cause — the concrete example this was
built for: `research-gateway job.reaped` and `audio-gateway podcast.failed`
were both `threshold: 0` in the same commit, fixed by the same two-line diff
in `vps/observability/alerts/`. `escalate()` groups every eligible `new`+
mapped item BY RESOLVED REPO and opens AT MOST ONE sideclaw dispatch per repo
per run (capped at `MAX_CLUSTER_SIGNATURES` = 5 members; the overflow items
stay in `new` and wait for a later run — never dropped, never silently
folded in anyway).

Cluster membership is derived, never its own column: every CARDED-state
`triage_items` row sharing a non-NULL `dispatch_job` IS one cluster
(`_cluster_groups()`). A dedicated `cluster_id` column would just duplicate
that fact under a different name.

The grouping is a **hypothesis**, never an assertion — the brief tells the
episode so explicitly and asks it to confirm or split it: if the signatures
do NOT share a root cause, the episode is asked to say so using the exact
phrase `UNRELATED SIGNATURES` in its summary/verdict/recommendation. The very
next run's `maybe_dissolve_clusters()` does a plain, case-sensitive substring
check for that phrase on a `verdict`-state cluster (`needs_human`/`pr_open`
clusters found something actionable, so dissolving doesn't apply there) and,
on a match, moves every member to `split` via `_dissolve_cluster()` — which
posts one final "Cluster split" message on the shared card and clears its
`card_channel`/`card_ts`/`card_hash`, but DELIBERATELY LEAVES `dispatch_job`
set on the now-`split` rows, purely as a cooldown anchor (`_cooldown_ok()`
still finds the dissolved dispatch's `created_at`). Without that, the SAME
run's `escalate()` call would see both members freshly eligible with zero
cooldown and instantly re-fuse them into an identical cluster — dissolve
would be a no-op in practice. Accepted tradeoff: a dissolved pair could
re-cluster again after `cooldownHours` if both are still open; a hard
permanent split would need a negative-relationship table this schema doesn't
have.

## The `split` state

Before Wave 3, a dissolved member reset all the way to `new`. Live on
2026-09-09 21:09-21:39Z (docs/history/state-log.md §43): two members carrying a real, correct
verdict about an active `hermes-agent` watchdog race were dissolved to `new`,
missed re-escalation inside `cooldownHours` (correctly — that's the cooldown
anchor working as designed), and were silently quiet-resolved by
`apply_resolutions()` before anyone ever saw the verdict — which survived
only in `dispatches.verdict_json`, which nothing reads. `new` is the one
state a silence path may discharge (`_SILENCE_RESOLVE_ELIGIBLE_STATES`,
deliberately an inclusion list of exactly that one state) precisely because a
`new` row carries no obligation yet — and a dissolved member does.

`split` is that obligation's home: a cluster member whose shared
investigation returned `UNRELATED SIGNATURES`. It carries the dissolve
verdict in `note` (`SPLIT_VERDICT_NOTE_PREFIX`, capped the same way a
dispatch brief is), so the verdict survives on the row itself rather than
only inside an unread `dispatches.verdict_json`.

It is deliberately **not carded** — `_cluster_groups()` groups CARDED rows by
`dispatch_job`, and a dissolved member still shares its (retained)
`dispatch_job` with its former cluster-mates, so carding `split` would
re-render, as one cluster card, the very cluster the dissolve just took
apart. Its own dissolve-notice update on the shared card IS its visible
history until it reaches `needs_human` (which IS carded).

`escalate()` is what advances it — as a **singleton**, never grouped with
another `split` item or with `new` items (grouping it would re-fuse the
cluster the dissolve just split, which its own Slack notice promises won't
happen: "Each will be re-evaluated individually"). Every eligible `split`
candidate across EVERY repo is attempted before any `new` cluster in any
repo — the ordering is global, not per-repo, so the shared
`MAX_OPEN_INVESTIGATIONS`/`DAILY_INVESTIGATE_BUDGET` ceilings go to obligated
items first. A `split` item still counts against its repo's
one-dispatch-per-run slot: a repo with an eligible `split` item
spends this run's slot on it, and its `new` items wait for the next run
(printed, never silently dropped). `STATE_DEADLINES` bounds it at 24h,
expiring to `needs_human` — the honest expiry for an unactioned verdict,
which is also what makes it visible on a card again. `sweep_deadlines()`
appends its generic expiry note to the split verdict rather than replacing
it, the one narrow exception to "the expiry note replaces whatever was
there" — every other state's prior note is history, this one is a still-live
obligation.

## Escalation — the two edges

`escalate_cluster()` calls straight into the Python lifecycle module, no
subprocess and no argv at all:

```python
target = _policy.resolve_repo(repo)
opened = _dispatch.open_episode(
    conn, target=target, tier="investigate", brief=brief, context=None, why=None, model=None,
    origin=_dispatch.Origin(channel=channel, thread_ts=None, event_id=primary["event_id"]),
    authorized_by=None,
)
```

The brief is a plain function argument (never argv, never stdin to shell
out over — see `docs/dispatch-bridge.md`'s "brief is data, never command"
rule for why that invariant existed in the first place); capped in Python at
`MAX_BRIEF_CHARS` = 8000 BEFORE this call, by `_build_cluster_brief()`'s own
`_cap_brief()`, matching `lifecycle/dispatch.py`'s own `normalize_brief()`
ceiling (triage.py never reaches that function — it always caps first).
`origin.event_id` takes exactly one `events.id` (one `dispatches` row = one
episode), so it's the cluster's PRIMARY member; `thread_ts` is omitted
because the card doesn't exist yet at dispatch time (see *Carded states* —
`new` never has one). Any `WardenError` — a denied/unresolvable repo, a
remote failure — is caught, logged to stderr, and returns `None`, exactly
like the old subprocess-refusal path did. The card is posted immediately
AFTER the dispatch succeeds (the first time this cluster becomes
`investigating`, a CARDED state), and `escalate_cluster()` then does one
small follow-up `UPDATE dispatches SET origin_thread_ts=?` — the same "extra
writer touching one column it doesn't own" pattern `dispatch-sweep.py`
already uses for `status`/`verdict_json`/`artifact_url`/`merged_at`/
`poll_misses`/`reported_at` on this same table — so `dispatch-sweep.py`'s
existing actionable-dispatch nudge still lands on the card's own thread.

Two edges were always NULL before this file:

- `dispatches.origin_event_id` — `open_episode()`'s own `INSERT` writes this
  from `origin.event_id` directly (for the primary member only — one
  `dispatches` row takes a single origin event). No second writer races
  `dispatches` for this column.
- `events.dispatch_id` — nothing wrote this. `escalate_cluster()` looks up the
  freshly-inserted `dispatches.id` by `job_id` and does the
  `UPDATE events SET dispatch_id=?` for EVERY member of the cluster, not just
  the primary — this is the one edge nothing else can write, and it's what
  makes `watchdog-poll.py`'s existing `_dispatch_status()`/`_dispatch_summary()`
  projection work for every signature in the cluster, not only the first.

## The brief

Built from data actually available in this DB, never fabricated: per repo,
per member signature — occurrence count, first/last seen (absolute, UTC), up
to 3 distinct raw text snippets (`event.title` plus a grouped signature's
`payload_json.first_text`/`first_line` if distinct — **the schema does not
retain a full history of individual occurrences**, so this is a documented
best-effort rather than a fabricated 3-item history), and — the fix for the
61-re-triage scenario — that member's own `artifact_url` if a prior
investigation of this EXACT signature already produced one (see *Reopen
preserves history*) — plus up to 5 sibling open `triage_items` in the same
repo (excluding every cluster member). For a multi-member cluster, the brief
states explicitly that these signatures fired together and may share one
root cause, and asks the episode to confirm or split the hypothesis (see
*Clustering*).

## Cards

One Slack message per CLUSTER (not per item — see *Clustering*), in
`config/triage-policy.json`'s `cardChannel` (currently `C0BVDE5R562`,
`#agents` — shared with the agent-overview and project-narratives digests, a
deliberate reuse rather than a new channel), posted only once the cluster
reaches a carded state (see *Carded states*). First post -> `chat.postMessage`;
every subsequent change -> `chat.update` on the stored `card_ts`, never a
second `chat.postMessage` for the same cluster. Every member row carries an
identical copy of `card_channel`/`card_ts`/`card_hash` (rather than one
"owning" row), so cluster membership stays self-describing even after a
process restart. **The API call is skipped entirely when the rendered Block
Kit content's sha256 (`card_hash`) is unchanged since the last sync** — this
is the property that stops the channel becoming a firehose again, and it is
what makes "resolution updates the card once and then stops" fall directly
out of the state machine rather than needing special-case code.

Content: a header (state emoji + either the single member's title, or "N
related alerts in `repo`" for a multi-member cluster), a context line listing
EACH member's own signature and occurrence count, then state-dependent body —
the investigation job id while `investigating`; on `verdict`/`needs_human`/
`pr_open`, the summary + confidence + up to 3 evidence lines (all read live
from `dispatches.verdict_json` via `dispatch_job`, never duplicated into
`triage_items`) + the artifact URL as a link; `needs_human` additionally shows
the blocker (stored in `triage_items.note`, the one field this file uses for
free text). A footer context line always names the snooze command. **No
buttons in this change** — the interactive layer (Approve/Deny-style actions
on a card) is a deliberate follow-up, matching the note in the brief that
shipped this file.

## Reopen preserves history

A grouped or state source's `events` row is reused across a resolve -> recur
cycle (same `UNIQUE(source, external_id)` constraint `upsert_grouped()`/
`reconcile()` already rely on). `reopen_if_needed()` flips a `quiet`/`fixed`/
`closed`/`dismissed` `triage_items` row back to `new` without ever clearing `artifact_url` or
`dispatch_job`. The next time that signature escalates, `_build_brief()`
includes the prior artifact URL and tells the episode to check whether it
already fixes the problem — including whether it simply hasn't been merged
yet — before proposing something new. This is the direct fix for the
scenario in the brief that shipped this file: a PR already existed and 61
re-triages never noticed.

## Propose mappings

`config/triage-policy.json` was designed to grow only by a human reading the
daily unmapped-signature digest and hand-editing the file — measurably not
happening: an `api-*-red-circle-down-*` signature pair fired in May 2026, was
hand-fixed once, and never entered the map, so it matched nothing when it
fired again four months later. `propose_mappings()` (step 8 in the loop
above) is the ONE LLM call anywhere in `scripts/triage.py` — everywhere else,
"no LLM call" still means exactly what it always did (the dispatched
`investigate` episode itself running Claude Code is inherent to what
"investigate" means, not something this loop does itself).

**Cadence and input.** At most once per 24h — a timestamp cursor in the same
`cursors` table the daily digest already uses (`triage_propose_mappings_last_run`),
checked before the model is ever called. Candidates: every `triage_items` row
in `state=new` with no `repo`/`verb` (i.e. `classify()` found no rule for it)
whose event has stayed open at least `proposeMappingsAgeDays` (policy knob,
default 7 — a signature younger than that may still be a one-off, and mapping
it wastes a whole investigate episode), oldest first, capped at
`PROPOSE_MAPPINGS_MAX_SIGNATURES` (25). The rest simply wait for a later run.

**The call.** One batched request against the Hermes brain over the same
OpenAI-compatible endpoint `config.yaml` already configures
(`OPENAI_BASE_URL`/`OPENAI_API_KEY`, model `gpt-5.6-luna`, `chat_completions`
— never the Responses-API leg the main agent uses), secrets resolved the same
way every other secret in this file is (`_resolve_openai_api_key()` mirrors
`resolve_slack_token()`'s own env-var-then-`secrets-run` shape — never a
plaintext key). Bounded on every axis this loop can bound: a hard timeout
(`PROPOSE_MAPPINGS_TIMEOUT`, default 90s), a cap on output tokens
(`PROPOSE_MAPPINGS_MAX_OUTPUT_TOKENS`, 2000), and the input cap above. Strict
JSON only, one of three shapes per signature: `{"action": "ignore", "reason":
…}`, `{"action": "map", "repo": …, "reason": …}`, or `{"action": "unsure"}`.
A failed, timed-out, or unparseable call is logged to stderr and otherwise a
no-op — this loop must never depend on it succeeding, exactly like every
other externally-visible call in this file.

**Applying a proposal.** `ignore` and `map` are NEVER written to
`triage_items` directly — they only ever become policy entries, picked up by
`classify()` on a LATER run, exactly like a hand-written rule would be. A
`map` proposal's `repo` is re-validated against `_discoverable_repos()` (which
delegates to `lifecycle.policy`'s own `load_dispatch_policy()`/
`_discoverable()` — the SAME discovery `lifecycle.policy.resolve_repo()`
uses: every non-dotted, non-`deny`d entry directly under `root` with a
`.git` subdirectory) AND the deny list — never trusted from the model's own
claim, or from the prompt's own repo list, alone. `unsure` writes a cooldown
marker
(`triage_items.propose_unsure_at`) directly, so the SAME signature is not
re-billed into the model for `PROPOSE_UNSURE_COOLDOWN_DAYS` (7).

**Writing the file.** Every applied entry is stamped `proposedAt` (ISO
timestamp), `proposedBy: "triage-auto"`, and the model's own one-line
`reason`, appended to `rules` (for `map`) or `ignore` (for `ignore` — that
array can now hold either a bare pattern string, hand-authored, or a stamped
object; `load_policy()` reads the `match` field of either shape identically).
`_dump_policy_json()` writes the whole file back preserving `_readme` and
top-level key order, rendering each array one element per line rather than
`json.dump`'s default fully-expanded nesting, so a small addition doesn't
turn into a whole-file diff.

**Committing.** `git add` + `git commit` — never `git push` — ONLY
`config/triage-policy.json`, in this repo's own checkout
(`TRIAGE_REPO_DIR`, resolved from `scripts/triage.py`'s own `__file__`, which
follows the `~/.hermes/scripts/` → `~/SourceRoot/hermes-agent/scripts/`
symlink to the real checkout). The file is symlinked live into
`~/.hermes/config/`, so the change already took effect the moment the write
returned — committing turns that into a reviewable diff instead of the
dirty-working-tree drift this repo has been bitten by before. No lock is
taken (this is the only writer of this file), but the path's own `git status
--porcelain` is checked BEFORE the write; if it already carries a pending
change, this run's proposals are skipped entirely — logged loudly — rather
than sweeping an unrelated in-progress edit into an auto-authored commit.

**Announcing it.** The applied proposals are handed straight from
`propose_mappings()`'s return value into that same run's
`maybe_post_daily_digest()` call, which gains a third section naming exactly
what was auto-added and why — so an auto-authored policy commit is announced
the same day, not only discoverable later in `git log`.

**A known gap, found by running this against a copy of the live DB.**
`resolve_quiet_grouped()` (step 4b) runs BEFORE `propose_mappings()` in the
same cycle, and a `slack_alert`/`hermes_log` (`GROUPED_TRIAGE_SOURCES`) item
that has produced no new occurrence in `quietResolveHours` (2h default) flips
to `quiet` there — before `propose_mappings()`'s own `state=new` candidate
query ever runs. In practice this means most currently-unmapped grouped-source
signatures (the majority of a real backlog — 16 of the 19 unmapped signatures
in the live DB at the time this was built) never reach the candidate pool at
all; only unmapped STATE-source signatures (`uk`, `docker_*`, `op_refs_*`,
which disappearance-resolve via `events.resolved_at` instead) stay stably
`new`. This is a faithful implementation of the brief's literal input
contract ("every signature in STATE_NEW"), not a bug — but it is a real
limitation worth a deliberate follow-up decision (e.g. whether the candidate
query should also examine `quiet` grouped rows that were never escalated,
i.e. `dispatch_job IS NULL`), not something this change decided unilaterally.

## `config/triage-policy.json` contract

```json
{
  "cardChannel": "C0BVDE5R562",
  "minOccurrences": 3,
  "minOpenMinutes": 30,
  "cooldownHours": 6,
  "quietResolveHours": 2,
  "proposeMappingsAgeDays": 7,
  "ignoreUnstructuredSlackProse": true,
  "rules": [
    {"match": "<fnmatch on either match target>", "repo": "<repo name>",
     "evidence": ["<EVIDENCE_ALLOWLIST key>", "..."]},
    {"match": "<fnmatch on either match target>", "verb": "<VERB_ALLOWLIST key>"}
  ],
  "ignore": [
    "<fnmatch on either match target>",
    {"match": "<fnmatch on either match target>", "proposedAt": "<ISO timestamp>",
     "proposedBy": "triage-auto", "reason": "<model's one-line reason>"}
  ]
}
```

`evidence` is optional and only ever valid alongside `repo` (never `verb` — a
verb outcome is already a deterministic local probe, not an episode with a
brief to fence evidence into). `quietResolveHours` and `proposeMappingsAgeDays`
are top-level, not per-rule — see *Grouped-source resolution* and *Propose
mappings* above. `ignore` entries are usually a bare pattern string
(hand-authored); `propose_mappings()` instead appends a stamped object — both
shapes classify() identically (only `match` is ever used for fnmatch). A
`rules` entry `propose_mappings()` writes carries the same `proposedAt`/
`proposedBy`/`reason` stamp alongside its `match`/`repo`.

A "signature" is `source:external_id` (`triage_items.signature`, and the
argument every `--snooze`/`--ignore`/`--reopen` CLI verb takes) — this is
distinct from a "match target", which is one of the two strings a `rules`/
`ignore` pattern is actually tried against (see *Match targets*). A rule
carries EITHER `repo` (escalate to a sideclaw episode) OR `verb` (run a
declared local command — see *Verb outcomes*), never both; `rules` is
matched top-to-bottom, first match across either target wins.

Evaluation order (see *The loop, per run* step 3, *Notes vs ignored*):
`ignore` first (checked both targets, wins over everything — genuine
recoveries/known-benign), then `ignoreUnstructuredSlackProse` (structural,
`slack_alert` only, routes to `STATE_NOTE` not `ignored`), then `rules`.

Every `repo` MUST resolve under `root` in `config/dispatch-repos.json` and
MUST NOT be in that file's `deny` list — `triage.py` checks the deny list
itself before ever shelling out, so a bad rule degrades to "never escalates"
(logged to stderr) rather than a crash, but it is still a policy bug, not a
feature; fix the rule, don't rely on the guard. Every `verb` MUST be a key in
`scripts/triage.py`'s `VERB_ALLOWLIST` — `load_policy()` drops (with a
stderr warning) any rule naming an unknown verb, rather than silently never
matching it at classify() time.

**Which repo owns an infra-signal alert** (a real VPS outage trace caught
this policy wrong once already — the argo rules used to point at `argo`):
an infra-signal alert about a RollHook-managed app maps to the repo that owns
its COMPOSE FILE AND DEPLOY TARGET, not the repo that owns its source.
`argo` is compose-managed manually inside `vps` (`apps/argo/compose.yml`,
`make argo-up` — nothing in argo's own repo redeploys it), so a downed argo
container — and its `api-*`/`dashboard-*` UptimeKuma child monitors — maps
to `vps`. Most OTHER vps-hosted apps in this policy (audio-gateway,
research-gateway, meteo, image-gen, image-share) are the opposite:
`dispatch-repos.json`'s own comment documents that they deploy to the VPS on
every push to THEIR OWN repo's master (GitHub Actions -> RollHook), so a code
fix in their own repo auto-redeploys — mapping those to their own repo is
correct and deliberate. Before adding a new vps-app rule, check
`vps/apps/<name>/compose.yml` AND whether that app's own repo has a deploy
workflow. HyperDX-origin alerts (`job.reaped`, `podcast.failed`,
`vps-edge-*`, ...) map to `vps` for a related but distinct reason: their
threshold/definition lives in `vps/observability/alerts/*.json`, which is
where THAT fix lands regardless of which service the alert is about.

Extend the file with:

```bash
sqlite3 ~/.hermes/watchdog.db \
  "SELECT source, external_id, title FROM events WHERE resolved_at IS NULL ORDER BY first_seen DESC LIMIT 40;"
```

and a rule (or ignore pattern) for anything real that shows up unmapped —
remembering a `uk` rule almost always wants the title-derived target
(`uk:some-monitor-name-push`), never the bare numeric id — exactly what the
daily digest is for.

The `_readme` key is a JSON-native comment block (JSON has no real comments);
it explains the same contract from inside the file itself.

## Bounds

| Constant | Default | Env override | Why |
|-|-|-|-|
| `MAX_OPEN_INVESTIGATIONS` | 3 | `TRIAGE_MAX_OPEN_INVESTIGATIONS` | Concurrency ceiling on open CLUSTERS (distinct `dispatch_job`s in `investigating`) — sideclaw's own concurrency is shared with every other dispatch source |
| `DAILY_INVESTIGATE_BUDGET` | 8 | `TRIAGE_DAILY_INVESTIGATE_BUDGET` | Well under `WARDEN_DAILY_BUDGET`'s own 20/day (`lifecycle/policy.py`) so a triage storm can never starve interactive dispatch. Counts clusters, not member items |
| `MAX_CLUSTER_SIGNATURES` | 5 | — | Signatures riding in one cluster's brief; the rest wait for a later run |
| `VERB_TIMEOUT` | 260s | — | `env-check` runs TWO sequential ssh probes, each individually bounded by hermes-ops.sh's own `SSH_TIMEOUT=120` — the outer bound has to clear 240s or it would kill a legitimately slow-but-healthy probe |
| `MAX_BRIEF_CHARS` | 8000 | — | Mirrors `lifecycle/dispatch.py`'s own `MAX_BRIEF_CHARS`; enforced in Python (`_cap_brief()`) BEFORE `open_episode()` is ever called, so an oversize brief is capped, never refused |
| `EVIDENCE_TIMEOUT` | 20s | `TRIAGE_EVIDENCE_TIMEOUT` | Hard wall-clock bound per evidence gatherer (a stuck file read or a slow argo API call must never stall a 10-minute cron) |
| `EVIDENCE_CAP_CHARS` | 1200 | — | Per-key cap on rendered evidence text, before the whole-block cap below |
| `EVIDENCE_TOTAL_CAP_CHARS` | 3200 | — | Whole evidence block cap — well under `MAX_BRIEF_CHARS` so a 5-signature cluster (each pulling its own evidence) still leaves room for the rest of the brief |
| `DEFAULT_QUIET_RESOLVE_HOURS` | 2h | policy `quietResolveHours` | Grouped-source (`slack_alert`/`hermes_log`) quiet-timer resolve — see *Grouped-source resolution* |

`DAILY_INVESTIGATE_BUDGET` is counted from `dispatches.origin_event_id IS NOT
NULL AND created_at >= <today, UTC>` — the same marker `escalate_cluster()`
writes, so no separate accounting column is needed. Both caps are checked
once per run and decremented as clusters open, so a later repo in the same
run correctly sees an exhausted cap — including under `--dry-run`, where
`escalate_cluster()` always returns `None` (it never calls sideclaw, GitHub
or Slack — the `clients`/`lifecycle` modules are the boundary, and dry-run
never crosses it), so the caps still advance on the dry-run path
specifically so a multi-repo preview simulates what a real run would
actually allow. A Slack or sideclaw failure for one cluster logs to stderr
and returns without aborting the rest of the run — every DB write in the
loop is per-cluster and independently committed.

## CLI verbs

`triage.py` itself: `--run` (default) · `--dry-run` · `--db <path>` ·
`--snooze <signature> --hours N` · `--ignore <signature>` · `--reopen
<signature>` · `--close <signature> --reason <text>` · `--list`.
`--snooze`/`--ignore`/`--reopen`/`--close` mutate `triage_items` and exit
immediately — they never call Slack or sideclaw. `--close` transitions to
`closed` and refuses (non-zero) an empty `--reason` or an unknown signature,
same as `_set_state()` already refuses a reasonless `dismissed`. `--dry-run` runs the local bookkeeping
passes for real (ingest/reopen/unsnooze/classify/resolve — all side-effect-free
against `triage_items` alone) so a preview against a throwaway copy of
`watchdog.db` is meaningful, but never calls Slack (`post_blocks`/
`update_blocks`), sideclaw or GitHub — those are the only externally-visible
actions this file can take.

Separately, `scripts/warden` — the general-purpose CLI over the same
`clients`/`lifecycle` modules this loop calls as functions, for a human or an
agent driving a dispatch by hand:

| Verb | What |
|-|-|
| `dispatch <repo>` | Open a BARE sideclaw episode, no `triage_items` row (`--tier`, `--why`, `--context-file`, `--origin-*`) |
| `run <repo>` | Open an ITEM riding this file's own lifecycle (`--tier investigate\|implement`, `--why`, `--origin-*`) — see *Origins* below |
| `status <job>` | Read one job's current status |
| `list` | Open/today/all dispatches |
| `merge <job> --why --confirm` | Land a completed `implement` episode's pull request |
| `abort <item> --why` | Cancel an in-flight episode for a triage item |
| `revert <item> --pr N --why` | Record that a later, named PR undid this item's fix (→ `STATE_REVERTED`) |

`--run`/`--dry-run`/`--close`/… stay on `triage.py` — those are the loop's own
bookkeeping verbs, not dispatch-lifecycle ones, and have no reason to move.

## Why a LaunchAgent, not `hermes cron`

**This file originally proposed registering `triage.py` as a `hermes cron`
`no_agent` job** (via a thin `triage-cron.py` loader, the same shape as
`agents-cron.py`/`narratives-cron.py`/`dispatch-sweep-cron.py`). An adversarial
review found the structural flaw before that registration ever happened: a
`hermes cron` job is scheduled and run *inside* the `ai.hermes.gateway`
process, so the loop whose entire job is noticing Hermes is broken cannot run
when Hermes is down. Four real, currently-open `hermes_log` rows in
`watchdog.db` are exactly that class — one (`slack_bolt.AsyncApp: Failed to
connect`) open 11 days with `reminder_count` 5 and no action ever taken,
because the only delivery path a gateway-scheduled job has is the same Slack
connection that row says is broken.

`triage.py` is instead installed by `make setup` as its own LaunchAgent
(`com.jkrumm.warden-loop`, `launchd/com.jkrumm.warden-loop.plist.template`,
`StartInterval 600`) invoking this repo's own `.venv/bin/python3
scripts/triage.py --run` directly — no gateway process in the loop at all.
This is safe specifically because Slack delivery here was already independent
of the gateway: `post_blocks`/`update_blocks` call `chat.postMessage`/
`chat.update` over plain HTTP with a token from `resolve_slack_token()`
(`secrets-run read op://hermes/slack/bot-token`, same as `agents-overview.py`),
never the gateway's live `slack_bolt.AsyncApp` connection — so this keeps
working with the gateway fully stopped, which is the one case it exists for.

`scripts/triage-cron.py`, the thin `hermes cron`-registered loader this
section used to describe, is deleted — a `hermes cron` registration is now
the wrong home for this loop, and a dead loader that still works correctly is
a trap for the next reader (it would silently duplicate every card/dispatch if
ever registered by hand). `dispatch-sweep.py`'s own cron loader
(`dispatch-sweep-cron.py`) is unrelated and unaffected: that job only *reads*
sideclaw and folds a verdict onto a card `triage.py` already wrote, so it has
no chicken-and-egg dependency on the gateway being up.

## Operations — the crash-recovery unit (schema 5)

Steps 6, 8 and the deploy that follows a merge each make one external,
non-atomic call — `lifecycle/dispatch.py`'s `open_episode(tier="implement")`,
`lifecycle/merge.py`'s `plan_or_land(confirm=True)`, and its own
`rollout_after_merge()` — that this ledger cannot undo if the process dies
mid-call. `operations` (schema 5) is what makes restarting mid-chain lossless
rather than duplicating or misreporting the result, per DESIGN.md § Crash
recovery.

**Scope: the mutating chain only, three kinds.** `implement` covers the call
that opens a branch and a draft PR. `merge` covers ready-for-review + `PUT
/pulls/:pr/merge` + a branch delete. `deploy` covers the rollout AFTER a
merge — its own write-point since Wave 5, not folded into `merge` any more:
the ssh-argv path (`autoDeploy`) and the GitHub-Actions path
(`deployOnMerge`) are each their own external mutation with their own crash
window, and now that both run as in-process Python calls (not one bash
subprocess covering merge+deploy together) there is nothing stopping a write
between them — so there is one. `investigate`/validation episodes never get
one — they are read-only, run in their own sideclaw worktree, mutate nothing
outside it, and `dispatch-sweep.py`'s own pruned-job handling already covers
a forgotten job.

**`event_id` may be NULL.** Every operation this loop itself opens
(`maybe_auto_implement()`, `poll_validation_jobs()`) carries the triage
item's `event_id`. A Slack-door `implement` (`scripts/warden dispatch --tier
implement`) or a signed-approval spend (`lifecycle/approvals.py`'s
`execute_approved()`) has no triage item behind it at all — its operation's
`event_id` is NULL, and `reconcile_operations()` resolves it exactly the
same way, just skipping every item-state step afterward (there is no item to
move).

**The write order, always.** `record_operation()` INSERTs and commits BEFORE
the external call runs — the commit is the entire contract: the row has to be
durable before this process goes on to make the call. `complete_operation()`
writes the outcome AFTER the call returns — but ONLY when the return is
unambiguous. A `RemoteError` can follow a call the external system already
accepted (sideclaw may have already accepted the submission, or the merge may
have already landed before the response was lost), so the call sites
deliberately leave the row's `outcome` NULL in that case rather than guess —
"an ambiguous return is not a refusal." (`open_episode()` and `plan_or_land()`
each resolve their OWN gated call to `unknown` immediately when they catch
exactly this ambiguity in-process — that is a different thing from a row left
NULL by a genuine crash; see `lifecycle/operations.py`'s own docstring.)

**`outcome` is one of three values**, a closed allowlist (`operations.OUTCOMES`,
aliased in triage.py as `_OPERATION_OUTCOMES`): `done`, `failed`, or
`unknown`. `failed` means the call definitely never took effect (the request
never reached anything, or the external system gave a plain, parseable
refusal). `unknown` means this process cannot tell — and an item behind an
`unknown` operation is never retried: it stays in whatever non-`new` state
the claim put it in (`implementing`/`validating`, never rolled back), which
keeps it out of every eligibility query that could re-fire the same call.

**`reconcile_operations()` runs FIRST in every pass**, before even
`drain_intents()` — see `run()`'s own ordering. Every `operations` row with
`outcome IS NULL` (a genuine crash — nothing ever ran the completion code at
all) gets asked about directly: `clients.sideclaw.get(job_id)` for
`implement` (a pruned job returns a bare `None`, byte-identical to a job id
that never existed, so absence maps to `unknown`, never `failed`; `cancelled`
joins `failed`/`interrupted`); `gh pr view <pr> --repo <owner>/<repo>` for
`merge` (unchanged — GitHub is authoritative, and the PR is parsed from
`dispatches.artifact_url`). A `merge` operation GitHub reports as merged
always resolves to `done`, carrying `mergeCommit` in its receipt — never
`merge_blocked`, which is the exact "silently read as failure" bug DESIGN.md
names. For `deploy`: a `mergeCommit` (recovered from the receipt, or from the
sibling `merge` operation's own completed receipt) is looked up via
`clients.github.actions_runs()` — any run found resolves `done`, folding the
runs into the receipt, and (if the item is still `merged` on a
`deployOnMerge` repo) advances it to `liveness_pending`; no run found and the
operation started over 2h ago resolves `failed`; no run found and it is still
recent leaves the row genuinely open, no write at all, for the next pass to
ask again. An ssh (`autoDeploy`) deploy left open by a genuine crash has no
remote receipt to ask for at all, so it resolves `unknown` outright.
**An operation that reconciliation still cannot resolve moves its item to
`needs_human`** — "we do not know whether the world changed" is exactly the
case this system routes to a person.

**Proving it, deliberately: `WARDEN_KILL_AT`.** `scripts/lifecycle/chaos.py`'s
`crash_point(name)` is a no-op unless the env var `WARDEN_KILL_AT` names that
exact point, in which case the process exits hard (`os._exit(137)`, no
cleanup) right there — the same shape a real `kill -9` or a power loss would
leave behind. This is how the crash-recovery claims above are actually
exercised rather than merely reasoned about (DESIGN.md § Migration Wave 3's
stop-condition exercise). The closed list of points, in call order across a
chain: `before-implement-open`, `after-implement-op`, `after-implement-submit`
(all three in the implement path), `before-merge`, `after-merge-op`,
`after-merge-put`, `after-merged-at`, `after-deploy-op`,
`after-merge-before-state` (the merge/deploy path), `before-fixed` (the
liveness-confirm write). A name outside this list raises `ValueError` —
chaos points are a closed allowlist, same reasoning as `_SET_STATE_COLUMNS`.

## Closing the loop — verdict → implement → validate → merge → deploy → verify

The triage loop used to stop at a verdict. Five functions close the rest of
the chain, each polling its own state once per run and re-deriving its own
eligibility from the DB every time — no step trusts a previous step's memory,
only what is actually recorded. No LLM call happens in any of them; the
dispatched episodes each run one, same as `escalate_cluster()` always has.

**Step 6 — `maybe_auto_implement()`.** A `verdict`-state item whose folded
investigate verdict already reads `nextAction: implement` at
`confidence: high`, and has never been auto-implemented before
(`implement_job IS NULL`), is checked against `lifecycle.policy`'s own
`require_auto_from_item()` and `check_repo_not_in_flight()` — the same
precondition/in-flight checks a `--auto-from-item` dispatch always ran, now
read straight off the ledger instead of re-derived inside a subprocess. A
refusal here is a DEFERRAL, never a rollback: nothing was claimed yet, so
the item stays in `verdict` with the reason written into `note`, prefixed
`deferred: ` (DESIGN.md § What must not be lost — a deferral must be
visible), and the card is synced immediately. On success the item is claimed
(compare-and-set to `implementing`, BEFORE the episode opens — this is the
whole fix for the historical duplication bug, see `lifecycle/dispatch.py`'s
own `open_episode()`), then `open_episode(tier="implement", …)` is called
directly — no subprocess, no CLI door. `RemoteError(maybe_mutated=True)`
(sideclaw may have already accepted the job) leaves the item claimed with
the operation already resolved `unknown` by `open_episode()` itself; every
other refusal hands the claim back to `verdict`. State: `verdict` →
`implementing`.

**Step 6 → 7 — `poll_implement_jobs()` reads a TYPED outcome, not just
`artifactUrl`.** A `done` implement job's `result.outcome`
(`clients.sideclaw.DISPATCH_OUTCOMES`, schema version
`DISPATCH_SCHEMA_VERSION` — pinned in `clients/sideclaw.py`, checked by
`assert_result_schema()` on every poll; a mismatch is a loud `needs_human`,
never a best-effort parse) drives the state, not a guess from which fields
happen to be present:

| `result.outcome` | next state | note |
|-|-|-|
| `pr_opened` (with `artifactUrl`) | `validating` | opens the step-7 `review` job below |
| `checks_failed` | `needs_human` | a red check is a human's, never a PR |
| `no_changes` | `merge_blocked` | the episode's own reason |
| `diff_refused` / `branch_no_pr` / `pr_failed` / `withheld` | `merge_blocked` | outcome named in the note |
| `salvaged` | `needs_human` | sideclaw itself failed to get a structured verdict |
| `issue_declined` / `issue_failed` / `issue_filed` / `verdict_only` | `needs_human` | wrong tier's outcome — never guessed |
| missing / unrecognized | `needs_human` | `unknown implement outcome '<x>'` |
| `result.nextAction == "human"` | `needs_human` | overrides every row above |

A non-`done` terminal status (`failed`/`interrupted`/`cancelled`) blocks the
chain outright — `merge_blocked`, never a silent drop. State: `implementing`
→ `validating` | `merge_blocked` | `needs_human`.

**Step 7 — `_open_validation_dispatch()` → sideclaw's own `review` job, not a
second `investigate` episode.** Once the implement job's outcome is
`pr_opened`, this parses the PR number out of `artifactUrl` (a strict
`/pull/(\d+)$` regex — unparseable is `merge_blocked` with `could not parse
the PR number`, never a guess) and opens a sideclaw **`review`** job
(`clients/sideclaw.py`'s `submit_review()`, `lifecycle/dispatch.py`'s
`open_review()`) against that PR, in a throwaway read-only worktree sideclaw
manages itself — not a second `dispatch` episode on a different model asked
to end its prose with a marker phrase. `review` already runs a multi-angle
synthesis (architect, senior-dev, security, ... — its own router picks the
rest) and returns a TYPED verdict (`outcome`/`blocking`/`improvements`/
`discussions`/`testGaps`/`summary`), schema version `REVIEW_SCHEMA_VERSION`.
The `dispatches` row this opens carries `tier='review'` — sharing
`open_episode()`'s exact INSERT shape via `open_review()`, so `dispatches`
never grows a third writer — and has no `origin_channel` (`Origin(event_id=…)`
only), so `dispatch-sweep.py` never tries to render it as a verdict; it rides
the ordinary "no origin_channel → closed with the undeliverable sentinel"
path every other originless dispatch already takes. `validation_job_id` on
the IMPLEMENT dispatch's own row is bound the moment this opens, same as
before. State: `implementing` → `validating`.

**Step 8 — `poll_validation_jobs()` → `lifecycle.merge.plan_or_land()`.**
Reads the `review` job's TYPED outcome, not a marker substring-matched out of
prose:

| review `result` | `dispatches.validation_status` | item state |
|-|-|-|
| `outcome == "clean"`, or `"actionable"` with `blocking` EMPTY | `confirmed` | calls `merge` |
| `blocking` non-empty (any outcome) | `blocked` | `merge_blocked` — note is the first three findings as `file:line — message`, ≤600 chars |
| `outcome == "needs-human"` | `needs_human` | `needs_human` — note is the review's own summary |
| FAILED / ERRORED / CANCELLED | `error` | `merge_blocked` |

`result.schemaVersion` is asserted against `REVIEW_SCHEMA_VERSION` first — a
mismatch is a loud `needs_human`, never a best-effort parse, same rule as
step 6→7. Only `confirmed` calls `plan_or_land(confirm=True, dry_run=False)`
in-process. `confirm=True` is instruction-level, not signed (owner decision —
confirming the implement WAS the approval, landing it finishes the thing
already said yes to), so this call is not itself a trust boundary — the real
bounds are inside `plan_or_land()`/`merge_gate_check()` itself, re-keyed off
**declared path scope**: see that module's own docstring for the full gate
(`autoMergePaths`, `noCiRequired`, the step-7 `validation_status` check).
`plan_or_land()` owns its own `merge` operation end to end (recorded before
the mutating call, completed after) — triage.py no longer records one
itself. A `PolicyError`/`PreconditionError` blocks the merge; a
`RemoteError(maybe_mutated=True)` leaves the item in `validating` untouched
(the merge may already have landed — `reconcile_operations()` asks GitHub on
the next pass); any other `RemoteError` blocks. State: `validating` →
`merged` | `liveness_pending` | `merge_blocked` | `needs_human`.

**Step 9 — deploy, inside `plan_or_land()`'s own `rollout_after_merge()`, OFF
by default.** Merging a repo like `vps` is not shipping — `observability/`
has no CI, so an alert change only takes effect once `make hyperdx-apply
ENV=prod` actually runs. On a successful merge, `rollout_after_merge()`
checks the repo's own `config/triage-policy.json` entry: `autoDeploy`
(**true today for `vps`** — the mechanism has been proven safe with it off,
see STATE.md, and is now live) and a `deploy` key from its own closed
allowlist (`clients/rollout.py`'s `ROLLOUTS` — a policy file names a KEY,
never a command, same principle as `verb`/`evidence` above). This is its own
`deploy` operation now (schema 7), recorded and completed by
`rollout_after_merge()` itself, never folded into the `merge` operation's
receipt. For every path matching `observability/alerts/*.json` in the merged
diff, the deploy step also fetches that file's content AT THE MERGE SHA
(GitHub's contents API, never the local checkout) and folds
`name`/`threshold`/`thresholdType` into `poll_validation_jobs()`'s own
`deploy_expect_json` write on the triage item — what step 10 verifies
against. State: `merged` (no deploy) | `liveness_pending` (deploy attempted
and succeeded).

**Step 8b — merge-is-deploy (`deployOnMerge`, item 1b).** A third outcome of
the same `poll_validation_jobs()` merge branch, checked only once the
`deploy.attempted && deploy.ok` case above has already said no (a
`deployOnMerge` repo never declares `autoDeploy`, so the two never actually
compete): if the repo's `config/triage-policy.json` entry sets
`"deployOnMerge": true` and `plan_or_land()`'s own `MergeResult.merge_commit`
carried a usable 40-char-hex sha, the item goes straight to
`liveness_pending` on that sha — `deploy_expect_json` becomes
`[{"commit": "<sha>"}]`, the same list-of-dicts shape step 9's
alert-expectation payload already uses, just with one key instead of three.
There is no ssh half here for `deploy` to be `attempted`/`ok` about — this
repo's own CI/CD (GitHub Actions → RollHook) *is* the deploy, which
`DESIGN.md` § Deploy already claimed and had no code path for until this
slice (see docs/history/state-log.md §47's reconnaissance). The receipt `ssh <host> make
<target>` structurally cannot provide — an exit code to a process that is
dead the moment it returns — a GitHub Actions run CAN: it has an id,
queryable after the fact by anyone. `rollout_after_merge()`'s own `deploy`
operation already carries the run identity (`clients.github.actions_runs()`
— `"unknown"` only when the read itself failed, an empty list, left OPEN for
`reconcile_operations()`, when the workflow genuinely has not appeared yet;
neither is fabricated, and neither is retried in a loop by the live path).
`poll_validation_jobs()` itself does not re-query Actions at all any more —
the receipt is already on the deploy operation by the time it reads
`result.deploy`/`result.merge_commit`. A merge result with no usable sha
falls through to plain `merged` with a reason instead — entering
`liveness_pending` with nothing to compare against would just sit until
`liveness_deadline` and reopen the item, worse than an honest `merged`.
State: `validating` → `liveness_pending` (deployOnMerge, usable sha) |
`merged` (deployOnMerge, no usable sha).

**Step 10 — `maybe_check_liveness()`.** The item must not close because the
alert went quiet — a fully-down service is also quiet, the same principle
`resolve_quiet_grouped()`/`resolve_recovery_paired()` already apply above.
Runs the repo's declared `liveness` key (`config/triage-policy.json`, same
closed-allowlist shape) against `deploy_expect_json`; seeded with two:
`hyperdx-alert-state`, which re-reads every expected alert's LIVE
`threshold`/`thresholdType` from `GET
https://hyperdx.jkrumm.com/api/api/v2/alerts` (the SAME REST endpoint
`vps/scripts/hyperdx-sync.sh`'s own `export`/`apply` already use — read from
that script, never a fabricated endpoint) and asserts it matches what the
merged diff set; and `argo-commit-live` (item 1b), which re-reads argo's own
`GET https://argo.jkrumm.com/api/health` and asserts its `commit` field
matches the merge sha EXACTLY — never inferred from the service merely being
reachable, since a restart-time-only probe cannot distinguish a landed
deploy from a container that bounced for an unrelated reason (the exact
ambiguity docs/history/state-log.md §47's own research-gateway/meteo reconnaissance hit, and
part of why `argo` is the deployOnMerge repo seeded here rather than one of
those two). Both gatherer functions own their own endpoint directly, never a
config-driven URL — a policy file may name and parameterise a behaviour,
never express one (`DESIGN.md` principle 4). Only a genuine POSITIVE match resolves the item
(`LIVENESS_CONFIRMED_NOTE_PREFIX`, the one prefix in this file that actually
claims "fixed" — every other resolve note deliberately doesn't). Past
`liveness_deadline` (`LIVENESS_WINDOW_HOURS`, default 2h) with no positive
match, the item is REOPENED to `new`, carrying the PR link and the last
liveness check on the existing card thread (a direct `update_blocks()` call,
mirroring `_dissolve_cluster()`'s own final-update-then-reset shape — never
through `sync_card()`, because a `new` row must never be carded) rather than
sitting "deployed" forever or silently vanishing. This is precisely the
context that was missing when the same alert was re-diagnosed 61 times
before this file existed at all.

### `config/triage-policy.json`'s `repos` object

```json
{
  "repos": {
    "vps": {
      "autoMergePaths": ["observability/**"],
      "noCiRequired": true,
      "deploy": "hyperdx-apply",
      "autoDeploy": true,
      "liveness": "hyperdx-alert-state"
    },
    "argo": {
      "deployOnMerge": true,
      "liveness": "argo-commit-live"
    }
  }
}
```

`autoMergePaths`/`noCiRequired`/`deploy`/`autoDeploy` are read by
`scripts/lifecycle/merge.py` (via `WARDEN_TRIAGE_POLICY`, defaulting to this
same file); `liveness` and `deployOnMerge` are read here, by
`maybe_check_liveness()` and `poll_validation_jobs()` respectively. Every
changed path in a PR must match `autoMergePaths` or the merge refuses
outright — a repo absent from `repos`, or with no `autoMergePaths`, refuses
too; there is no implicit allow. `noCiRequired` is the explicit
acknowledgement that a repo has zero PR-time required checks, so their
absence reads as a KNOWN condition rather than `mergeable_state: clean`
being silently read as "CI passed" (measured wrong — `clean` is vacuously
true whenever nothing ran, which is `vps`'s and `research-gateway`'s exact
shape: no `.github/workflows` at all). A repo with zero check-runs and no
`noCiRequired` entry FAILS the gate now, on purpose.

`argo`'s entry above is seeded with **deliberately no `autoMergePaths`** —
`merge_gate_check()` still refuses every merge for this repo outright
(`NOPATHS`, there is no implicit allow), so `deployOnMerge` builds the
merge-is-deploy MECHANISM (item 1b) with the merge gate closed. Opening it is
a separate operator decision; the live policy file says so next to the entry
so nobody "completes" it by adding a scope later without meaning to.

## Silencing `#alerts`

`config.yaml`'s `slack.require_mention_channels` now includes `C0AS1LAUQ3C`
(`#alerts`) alongside the two pre-existing echo channels — see that file's own
comment block for the full before/after. Mentioning Hermes directly in
`#alerts` still works for an ad-hoc question; only the reflexive per-message
LLM turn is gone.

## Tests

`tests/test_triage.py`, run with
`.venv/bin/python3 tests/test_triage.py` (this repo's
`venv` has no pytest — see that file's own docstring; every `test_*` function
is still plain-`assert`, argument-free, so it is valid standalone pytest input
too, and the test file's `main()` runner would be redundant if pytest is ever
installed). 30 cases, covering: dedup (one card, one investigation across
repeated runs), an unescalated (`new`) item never getting a card — mapped or
not, the card-hash short-circuit, both edges (`events.dispatch_id`,
`dispatches.origin_event_id`), `minOccurrences`/`minOpenMinutes`/snooze/ignore/
`ignoreUnstructuredSlackProse` withholding, unstructured prose landing in
`STATE_NOTE` (not `ignored`), producing zero cards, and appearing in the
digest payload, `uk` mapping via the title-derived match target rather than
its opaque external_id, `op_refs_homelab`/`op_refs_vps` being ingested and
routed to the `env-check` verb rather than an episode (both the dangling-item
and the nothing-dangling shapes), an unknown verb key being rejected at
`load_policy()` time, the shipped `config/triage-policy.json`'s `api-*`/
`dashboard-*`/argo rules resolving to `vps` (not `argo`, not the service's
own repo), the concurrency and daily budget caps (including their simulation
under `--dry-run` across multiple repos in one pass), a denied and an
unmapped repo never dispatching, two eligible same-repo items clustering into
exactly one dispatch/card with both edges written on every member and both
signatures in the brief, two eligible different-repo items producing two
independent dispatches, a cluster dissolving back to individually-eligible
`new` items on a `UNRELATED SIGNATURES` verdict, the brief traveling on
stdin capped at 8000 chars (the one test using a real subprocess stub rather
than the in-process fake dispatcher), resolution updating the card exactly
once, `--dry-run` touching neither Slack nor sideclaw, artifact-url survival
across a reopen, `fold_dispatch_verdict()` updating every member of a cluster
(not just the primary), and — directly against `scripts/watchdog-poll.py`,
not through triage.py — two `raw:` op-refs stderr strings differing only in
their timestamp producing the identical dedup key.

42 cases total — the later 12 cover *Evidence commands* and *Grouped-source
resolution*: each of the four evidence keys producing correctly-shaped,
bounded output (`meteo-health` summarizing rather than dumping, `gateway-
starts` showing only the 5 most recent, `hermes-log-tail` excluding every
line before its computed gateway-start boundary, `kuma-push-last` picking the
chronologically latest bracket match and normalizing away the glyph), a
raising gatherer folding into an error string without taking down a sibling
key's evidence or the run, an unknown evidence key dropping the whole rule at
`load_policy()`, a brief whose evidence had to be cut still landing under
`MAX_BRIEF_CHARS` with its closing structure intact and a truncation note
present, a grouped item resolving after `quietResolveHours` and updating its
card exactly once, an in-flight `investigating` item never being yanked to
resolved by the quiet timer, the `✅`-recovery-pairing path resolving on the
very next run without waiting out the timer, and `--dry-run` making zero
Slack calls for the pairing check.

54 cases total as of the auto-implement chain (steps 6-10) — the final 12
cover: a `new -> resolved` transition with no prior card making zero Slack
calls and a carded `new` item's resolve making exactly one `chat.update` and
zero `chat.postMessage` (the 2026-09-08 correction, both directions), plus an
`investigating` item NOT being discharged when its signal disappears
(_SILENCE_RESOLVE_ELIGIBLE_STATES); `maybe_auto_implement()` firing on a `confidence: high` verdict
and never firing at `medium`; `poll_implement_jobs()` opening the step-7
validation on a successful implement episode and blocking outright on a
failed one; a `review` verdict carrying `blocking` findings refusing the
merge without ever calling it and writing `validation_status='blocked'`; a
CONFIRMED validation
merging and landing `merged` when deploy is off, or `deploy_expect_json`
lands correctly and `liveness_pending` is entered when deploy succeeds;
liveness CONFIRMING and resolving the item (exactly one `chat.update`),
liveness FAILING past its deadline and REOPENING the item to `new` with the
PR and last check on the card (exactly one `chat.update`, zero
`chat.postMessage`), and liveness still inside its window neither resolving
nor reopening (zero Slack calls).

64 cases total as of `propose_mappings()` — the final 9 cover: the 24h
cursor preventing a second model call within the same window (and allowing
one past it), the age threshold excluding a signature younger than
`proposeMappingsAgeDays`, an unparseable model response being non-fatal
(no raise, nothing applied), a proposed repo that does not resolve under the
dispatch root being dropped (and separately, one that resolves but is
`deny`d), an `unsure` verdict suppressing re-proposal of that exact signature
for 7 days and lifting after, the policy file round-tripping with `_readme`
and top-level key order intact after an applied `map` proposal, the commit
(and the write itself) being skipped when `config/triage-policy.json` already
carries a pending change, and `--dry-run` making zero model calls.

**The current total is 107.** Every figure above (30 / 42 / 54 / 64 / 96 / 102)
is the running tally *as each group of cases landed*, not the count today —
they are kept because each paragraph describes what its own group covers. The
number that governs is the one in `CLAUDE.md`: the regression gate, at
**148/148**, and any other number is a finding rather than a count to edit.
The 32 cases past 64 are Wave 1's: the quiet rule (silence-resolve restricted
to `new`), the intent queue's loop drain, and the deadline table. The 6 past
96 are the `occurrence_mark` reopen fix (§ Known: grouped reopen churn). The 5
past 102 are the `resolved` split (§ The `resolved` split above):
`resolve_recovery_paired()` never producing `fixed`, a `quiet`/`fixed`/`closed`
card rendering with its caveat/reason, `_set_state()` recording exactly one
`item_transitions` row per real change and none for a column-only write,
`cmd_close`, and `sync_card()`'s never-carded guard reaching all four
never-carded-first states.

## Known: grouped reopen churn, and why brain-sync is invisible

One of two related defects, both measured 2026-09-09, is now fixed; the other
is still open. Both are written down because each is easy to re-derive
wrongly.

### A grouped item reopened every run — FIXED

`reopen_if_needed()` used to reopen a `resolved` (and, since Wave 1, a
`dismissed`) triage item whenever its event had `resolved_at IS NULL`. For a
grouped source (`slack_alert`, `hermes_log`) that column stays NULL for
months — `watchdog-poll.py`'s `sweep_stale_grouped()` only clears it after 7
idle days, deliberately — so that predicate was true on every single pass. A
quiet-resolved grouped item was reopened on the very next run, quiet-resolved
again, and repeated every ten minutes forever.

It was invisible only by luck: the re-rendered card is byte-identical, so
`card_hash` short-circuited the Slack call. It was not harmless — it destroyed
history. **Measured on the live ledger 2026-09-09: 23 of 30 `resolved` rows**
were stuck in this loop, and it had already overwritten the only two
positive-signal closes the system has ever produced (items 931/932, closed by
`resolve_recovery_paired()` after `jkrumm/vps#8` merged): the very next pass
reopened them and `resolve_quiet_grouped()` wrote its own `signal quiet
since …` note over `RECOVERY_PAIRED_NOTE_PREFIX`, which appears on zero rows
in the live database as a result.

The fix (ledger schema_version 3, `triage_items.occurrence_mark`) reopens on a
**new occurrence**, never on `resolved_at IS NULL`. `_occurrence_mark()`
fingerprints the event with five `|`-separated slots — `payload_json.ts_last`,
`last_reminder_at`, `notified_at`, `first_seen`, `reminder_count` — and
`reopen_if_needed()` compares the current fingerprint against the one
`_set_state()` stamped at the row's last transition, with `!=`, never `>`.
That `!=`-not-`>` distinction is load-bearing: `ts_last` is a Slack `ts`
float-string ("1788850795.862159"), the other four slots are ISO-8601
("2026-09-08T07:00:20…") — two different clocks in two different formats, and
a `MAX()`/`>` across them is a lexical compare of "1788…" against "2026…"
that reads as correct and is not. Fixed slots plus whole-string equality never
compares one clock against the other. Neither family alone would do: a
suppressed grouped occurrence moves only `ts_last` (`upsert_grouped()`
re-stamps `last_reminder_at`/`notified_at` only when it actually emits,
cooldown-gated), and a state source has no `ts_last` at all — its reopen
signal is `watchdog-poll.py:878` resetting `resolved_at`/`first_seen`/
`notified_at`/`last_reminder_at`/`reminder_count`, which moves the ISO slots.

A `resolved`/`dismissed` row whose stored `occurrence_mark` is NULL (closed
before this column existed — every row on the live ledger, at cutover) is
**not** reopened: `reopen_if_needed()` treats NULL as "baseline unknown",
stamps the row's current mark, and leaves the state alone. It then reopens on
the next genuine occurrence like any other row. That adoption rule is the
entire backfill; the migration writes no data itself.

**One asymmetry the mark inherits, measured 2026-09-09 and deliberately not
fixed here.** `hermes_log` is a grouped source whose payload has **no
`ts_last`** — `poll_hermes_logs()` builds `{"first_line": …}`, not the
`{"ts_first", "ts_last"}` shape `upsert_grouped()`'s Slack grouping produces.
So on a *cooldown-suppressed* occurrence (`REM_HOURS["hermes_log"]` is 24h)
that source writes only `batch_count_last`, which is not in the mark, and the
event row is otherwise byte-identical: 11 of the 30 live `resolved` rows carry
a mark with an empty first slot, 6 of them `hermes_log`. Such a row does not
reopen until the 24h cooldown lets `upsert_grouped()` emit and move
`last_reminder_at`.

That is **not** a regression and not a lost signal. Under the old predicate the
same row was reopened and then re-resolved by `resolve_quiet_grouped()` inside
the *same* pass — `escalate()` runs after both — so a suppressed occurrence
never re-escalated anything then either. Re-escalation latency stays bounded by
the source's own cooldown, which is what the cooldown is for. It is recorded
because it looks like an accident and is not, and because it belongs with the
self-tuning quiet window below: that is where "the signal is still firing but
suppressed" is the actual subject.

**A second asymmetry the mark inherits, also measured 2026-09-09 and also
deliberately not fixed here.** For a **state** source (`uk`, `docker_*`,
`github_pr`, `github_issue`, `hermes_cron`, `op_refs_*`, `stray_skill` —
anything NOT in `GROUPED_SOURCES`) whose event's `resolved_at` stays
continuously NULL — the monitor never clears, it just keeps firing — while the
item itself has already gone terminal (`dismissed` after the 7-day
`needs_human` fuse, or `closed`), the only writer in `watchdog-poll.py` that
moves any of `_occurrence_mark()`'s five slots is `reconcile()`'s reminder
bump at line 928 (`last_reminder_at`, `reminder_count`), gated by
`REM_HOURS[source]`. Everything else a recurrence writes —
`title`/`url`/`payload_json` at line 887 — is deliberately outside the mark
(see the grouped-source paragraph above for why: those columns are cosmetic,
not occurrence signal). So reopen latency for such a row is bounded by the
source's own reminder cadence: **6h** for `uk`/`docker_*`/`op_refs_*`/
`hermes_cron`, **72h** for `github_pr`, **168h** (a full week) for
`github_issue` and `stray_skill`. Live, 13 of 32 stamped marks carry an empty
`ts_last` slot: 7 `hermes_log` (the asymmetry above) and **6 `uk`** — the
`hermes_log` paragraph alone does not account for the other half.

Both facts, honestly. Under the OLD `resolved_at IS NULL` predicate, a row
like this reopened on the very next pass — which sounds better and is not: it
meant `dismissed` was **effectively unreachable** for a state source with a
continuously-open event, because the 7-day fuse could fire and the very next
10-minute pass would reopen the item before anything downstream ever saw it
sit dismissed. The new bound trades that for a real latency — up to a week, on
two low-volume sources — in exchange for a `dismissed` state that actually
retires something.

### brain-sync is never investigated

27 `Brain Sync - Push` DOWN messages in a week, the third-largest alert source,
and the loop has never once looked at it. Three causes stack:

- **The correctly mapped signature never opens.** `uk:brain-sync-push` → `dotfiles`
  is a real rule, but `uk:209` requires the monitor to still be down at the next
  poll (`UK_DOWN_GATE_MIN = 30`). brain-sync recovers in minutes, so the event
  resolved on 2026-09-07 with `notified_at` NULL and has not reopened since.
- **What reaches Slack is a different signature.** The message a human sees is the
  `[Local]` parent group's `Child monitors down: Brain Sync - Push`, which passes
  `skip_uk_push` (no `Push]` in its own brackets), lands as `slack_alert`, and is
  **unmapped** — so it never escalates.
- **It quiet-resolves before it can accumulate.** Median gap between firings is
  1.4h, but **10 of 26 gaps exceed the 2h quiet window**, so the item closes
  during an ordinary lull and starts over.

A fixed quiet window silently defeats itself on any signal whose own period is
near it. The intended fix is to widen the window per resolve/recur cycle —
self-tuning, needing no knowledge of the signal's period — and it was blocked
only on the reopen churn above being invisible: widening the window varies the
resolve note, which used to turn the silent churn into a `chat.update` every
ten minutes. With `occurrence_mark` in place there is no more churn to
uncover, so this fix is now **unblocked** and still open.
