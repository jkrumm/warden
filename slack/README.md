# Slack app identity

App ID: `A0C13NMFLD9` — use it for every later `apps.manifest.update`.

`app-manifest.json` declares warden's own Slack bot, so triage cards,
remediation receipts and reminders are attributable at a glance instead of
riding under the Hermes bot's username (HomeLab, Hermes, VPS and Argo already
each have their own app — this is warden's).

Create or update it with an **app configuration token** — a human-only,
12-hour credential minted at https://api.slack.com/apps → *Your App
Configuration Tokens*. Never store it; pass it to one `make` target and it
never touches disk:

```bash
# create
make slack-app-create SLACK_CONFIG_TOKEN=xoxe-...
# update (after editing app-manifest.json)
make slack-app-update SLACK_CONFIG_TOKEN=xoxe-... APP_ID=<app-id-from-create>
```

Equivalently, by hand:

```bash
# create
curl -s -X POST https://slack.com/api/apps.manifest.create \
  -H "Authorization: Bearer <xoxe-config-token>" -H 'content-type: application/json' \
  -d "$(jq -n --slurpfile m app-manifest.json '{manifest: $m[0]}')" | jq '{ok, app_id, error}'
# update
curl -s -X POST https://slack.com/api/apps.manifest.update \
  -H "Authorization: Bearer <xoxe-config-token>" -H 'content-type: application/json' \
  -d "$(jq -n --arg id <APP_ID> --slurpfile m app-manifest.json '{app_id: $id, manifest: $m[0]}')" | jq '{ok, error}'
```

Then, in order — the owner's steps, nothing here can do these for you:

1. Install it to the workspace once in the UI (*OAuth & Permissions →
   Install*). Re-install after every scope change — the token keeps its
   value but only gains scopes on install.
2. Store the Bot User OAuth Token in 1Password at
   `op://common/slack/WARDEN_BOT_TOKEN`.
3. Only after step 2 has a value: add that ref to
   `~/SourceRoot/dotfiles-private/headless.refs` and run `make secrets-seed`
   on the MacBook. Not before — `secrets-seed.sh` is `set -euo pipefail` and
   dies on an unresolvable ref, which breaks the next reseal for every
   consumer on the mini. The mini resolves secrets from the cache that seeds — the mini resolves secrets from the
   offline cache that seeds, never `op` directly (see `~/.claude/CLAUDE.md`
   § Secrets).
4. Nothing else. `chat:write.public` reaches `#agents` (`C0BVDE5R562`,
   public) without an invite — no channel-invite step, no incoming-webhook,
   no socket mode.
