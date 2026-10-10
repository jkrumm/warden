# warden — the control plane. See DESIGN.md for what it is, STATE.md for where it is.
#
# Everything here assumes the mini. warden's loop, its pollers, its backup and its
# read-only HTTP API are LaunchAgents, never `hermes cron` jobs: the loop that
# notices Hermes is broken cannot depend on Hermes being up to run it. That
# argument is DESIGN.md's reason for the whole repo, and it is why these five
# plists exist. com.jkrumm.warden-api is the odd one out shape-wise — a
# long-running server (KeepAlive, no StartInterval) rather than a periodic job —
# see its own template for why.

WARDEN_REPO := $(shell pwd)
WARDEN_HOME := $(HOME)/.warden
VENV        := $(WARDEN_REPO)/.venv
PY          := $(VENV)/bin/python3

# Pinned deliberately. The extracted loop is 3400 lines written and proven against
# 3.11.15 (the interpreter hermes-agent's venv runs); moving it to a new interpreter
# in the same change as moving it to a new repo would mix two failure sources. The
# upgrade is its own verifiable step, later.
BASE_PY     := python3.11

WARDEN_PLISTS := com.jkrumm.warden-loop com.jkrumm.warden-poll \
                 com.jkrumm.warden-sweep com.jkrumm.warden-backup \
                 com.jkrumm.warden-api

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "warden"
	@echo "  make setup     venv + plists + load the agents + warden on PATH"
	@echo "  make venv      create .venv from $(BASE_PY) and install requirements"
	@echo "  make check     compile every script + run every test suite"
	@echo "  make test      run every tests/*.py (hand-rolled runners, not pytest)"
	@echo "  make deploy    kickstart warden-api on this checkout, health-check, roll back on failure"
	@echo "  make verify    production health: /health ok and every agent's last exit 0"
	@echo "  make logs      bounded tail of every agent's logs"
	@echo "  make status    what is loaded, what ran last, is the ledger reachable"
	@echo "  make agents    (re)load the LaunchAgents from launchd/"
	@echo "  make unload    unload the LaunchAgents"
	@echo "  make slack-app-create SLACK_CONFIG_TOKEN=xoxe-...           create the Warden Slack app"
	@echo "  make slack-app-update SLACK_CONFIG_TOKEN=xoxe-... APP_ID=... update it — see slack/README.md"

# `assert-main-checkout` gates every target that writes machine-global state (render-plists,
# agents, link). `venv` writes only this checkout's .venv, so it stays usable from a worktree
# (`make check` there needs it). `setup` runs the gate first and only then the rest in a
# sub-make, so under `make -j setup` nothing — not even venv — starts before the gate fails.
.PHONY: setup
setup: assert-main-checkout
	@$(MAKE) --no-print-directory -f $(firstword $(MAKEFILE_LIST)) venv render-plists agents link
	@echo "warden: setup complete — run 'make status'"

# A wrapper, not a symlink: scripts/warden resolves the venv relative to its own path.
BIN_LINK := $(HOME)/.local/bin/warden

# WARDEN_REPO is $(shell pwd), so `make link` from a linked worktree would repoint
# ~/.local/bin/warden at a checkout that is deleted when the worktree goes — an episode
# once left the CLI dangling that way. Refuse unless this is the main checkout: a linked
# worktree reports a per-worktree --git-dir but shares the main --git-common-dir.
#
# The guard must discover the checkout actually being invoked, not an inherited override:
# clear GIT_DIR / GIT_COMMON_DIR / GIT_WORK_TREE first, or `GIT_DIR=/main/.git make link`
# from a worktree reports the main dir for both answers and the check passes.
#
# Fail closed: if git cannot answer at all (not on PATH, not a git repo, or the worktree
# metadata is already gone) both substitutions are empty and `[ "" = "" ]` would be true,
# treating an unknown checkout as the main one and overwriting the wrapper anyway. Require
# both answers to be non-empty before the equality test can pass.
.PHONY: assert-main-checkout
assert-main-checkout:
	@unset GIT_DIR GIT_COMMON_DIR GIT_WORK_TREE; \
	 dir="$$(git -C "$(WARDEN_REPO)" rev-parse --git-dir 2>/dev/null)"; \
	 common="$$(git -C "$(WARDEN_REPO)" rev-parse --git-common-dir 2>/dev/null)"; \
	[ -n "$$dir" ] && [ "$$dir" = "$$common" ] || { \
		echo "warden: refusing — cannot confirm $(WARDEN_REPO) is the main checkout."; \
		echo "        git reports a worktree, or cannot read the checkout at all."; \
		echo "        The wrapper would point every shell at a checkout deleted with the worktree."; \
		echo "        Run 'make link' from the live checkout ($(HOME)/SourceRoot/warden) instead."; \
		exit 1; \
	}

.PHONY: link
link: assert-main-checkout
	@mkdir -p "$(dir $(BIN_LINK))"
	@printf '#!/bin/sh\nexec "%s/scripts/warden" "$$@"\n' "$(WARDEN_REPO)" > "$(BIN_LINK)"
	@chmod +x "$(BIN_LINK)"
	@echo "  ✓ $(BIN_LINK) -> $(WARDEN_REPO)/scripts/warden"

# ---------------------------------------------------------------------------
# venv
# ---------------------------------------------------------------------------

.PHONY: venv
venv:
	@command -v $(BASE_PY) >/dev/null 2>&1 || { \
		echo "warden: $(BASE_PY) not found. It is pinned on purpose — see BASE_PY above."; \
		echo "        install it (uv python install 3.11) rather than switching interpreters here."; \
		exit 1; \
	}
	@if [ ! -x "$(PY)" ]; then \
		echo "  creating $(VENV) from $$($(BASE_PY) -V)"; \
		$(BASE_PY) -m venv "$(VENV)"; \
	fi
	@"$(PY)" -m pip install --quiet --upgrade pip
	@"$(PY)" -m pip install --quiet -r "$(WARDEN_REPO)/requirements.txt"
	@echo "  ✓ venv ($$("$(PY)" -V))"

# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

# The suites are hand-rolled: each file collects its own `test_*` functions and
# exits non-zero on the first failure it reports. There is no pytest in this venv
# and the suites do not need one — every test function is argument-free, so the
# files stay valid pytest input if that ever changes.
.PHONY: test
test:
	@[ -x "$(PY)" ] || { echo "warden: no venv — run 'make venv'"; exit 1; }
	@fail=0; ran=0; \
	for t in $(WARDEN_REPO)/tests/test_*.py; do \
		[ -e "$$t" ] || continue; \
		ran=$$((ran+1)); \
		printf '  %-32s ' "$$(basename $$t)"; \
		if out=$$("$(PY)" "$$t" 2>&1); then \
			echo "$$out" | tail -1; \
		else \
			echo "FAILED"; echo "$$out"; fail=1; \
		fi; \
	done; \
	[ $$ran -gt 0 ] || { echo "  no tests found"; exit 1; }; \
	exit $$fail

# `test` is a prerequisite, not a `$(MAKE) test` recipe line: GNU make runs any recipe
# line naming $(MAKE) even under -n, and the loop probes this target with `make -n check`.
.PHONY: compile
compile:
	@[ -x "$(PY)" ] || { echo "warden: no venv — run 'make venv'"; exit 1; }
	@"$(PY)" -m compileall -q "$(WARDEN_REPO)/scripts" "$(WARDEN_REPO)/tests" >/dev/null
	@echo "  ✓ compileall"

.PHONY: check
check: compile test

# `check` runs its prerequisites in order even under `make -j`: `compile` is the gate that
# says the scripts import before the suites start. (GNU make 4.4+ scopes this to `check`'s
# prerequisites; make 3.81, the macOS default, applies it to the whole run.)
.NOTPARALLEL: check

# ---------------------------------------------------------------------------
# Deploy / verify / logs — the repo contract
# ---------------------------------------------------------------------------

.PHONY: deploy
deploy:
	@"$(WARDEN_REPO)/scripts/deploy.sh" deploy

.PHONY: verify
verify:
	@"$(WARDEN_REPO)/scripts/deploy.sh" verify

.PHONY: logs
logs:
	@for f in $(HOME)/Library/Logs/warden-*.log $(HOME)/Library/Logs/warden-*.err; do \
		[ -s "$$f" ] || continue; echo "== $$f"; tail -n 30 "$$f"; \
	done

# ---------------------------------------------------------------------------
# LaunchAgents
# ---------------------------------------------------------------------------

LA := $(HOME)/Library/LaunchAgents

.PHONY: render-plists
render-plists: assert-main-checkout
	@mkdir -p "$(LA)"
	@for name in $(WARDEN_PLISTS); do \
		src="$(WARDEN_REPO)/launchd/$$name.plist.template"; \
		dst="$(LA)/$$name.plist"; \
		[ -f "$$src" ] || { echo "  ✗ $$name [no template]"; continue; }; \
		tmp="$$(mktemp)"; \
		sed "s|__HOME__|$(HOME)|g" "$$src" > "$$tmp"; \
		if [ -f "$$dst" ] && cmp -s "$$tmp" "$$dst"; then \
			rm -f "$$tmp"; echo "  · $$name (ok)"; \
		else \
			mv "$$tmp" "$$dst"; echo "  ✓ $$name (rendered)"; \
		fi; \
	done

# Unchanged content is a no-op above, so re-running never rewrites a plist — but a
# rewritten one still has to be re-bootstrapped for launchd to see it.
.PHONY: agents
agents: render-plists assert-main-checkout
	@mkdir -p "$(WARDEN_HOME)"
	@chmod +x "$(WARDEN_REPO)"/scripts/*.sh "$(WARDEN_REPO)"/scripts/warden 2>/dev/null || true
	@for name in $(WARDEN_PLISTS); do \
		dst="$(LA)/$$name.plist"; \
		[ -f "$$dst" ] || continue; \
		launchctl bootout gui/$$(id -u)/$$name 2>/dev/null || true; \
		launchctl bootstrap gui/$$(id -u) "$$dst" 2>/dev/null \
			&& echo "  ✓ $$name loaded" \
			|| echo "  ✗ $$name [bootstrap failed]"; \
	done

.PHONY: unload
unload:
	@for name in $(WARDEN_PLISTS); do \
		launchctl bootout gui/$$(id -u)/$$name 2>/dev/null \
			&& echo "  ✓ $$name unloaded" || echo "  · $$name (not loaded)"; \
	done

# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

.PHONY: status
status:
	@echo "warden"
	@printf '  %-24s ' "venv"; \
	[ -x "$(PY)" ] && "$(PY)" -V || echo "MISSING — run 'make venv'"
	@echo "  agents:"
	@# launchctl list prints PID, LAST EXIT STATUS, LABEL. Being loaded is not the
	@# same fact as having worked: on 2026-09-09 the loop died on `database is
	@# locked` and this target printed ✓ over an exit status of 1 for ten minutes.
	@# A control plane whose job is noticing that something stopped must not have a
	@# status surface that hides its own stopping — see DESIGN.md's "deferral must
	@# be visible". So: ✓ only when loaded AND the last exit was 0 (or it has not
	@# run yet, which launchctl prints as `-`); otherwise ✗, the code, and the
	@# exact log to read next.
	@for name in $(WARDEN_PLISTS); do \
		line=$$(launchctl list 2>/dev/null | grep -E "[[:space:]]$$name$$" || true); \
		if [ -z "$$line" ]; then \
			echo "    ✗ $$name  [not loaded]"; \
			continue; \
		fi; \
		pid=$$(echo "$$line" | awk '{print $$1}'); \
		rc=$$(echo "$$line" | awk '{print $$2}'); \
		short=$$(echo "$$name" | sed 's/^com\.jkrumm\.warden-//'); \
		if [ "$$rc" = "0" ] || [ "$$rc" = "-" ]; then \
			echo "    ✓ $$name  [pid $$pid, last exit $$rc]"; \
		else \
			echo "    ✗ $$name  [pid $$pid, LAST EXIT $$rc] — read $(HOME)/Library/Logs/warden-$$short.err"; \
		fi; \
	done
	@# com.jkrumm.warden-api is a KeepAlive server, not a periodic job — the loop
	@# above already reads it correctly (a live PID with last exit 0 is ✓, a
	@# crash-loop shows no PID with a non-zero exit and is ✗, exactly like every
	@# other agent here), but "loaded" is not "answering." This is the one check
	@# that actually asks the process something, over the loopback bind itself.
	@printf '  %-24s ' "api (/health)"; \
	if out=$$(curl -fsS --max-time 2 http://127.0.0.1:7735/health 2>/dev/null); then \
		if echo "$$out" | grep -q '"ok": true'; then \
			echo "✓ reachable, ok"; \
		else \
			echo "✗ reachable but degraded — $$out"; \
		fi; \
	else \
		echo "✗ unreachable (is com.jkrumm.warden-api loaded?)"; \
	fi
	@printf '  %-24s ' "ledger"; \
	if [ -f "$(WARDEN_HOME)/warden.db" ]; then \
		ls -lh "$(WARDEN_HOME)/warden.db" | awk '{print $$5, $$6, $$7, $$8}'; \
	else \
		echo "absent ($(WARDEN_HOME)/warden.db)"; \
	fi

# ---------------------------------------------------------------------------
# Slack app identity
# ---------------------------------------------------------------------------

# See slack/README.md for the full flow (owner steps, token seeding). The app
# configuration token is human-only and 12-hour-lived — it lives only in this
# shell's argv for the one curl call below and is never written to disk.

.PHONY: slack-app-create
slack-app-create:
	@[ -n "$(SLACK_CONFIG_TOKEN)" ] || { echo "warden: SLACK_CONFIG_TOKEN not set — see slack/README.md"; exit 1; }
	@curl -s -X POST https://slack.com/api/apps.manifest.create \
		-H "Authorization: Bearer $(SLACK_CONFIG_TOKEN)" -H 'content-type: application/json' \
		-d "$$(jq -n --slurpfile m "$(WARDEN_REPO)/slack/app-manifest.json" '{manifest: $$m[0]}')" \
		| jq '{ok, app_id, error}'

.PHONY: slack-app-update
slack-app-update:
	@[ -n "$(SLACK_CONFIG_TOKEN)" ] || { echo "warden: SLACK_CONFIG_TOKEN not set — see slack/README.md"; exit 1; }
	@[ -n "$(APP_ID)" ] || { echo "warden: APP_ID not set — e.g. make slack-app-update APP_ID=A0123456789"; exit 1; }
	@curl -s -X POST https://slack.com/api/apps.manifest.update \
		-H "Authorization: Bearer $(SLACK_CONFIG_TOKEN)" -H 'content-type: application/json' \
		-d "$$(jq -n --arg id "$(APP_ID)" --slurpfile m "$(WARDEN_REPO)/slack/app-manifest.json" '{app_id: $$id, manifest: $$m[0]}')" \
		| jq '{ok, error}'
