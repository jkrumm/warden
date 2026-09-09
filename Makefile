# warden — the control plane. See DESIGN.md for what it is, STATE.md for where it is.
#
# Everything here assumes the mini. warden's loop, its pollers and its backup are
# LaunchAgents, never `hermes cron` jobs: the loop that notices Hermes is broken
# cannot depend on Hermes being up to run it. That argument is DESIGN.md's reason
# for the whole repo, and it is why these four plists exist.

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
                 com.jkrumm.warden-sweep com.jkrumm.warden-backup

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "warden"
	@echo "  make setup     venv + plists + load the agents"
	@echo "  make venv      create .venv from $(BASE_PY) and install requirements"
	@echo "  make test      run every tests/*.py (hand-rolled runners, not pytest)"
	@echo "  make status    what is loaded, what ran last, is the ledger reachable"
	@echo "  make agents    (re)load the LaunchAgents from launchd/"
	@echo "  make unload    unload the LaunchAgents"

.PHONY: setup
setup: venv render-plists agents
	@echo "warden: setup complete — run 'make status'"

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

# ---------------------------------------------------------------------------
# LaunchAgents
# ---------------------------------------------------------------------------

LA := $(HOME)/Library/LaunchAgents

.PHONY: render-plists
render-plists:
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
agents: render-plists
	@mkdir -p "$(WARDEN_HOME)"
	@chmod +x "$(WARDEN_REPO)"/scripts/*.sh 2>/dev/null || true
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

# The two copies of the dispatch policy — sideclaw's, which is the boundary, and
# hermes-agent's, which is defence in depth — are meant to agree, and nothing
# else notices when they stop. Drift here does not read as an error; it reads as
# working, right up until the boundary admits something the control plane
# believes it forbids.
.PHONY: check-policy
check-policy:
	@[ -x "$(PY)" ] || { echo "warden: no venv — run 'make venv'"; exit 1; }
	@"$(PY)" "$(WARDEN_REPO)/scripts/check-dispatch-policy.py"

.PHONY: status
status:
	@echo "warden"
	@printf '  %-24s ' "venv"; \
	[ -x "$(PY)" ] && "$(PY)" -V || echo "MISSING — run 'make venv'"
	@echo "  agents:"
	@for name in $(WARDEN_PLISTS); do \
		line=$$(launchctl list 2>/dev/null | grep -E "[[:space:]]$$name$$" || true); \
		if [ -n "$$line" ]; then \
			echo "    ✓ $$name  [$$line]"; \
		else \
			echo "    ✗ $$name  [not loaded]"; \
		fi; \
	done
	@printf '  %-24s ' "policy"; \
	if out=$$("$(PY)" "$(WARDEN_REPO)/scripts/check-dispatch-policy.py" 2>&1); then \
		echo "$$out" | grep -E '^(✓|✗)' | head -1 | sed 's/^ *//'; \
	else \
		echo "DISAGREES with sideclaw — run 'make check-policy'"; \
	fi
	@printf '  %-24s ' "ledger"; \
	if [ -f "$(WARDEN_HOME)/warden.db" ]; then \
		ls -lh "$(WARDEN_HOME)/warden.db" | awk '{print $$5, $$6, $$7, $$8}'; \
	else \
		echo "absent ($(WARDEN_HOME)/warden.db)"; \
	fi
