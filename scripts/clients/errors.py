"""The exit taxonomy from the retired bash CLI (lines 228-234), as exceptions.

0 ok · 2 precondition failed · 3 remote failed · 4 budget/policy refusal ·
64 usage error — unchanged from the bash script, because the lifecycle half
that comes in a later wave maps these straight back onto the same process
exit codes the shell version used.
"""

from __future__ import annotations


class WardenError(Exception):
    """Base of the taxonomy. Never raised directly — always one of the four
    below, so a caller can `except WardenError` for the message and still
    branch on `.exit_code` or the concrete subclass."""

    exit_code: int = 1


class UsageError(WardenError):
    """Bad argument shape, unknown verb/flag, empty/oversize brief — a caller
    mistake, never a remote effect."""

    exit_code = 64


class PreconditionError(WardenError):
    """Missing tool/file/policy, DB unreadable, token unresolvable — nothing
    was attempted over the network because a local precondition failed
    first."""

    exit_code = 2


class RemoteError(WardenError):
    """agent-gateway/GitHub unreachable or non-2xx. MAY have happened remotely —
    a request can fail after it was already sent, which is what
    `maybe_mutated` is for."""

    exit_code = 3

    def __init__(self, *args: object, maybe_mutated: bool = False) -> None:
        super().__init__(*args)
        # Set by callers that raise after a mutating request was SENT but
        # whose outcome could not be confirmed — the in-process replacement
        # for the old "exit 3 after a mutation" shell heuristic, and the
        # future `operations.outcome = "unknown"` path.
        self.maybe_mutated = maybe_mutated


class CheckRunsUnreadable(RemoteError):
    """The credential may not read a commit's check-runs — a fine-grained PAT
    without `Checks: read`, which only bites on a private repository (§110:
    `GET …/commits/<sha>/check-runs` → 403 "Resource not accessible by personal
    access token", `x-accepted-github-permissions: checks=read`, on
    `weatherorb`).

    Its own class because this is a fact about the token, not about the commit,
    and the *caller* owns the answer: `lifecycle/merge.py` falls back to the
    commit's GitHub Actions workflow runs, and an empty fallback is pending,
    never "no checks to gate on"."""


class SubmitRefused(RemoteError):
    """agent-gateway answered a job submit with a 4xx: it REFUSED (repo outside its
    allowlist, tier above the repo's ceiling, unknown model, bad params). The
    same submit will be refused again, so a caller ends the item with
    `str(exc)` and never retries; a 5xx or a connection failure stays a plain
    `RemoteError` and keeps its retry behaviour. Exits 4 like any other
    refusal, so the CLI contract for "refused" does not move."""

    exit_code = 4

    def __init__(self, *args: object, status: int) -> None:
        super().__init__(*args)
        self.status = status


class PolicyError(WardenError):
    """Refused by policy/budget/gate — the remote call either never happened
    or happened and was correctly rejected by the far side; either way
    nothing was mutated that shouldn't have been."""

    exit_code = 4


class HeadMoved(PolicyError):
    """The pull request's head is not the commit the caller pinned: GitHub's merge
    call answered 409 for the `sha` it was given, or the PR read before it showed a
    different head. Nothing was merged. The merge train answers by bringing the PR up
    to date again — it is not a refusal of the change."""
