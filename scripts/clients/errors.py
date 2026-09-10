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
    """sideclaw/GitHub unreachable or non-2xx. MAY have happened remotely —
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


class PolicyError(WardenError):
    """Refused by policy/budget/gate — the remote call either never happened
    or happened and was correctly rejected by the far side; either way
    nothing was mutated that shouldn't have been."""

    exit_code = 4
