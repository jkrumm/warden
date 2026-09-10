"""The approval canonical-string spec and its Ed25519 verifier — the Python
port of the retired bash CLI's `approval_hash` (963-972) and
`require_signed_approval`'s verification step, and of
`hermes-agent/plugins/dispatch-approval/__init__.py`'s `payload_hash` /
`canonical_message`, which this module must stay byte-identical to. The
cross-repo contract itself is `config/approval-spec.json`; this module is
one of its two implementations (the plugin, in the other repo, is the
other) — see that file's `vectors` for the fixtures both sides are checked
against.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from .errors import PolicyError, PreconditionError

SPEC_VERSION = "v1"


def payload_hash(verb: str, repo: str, tier: str, body: str, why: str = "", context: str = "") -> str:
    """sha256 over each of the six fields, UTF-8, each followed by one NUL
    byte including the last. Lowercase hex."""
    h = hashlib.sha256()
    for part in (verb, repo, tier, body, why, context):
        h.update(part.encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()


def canonical_message(nonce: str, payload_hash: str, decision: str, decided_by: str, expires_at: str) -> bytes:
    return "|".join([SPEC_VERSION, nonce, payload_hash, decision, decided_by or "", expires_at]).encode("utf-8")


def key_id(pub_hex: str) -> str:
    """A stable public identifier of a key, recorded on approval rows at
    mint so a rotated gateway key reads "superseded" instead of a silent
    verification failure."""
    return hashlib.sha256(bytes.fromhex(pub_hex)).hexdigest()[:16]


def load_pubkey(path: Path) -> str:
    if not path.is_file():
        raise PolicyError(
            f"no approval public key at {path} — the dispatch-approval plugin is not "
            f"loaded or the gateway has not started"
        )
    text = path.read_text(encoding="utf-8").strip()
    try:
        raw = bytes.fromhex(text)
    except ValueError:
        raise PreconditionError(f"approval public key at {path} is not valid hex")
    if len(raw) != 32:
        raise PreconditionError(f"approval public key at {path} is not 32 bytes (got {len(raw)})")
    return text


def verify(pub_hex: str, signature_hex: str, message: bytes) -> bool:
    """False on InvalidSignature or ValueError — never raises for bad
    input, so a caller can treat "does not verify" and "is not even
    shaped like a signature" identically."""
    try:
        pubkey = Ed25519PublicKey.from_public_bytes(bytes.fromhex(pub_hex))
        signature = bytes.fromhex(signature_hex)
        pubkey.verify(signature, message)
        return True
    except (InvalidSignature, ValueError):
        return False
