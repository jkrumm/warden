"""The GitHub REST/GraphQL transport — the Python port of the retired bash CLI's
`gh_token` (1278-1283), `github_api` (1295-1309), `pick_merge_method`
(2136-2146), `mark_ready_for_review` (2149-2172) and the calls inside
`cmd_merge` (1896-2131).

The token is read once per process and never reaches argv or a log line —
`api()` puts it in an `Authorization: Bearer` header only, exactly as the
bash version kept it out of `ps` by passing it to curl over a config file on
stdin.
"""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from .errors import PolicyError, PreconditionError, RemoteError

GH_OWNER = "jkrumm"
_GH_TOKEN_REF = "op://mini/github/token"
_TIMEOUT_S = 30
_TOKEN_TIMEOUT_S = 15

_PR_URL_RE = re.compile(r"^https://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/pull/([0-9]+)$")

# A commit sha reaches check_runs()/actions_runs() off a pull request GitHub
# itself returned, but per this repo's own threat model that PR is
# attacker-influenceable — a malformed value ('../', '?', '#') must never
# reach a URL path segment. Validated, never quoted: a sha that isn't
# 40-hex is a defect worth a loud refusal, not a value worth escaping.
_SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")

_token_cache: str | None = None


def _base() -> str:
    return os.environ.get("WARDEN_GH_API", "https://api.github.com")


def _secrets_run() -> str:
    return os.environ.get("WARDEN_SECRETS_RUN") or str(Path.home() / ".local" / "bin" / "secrets-run")


def token() -> str:
    """Memoized in a module global — re-read only by restarting the process,
    same as the bash script's `GH_TOKEN` variable living for one
    invocation."""
    global _token_cache
    if _token_cache:
        return _token_cache
    try:
        r = subprocess.run(
            [_secrets_run(), "read", _GH_TOKEN_REF],
            capture_output=True, text=True, timeout=_TOKEN_TIMEOUT_S,
        )
    except (OSError, subprocess.SubprocessError):
        raise PreconditionError(
            f"could not resolve the GitHub token via secrets-run — the merge needs a "
            f"GitHub credential and this machine resolves it from the sealed cache"
        )
    if r.returncode != 0 or not r.stdout.strip():
        raise PreconditionError(
            f"{_GH_TOKEN_REF} resolved empty. Re-seed the cache (make secrets-seed in "
            f"dotfiles, biometric, MacBook-only)"
        )
    _token_cache = r.stdout.strip()
    return _token_cache


def api(method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
    """One bounded REST/GraphQL call. Never raises on HTTP status — a 404 on
    a pull request and a 404 on a branch delete are not the same severity,
    and the caller is the one who knows which."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Authorization": f"Bearer {token()}",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(f"{_base()}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:
            status, text = resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        status, text = e.code, e.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, TimeoutError, OSError):
        # A transport-level failure on a mutating verb is ambiguous — the
        # request may have reached GitHub and mutated something before the
        # response was lost. A GET never mutates, so it is always a clean
        # miss.
        raise RemoteError(
            f"GitHub request failed: {method} {path}",
            maybe_mutated=method in ("PUT", "POST", "DELETE"),
        )

    try:
        parsed = json.loads(text) if text else None
    except json.JSONDecodeError:
        parsed = None
    return status, parsed


def read_pr(owner: str, repo: str, number: int) -> dict[str, Any]:
    status, body = api("GET", f"/repos/{owner}/{repo}/pulls/{number}")
    if status != 200:
        raise RemoteError(f"GitHub returned HTTP {status} for {owner}/{repo}#{number}")
    return body


def read_repo(owner: str, repo: str) -> dict[str, Any]:
    status, body = api("GET", f"/repos/{owner}/{repo}")
    if status != 200:
        raise RemoteError(f"GitHub returned HTTP {status} reading {owner}/{repo}")
    return body


def pr_files(owner: str, repo: str, number: int) -> list[dict[str, Any]]:
    status, body = api("GET", f"/repos/{owner}/{repo}/pulls/{number}/files?per_page=100")
    if status != 200:
        raise RemoteError(f"GitHub returned HTTP {status} listing files on {owner}/{repo}#{number}")
    return body


def check_runs(owner: str, repo: str, sha: str) -> list[dict[str, Any]]:
    if not _SHA_RE.match(sha):
        raise PreconditionError(f"{sha!r} is not a 40-hex commit sha — refusing before the request")
    status, body = api("GET", f"/repos/{owner}/{repo}/commits/{sha}/check-runs")
    if status != 200:
        raise RemoteError(f"GitHub returned HTTP {status} reading check-runs for {owner}/{repo}@{sha}")
    if not isinstance(body, dict):
        raise RemoteError(f"GitHub returned a non-object body reading check-runs for {owner}/{repo}@{sha}")
    return body["check_runs"]


def mark_ready_for_review(node_id: str) -> None:
    """Un-drafting is GraphQL-only — REST has no ready-for-review
    transition."""
    query = (
        "mutation($id:ID!){markPullRequestReadyForReview(input:{pullRequestId:$id})"
        "{pullRequest{isDraft}}}"
    )
    status, body = api("POST", "/graphql", {"query": query, "variables": {"id": node_id}})
    if status != 200:
        raise RemoteError(
            f"GitHub returned HTTP {status} marking the pull request ready for review. "
            f"Nothing was merged."
        )
    if isinstance(body, dict) and body.get("errors"):
        raise RemoteError(
            f"GitHub rejected the ready-for-review mutation: {json.dumps(body)[:300]}. "
            f"Nothing was merged."
        )


def merge_pr(owner: str, repo: str, number: int, *, sha: str, method: str) -> dict[str, Any]:
    status, body = api(
        "PUT", f"/repos/{owner}/{repo}/pulls/{number}/merge",
        {"sha": sha, "merge_method": method},
    )
    if status == 200:
        return body
    if status == 409:
        raise PolicyError(
            f"GitHub refused the merge (409): the head moved since it was inspected, or "
            f"the branch is not in a mergeable state. Nothing was merged."
        )
    if status == 405:
        raise PolicyError(
            f"GitHub refused the merge (405): the pull request is not mergeable under "
            f"this repo's rules. Nothing was merged."
        )
    raise RemoteError(
        f"GitHub returned HTTP {status} merging {owner}/{repo}#{number}: {json.dumps(body)[:300]}",
        maybe_mutated=True,
    )


def delete_branch(owner: str, repo: str, branch: str) -> bool:
    # `safe="/"`, not `""` — a dispatch branch is `dispatch/<repo>-<n>`, one
    # legitimate embedded slash, exactly like contents()'s path below.
    quoted_branch = urllib.parse.quote(branch, safe="/")
    status, _ = api("DELETE", f"/repos/{owner}/{repo}/git/refs/heads/{quoted_branch}")
    return status in (204, 422)


def contents(owner: str, repo: str, path: str, *, ref: str) -> bytes | None:
    quoted_path = urllib.parse.quote(path, safe="/")
    quoted_ref = urllib.parse.quote(ref, safe="")
    status, body = api("GET", f"/repos/{owner}/{repo}/contents/{quoted_path}?ref={quoted_ref}")
    if status != 200 or not isinstance(body, dict):
        return None
    try:
        return base64.b64decode(body.get("content", ""))
    except (ValueError, TypeError):
        return None


def actions_runs(owner: str, repo: str, *, head_sha: str) -> list[dict[str, Any]]:
    if not _SHA_RE.match(head_sha):
        raise PreconditionError(f"{head_sha!r} is not a 40-hex commit sha — refusing before the request")
    status, body = api("GET", f"/repos/{owner}/{repo}/actions/runs?head_sha={head_sha}&per_page=20")
    if status != 200:
        raise RemoteError(f"GitHub returned HTTP {status} reading Actions runs for {owner}/{repo}@{head_sha}")
    if not isinstance(body, dict):
        raise RemoteError(f"GitHub returned a non-object body reading Actions runs for {owner}/{repo}@{head_sha}")
    return body["workflow_runs"]


def pick_merge_method(repo_json: dict[str, Any]) -> str | None:
    """Squash first: a dispatch branch is one unit of work by construction.
    Rebase next, merge commit last — a merge commit on a linear-history repo
    is refused by GitHub anyway."""
    if repo_json.get("allow_squash_merge"):
        return "squash"
    if repo_json.get("allow_rebase_merge"):
        return "rebase"
    if repo_json.get("allow_merge_commit"):
        return "merge"
    return None


def parse_pr_url(url: str) -> tuple[str, str, int] | None:
    m = _PR_URL_RE.match(url)
    if not m:
        return None
    return m.group(1), m.group(2), int(m.group(3))
