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

import json
import os
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from .errors import CheckRunsUnreadable, HeadMoved, PolicyError, PreconditionError, RemoteError

GH_OWNER = "jkrumm"
_GH_TOKEN_REF = "op://mini/github/token"
_TIMEOUT_S = 30
_TOKEN_TIMEOUT_S = 15

_PR_URL_RE = re.compile(r"^https://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/pull/([0-9]+)$")

# A commit sha reaches check_runs() off a pull request GitHub
# itself returned, but per this repo's own threat model that PR is
# attacker-influenceable — a malformed value ('../', '?', '#') must never
# reach a URL path segment. Validated, never quoted: a sha that isn't
# 40-hex is a defect worth a loud refusal, not a value worth escaping.
_SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")

# GitHub's own text on `GET /repos/{o}/{r}/rules/branches/{b}` when the
# repository is private and the plan carries no rulesets (a paid feature
# there). Matched literally, and only on a 403: this is "the feature does not
# exist for this repo", never "you may not look" (§108).
_RULESETS_UNAVAILABLE_RE = re.compile(r"Upgrade to GitHub Pro or make this repository public")

# GitHub's own text for "this credential may not make that request" — the
# fine-grained-PAT permission gap (§110), as opposed to a resource that does
# not exist (404) or a repository-level refusal.
_TOKEN_CANNOT_READ_RE = re.compile(r"Resource not accessible by personal access token")

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
    """Every check-run GitHub recorded for `sha`.

    A 403 carrying GitHub's own "Resource not accessible by personal access
    token" is raised as `CheckRunsUnreadable`, not a plain RemoteError: it means
    the credential lacks `Checks: read` (only reachable on a private repo), so
    the caller (`lifecycle/merge.py`) decides (§110). Every other non-200 stays a
    loud refusal."""
    if not _SHA_RE.match(sha):
        raise PreconditionError(f"{sha!r} is not a 40-hex commit sha — refusing before the request")
    status, body = api("GET", f"/repos/{owner}/{repo}/commits/{sha}/check-runs")
    if status == 403 and isinstance(body, dict) and _TOKEN_CANNOT_READ_RE.search(body.get("message") or ""):
        raise CheckRunsUnreadable(
            f"GitHub returned HTTP 403 reading check-runs for {owner}/{repo}@{sha} — the token may "
            f"not read checks (Checks: read): {body.get('message')}"
        )
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
        # GitHub's 409 on this endpoint is "Head branch was modified": `sha` is no longer the
        # PR's head. (An unmergeable PR is a 405, below.)
        raise HeadMoved(
            f"GitHub refused the merge (409): the head moved from {sha[:12]} since it was "
            f"inspected. Nothing was merged."
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


def close_pr(owner: str, repo: str, number: int, *, comment: str | None = None) -> None:
    """Close a pull request this bridge opened, optionally saying why first.
    The comment is best-effort (a 403 on Issues: Write must not keep a
    superseded PR open); the close itself raises on failure."""
    if comment:
        try:
            api("POST", f"/repos/{owner}/{repo}/issues/{number}/comments", {"body": comment})
        except RemoteError:
            pass
    status, body = api("PATCH", f"/repos/{owner}/{repo}/pulls/{number}", {"state": "closed"})
    if status != 200:
        raise RemoteError(f"closing {owner}/{repo}#{number} returned HTTP {status}: {str(body)[:200]}")


def delete_branch(owner: str, repo: str, branch: str) -> bool:
    # `safe="/"`, not `""` — a dispatch branch is `dispatch/<repo>-<n>`, one
    # legitimate embedded slash, exactly like contents()'s path below.
    quoted_branch = urllib.parse.quote(branch, safe="/")
    status, _ = api("DELETE", f"/repos/{owner}/{repo}/git/refs/heads/{quoted_branch}")
    return status in (204, 422)


def read_issue(owner: str, repo: str, number: int) -> dict[str, Any]:
    """GET /repos/{owner}/{repo}/issues/{number} — the single-issue fallback
    `ingest_github_issues()`'s disappearance-resolve calls before treating a
    `github_go` event as closed. `search_issues()` silently omits a repo the
    token cannot search (e.g. a fine-grained PAT missing `Issues: read` on a
    private repo) exactly the same way it omits a genuinely closed issue —
    this is the only way to tell the two apart. Raises on anything but 200,
    same fail-closed contract as `read_pr()`/`read_repo()`: the caller must
    never resolve an event it could not actually confirm is closed."""
    status, body = api("GET", f"/repos/{owner}/{repo}/issues/{number}")
    if status != 200 or not isinstance(body, dict):
        raise RemoteError(f"GitHub returned HTTP {status} reading {owner}/{repo}#{number}")
    return body


_SEARCH_PER_PAGE = 50
_SEARCH_MAX_PAGES = 10


def search_issues(*, owner: str, skip_label: str) -> list[dict[str, Any]]:
    """`GET /search/issues` for every OPEN issue in an `owner` repo, minus
    anyone carrying `skip_label` — the poll behind no-label issue intake
    (loop/intake.py's `ingest_github_issues()`). Every open issue is a warden item
    by default; `skip_label` is the one opt-out a human can apply to keep a
    specific issue out, not a gate an issue has to earn its way through.
    `repo` in each returned dict is the SHORT name (`nameWithOwner`'s tail,
    read off `repository_url`, never the search hit's own `html_url`, which
    is not guaranteed to be parseable the same way) — the value
    `open_origin_item()` and `policy.repo_cwd()` expect, not `owner/repo`.

    `query` is percent-encoded before it ever reaches `api()` (`safe="+:"` so
    the `+` word-separators and `:` qualifier colons GitHub's search syntax
    needs stay literal) — `api()` sends whatever path it is given verbatim,
    with no encoding of its own, so an unescaped `"` or `&` inside `skip_label`
    would otherwise land in the URL unescaped and could smuggle extra query
    parameters into the request.

    Paged through `total_count`, up to `_SEARCH_MAX_PAGES` pages of
    `_SEARCH_PER_PAGE` — `ingest_github_issues()` resolves any still-open
    `github_go` event NOT in this result set, so a silently-truncated first
    page (the old shape: one `per_page=50` request, no paging) would read a
    live item past #50 as gone and resolve it out from under a human still
    waiting on it (DESIGN.md § What must not be lost: "overflow waits, never
    drops"). If GitHub's own `total_count` still exceeds what
    `_SEARCH_MAX_PAGES` pages fetched, this raises rather than returning a
    partial set — `ingest_github_issues()`'s own `except RemoteError` then
    skips resolution for this tick entirely, the fail-closed side of that
    same rule."""
    # Unquoted on purpose: GitHub's search index returns NOTHING for
    # `-label:"warden:skip"` (quoted) and the matching behaviour for
    # `-label:warden:skip` (observed live 2026-09-11 against
    # jkrumm/dispatch-scratch#9, the same finding as the old `warden:go`
    # query). A label with a space would need quotes; ours never carry one.
    query = f"owner:{owner}+is:issue+is:open+-label:{skip_label}"
    encoded_query = urllib.parse.quote(query, safe="+:")

    items: list[dict[str, Any]] = []
    total_count: int | None = None
    for page in range(1, _SEARCH_MAX_PAGES + 1):
        path = f"/search/issues?q={encoded_query}&per_page={_SEARCH_PER_PAGE}"
        if page > 1:
            path += f"&page={page}"
        status, body = api("GET", path)
        if status != 200:
            raise RemoteError(f"GitHub returned HTTP {status} searching issues for owner {owner!r}")
        if not isinstance(body, dict) or not isinstance(body.get("items"), list):
            raise RemoteError(f"GitHub returned an unusable body searching issues for owner {owner!r}")
        if body.get("incomplete_results"):
            # GitHub's search index can time out and still return HTTP 200 —
            # `incomplete_results: true` is the only signal that this page is
            # not authoritative. Trusting it here would let `ingest_github_issues()`
            # read a genuinely-still-open issue as "not in this result set" and
            # resolve its event out from under a human still waiting on it,
            # exactly the failure the total_count check below already guards
            # against for a truncated page count — this is the same guard for
            # a page GitHub itself flags as unreliable.
            raise RemoteError(
                f"GitHub flagged this search as incomplete_results for owner {owner!r} — refusing to "
                f"resolve missing github_go events against an unreliable result set"
            )
        total_count = body.get("total_count")
        page_items = body["items"]
        items.extend(page_items)
        if len(page_items) < _SEARCH_PER_PAGE:
            break
        if isinstance(total_count, int) and len(items) >= total_count:
            break

    if isinstance(total_count, int) and len(items) < total_count:
        raise RemoteError(
            f"GitHub reports {total_count} open issues for owner {owner!r} but only {len(items)} "
            f"were fetched across up to {_SEARCH_MAX_PAGES} pages of {_SEARCH_PER_PAGE} — refusing "
            f"to resolve missing github_go events against a partial result set"
        )

    out: list[dict[str, Any]] = []
    for it in items:
        repo_url = it.get("repository_url") or ""
        repo = repo_url.rsplit("/", 1)[-1] if repo_url else "?"
        author_obj = it.get("user") or {}
        author = author_obj.get("login") if isinstance(author_obj, dict) else None
        raw_labels = it.get("labels")
        labels = (
            [l.get("name") for l in raw_labels if isinstance(l, dict) and l.get("name")]
            if isinstance(raw_labels, list)
            else []
        )
        out.append({
            "repo": repo,
            "number": it.get("number"),
            "title": it.get("title", "?"),
            "body": it.get("body") or "",
            "url": it.get("html_url", ""),
            "author": author,
            "updated_at": it.get("updated_at"),
            "labels": labels,
        })
    return out


def create_issue_comment(repo_full: str, number: int, body: str) -> dict[str, Any]:
    """`repo_full` is `owner/repo` — the comment-back door for a `github_issue`
    item whose verdict just landed (triage.py's own trust check happens
    before this is ever called; this function posts unconditionally)."""
    if "/" not in repo_full:
        raise PreconditionError(f"{repo_full!r} is not an owner/repo full name")
    owner, repo = repo_full.split("/", 1)
    status, resp = api("POST", f"/repos/{owner}/{repo}/issues/{number}/comments", {"body": body})
    if status not in (200, 201):
        raise RemoteError(
            f"GitHub returned HTTP {status} commenting on {repo_full}#{number}", maybe_mutated=True,
        )
    return resp


def branch_rules(owner: str, repo: str, branch: str) -> list[dict[str, Any]]:
    """Every ruleset rule GitHub itself enforces on `branch` — read to pick a
    merge method GitHub will accept (`pick_merge_method`), never to pre-empt
    GitHub's own answer to the merge call.

    A `403` whose own text says the feature needs GitHub Pro on a private
    repository is not a refusal to read: rulesets are a paid feature there, so
    such a repo cannot have one and `[]` is the true answer, not a guess
    (§108). Raising instead made `weatherorb` — private, `autoDeploy` —
    permanently unmergeable: items 1276 and 1277
    parked on this 403 while their step-7 reviews had already confirmed.
    Every other non-200 stays a loud refusal (a token that cannot read a
    repository's rules must never read as "no rules"); an unreadable
    *classic* protection is enforced by the merge call itself, which also
    pins the head SHA."""
    status, body = api("GET", f"/repos/{owner}/{repo}/rules/branches/{branch}")
    if status == 403 and isinstance(body, dict) and _RULESETS_UNAVAILABLE_RE.search(body.get("message") or ""):
        return []
    if status != 200 or not isinstance(body, list):
        raise RemoteError(f"GitHub returned HTTP {status} reading the rules on {owner}/{repo}:{branch}")
    return body


def pick_merge_method(repo_json: dict[str, Any], rules: list[dict[str, Any]] | None = None) -> str | None:
    """Squash first: a dispatch branch is one unit of work by construction.
    Rebase next, merge commit last. The branch's rules narrow the choice:
    `required_linear_history` rules out a merge commit, and a pull_request
    rule's `allowed_merge_methods` is the list GitHub will actually accept."""
    rules = rules or []
    allowed = {"squash", "rebase", "merge"}
    for r in rules:
        if r.get("type") == "required_linear_history":
            allowed.discard("merge")
        methods = (r.get("parameters") or {}).get("allowed_merge_methods") if r.get("type") == "pull_request" else None
        if isinstance(methods, list) and methods:
            allowed &= set(methods)
    for method, flag in (("squash", "allow_squash_merge"), ("rebase", "allow_rebase_merge"),
                         ("merge", "allow_merge_commit")):
        if method in allowed and repo_json.get(flag):
            return method
    return None


def parse_pr_url(url: str) -> tuple[str, str, int] | None:
    m = _PR_URL_RE.match(url)
    if not m:
        return None
    return m.group(1), m.group(2), int(m.group(3))
