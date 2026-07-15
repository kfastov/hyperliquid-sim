#!/usr/bin/env python3
"""Minimal GitHub Project Protocol v0.1 helper (stdlib + gh CLI)."""
from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

PROTOCOL = "gpp/0.1"
GPP_RE = re.compile(r"<!--\s*gpp\s*(\{.*?\})\s*-->", re.DOTALL)
HANDOFF_HEADINGS = ("## Completed", "## Remaining", "## Known failures", "## Decisions", "## Next action", "## Verification")
PROTECTED_PATTERNS = (".github/**", "protocol/v0.1/**", "tools/gpp.py", "AGENTS.md", "CODEOWNERS")


class ProtocolError(RuntimeError):
    pass


def run(*args: str, cwd: Path | None = None, check: bool = True) -> str:
    cp = subprocess.run(args, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if check and cp.returncode:
        raise ProtocolError(f"command failed ({cp.returncode}): {' '.join(args)}\n{cp.stderr.strip()}")
    return cp.stdout.strip()


def sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def canonical(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ProtocolError(message)


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"cannot read JSON {path}: {exc}") from exc
    require(isinstance(value, dict), f"{path}: root must be an object")
    return value


def nonempty_strings(value: Any, field: str, *, allow_empty: bool = True) -> list[str]:
    require(isinstance(value, list), f"{field} must be an array")
    if not allow_empty:
        require(bool(value), f"{field} must not be empty")
    require(all(isinstance(x, str) and x.strip() for x in value), f"{field} must contain non-empty strings")
    return value


def validate_contract_data(data: dict[str, Any], path: Path | None = None) -> None:
    prefix = f"{path}: " if path else ""
    required = {"protocol", "issue", "objective", "scope", "inputs", "dependencies", "acceptance", "deliverables", "risks"}
    missing = sorted(required - data.keys())
    require(not missing, prefix + f"missing fields: {', '.join(missing)}")
    allowed = required | {"notes"}
    extra = sorted(data.keys() - allowed)
    require(not extra, prefix + f"unknown fields: {', '.join(extra)}")
    require(data["protocol"] == PROTOCOL, prefix + f"protocol must be {PROTOCOL}")
    require(isinstance(data["issue"], int) and data["issue"] > 0, prefix + "issue must be a positive integer")
    require(isinstance(data["objective"], str) and data["objective"].strip(), prefix + "objective must be non-empty")
    scope = data["scope"]
    require(isinstance(scope, dict) and set(scope) == {"write", "forbidden"}, prefix + "scope must contain only write and forbidden")
    nonempty_strings(scope["write"], prefix + "scope.write", allow_empty=False)
    nonempty_strings(scope["forbidden"], prefix + "scope.forbidden")
    nonempty_strings(data["inputs"], prefix + "inputs")
    require(isinstance(data["dependencies"], list) and all(isinstance(x, int) and x > 0 for x in data["dependencies"]), prefix + "dependencies must be positive issue numbers")
    acceptance = data["acceptance"]
    require(isinstance(acceptance, dict) and set(acceptance) == {"commands", "assertions"}, prefix + "acceptance must contain only commands and assertions")
    nonempty_strings(acceptance["commands"], prefix + "acceptance.commands", allow_empty=False)
    nonempty_strings(acceptance["assertions"], prefix + "acceptance.assertions", allow_empty=False)
    nonempty_strings(data["deliverables"], prefix + "deliverables", allow_empty=False)
    nonempty_strings(data["risks"], prefix + "risks")
    if "notes" in data:
        require(isinstance(data["notes"], str), prefix + "notes must be a string")


def validate_contract(path: Path) -> dict[str, Any]:
    data = load_json(path)
    validate_contract_data(data, path)
    expected = f"{data['issue']}.json"
    require(path.name == expected, f"{path}: filename must be {expected}")
    return data


def git_root() -> Path:
    return Path(run("git", "rev-parse", "--show-toplevel"))


def git_blob(root: Path, revision: str, rel: str) -> str:
    return run("git", "rev-parse", f"{revision}:{rel}", cwd=root)


def gh_json(args: list[str], cwd: Path) -> Any:
    out = run("gh", *args, cwd=cwd)
    return json.loads(out)


def issue_data(root: Path, issue: int) -> dict[str, Any]:
    return gh_json(["issue", "view", str(issue), "--json", "number,title,body,state,updatedAt,url"], root)


def resolve_base_revision(root: Path) -> str:
    """Resolve the task input revision, never the mutable implementation HEAD."""
    try:
        default_branch = run("gh", "repo", "view", "--json", "defaultBranchRef", "--jq", ".defaultBranchRef.name", cwd=root)
        remote_ref = f"origin/{default_branch}"
        run("git", "rev-parse", "--verify", remote_ref, cwd=root)
        return run("git", "merge-base", "HEAD", remote_ref, cwd=root)
    except ProtocolError:
        return run("git", "rev-parse", "HEAD", cwd=root)


def read_git_file(root: Path, revision: str, rel: str) -> bytes:
    cp = subprocess.run(["git", "show", f"{revision}:{rel}"], cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if cp.returncode:
        raise ProtocolError(f"required input does not exist at {revision}: {rel}")
    return cp.stdout


def build_context(root: Path, issue: int, revision: str | None = None) -> tuple[bytes, dict[str, Any]]:
    issue_obj = issue_data(root, issue)
    base_sha = revision or resolve_base_revision(root)
    contract_rel = f"protocol/tasks/{issue}.json"
    try:
        contract_bytes = read_git_file(root, base_sha, contract_rel)
        contract = json.loads(contract_bytes)
        require(isinstance(contract, dict), f"{contract_rel}: root must be an object")
        validate_contract_data(contract, Path(contract_rel))
        require(contract["issue"] == issue, f"{contract_rel}: issue mismatch")
        strict = True
    except ProtocolError as exc:
        if "required input does not exist" not in str(exc):
            raise
        strict = False
        contract = None
        contract_bytes = b""
    entries: list[dict[str, str]] = []
    sections = [f"# Task #{issue}: {issue_obj['title']}", "", "## Issue", "", issue_obj.get("body") or ""]
    if contract is not None:
        sections += ["", "## Strict task contract", "", "```json", json.dumps(contract, indent=2, ensure_ascii=False, sort_keys=True), "```"]
        for rel in contract["inputs"]:
            content = read_git_file(root, base_sha, rel)
            entries.append({"path": rel, "blob": git_blob(root, base_sha, rel), "sha256": sha256(content)})
            sections += ["", f"## Input: `{rel}`", "", "```", content.decode(errors="replace"), "```"]
    context = ("\n".join(sections).rstrip() + "\n").encode()
    manifest: dict[str, Any] = {
        "protocol": PROTOCOL,
        "repository": run("gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner", cwd=root),
        "base_sha": base_sha,
        "issue": issue,
        "issue_updated_at": issue_obj["updatedAt"],
        "issue_body_sha256": sha256((issue_obj.get("body") or "").encode()),
        "mode": "strict" if strict else "light",
        "inputs": entries,
        "context_sha256": sha256(context),
    }
    if strict:
        manifest["contract"] = {"path": contract_rel, "blob": git_blob(root, base_sha, contract_rel), "sha256": sha256(contract_bytes)}
    manifest["digest"] = sha256(canonical(manifest))
    return context, manifest


def parse_metadata(body: str) -> dict[str, Any]:
    matches = GPP_RE.findall(body or "")
    require(len(matches) == 1, "PR body must contain exactly one <!-- gpp {...} --> block")
    try:
        data = json.loads(matches[0])
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"invalid GPP metadata JSON: {exc}") from exc
    require(isinstance(data, dict), "GPP metadata must be an object")
    require(isinstance(data.get("task"), int) and data["task"] > 0, "GPP task must be a positive issue number")
    require(data.get("mode") in {"light", "strict"}, "GPP mode must be light or strict")
    digest = data.get("context_digest")
    require(isinstance(digest, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", digest), "context_digest must be sha256:<64 hex>")
    if data["mode"] == "strict":
        require(data.get("contract") == f"protocol/tasks/{data['task']}.json", "strict task contract path must match task number")
    return data


def matches_any(path: str, patterns: list[str] | tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatchcase(path, p) or (p.endswith("/**") and (path == p[:-3] or path.startswith(p[:-2]))) for p in patterns)


def validate_paths(changed: list[str], contract: dict[str, Any]) -> None:
    allowed = contract["scope"]["write"]
    forbidden = contract["scope"]["forbidden"]
    bad_forbidden = [p for p in changed if matches_any(p, forbidden)]
    require(not bad_forbidden, "forbidden paths changed: " + ", ".join(bad_forbidden))
    out_of_scope = [p for p in changed if not matches_any(p, allowed)]
    require(not out_of_scope, "paths outside task write scope: " + ", ".join(out_of_scope))


def changed_files(root: Path, base_sha: str, head_sha: str) -> list[str]:
    out = run("git", "diff", "--name-only", f"{base_sha}...{head_sha}", cwd=root)
    return [x for x in out.splitlines() if x]


def validate_dependencies(root: Path, dependencies: list[int]) -> None:
    for dep in dependencies:
        data = gh_json(["issue", "view", str(dep), "--json", "state,closedByPullRequestsReferences"], root)
        merged = any(pr.get("mergedAt") for pr in data.get("closedByPullRequestsReferences") or [])
        require(data.get("state") == "CLOSED" and merged, f"dependency #{dep} is not accepted by merged PR")


def validate_unique_pr(root: Path, task: int, current_number: int) -> None:
    prs = gh_json(["pr", "list", "--state", "open", "--limit", "200", "--json", "number,body"], root)
    collisions = []
    for pr in prs:
        try:
            meta = parse_metadata(pr.get("body") or "")
        except ProtocolError:
            continue
        if meta["task"] == task and pr["number"] != current_number:
            collisions.append(pr["number"])
    require(not collisions, f"task #{task} already has open canonical PR(s): {collisions}")


def validate_pr(root: Path, event_path: Path) -> None:
    event = load_json(event_path)
    pr = event.get("pull_request")
    require(isinstance(pr, dict), "event has no pull_request")
    assert isinstance(pr, dict)
    body = pr.get("body") or ""
    meta = parse_metadata(body)
    task = meta["task"]
    issue = issue_data(root, task)
    require(issue.get("state") == "OPEN", f"task Issue #{task} must be open")
    validate_unique_pr(root, task, int(pr["number"]))
    changed = changed_files(root, pr["base"]["sha"], pr["head"]["sha"])
    protected = [p for p in changed if matches_any(p, PROTECTED_PATTERNS)]
    if meta["mode"] == "strict":
        contract_path = root / meta["contract"]
        contract = validate_contract(contract_path)
        require(contract["issue"] == task, "contract Issue does not match PR task")
        # Strict contracts must pre-exist on the base revision, so a PR cannot authorize itself.
        try:
            base_contract = json.loads(run("git", "show", f"{pr['base']['sha']}:{meta['contract']}", cwd=root))
        except ProtocolError as exc:
            raise ProtocolError("strict contract must exist on the PR base before implementation starts") from exc
        validate_contract_data(base_contract)
        require(base_contract == contract, "task contract changed inside its implementation PR")
        validate_paths(changed, contract)
        validate_dependencies(root, contract["dependencies"])
        _, manifest = build_context(root, task, pr["base"]["sha"])
        require(meta["context_digest"] == manifest["digest"], "context_digest is stale; rerun `gpp context` and update PR metadata")
    else:
        require(not protected or all(p.startswith("protocol/tasks/") for p in protected), "light task may not modify protected protocol surfaces")
    if not pr.get("draft", False):
        missing = [h for h in HANDOFF_HEADINGS if h not in body]
        require(not missing, "ready PR is missing handoff headings: " + ", ".join(missing))
    print(json.dumps({"ok": True, "task": task, "mode": meta["mode"], "changed": changed, "protected": protected}, indent=2))


def validate_repo(root: Path) -> None:
    task_dir = root / "protocol" / "tasks"
    count = 0
    if task_dir.exists():
        for path in sorted(task_dir.glob("*.json")):
            validate_contract(path)
            count += 1
    print(f"validated {count} strict task contract(s)")


def main() -> int:
    parser = argparse.ArgumentParser(prog="gpp")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate-repo")
    p_contract = sub.add_parser("validate-task")
    p_contract.add_argument("path", type=Path)
    p_context = sub.add_parser("context")
    p_context.add_argument("issue", type=int)
    p_context.add_argument("--output", type=Path)
    p_pr = sub.add_parser("validate-pr")
    p_pr.add_argument("--event", required=True, type=Path)
    args = parser.parse_args()
    root = git_root()
    try:
        if args.command == "validate-repo":
            validate_repo(root)
        elif args.command == "validate-task":
            validate_contract(args.path)
            print(f"valid: {args.path}")
        elif args.command == "context":
            context, manifest = build_context(root, args.issue)
            output = args.output or root / ".gpp" / "context" / str(args.issue)
            output.mkdir(parents=True, exist_ok=True)
            (output / "context.md").write_bytes(context)
            (output / "manifest.json").write_bytes(canonical(manifest))
            print(json.dumps({"output": str(output), "digest": manifest["digest"], "mode": manifest["mode"]}, indent=2))
        elif args.command == "validate-pr":
            validate_pr(root, args.event)
    except ProtocolError as exc:
        print(f"protocol error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
