# Dependency validation evidence

## Observed GitHub CLI boundary

For dependency Issue #1, this read-only command:

```bash
gh issue view 1 --json state,closedByPullRequestsReferences
```

returned a closed Issue whose linked pull-request object had `id`, `number`, `repository`, and `url`, but no nested merge fields:

```json
{
  "closedByPullRequestsReferences": [
    {
      "id": "PR_kwDOTZPSxc7yALh3",
      "number": 8,
      "repository": {
        "id": "R_kgDOTZPSxQ",
        "name": "hyperliquid-sim",
        "owner": {
          "id": "MDQ6VXNlcjEzMTI3NDQ=",
          "login": "kfastov"
        }
      },
      "url": "https://github.com/kfastov/hyperliquid-sim/pull/8"
    }
  ],
  "state": "CLOSED"
}
```

A separate read-only lookup supplies authoritative live merge data:

```bash
gh pr view 8 --json state,mergedAt,mergeCommit
```

```json
{
  "mergeCommit": {"oid": "723c5f7132c62d4a73886edccd7637b1e713dff2"},
  "mergedAt": "2026-07-15T11:57:25Z",
  "state": "MERGED"
}
```

## Validation rule

Dependency validation therefore reads the Issue first, requires `state == "CLOSED"`, extracts positive integer PR numbers from `closedByPullRequestsReferences`, and queries every linked PR for `state`, `mergedAt`, and `mergeCommit`. It accepts the dependency only when at least one live PR response consistently reports `MERGED`, a non-empty `mergedAt`, and a merge-commit object.

All calls are `gh issue view` or `gh pr view`; validation performs no GitHub mutation. Command failures, malformed Issue references, and malformed live PR responses raise an explicit protocol error. Missing references or linked PRs that are open or closed without merge data reject the dependency.
