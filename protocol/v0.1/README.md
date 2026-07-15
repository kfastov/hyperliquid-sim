# GitHub Project Protocol v0.1

## Purpose

Provide a small runtime-neutral contract for observable work across human and coding-agent sessions. The protocol does not launch agents or preserve local process state.

## Canonical objects

- Issue: task identity, discussion, priority and decisions.
- `protocol/tasks/<issue>.json`: immutable strict execution contract on the default branch.
- One linked Draft PR and remote branch: durable implementation checkpoint.
- Check runs bound to the PR head SHA: evidence.
- Merge into protected `main`: final acceptance.

## Task modes

`light` uses the structured Issue as a mutable contract and is intended for bootstrap/tiny low-risk work. `strict` requires a versioned JSON contract and is mandatory for autonomous product implementation.

## Derived lifecycle

`DRAFT -> READY -> ACTIVE -> REVIEW -> ACCEPTED`

- READY: valid contract and accepted dependencies, no canonical PR.
- ACTIVE: exactly one linked open canonical PR.
- REVIEW: PR is ready and required checks pass on current head.
- ACCEPTED: canonical PR is merged.
- CANCELLED: Issue closed without an accepted PR.
- BLOCKED is an annotation, not a terminal state.

No worker writes `done`; merge is the acceptance event.

## Bootstrap boundary

The seed commit is Phase 0 and necessarily predates its own enforcement. After Phase 0, product changes use the protocol. Protocol changes require explicit task scope and owner review.

## Recovery guarantee

A new worker can recover all published state from the Issue, contract, remote branch, PR, checks, reviews, and handoff. Dirty worktrees, stashes, unpushed commits and private model memory are outside the guarantee.

## Commands

```bash
python3 tools/gpp.py validate-repo
python3 tools/gpp.py context 12
python3 tools/gpp.py validate-pr --event "$GITHUB_EVENT_PATH"
```
