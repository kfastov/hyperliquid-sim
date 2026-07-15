# Agent bootstrap

This repository uses GitHub Project Protocol v0.1. Agents are replaceable external workers; GitHub objects and Git commits are durable state.

## Start or resume work

1. Receive an explicit GitHub Issue number. Do not autonomously drain unrelated work.
2. Run `python3 tools/gpp.py context <issue>` and read the generated context.
3. For strict tasks, confirm `protocol/tasks/<issue>.json` exists on `origin/main` and its dependencies are accepted.
4. Create or check out `task/<issue>-<slug>` from the packet's base SHA.
5. Open one linked Draft PR before substantial implementation. Put a valid `<!-- gpp {...} -->` block in its body.
6. Push semantic checkpoint commits. Unpushed state is not a handoff.
7. Keep the PR handoff sections current: Completed, Remaining, Known failures, Decisions, Next action.
8. Run the task's acceptance commands and repository checks. Do not self-report success without command evidence.
9. Never push product code directly to `main`, merge your own work, weaken checks, or modify protocol/workflow files unless the task explicitly allows it.

## Durable state

Only the Issue, contract revision, remote branch, PR head/diff, checks, reviews, and PR handoff are recoverable. Dirty worktrees, stashes, local databases, and agent memory are not project state.

## Project commands

Until the Rust workspace exists, run:

```bash
python3 -m unittest discover -s tests -v
python3 tools/gpp.py validate-repo
```

Once `Cargo.toml` exists, also run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
