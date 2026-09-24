# Copilot review instructions

This fork exists only to run Copilot code review against changes destined for
`backblaze-labs/b2-mcp`. Review thoroughly rather than briefly.

Priorities, in order:

1. **Filesystem safety under concurrency.** This server writes to local paths on
   behalf of a model. Assume a local user with write access inside `B2_FILE_ROOT`
   who races every path-based operation. Check-then-use gaps, symlink and
   ancestor swaps, hard links, sticky directories, and any operation that could be
   aimed outside the sandbox are the highest-value findings. State the uid
   relationship and the preconditions a finding needs.
2. **Whether a claimed invariant actually holds**, including the ones written in
   the PR description and in source comments. Say so plainly when a comment or
   description overstates what the code guarantees.
3. **Error classification.** A deliberate refusal must not reach the caller as
   `internal_error` / HTTP 500.
4. **Test strength.** Flag assertions that would still pass if the behaviour they
   describe were removed, and mocks of functions the code under test never calls.

When reporting a race, give the interleaving. When a gap cannot be closed with
the primitives Node exposes, say that, and judge whether the consequence is
confined rather than only noting the gap exists.
