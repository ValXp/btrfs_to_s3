Agent Instructions

Loop behavior
- Read `tasks.md` and `progress.md`.
- Work on the first task that is not yet completed.
- Append any findings (errors, gotchas, FYIs) to `learnings.md`.
- Upon task completion, make a commit of the changes you made. Do not attempt to push; there is no network access.
- Ensure all `.md` files are committed as well.
- Update `progress.md` after each run (done or blocked) with status, date and hour (local time), and commit message (no commit hash).
- If you change privilege requirements, update `testing/README.md`.
- If blocked or if all tasks are done, create `stop.md` with a short reason and stop; the loop will end when `stop.md` exists.
