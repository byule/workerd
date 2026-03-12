---
name: investigation-notes
description: Structured scratch document for tracking investigation state during bug hunts - prevents re-reading code, losing context, and rabbit holes; maintains external memory so you don't re-derive conclusions
---

# Investigation Notes

## Overview

During bug investigations, maintain a lightweight scratch document as external memory. This prevents re-reading code you've already analyzed, losing track of which hypothesis you're testing, and silently drifting into rabbit holes.

**Core principle:** Write it down once, refer to it later. Re-reading your one-line note is faster than re-reading 200 lines of source.

## The Document

Create `~/tmp/investigate-<short-name>.md` during orientation (step 2 of `/investigate`). The short name should be descriptive enough to identify the investigation (e.g., `investigate-concurrent-write.md`, `investigate-pipe-zombie-state.md`).

**Do not** create the document preemptively for every investigation. Refer to the rules below for when to create it.

### Format

```markdown
# Investigation: <one-line bug description>

## Error

<assertion/crash message, file:line — written once, never changes>

## Current Focus

<single sentence: what you are doing RIGHT NOW>

## Hypotheses

1. [TESTING] <one sentence> — test: <test name or "not yet written">
2. [REJECTED] <one sentence> — disproved by: <one sentence>
3. [CONFIRMED] <one sentence> — evidence: <test name + result>

## Code Read

- `file:line-range` — <what you learned, one sentence>

## Tests

- `file:line` "test name" — <result + what it means, one sentence>

## Ruled Out

- <thing you investigated and eliminated, one sentence why>

## Next

1. <concrete next action>
2. <fallback>
```

## Rules

### Creation

**Create the document when you notice any of these:**

- You have more than one hypothesis to track
- You've read more than 3 files/functions and need to remember what you learned
- You're about to re-read code you already read
- The investigation is going to span multiple iterations of test-write-run

When you do create it, populate Error, Current Focus, your current hypotheses, and anything you've already learned (backfill "Code Read" and "Tests" from what you've done so far).

### Updates

Update the document after each significant action:

- After reading a function or file section → add to "Code Read"
- After forming or rejecting a hypothesis → update "Hypotheses"
- **After running a test → update BEFORE doing anything else.** Record: what test ran, what the result was, what it means for the active hypothesis. This is non-negotiable — it takes 30 seconds and prevents the pattern of running multiple tests, losing track of what they proved, and falling back to code reading.
- After starting a new thread of work → update "Current Focus"

Do NOT update after every tool call. Do NOT polish or reorganize. Append and move on.

### Entries

Every entry is **1-2 sentences max**. Use `file:line` references, not code dumps. If you're writing a paragraph, you're procrastinating.

You may include brief, simple diagrams if they help you understand and retain information.

You may write a wrong hypothesis or a test that doesn't reproduce the bug. That's valuable.

The point is to externalize your thinking and get feedback from the code, not to be right on the first try.

### Before Re-Reading Code

**Check "Code Read" first.** If the file and line range are already listed, use your note. You already extracted what you needed. If the note isn't sufficient, that's a sign the note was too vague — read the code, then write a better note. Do not make a habit of this.

### Current Focus

Update "Current Focus" before starting any new thread of work. If your current focus doesn't relate to a hypothesis in the list, you've drifted. Either add a hypothesis for what you're doing or stop and return to the active one.

### Hypothesis Limits

- **Maximum 3 hypotheses total.** If you want to add a fourth, reject or merge one first.
- **Maximum 1 `[TESTING]` at a time.** Commit to one, test it, resolve it, then move on.
- **Every hypothesis must have a test or a concrete plan to write one.** "Need to investigate further" is not a valid state — that's analysis paralysis wearing a label.

Valid statuses:

- `[UNTESTED]` — formed but not yet tested. Must become `[TESTING]` or `[REJECTED]` soon.
- `[TESTING]` — actively being tested. Only one at a time.
- `[CONFIRMED]` — test reproduced the bug as predicted.
- `[REJECTED]` — test disproved it, or evidence rules it out. Include why.
- `[SUPERSEDED]` — replaced by a more specific hypothesis. Reference the replacement.

### Priority

**Maintaining the document never takes priority over writing or running a test.** If you're choosing between updating notes and writing a test, write the test. Update the notes after.

The document is scratch paper. It exists to serve the investigation, not to document it.

Delete the document when the investigation is over.

The human will tell you when the investigation is complete.

## Anti-Patterns

| You're doing this                                                              | Do this instead                                           |
| ------------------------------------------------------------------------------ | --------------------------------------------------------- |
| Writing multi-line entries                                                     | One sentence. Use `file:line`.                            |
| Dumping code into the doc                                                      | Reference it: `file:line` — "does X"                      |
| Reorganizing or reformatting the doc                                           | Append and move on                                        |
| Updating notes instead of writing a test                                       | Write the test first                                      |
| 3 `[UNTESTED]` hypotheses and no tests                                         | Pick one, write a test, run it                            |
| Re-reading a file listed in "Code Read"                                        | Use your note                                             |
| "Current Focus" hasn't changed in a while but you're doing something different | You drifted. Update it or go back.                        |
| You've run 2+ tests without updating the document                              | You're accumulating conclusions in your head. Update now. |
| The doc is longer than ~50 lines                                               | You're over-documenting. Trim or start fresh.             |
