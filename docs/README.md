# RDS documentation index

Status: current

Reviewed against code: 2026-10-21

Last verified maintenance update: 2026-10-21

Baseline: `main` commit `7b57359555d75ec75de59f945f1512a3a503c84a`

This index separates documentation that was checked against the repository from
historical drafts and external reference material. A review date means the
document's statements were checked against the named baseline; it does not mean
that every described feature is complete or operational.

## Current, code-verified documents

| Document | Status | Scope |
|---|---|---|
| [`current/CODEBASE_STATUS.md`](current/CODEBASE_STATUS.md) | Verified current, reviewed 2026-10-21 | Implemented pipeline, entry points, and known gaps |
| [`current/INSTALLATION_REQUIREMENTS.md`](current/INSTALLATION_REQUIREMENTS.md) | Verified current inventory, reviewed 2026-10-19 | Dependency declarations and unresolved packaging gaps |
| [`../README.md`](../README.md) | Verified current pointer, reviewed 2026-10-19 | Repository introduction and documentation entry point |

## Future capabilities

Documents under [`future/`](future/) describe planned capabilities, not current
RDS behavior.

| Document | Status | Scope |
|---|---|---|
| [`future/INTER_RCC_MESSAGING_AND_COORDINATION.md`](future/INTER_RCC_MESSAGING_AND_COORDINATION.md) | Proposed; architecture reviewed 2026-10-21 | Integrated RCC messaging, alert/case linkage, and deterministic SRR coordination recommendations |
| [`future/OPERATIONAL_VISUALIZATION_AND_SRR_COORDINATION.md`](future/OPERATIONAL_VISUALIZATION_AND_SRR_COORDINATION.md) | Proposed; implementation deferred until the core system is stable | Map-to-globe visualization, satellite intelligence, timeline, SRR-boundary detection, and inter-command-center chat/sharing |

## Archived project documentation

Superseded, draft, and unverified project documents are preserved in
[`archive/2025/`](archive/2025/). See its
[`README.md`](archive/2025/README.md) for the original-to-archive mapping and
the reason each file was not certified as current.

Archived files are retained as historical evidence. Do not silently correct
them in place. Create or update a document under `docs/current/`, and record
the relationship in the archive index.

## External references

Third-party specifications and manuals live under [`reference/`](reference/).
They are not assertions about the current RDS implementation. Their version and
publisher should be checked before operational use.

## Documentation convention

- `docs/current/`: maintained documents checked against a named commit.
- `docs/archive/YYYY/`: immutable historical project documents. Use
  `<subject>_v<version>_<source-date>.<ext>` when version/date are known and
  `_undated` when the source has no reliable date.
- `docs/reference/`: third-party source material, named with its published
  version where known.
- `docs/future/`: planned capabilities that are not yet implemented.
- Every current document must state its status, review date, and baseline
  commit. Preserve any earlier creation/revision dates; the review date is an
  additional field, not a replacement.

Moving a tracked file into the archive should use a Git rename so its history
remains traceable. Archived binary documents should remain byte-for-byte
unchanged.

## Git snapshot and tagging policy

`main` remains the integration branch. Documentation and code changes are made
on short-lived branches and merged by pull request.

Use annotated tags for recoverable whole-repository snapshots:

- Tag format: `archive/YYYY-MM-DD`.
- If more than one snapshot is required on a date, append `-2`, `-3`, and so on.
- Create a tag after the relevant pull request has merged, pointing at the
  reviewed commit on `main`; do not tag an unmerged working branch.
- Create snapshots at meaningful releases or migrations and, when `main` has
  changed, at least monthly during active development.
- Treat snapshot tags as immutable. If a tag is wrong, create a corrected tag
  and document why; do not move the original silently.
- Do not maintain a parallel archive branch. Git history plus annotated tags
  preserves the entire repository, while `docs/archive/` provides a browsable
  home for superseded document files.

The annotated `archive/2026-10-19` tag is the first snapshot under this policy.
It points to the merge of the initial documentation reconciliation and remains
the historical anchor for that review; subsequent repairs remain in normal Git
history and do not move the tag.
