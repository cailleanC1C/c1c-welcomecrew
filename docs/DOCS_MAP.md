# Repository Documentation Map

_Last updated: 2025-12-10_

## Global Docs
- **Purpose**: Provide organization-wide engineering guardrails, development workflows, and supporting references.
- **Location pattern**: `docs/*.md`
- **Naming convention**: Uppercase snake-case filenames (e.g., `ENGINEERING.md`).
- **Create when**: Introducing or revising global processes, onboarding guidance, or top-level handbooks.
- **Current files (3)**:
  - `docs/DOCS_GLOSSARY.md` — Definitions for guardrails terminology.
  - `docs/DOCS_MAP.md` — This navigational index of documentation assets.
  - `docs/ops/Config.md` — Watchdog and health configuration guide for WelcomeCrew.

## Architectural Decision Records (ADRs)
- **Purpose**: Capture significant architectural and process decisions with rationale and status.
- **Location pattern**: `docs/ADR/*.md`
- **Naming convention**: Sequential numeric prefix plus short slug (e.g., `0001-use-structure-lint.md`).
- **Create when**: Finalizing decisions that impact architecture, tooling, or guardrails scope.
- **Current files**: _None yet._

## Module Reviews & Plans
- **Purpose**: Store module-level reviews, plans, and acceptance checklists for specific components.
- **Location pattern**: `REVIEW/MODULE_*/**`
- **Naming convention**: Directory per module prefixed with `MODULE_`, containing markdown/csv artifacts as needed.
- **Create when**: Conducting module-specific audits, planning, or quality reviews.
- **Current entries (1)**:
  - `REVIEW/BOOTSTRAP_GUARDRAILS/` — Planning pack for guardrails rollout (inventory, gaps, plan, acceptance checklist).
- **Legacy artifacts requiring migration**:
  - `REVIEW/ARCH_MAP.md`
  - `REVIEW/FINDINGS.md`
  - `REVIEW/HOTSPOTS.csv`
  - `REVIEW/LINT_REPORT.md`
  - `REVIEW/PERF_NOTES.md`
  - `REVIEW/REVIEW.md`
  - `REVIEW/TESTPLAN.md`
  - `REVIEW/THREATS.md`
  - `REVIEW/TODOS.md`
  - `REVIEW/TYPECHECK_REPORT.md`

## Issue Batches
- **Purpose**: Track curated sets of issues for rollout phases or thematic workstreams.
- **Location pattern**: `.github/issue-batches/*.json`
- **Naming convention**: Hyphen-separated slugs summarizing the batch (e.g., `guardrails-rollout.json`).
- **Create when**: Planning coordinated work packages that require synchronized delivery.
- **Current files (1)**:
  - `.github/issue-batches/guardrails-rollout.json` — Planning batch for guardrails rollout phase approval.

## Workflows
- **Purpose**: Automate guardrails enforcement, CI, and repository hygiene through GitHub Actions.
- **Location pattern**: `.github/workflows/*.yml`
- **Naming convention**: Verb-oriented descriptors (e.g., `guardrails-ci.yml`).
- **Create when**: Automating linting, structure checks, or other guardrails once planning is approved.
- **Current files**: _None yet._

## Labels Canon
- **Purpose**: Define authoritative label taxonomy for issues and PRs.
- **Location pattern**: `.github/labels/harmonized.json`
- **Naming convention**: Single JSON file containing label metadata.
- **Create when**: Establishing or updating label taxonomy.
- **Current files**: _None yet._

## Static Assets
- **Purpose**: Support reviews with supplemental data (e.g., metrics, diagrams) that are not core docs.
- **Location pattern**: `REVIEW/**/*.{csv,png,json}` and other non-markdown assets.
- **Naming convention**: Descriptive lowercase names with extensions indicating data type.
- **Create when**: Producing auxiliary assets during audits or analyses.
- **Current files (1)**:
  - `REVIEW/HOTSPOTS.csv` — Historical hotspots dataset from previous review.
