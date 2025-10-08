# Guardrails Migration Plan

## Scope
- Draft and publish `docs/ENGINEERING.md` (Owner: Engineering Enablement, Est. 2d).
- Draft and publish `docs/DEVELOPMENT.md` (Owner: Developer Productivity, Est. 2d).
- Establish `docs/ADR/` structure with initial index template (Owner: Architecture Guild, Est. 1d).
- Normalize REVIEW artifacts into `REVIEW/MODULE_*` directories (Owner: QA Lead, Est. 2d).
- Define `.github/labels/harmonized.json` and align repository labels (Owner: Project Ops, Est. 1d).
- Seed `.github/ISSUE_TEMPLATE/` and `.github/PULL_REQUEST_TEMPLATE.md` plus `CODEOWNERS` (Owner: Engineering Enablement, Est. 1d).
- Design initial guardrails automation workflows (planning only in this phase; implementation deferred) (Owner: DevOps, Est. 0.5d planning).

## Execution Steps
1. **Author core documentation** (ENGINEERING.md, DEVELOPMENT.md, ADR index) to establish authoritative references before process changes.
2. **Restructure review artifacts** by migrating legacy REVIEW files into module-specific folders and documenting mapping decisions.
3. **Publish documentation aids** such as DOCS_MAP, DOCS_GLOSSARY updates, and cross-linking to new global docs.
4. **Introduce label canon** via harmonized.json, communicating planned taxonomy to stakeholders.
5. **Audit existing labels** against the canon and schedule cleanup tickets if drift is found.
6. **Add collaboration templates** (issue/PR) and `CODEOWNERS` to encode review expectations.
7. **Draft workflow specifications** (no code changes yet) describing upcoming automation for subsequent implementation PRs.

## Rollback Strategy
- Retain copies of legacy REVIEW artifacts until module migration is verified; revert by restoring previous top-level files from git history.
- If new documentation causes confusion, roll back by reverting the specific doc commits while leaving planning artifacts intact.
- Pause label canon rollout by withholding publication of harmonized.json or removing labels via git revert before repository labels are mutated.

## Dependencies
- Documentation (Steps 1–3) must complete before label canon or workflow planning to ensure references exist.
- Label canon must be agreed upon prior to raising any automation or enforcement issues that depend on consistent labels.
- CODEOWNERS definition depends on ENGINEERING.md and DEVELOPMENT.md specifying ownership boundaries.
