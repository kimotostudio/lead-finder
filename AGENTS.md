# AGENTS.md

## Role

`lead-finder` is responsible for lead discovery, filtering, scoring, and normalized handoff output for the review-first demo outreach pipeline.

Pipeline position:

```text
lead-finder -> demo-generator -> human review -> playwright-automation
```

## Current State

- Lead generation works on Ubuntu/WSL.
- Normalized handoff adapter exists and is committed.
- Relevant commits:
  - `841bbc2 Add normalized handoff and display name support`
  - `7bb1883 Add display name cleaning for lead handoff`
- Normalized handoff CSV generation worked.
- Sample run produced 22 rows.
- Deterministic `lead_id` and domain handling are in place.
- Non-web schemes such as `tel:`, `mailto:`, `line:`, `sms:`, `javascript:`, and `data:` should be ignored during domain extraction.

## Safe Work

- Local CSV transforms.
- Schema validation.
- Deterministic ID/domain handling.
- Handoff field compatibility improvements.
- `python3 -m py_compile` and local smoke tests.
- Documentation for data contracts and runbooks.

## Do Not Commit

- Real generated lead CSVs.
- Private lead data.
- Generated handoff output CSVs under `web_app/output/`.
- Logs or local operational artifacts.

## Working Rules

- Read `/home/kimoto/projects/PROJECT_STATE.md` first.
- Inspect `git status --short` before edits.
- Keep patches small and repo-specific.
- Preserve original source columns when normalizing.
- Prefer stable domain-derived IDs where possible.
- Ask before scraping new external targets or running network searches.
- End with a detailed `Report for ChatGPT` following `/home/kimoto/projects/prompts/final_report_template.md`, plus Discord notification when possible.
