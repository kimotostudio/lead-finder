# Normalized Handoff CSV

`tools/normalize_handoff_csv.py` converts local `lead-finder` CSV output into a review-first handoff CSV for the next pipeline stages.

It is local-only: it does not access websites, generate demo pages, open a browser, or submit forms.

## Command

```bash
python3 tools/normalize_handoff_csv.py \
  --input web_app/output/outreach_ready_fukuoka_20260216_132739.csv
```

By default, the output is written next to the input as:

```text
web_app/output/handoff_normalized_fukuoka_20260216_132739.csv
```

Use `--output path/to/file.csv` to choose a different location.

## Core Fields

The adapter writes a stable set of normalized fields first:

| Field | Purpose |
| --- | --- |
| `lead_id`, `id` | Stable row identifier for downstream tools. When missing, generated from domain first, then business name plus source row. |
| `business_name`, `company_name`, `salon_name`, `brand_name`, `店名` | Name aliases used by review, demo, and automation tools. |
| `website`, `url`, `reference_url`, `url(旧)` | Original business website aliases. |
| `contact_page`, `contact_url` | Contact or form URL when present in the source CSV. |
| `industry`, `business_type` | Business category when present. |
| `location`, `area` | Location or area signal when present. |
| `score`, `solo_score` | Lead quality or solo-business score when present. |
| `notes` | Source notes or reason text when present. |
| `domain` | Domain extracted from HTTP/HTTPS URLs, `www.` domains, or plain domain-like strings when possible. Non-web schemes such as `tel:` and `mailto:` are ignored. |
| `demo_path`, `demo_url`, `url(デモ)` | Empty placeholders for demo-generator output. |
| `message_path`, `message` | Empty placeholders for approved outreach message assets. |
| `status`, `reason` | Empty placeholders for human review or automation status. |
| `template`, `image`, `therapist_image` | Empty placeholders compatible with demo-generator TSV-style inputs. |
| `source_csv`, `source_row` | Local provenance for auditability. |

## Preserved Source Fields

Original input columns are preserved as `original__<column_name>` fields. This keeps private lead data local and reviewable without changing the original source file.

## Compatibility Notes

- `demo-generator/generate.py` can use `id`, `brand_name`, `reference_url`, `template`, `image`, and `therapist_image` after template/image choices are filled.
- `playwright-automation/src/main.py` can read `id`, `店名`, `url(旧)`, and `url(デモ)` via its existing aliases.
- The handoff CSV is not an approval record by itself. Human review should fill or confirm demo/message/status fields before any SEMI_AUTO browser run.
