# Hybrid Legal Intelligence Dashboard

This repository turns your original West Bengal corporate-fraud dashboard concept into a buildable MVP scaffold that is easier to run, easier to explain, and more realistic to commercialize.

The codebase includes:

- a modular ingestion-to-dashboard pipeline
- synthetic demo data so the product can be shown safely without live allegations
- explainable quality and risk scoring
- repeat-entity and cluster analytics
- CSV snapshot outputs for local operation
- a Gradio dashboard fallback for environments where it is practical
- a richer Streamlit workspace for deeper analysis, filtering, and downloads

## What changed from the original blueprint

The original definition was strong on purpose and workflow. This implementation adds the product and engineering changes needed for commercial viability:

- provider abstraction so new data connectors can be added without rewriting the pipeline
- safe demo mode for sales demos and internal testing
- explicit scoring logic for quality, reliability, and risk
- clearer output layers: source log, master records, enforcement cases, repeat offenders, risk scores, clusters
- search-first dashboard workflow with drill-down tables
- a documented scale path from CSVs to DuckDB/Postgres and background workers
- a commercialization plan focused on subscriptions, alerts, exports, and enterprise research workflows

## Project structure

```text
hybrid_legal_dashboard/
  app.py
  config.py
  demo_data.py
  pipeline.py
  schemas.py
  services/
docs/
tests/
data/output/
```

## Quick start

```bash
python -m hybrid_legal_dashboard.pipeline --mode demo
python -m hybrid_legal_dashboard.app
```

Recommended interface on Python 3.9:

```bash
python3 -m streamlit run hybrid_legal_dashboard/streamlit_app.py
```

If you are using macOS system Python 3.9, install the pinned dependencies first:

```bash
python3 -m pip install -r requirements.txt
```

This project keeps `urllib3>=1.26.18,<2` on Python versions below 3.10 to avoid the common LibreSSL warning seen with Apple system Python without falling back to ancient broken releases.
It also keeps `networkx<3.3` so installs remain compatible with Python 3.9.
On Python 3.9, Streamlit is the supported interface path. Gradio remains supported on Python 3.10+.

## Live sources

`--mode live` is now wired to real public providers:

- official SFIO English homepage
- official SFIO `What's New` listing
- official SFIO `Notifications` listing
- official SFIO `Investigations Completed` page
- official SFIO `Summons/Notices` page
- official NCLT Kolkata Bench page
- official NCLT Kolkata order-date, case-number, party-name, advocate-name, judgment-date, and judge-wise search gateways
- official NCLT Kolkata final-order listing with company-level PDF links
- official NCLT archive notice/circular listings filtered for Kolkata material
- official NCLAT daily-order material filtered for West Bengal and Kolkata-linked matters
- official IBBI NCLT and NCLAT order listings with pagination for Kolkata-bench and West Bengal-linked matters
- official IBBI liquidation-auction listings with West Bengal, Kolkata, Howrah, Hooghly, Medinipur, Parganas, Durgapur, Asansol, Siliguri, and Haldia keyword coverage
- official Calcutta High Court notice listings filtered for company, liquidation, and fraud-related notices
- official Calcutta High Court company-liquidation notice listings
- official Calcutta High Court eCourts Original Side, Appellate Side, and Jalpaiguri cause-list gateways
- official Calcutta High Court eCourts Original Side, Appellate Side, and Jalpaiguri case-status/order gateways
- official Calcutta High Court order-search gateway
- official eCourts high-court judgments search portal
- official MCA / ROC Kolkata document sources for disqualified directors and struck-off companies
- Google News RSS search feeds for West Bengal and SFIO-related fraud queries

The default Google News RSS queries are:

- `West Bengal company fraud`
- `Kolkata corporate fraud company`
- `SFIO company fraud West Bengal`
- `section 447 company fraud Kolkata`

Run live mode with:

```bash
python3 -m hybrid_legal_dashboard.pipeline --mode live
python3 -m hybrid_legal_dashboard.app
```

Optional live-mode environment variables:

- `LEGAL_DASHBOARD_SEARCH_QUERIES`
  Use `||` to separate queries. Example:
  `LEGAL_DASHBOARD_SEARCH_QUERIES="West Bengal company fraud||SFIO Kolkata company fraud"`
- `LEGAL_DASHBOARD_EXTRA_RSS_URLS`
  Add extra public RSS endpoints, separated by `||`
- `LEGAL_DASHBOARD_SOURCE_LIMIT`
  Controls the max items collected per source provider

## Local MCA PDFs

The pipeline now accepts locally downloaded MCA PDFs from `data/raw/` and its subfolders.
This is the fastest way to unlock large official company batches when the MCA site blocks automated download.

How it works:

- if a local copy of a known MCA PDF is present, the pipeline uses that local file for parsing while keeping the official MCA source identity
- if you drop in additional MCA PDFs with recognizable names, the pipeline auto-registers them and tries to parse them into company rows
- local PDFs are only used in `--mode live`

Recommended filenames to preserve:

- `directordisqualificationKolkatta_16112017.pdf`
- `STK5RocWB_03072018.pdf`
- `ListROCKolkata_21072017.pdf`

Suggested workflow:

```bash
mkdir -p data/raw/mca
# optional: let the helper attempt the official MCA downloads for you
python3 -m hybrid_legal_dashboard.download_raw_sources
# if MCA blocks automation, place the PDFs in data/raw/ or data/raw/mca/
python3 -m hybrid_legal_dashboard.pipeline --mode live
```

The current local-file heuristics are strongest for:

- director-disqualification PDFs under section 164
- STK-5 or STK-7 struck-off / dissolution company lists under section 248

If those PDFs are present locally, the row caps are high enough to expand them into hundreds or thousands of official company records.

The downloader helper creates the folder if needed and attempts the currently configured MCA PDFs:

```bash
python3 -m hybrid_legal_dashboard.download_raw_sources
```

If MCA blocks automated download, the helper prints the exact browser-save fallback:

- open the official URL in your browser
- save the PDF into `data/raw/mca/`
- rerun the pipeline

## Local Insta company seeds

If InstaFinancials blocks the public `A-Z` directory or sitemap with CAPTCHA, the pipeline can now accept local CSV seed files from `data/raw/insta/`.
This is the simplest fallback when you already know company CINs or profile URLs.

Supported CSV columns:

- `company_name`
- `cin`
- `url`
- `company_url`
- `link`
- `profile_url`
- `company_status`
- `registered_address`
- `paid_up_capital`
- `query_text`

Only `cin` plus either `company_name` or `url` is really needed. The pipeline will:

- load every `.csv` file in `data/raw/insta/`
- keep only West Bengal CINs
- turn those rows into registry candidates
- fetch the company profile pages when accessible
- merge them into the live registry output

Template file:

- [company_seed_template.csv](/Users/aniruddharoy/Documents/Playground/data/raw/insta/company_seed_template.csv)

Example workflow:

```bash
python3 -m hybrid_legal_dashboard.pipeline --mode live
```

If you save verified company seeds like `SREI Infrastructure Finance Limited`, `SREI Equipment Finance Limited`, or any other West Bengal companies into `data/raw/insta/*.csv`, the pipeline will use them even when the live directory crawl is blocked.

## Historical range

The live pipeline can now widen its Google News historical search window to cover a year range such as `2000` to `2025`.
This is useful for building longer company timelines, but it depends on what historical material remains indexed by the upstream sources.

Example:

```bash
python3 -m hybrid_legal_dashboard.pipeline --mode live --include-historical --start-year 2000 --end-year 2025
```

Optional tuning:

- `--historical-window-years 5`
  Splits the range into five-year search windows to improve recall without generating an excessive number of feeds
- `LEGAL_DASHBOARD_INCLUDE_HISTORICAL=true`
- `LEGAL_DASHBOARD_START_YEAR=2000`
- `LEGAL_DASHBOARD_END_YEAR=2025`
- `LEGAL_DASHBOARD_HISTORICAL_WINDOW_YEARS=5`
- `LEGAL_DASHBOARD_HISTORICAL_QUERIES`
  Override the default historical queries with `||` separators

## Interface

The Streamlit workspace is now the recommended analyst UI. It is styled as a formal blue-and-white legal workspace with:

- rounded cards and panels instead of sharp-edged containers
- Times New Roman typography throughout
- blue-white chart and panel treatment for a more report-ready presentation
- softer input, button, and table treatment so the interface feels less mechanical
- a company-dossier explorer with filters, charts, and downloads
- cleaner hero, KPI, records, sources, and research-assistant sections

Launch it with:

```bash
python3 -m streamlit run hybrid_legal_dashboard/streamlit_app.py
```

## Outputs

Running the pipeline writes the following files into `data/output/`:

- `master_dataset_latest.csv`
- `company_reports_latest.csv`
- `companies_latest.csv`
- `enforcement_cases_latest.csv`
- `repeat_offenders_latest.csv`
- `risk_scores_latest.csv`
- `cluster_summary_latest.csv`
- `source_log_latest.csv`

Dated snapshots are also written alongside the latest files.

## Commercial roadmap summary

- MVP: CSV snapshots, Gradio app, explainable scoring, analyst workflows
- Growth: DuckDB or Postgres, scheduled jobs, FastAPI service layer, user accounts, exports, saved views
- Enterprise: multi-tenant search, alerting, case workspaces, RBAC, audit logs, API access, partner data ingestion

## Important notes

- The demo dataset is synthetic and exists only to make the product runnable out of the box.
- The platform is a decision-support system, not an adjudication engine.
- Risk scores indicate prioritization, not guilt.

## Troubleshooting

- If `python3 -m hybrid_legal_dashboard.app` fails on Python 3.9, prefer the Streamlit app at [hybrid_legal_dashboard/streamlit_app.py](/Users/aniruddharoy/Documents/Playground/hybrid_legal_dashboard/streamlit_app.py). Gradio is now treated as optional in older Python environments.
- If you see an `urllib3` LibreSSL warning on macOS, reinstall from [requirements.txt](/Users/aniruddharoy/Documents/Playground/requirements.txt) or use a newer Python build linked against OpenSSL.
- If live mode returns no rows, confirm that your internet connection can reach `sfio.gov.in` and `news.google.com`, and try lowering or simplifying the search queries.
- Some MCA PDFs are now protected behind TLS or anti-bot behaviour that may block direct automated download from macOS system Python. The pipeline keeps those official links in the source log, but in this environment they may not expand into row-level company data unless the PDFs are made available locally or the runtime uses a newer SSL stack.

Additional detail lives in:

- [/Users/aniruddharoy/Documents/Playground/docs/architecture.md](/Users/aniruddharoy/Documents/Playground/docs/architecture.md)
- [/Users/aniruddharoy/Documents/Playground/docs/product_strategy.md](/Users/aniruddharoy/Documents/Playground/docs/product_strategy.md)
