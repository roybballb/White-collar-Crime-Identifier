# Product Strategy and Commercial Upgrades

## Product positioning

The strongest commercial framing is not "fraud detector" but "legal and regulatory intelligence workspace." That positioning is safer, more accurate, and more broadly sellable to:

- legal research teams
- corporate compliance teams
- journalists and policy researchers
- due diligence firms
- consulting and risk teams

## Efficiency upgrades

### From the original concept

- Move from one-off batch updates to scheduled incremental ingestion.
- Cache unchanged source pages and only reprocess deltas.
- Deduplicate at both source and entity levels using stable keys.
- Score records once and reuse the outputs for the dashboard, alerts, and exports.

### Why it matters

This lowers compute cost, reduces dashboard latency, and makes refreshes operationally reliable.

## User-experience upgrades

### Recommended product changes

- Make the home screen search-first, not chart-first.
- Add source confidence badges beside every citation.
- Add entity drill-down pages with timeline, sections, districts, and sources in one place.
- Let users save filters, watchlists, and recurring queries.
- Provide "why this score" explanations beside every risk result.

### Why it matters

Analysts and paying enterprise users value trust, speed to answer, and exportability more than visual complexity alone.

## Commercial features to prioritize

- Email or in-app alerts for entity mentions and risk changes
- PDF and spreadsheet exports for internal reporting
- Role-based access for team research workflows
- API access for enterprise customers
- White-label deployment for consultancies and legal-tech partners

## Monetization model

### Tier 1: Analyst

- dashboard access
- saved searches
- exports

### Tier 2: Team

- shared watchlists
- alerts
- collaboration notes
- higher refresh frequency

### Tier 3: Enterprise

- private deployment
- API access
- custom data connectors
- audit logs
- SSO and RBAC

## Scalability plan

### Data layer

- MVP: CSV snapshots
- Next: DuckDB for analytics
- Scale: Postgres plus object storage

### Processing layer

- MVP: in-process Python jobs
- Next: scheduled workers
- Scale: queue-based ingestion and scoring services

### Experience layer

- MVP: Gradio
- Next: React or Next.js front end with FastAPI
- Scale: multi-tenant web platform with case workspaces

## Trust and compliance upgrades

- Preserve source citations for every claim shown in the UI.
- Use reliability labels and explain that the system prioritizes records, not guilt.
- Support analyst review workflows before promoting records to "verified insight."
- Add policy controls for retention, source restrictions, and audit logging in enterprise deployments.

