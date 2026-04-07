from __future__ import annotations

from html import escape
import os
from pathlib import Path
import socket
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from hybrid_legal_dashboard.config import OUTPUT_DIR
from hybrid_legal_dashboard.pipeline import run_pipeline
from hybrid_legal_dashboard.services.chat import answer_question
from hybrid_legal_dashboard.services.storage import load_outputs, load_run_metadata


_MPL_CONFIG_DIR = OUTPUT_DIR.parent / ".cache" / "matplotlib"
_MPL_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_MPL_CONFIG_DIR))

APP_CSS = """
:root {
  --app-blue: #154a96;
  --app-blue-deep: #0d3470;
  --app-blue-soft: #edf4ff;
  --app-blue-mid: #d7e7ff;
  --app-border: #c9daf7;
  --app-white: #ffffff;
  --app-text: #173965;
  --app-shadow: 0 18px 45px rgba(21, 74, 150, 0.12);
}

html, body, .gradio-container {
  font-family: "Times New Roman", Times, serif !important;
  background: linear-gradient(180deg, #e8f1ff 0%, #f9fbff 42%, #eef5ff 100%) !important;
  color: var(--app-text) !important;
}

.gradio-container {
  max-width: 1440px !important;
  padding: 24px 22px 46px !important;
}

.app-shell {
  background: rgba(255, 255, 255, 0.94);
  border: 1px solid rgba(201, 218, 247, 0.9);
  border-radius: 34px;
  padding: 24px;
  box-shadow: var(--app-shadow);
}

.hero-card {
  background: linear-gradient(135deg, rgba(255, 255, 255, 0.98) 0%, rgba(230, 241, 255, 0.96) 100%);
  border: 1px solid var(--app-border);
  border-radius: 30px;
  padding: 26px 30px;
  box-shadow: var(--app-shadow);
  margin-bottom: 20px;
}

.hero-topline {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 12px;
  padding: 8px 14px;
  border-radius: 999px;
  background: rgba(21, 74, 150, 0.08);
  border: 1px solid rgba(21, 74, 150, 0.16);
  color: var(--app-blue-deep);
  font-size: 15px;
}

.hero-card h1,
.hero-card h2,
.section-head h2 {
  margin: 0;
  color: var(--app-blue-deep);
  font-weight: 700;
}

.hero-card h1 {
  font-size: 2.2rem;
  margin-bottom: 10px;
}

.hero-card p,
.section-head p,
.kpi-card p,
.kpi-card span {
  margin: 0;
  color: var(--app-text);
}

.hero-copy {
  font-size: 1.05rem;
  line-height: 1.6;
  margin-bottom: 14px !important;
}

.hero-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  margin-top: 16px;
}

.hero-pill {
  background: var(--app-white);
  border: 1px solid var(--app-border);
  border-radius: 999px;
  padding: 8px 14px;
  color: var(--app-blue-deep);
  font-size: 0.95rem;
}

.section-head {
  background: rgba(237, 244, 255, 0.75);
  border: 1px solid rgba(201, 218, 247, 0.9);
  border-radius: 26px;
  padding: 18px 22px;
  margin-bottom: 16px;
}

.section-head h2 {
  font-size: 1.45rem;
  margin-bottom: 6px;
}

.kpi-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 16px;
  margin-bottom: 24px;
}

.kpi-card {
  background: linear-gradient(180deg, #ffffff 0%, #f3f8ff 100%);
  border: 1px solid var(--app-border);
  border-radius: 26px;
  padding: 18px 18px 20px;
  box-shadow: 0 14px 30px rgba(21, 74, 150, 0.08);
  transition: transform 0.18s ease, box-shadow 0.18s ease;
}

.kpi-card span {
  display: block;
  font-size: 0.98rem;
  margin-bottom: 10px;
}

.kpi-card strong {
  display: block;
  font-size: 2rem;
  color: var(--app-blue-deep);
  line-height: 1.1;
}

.rounded-panel,
.rounded-panel > div,
.gradio-container .gr-box,
.gradio-container .block,
.gradio-container .form,
.gradio-container .gr-group,
.gradio-container .gr-panel {
  border-radius: 26px !important;
}

.rounded-panel {
  background: rgba(255, 255, 255, 0.96);
  border: 1px solid var(--app-border);
  box-shadow: 0 16px 35px rgba(21, 74, 150, 0.08);
  padding: 10px 10px 6px !important;
}

.hero-card:hover,
.kpi-card:hover,
.rounded-panel:hover {
  transform: translateY(-1px);
  box-shadow: 0 20px 40px rgba(21, 74, 150, 0.12);
}

.gradio-container .tab-nav {
  gap: 8px;
  padding: 6px;
  background: rgba(224, 236, 255, 0.75);
  border-radius: 999px !important;
  border: 1px solid rgba(201, 218, 247, 0.9);
  margin-bottom: 16px;
}

.gradio-container .tab-nav button {
  border-radius: 999px !important;
  border: 1px solid transparent !important;
  background: transparent !important;
  color: var(--app-blue-deep) !important;
  font-family: "Times New Roman", Times, serif !important;
  font-size: 1rem !important;
}

.gradio-container .tab-nav button.selected {
  background: linear-gradient(135deg, #1a56ad 0%, #0f3c82 100%) !important;
  color: #ffffff !important;
  border-color: rgba(15, 60, 130, 0.7) !important;
}

.gradio-container input,
.gradio-container textarea,
.gradio-container select,
.gradio-container button {
  border-radius: 18px !important;
  font-family: "Times New Roman", Times, serif !important;
}

.gradio-container input,
.gradio-container textarea,
.gradio-container select {
  border: 1px solid var(--app-border) !important;
  background: rgba(248, 251, 255, 0.95) !important;
  color: var(--app-text) !important;
}

.gradio-container button.primary,
.gradio-container button[variant="primary"] {
  background: linear-gradient(135deg, #1f5fbf 0%, #123f8c 100%) !important;
  color: #ffffff !important;
  border: 1px solid rgba(18, 63, 140, 0.7) !important;
}

.gradio-container button {
  transition: transform 0.16s ease, box-shadow 0.16s ease, background 0.16s ease !important;
}

.gradio-container button:hover {
  transform: translateY(-1px);
  box-shadow: 0 12px 24px rgba(21, 74, 150, 0.12);
}

.gradio-container .wrap,
.gradio-container table,
.gradio-container .table-container,
.gradio-container .dataframe {
  border-radius: 22px !important;
  overflow: hidden !important;
}

.gradio-container table thead tr {
  background: #eaf2ff !important;
}

.gradio-container table tbody tr:nth-child(even) {
  background: rgba(240, 246, 255, 0.62) !important;
}

.gradio-container table tbody tr:hover {
  background: rgba(219, 233, 255, 0.7) !important;
}

.gradio-container a {
  color: #1c5cb5 !important;
}

@media (max-width: 768px) {
  .hero-card {
    padding: 22px 20px;
  }

  .hero-card h1 {
    font-size: 1.8rem;
  }
}
"""


def _patch_hf_hub_for_gradio() -> None:
    """Backfill the removed HfFolder API for older Gradio/HF Hub mixes."""

    try:
        import huggingface_hub as hf_hub
    except ImportError:
        return

    if hasattr(hf_hub, "HfFolder"):
        return

    class HfFolder:
        @staticmethod
        def get_token() -> Optional[str]:
            getter = getattr(hf_hub, "get_token", None)
            return getter() if getter else None

        @staticmethod
        def save_token(token: str) -> None:
            from huggingface_hub.constants import HF_TOKEN_PATH

            token_path = Path(HF_TOKEN_PATH).expanduser()
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text(token.strip(), encoding="utf-8")

        @staticmethod
        def delete_token() -> None:
            from huggingface_hub.constants import HF_TOKEN_PATH

            token_path = Path(HF_TOKEN_PATH).expanduser()
            if token_path.exists():
                token_path.unlink()

    hf_hub.HfFolder = HfFolder


_patch_hf_hub_for_gradio()

try:
    import gradio as gr
except ImportError as exc:
    raise SystemExit(
        "Gradio could not start in this environment. On Python 3.9, use the Streamlit interface instead: "
        "`python3 -m streamlit run hybrid_legal_dashboard/streamlit_app.py`. "
        "If you specifically want Gradio, use Python 3.10+ and reinstall the requirements."
    ) from exc


def ensure_datasets() -> dict[str, pd.DataFrame]:
    datasets = load_outputs(OUTPUT_DIR)
    if datasets:
        return datasets
    return run_pipeline(output_dir=OUTPUT_DIR, mode="demo")


def load_dashboard_state() -> tuple[dict[str, pd.DataFrame], dict]:
    datasets = ensure_datasets()
    metadata = load_run_metadata(OUTPUT_DIR)
    if not metadata:
        metadata = {
            "mode": "unknown",
            "notes": "No run metadata found.",
        }
    return datasets, metadata


def render_snapshot_banner(metadata: dict) -> str:
    mode = str(metadata.get("mode", "unknown")).lower()
    generated_at = metadata.get("generated_at", "unknown")
    source_count = metadata.get("source_count", "unknown")
    providers = metadata.get("providers", {})
    pages = providers.get("pages", 0)
    rss_feeds = providers.get("rss_feeds", 0)
    static_sources = providers.get("static_sources", 0)

    if mode == "demo":
        headline = "Current Snapshot: Demo Data"
        body = (
            "This workspace is currently presenting a synthetic walkthrough dataset. "
            "Switch to live mode to surface real official and public-source records."
        )
    elif mode == "live":
        headline = "Current Snapshot: Live Data"
        body = (
            "This dashboard is reading the latest live-ingested files from the configured official pages, "
            "judgment gateways, and news feeds."
        )
    else:
        headline = "Current Snapshot: Unknown Mode"
        body = "Processed files were found, but the run metadata could not confirm whether they came from demo or live mode."

    return f"""
    <div class="hero-card">
      <div class="hero-topline">Hybrid Legal Intelligence Workspace</div>
      <h1>West Bengal Corporate Fraud Intelligence Console</h1>
      <p class="hero-copy">
        A source-aware legal dashboard for structured fraud signals, adjudicatory references,
        repeat-entity analysis, and explainable risk prioritisation.
      </p>
      <div class="section-head" style="margin: 16px 0 0;">
        <h2>{escape(headline)}</h2>
        <p>{escape(body)}</p>
      </div>
      <div class="hero-meta">
        <div class="hero-pill">Generated: {escape(str(generated_at))}</div>
        <div class="hero-pill">Source Rows: {escape(str(source_count))}</div>
        <div class="hero-pill">Page Sources: {escape(str(pages))}</div>
        <div class="hero-pill">RSS/Search Feeds: {escape(str(rss_feeds))}</div>
        <div class="hero-pill">Official Documents: {escape(str(static_sources))}</div>
      </div>
    </div>
    """


def _kpi_values(datasets: dict[str, pd.DataFrame]) -> dict[str, int]:
    master = datasets.get("master_dataset", pd.DataFrame())
    risks = datasets.get("risk_scores", pd.DataFrame())
    sources = datasets.get("source_log", pd.DataFrame())

    usable_records = int(master["usable_for_analytics"].fillna(False).sum()) if not master.empty else 0
    repeat_entities = (
        int((risks["mention_count"].fillna(0).astype(int) > 1).sum()) if not risks.empty else 0
    )
    high_risk = int((risks["risk_band"].fillna("") == "High").sum()) if not risks.empty else 0
    sections = set()
    if not master.empty and "legal_sections" in master.columns:
        for value in master["legal_sections"].fillna(""):
            sections.update(item.strip() for item in str(value).split("|") if item.strip())

    official_sources = 0
    if not sources.empty and "reliability_label" in sources.columns:
        official_sources = int(
            sources["reliability_label"].fillna("").astype(str).isin(["official", "regulated"]).sum()
        )

    return {
        "Sources Tracked": len(sources),
        "Official / Regulated Rows": official_sources,
        "Usable Records": usable_records,
        "Repeat Entities": repeat_entities,
        "High-Risk Entities": high_risk,
        "Distinct Legal Sections": len(sections),
    }


def render_kpis(datasets: dict[str, pd.DataFrame]) -> str:
    cards = []
    for label, value in _kpi_values(datasets).items():
        cards.append(
            f"""
            <div class="kpi-card">
              <span>{escape(label)}</span>
              <strong>{escape(str(value))}</strong>
            </div>
            """
        )
    return f'<div class="kpi-grid">{"".join(cards)}</div>'


def render_section_header(title: str, description: str) -> str:
    return (
        f'<div class="section-head"><h2>{escape(title)}</h2>'
        f"<p>{escape(description)}</p></div>"
    )


def _apply_chart_style(fig: go.Figure, title: str) -> go.Figure:
    fig.update_layout(
        title=dict(text=title, font=dict(family="Times New Roman", size=22, color="#0d3470")),
        paper_bgcolor="rgba(0, 0, 0, 0)",
        plot_bgcolor="#ffffff",
        font=dict(family="Times New Roman", color="#173965", size=14),
        legend=dict(
            bgcolor="rgba(255,255,255,0.88)",
            bordercolor="#c9daf7",
            borderwidth=1,
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
        margin=dict(l=24, r=24, t=74, b=24),
    )
    fig.update_xaxes(showgrid=False, linecolor="#9ebdf0", tickfont=dict(family="Times New Roman"))
    fig.update_yaxes(gridcolor="#d7e7ff", zeroline=False, tickfont=dict(family="Times New Roman"))
    return fig


def risk_chart(risks: pd.DataFrame) -> go.Figure:
    if risks.empty:
        return go.Figure()

    companies = risks.loc[risks["entity_type"] == "company"].copy()
    companies = companies.sort_values("network_risk_score", ascending=False).head(10)
    if companies.empty:
        return go.Figure()

    fig = px.bar(
        companies,
        x="network_risk_score",
        y="entity_name",
        color="risk_band",
        orientation="h",
        category_orders={"risk_band": ["Low", "Medium", "High"]},
        color_discrete_map={"Low": "#b9d2ff", "Medium": "#6d98db", "High": "#1b4f9f"},
    )
    fig.update_traces(marker_line_color="#dce8ff", marker_line_width=1.1)
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    return _apply_chart_style(fig, "Top Company Risk Scores")


def reliability_chart(source_log: pd.DataFrame) -> go.Figure:
    if source_log.empty or "reliability_label" not in source_log.columns:
        return go.Figure()
    counts = source_log["reliability_label"].value_counts().reset_index()
    counts.columns = ["reliability_label", "count"]
    fig = px.pie(
        counts,
        names="reliability_label",
        values="count",
        color="reliability_label",
        color_discrete_sequence=["#123f8c", "#3e6fbe", "#79a2dd", "#d7e7ff"],
        hole=0.48,
    )
    fig.update_traces(textfont=dict(family="Times New Roman", color="#173965"))
    return _apply_chart_style(fig, "Source Reliability Mix")


def filter_records(datasets: dict[str, pd.DataFrame], district: str, risk_band: str) -> pd.DataFrame:
    master = datasets.get("master_dataset", pd.DataFrame()).copy()
    risks = datasets.get("risk_scores", pd.DataFrame()).copy()
    if master.empty:
        return master

    if not risks.empty and "entity_name" in risks.columns:
        risk_lookup = risks[["entity_name", "risk_band"]].rename(columns={"entity_name": "company_name"})
        master = master.merge(risk_lookup, on="company_name", how="left")

    if district != "All":
        master = master.loc[master["district"].fillna("") == district]
    if risk_band != "All" and "risk_band" in master.columns:
        master = master.loc[master["risk_band"].fillna("") == risk_band]

    preferred_columns = [
        "title",
        "company_name",
        "district",
        "legal_sections",
        "violation_type",
        "record_quality_score",
        "risk_band",
        "source_name",
        "url",
    ]
    available_columns = [column for column in preferred_columns if column in master.columns]
    return master[available_columns].reset_index(drop=True)


def build_dashboard() -> gr.Blocks:
    datasets, metadata = load_dashboard_state()
    master = datasets.get("master_dataset", pd.DataFrame())
    source_log = datasets.get("source_log", pd.DataFrame())
    risks = datasets.get("risk_scores", pd.DataFrame())
    clusters = datasets.get("cluster_summary", pd.DataFrame())

    districts = ["All"]
    if not master.empty and "district" in master.columns:
        districts.extend(sorted({value for value in master["district"].dropna().astype(str) if value}))

    risk_bands = ["All"]
    if not risks.empty and "risk_band" in risks.columns:
        risk_bands.extend(sorted({value for value in risks["risk_band"].dropna().astype(str) if value}))

    with gr.Blocks(title="Hybrid Legal Intelligence Dashboard", css=APP_CSS) as demo:
        with gr.Column(elem_classes=["app-shell"]):
            gr.HTML(render_snapshot_banner(metadata))
            gr.HTML(render_kpis(datasets))

            with gr.Tabs():
                with gr.Tab("Overview"):
                    gr.HTML(
                        render_section_header(
                            "Operational Overview",
                            "Track the highest-priority entities and the current mix of official versus broader-source coverage.",
                        )
                    )
                    with gr.Row():
                        with gr.Group(elem_classes=["rounded-panel"]):
                            gr.Plot(value=risk_chart(risks))
                        with gr.Group(elem_classes=["rounded-panel"]):
                            gr.Plot(value=reliability_chart(source_log))

                with gr.Tab("Records"):
                    gr.HTML(
                        render_section_header(
                            "Structured Records",
                            "Filter the processed intelligence layer by district and risk band to inspect the most useful records quickly.",
                        )
                    )
                    with gr.Group(elem_classes=["rounded-panel"]):
                        with gr.Row():
                            district_filter = gr.Dropdown(choices=districts, value="All", label="District")
                            risk_filter = gr.Dropdown(choices=risk_bands, value="All", label="Risk Band")
                        records_table = gr.Dataframe(
                            value=filter_records(datasets, "All", "All"),
                            interactive=False,
                            wrap=True,
                        )

                    def update_records(district: str, risk_band: str) -> pd.DataFrame:
                        return filter_records(datasets, district, risk_band)

                    district_filter.change(update_records, [district_filter, risk_filter], records_table)
                    risk_filter.change(update_records, [district_filter, risk_filter], records_table)

                with gr.Tab("Risk"):
                    gr.HTML(
                        render_section_header(
                            "Risk Prioritisation",
                            "Review the explainable risk roll-up for repeated entities, source diversity, and legal-section spread.",
                        )
                    )
                    with gr.Group(elem_classes=["rounded-panel"]):
                        gr.Dataframe(
                            value=risks.sort_values("network_risk_score", ascending=False).reset_index(drop=True),
                            interactive=False,
                            wrap=True,
                        )

                with gr.Tab("Clusters"):
                    gr.HTML(
                        render_section_header(
                            "Cluster View",
                            "Inspect connected groups of entities, sections, districts, and sources to identify structural patterns.",
                        )
                    )
                    with gr.Group(elem_classes=["rounded-panel"]):
                        gr.Dataframe(value=clusters.reset_index(drop=True), interactive=False, wrap=True)

                with gr.Tab("Sources"):
                    gr.HTML(
                        render_section_header(
                            "Source Register",
                            "Audit the origin of each item across official portals, courts, adjudicatory gateways, and broader search coverage.",
                        )
                    )
                    with gr.Group(elem_classes=["rounded-panel"]):
                        gr.Dataframe(value=source_log.reset_index(drop=True), interactive=False, wrap=True)

                with gr.Tab("Ask"):
                    gr.HTML(
                        render_section_header(
                            "Research Assistant",
                            "Ask focused follow-up questions about companies, legal sections, districts, sources, or risk signals and review citations immediately.",
                        )
                    )
                    with gr.Group(elem_classes=["rounded-panel"]):
                        question = gr.Textbox(
                            label="Follow-Up Question",
                            placeholder="Ask about a company, section, source, district, or risk signal",
                        )
                        ask_button = gr.Button("Ask", variant="primary")
                        answer = gr.Markdown()
                        citations = gr.Dataframe(
                            headers=["title", "source_name", "url"],
                            interactive=False,
                            wrap=True,
                        )

                    def handle_question(user_question: str) -> tuple[str, pd.DataFrame]:
                        return answer_question(user_question, datasets)

                    question.submit(handle_question, question, [answer, citations])
                    ask_button.click(handle_question, question, [answer, citations])

    return demo


def candidate_server_ports() -> list[int]:
    configured = os.environ.get("GRADIO_SERVER_PORT")
    if configured:
        return [int(configured)]

    candidates = []
    for port in range(7860, 8060):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            if sock.connect_ex(("127.0.0.1", port)) != 0:
                candidates.append(port)

    for port in range(8860, 8960):
        if port not in candidates:
            candidates.append(port)

    return candidates


def main() -> None:
    app = build_dashboard()
    last_error = None
    for port in candidate_server_ports():
        try:
            app.launch(server_port=port)
            return
        except OSError as exc:
            if "Cannot find empty port" not in str(exc):
                raise
            last_error = exc

    raise SystemExit(
        "Gradio could not find an open port. Set GRADIO_SERVER_PORT to a known free port and try again."
    ) from last_error


if __name__ == "__main__":
    main()
