from __future__ import annotations

from html import escape

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from hybrid_legal_dashboard.config import OUTPUT_DIR
from hybrid_legal_dashboard.pipeline import run_pipeline
from hybrid_legal_dashboard.services.chat import answer_question_detailed
from hybrid_legal_dashboard.services.storage import load_outputs, load_run_metadata


BLUE_SCALE = ["#e8f1ff", "#c5dafc", "#8cb2ef", "#4b7dc6", "#123f8c"]
ASSISTANT_QUICK_PROMPTS = [
    "How many company reports are in the snapshot?",
    "Which companies have the highest risk?",
    "Which legal sections appear most often?",
    "Show recent official records",
    "Summarize SREI",
    "Which liquidation-related companies should I review?",
]


def ensure_datasets() -> dict[str, pd.DataFrame]:
    datasets = load_outputs(OUTPUT_DIR)
    if datasets:
        return datasets
    return run_pipeline(output_dir=OUTPUT_DIR, mode="demo")


def load_dashboard_state() -> tuple[dict[str, pd.DataFrame], dict]:
    datasets = ensure_datasets()
    metadata = load_run_metadata(OUTPUT_DIR)
    if not metadata:
        metadata = {"mode": "unknown", "notes": "No run metadata found."}
    return datasets, metadata


def apply_page_style() -> None:
    st.set_page_config(
        page_title="Hybrid Legal Intelligence Workspace",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(
        """
        <style>
        html, body, [class*="css"]  {
          font-family: Georgia, Cambria, "Times New Roman", Times, serif !important;
        }
        .stApp {
          background: linear-gradient(180deg, #eef5ff 0%, #f9fbff 42%, #f1f6ff 100%);
          color: #173965;
        }
        .block-container {
          padding-top: 1.4rem;
          padding-bottom: 2rem;
        }
        .hero-card {
          background: linear-gradient(135deg, rgba(255,255,255,0.98) 0%, rgba(230,241,255,0.96) 100%);
          border: 1px solid #c9daf7;
          border-radius: 28px;
          padding: 1.6rem 1.8rem;
          box-shadow: 0 18px 40px rgba(21, 74, 150, 0.12);
          margin-bottom: 1.1rem;
        }
        .section-card {
          background: rgba(255,255,255,0.96);
          border: 1px solid #c9daf7;
          border-radius: 24px;
          padding: 1rem 1.2rem;
          box-shadow: 0 16px 30px rgba(21, 74, 150, 0.08);
        }
        .metric-chip {
          background: #ffffff;
          border: 1px solid #c9daf7;
          border-radius: 999px;
          padding: 0.35rem 0.8rem;
          display: inline-block;
          margin-right: 0.5rem;
          color: #123f8c;
        }
        .dossier-note {
          border-left: 4px solid #1b4f9f;
          background: rgba(237,244,255,0.85);
          border-radius: 18px;
          padding: 0.9rem 1rem;
          margin-bottom: 0.9rem;
        }
        .kpi-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
          gap: 1rem;
          margin: 0.8rem 0 1.4rem;
        }
        .kpi-card {
          background: linear-gradient(180deg, #ffffff 0%, #f4f8ff 100%);
          border: 1px solid #c9daf7;
          border-radius: 24px;
          padding: 1rem 1.1rem;
          box-shadow: 0 14px 28px rgba(21, 74, 150, 0.08);
          min-height: 132px;
          display: flex;
          flex-direction: column;
          justify-content: space-between;
        }
        .kpi-label {
          display: block;
          color: #4f678b !important;
          font-size: 0.9rem !important;
          letter-spacing: 0.02em;
          text-transform: uppercase;
          margin-bottom: 0.45rem;
          font-weight: 700;
        }
        .kpi-value {
          display: block;
          color: #123f8c !important;
          font-size: 2.15rem !important;
          font-weight: 700;
          line-height: 1.05;
        }
        div[data-testid="stDataFrame"] {
          border-radius: 22px;
          overflow: hidden;
        }
        div[data-testid="stDataFrame"] * {
          color: #173965 !important;
        }
        section[data-testid="stSidebar"] {
          background: linear-gradient(180deg, #edf4ff 0%, #f7fbff 100%);
          border-right: 1px solid #d5e3fb;
        }
        section[data-testid="stSidebar"] * {
          color: #173965 !important;
        }
        button[role="tab"] {
          background: #edf4ff !important;
          color: #1e4f92 !important;
          border: 1px solid #cddcf7 !important;
          border-radius: 999px !important;
          padding: 0.55rem 1rem !important;
          margin-right: 0.45rem !important;
        }
        button[role="tab"][aria-selected="true"] {
          background: linear-gradient(135deg, #1f5fbf 0%, #123f8c 100%) !important;
          color: #ffffff !important;
          border-color: #123f8c !important;
        }
        [data-baseweb="tab-highlight"] {
          background: transparent !important;
        }
        .stTabs [data-baseweb="tab-list"] {
          gap: 0.35rem;
          padding: 0.2rem 0 0.7rem;
        }
        [data-baseweb="select"] > div,
        [data-baseweb="base-input"] > div,
        .stDateInput > div > div,
        .stTextInput > div > div,
        .stNumberInput > div > div {
          background: #ffffff !important;
          border: 1px solid #c9daf7 !important;
          border-radius: 18px !important;
          color: #173965 !important;
          box-shadow: 0 8px 18px rgba(21, 74, 150, 0.06);
        }
        .stSelectbox [data-baseweb="select"] * {
          color: #173965 !important;
        }
        .stSelectbox svg {
          fill: #173965 !important;
        }
        .stSelectbox label,
        .stTextInput label,
        .stMultiSelect label,
        .stToggle label {
          color: #1f4574 !important;
          font-weight: 600 !important;
        }
        .stDownloadButton button,
        .stButton button {
          background: linear-gradient(135deg, #1f5fbf 0%, #123f8c 100%) !important;
          color: #ffffff !important;
          border: 1px solid #123f8c !important;
          border-radius: 16px !important;
        }
        .stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown h4 {
          color: #173965 !important;
        }
        .insight-panel {
          background: rgba(255,255,255,0.95);
          border: 1px solid #c9daf7;
          border-radius: 22px;
          padding: 1rem 1.05rem;
          box-shadow: 0 14px 28px rgba(21, 74, 150, 0.07);
          margin-bottom: 1rem;
        }
        .panel-label {
          color: #5a7296;
          font-size: 0.85rem;
          font-weight: 700;
          letter-spacing: 0.04em;
          text-transform: uppercase;
          margin-bottom: 0.55rem;
        }
        .chip-row {
          display: flex;
          flex-wrap: wrap;
          gap: 0.45rem;
        }
        .chip {
          background: #edf4ff;
          color: #17498c;
          border: 1px solid #cddcf7;
          border-radius: 999px;
          padding: 0.35rem 0.75rem;
          font-size: 0.92rem;
          font-weight: 600;
        }
        .readable-table-wrap {
          background: rgba(255,255,255,0.96);
          border: 1px solid #c9daf7;
          border-radius: 22px;
          overflow: auto;
          box-shadow: 0 16px 30px rgba(21, 74, 150, 0.08);
        }
        .readable-table {
          width: 100%;
          border-collapse: separate;
          border-spacing: 0;
          min-width: 720px;
        }
        .readable-table thead th {
          position: sticky;
          top: 0;
          z-index: 1;
          background: linear-gradient(180deg, #edf4ff 0%, #dfebff 100%);
          color: #123f8c;
          text-align: left;
          font-size: 0.9rem;
          font-weight: 700;
          padding: 0.9rem 0.95rem;
          border-bottom: 1px solid #c9daf7;
        }
        .readable-table tbody td {
          color: #173965;
          padding: 0.82rem 0.95rem;
          border-bottom: 1px solid #e3edff;
          vertical-align: top;
          font-size: 0.97rem;
          line-height: 1.45;
          background: rgba(255,255,255,0.95);
        }
        .readable-table tbody tr:nth-child(even) td {
          background: #f8fbff;
        }
        .readable-table tbody tr:hover td {
          background: #eef5ff;
        }
        .table-link {
          color: #145cc3 !important;
          font-weight: 700;
          text-decoration: none;
        }
        .table-link:hover {
          text-decoration: underline;
        }
        .muted-text {
          color: #5f7393;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def split_pipe_values(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    return [item.strip() for item in str(value).split("|") if item.strip()]


def render_stat_cards(cards: list[tuple[str, object]]) -> None:
    rendered = []
    for label, value in cards:
        rendered.append(
            f"""
            <div class="kpi-card">
              <span class="kpi-label">{escape(str(label))}</span>
              <span class="kpi-value">{escape(str(value))}</span>
            </div>
            """
        )
    st.markdown(f'<div class="kpi-grid">{"".join(rendered)}</div>', unsafe_allow_html=True)


def _clean_scalar(value: object, blank: str = "Not available") -> str:
    if value is None:
        return blank
    if isinstance(value, float) and pd.isna(value):
        return blank
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return blank
    return text


def _friendly_timestamp(value: object) -> str:
    cleaned = _clean_scalar(value, blank="")
    if not cleaned:
        return "Not available"
    parsed = pd.to_datetime(cleaned, errors="coerce", utc=True)
    if pd.isna(parsed):
        return cleaned
    return parsed.tz_convert(None).strftime("%d %b %Y %H:%M")


def _friendly_date(value: object) -> str:
    cleaned = _clean_scalar(value, blank="")
    if not cleaned:
        return ""
    parsed = pd.to_datetime(cleaned, errors="coerce", utc=True)
    if pd.isna(parsed):
        return cleaned
    return parsed.tz_convert(None).strftime("%d %b %Y")


def _compact_text(value: object, limit: int = 108) -> str:
    text = _clean_scalar(value, blank="")
    if not text:
        return "Not available"
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1].rstrip()}…"


def _friendly_section(value: object) -> str:
    cleaned = _clean_scalar(value, blank="")
    if cleaned.endswith(".0") and cleaned[:-2].isdigit():
        return cleaned[:-2]
    return cleaned


def render_chip_group(label: str, values: list[str], blank: str = "Not available") -> None:
    cleaned = [_clean_scalar(value, blank="") for value in values if _clean_scalar(value, blank="")]
    if not cleaned:
        cleaned = [blank]
    chips = "".join(f'<span class="chip">{escape(value)}</span>' for value in cleaned)
    st.markdown(
        f"""
        <div class="insight-panel">
          <div class="panel-label">{escape(label)}</div>
          <div class="chip-row">{chips}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_html_table(
    df: pd.DataFrame,
    *,
    link_columns: set[str] | None = None,
    height: int = 380,
    max_rows: int | None = None,
) -> None:
    if df.empty:
        st.info("No rows are available for this view yet.")
        return

    link_columns = link_columns or set()
    view = df.copy() if max_rows is None else df.head(max_rows).copy()
    header_html = "".join(f"<th>{escape(str(column))}</th>" for column in view.columns)
    row_html_parts: list[str] = []

    for _, row in view.iterrows():
        cell_html = []
        for column in view.columns:
            value = row[column]
            cleaned = _clean_scalar(value, blank="Not available")
            if column in link_columns and cleaned != "Not available":
                display_label = "Open source" if "URL" in column or "Reference" in column else cleaned
                body = (
                    f'<a class="table-link" href="{escape(cleaned)}" target="_blank">'
                    f"{escape(_compact_text(display_label, limit=34))}</a>"
                )
            else:
                title = escape(cleaned)
                body = f'<span title="{title}">{escape(_compact_text(cleaned))}</span>'
            cell_html.append(f"<td>{body}</td>")
        row_html_parts.append(f"<tr>{''.join(cell_html)}</tr>")

    st.markdown(
        f"""
        <div class="readable-table-wrap" style="max-height:{height}px;">
          <table class="readable-table">
            <thead><tr>{header_html}</tr></thead>
            <tbody>{''.join(row_html_parts)}</tbody>
          </table>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if max_rows is not None and len(df) > max_rows:
        st.caption(f"Showing the first {max_rows} rows out of {len(df)} for readability.")


def _display_company_reports_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    view = df.copy()
    view["cin"] = view["cin"].apply(lambda value: _clean_scalar(value, blank="Not extracted"))
    view["action_types"] = view["action_types"].apply(lambda value: _clean_scalar(value, blank="Other"))
    if "company_status" in view.columns:
        view["company_status"] = view["company_status"].apply(lambda value: _clean_scalar(value, blank="Not available"))
    if "incorporation_date" in view.columns:
        view["incorporation_date"] = view["incorporation_date"].apply(lambda value: _friendly_date(value) or "Not dated")
    if "last_balance_sheet_date" in view.columns:
        view["last_balance_sheet_date"] = view["last_balance_sheet_date"].apply(lambda value: _friendly_date(value) or "Not dated")
    if "dominant_legal_section" in view.columns:
        view["dominant_legal_section"] = view["dominant_legal_section"].apply(lambda value: _friendly_section(value) or "Not mapped")
    view["latest_published_at"] = view["latest_published_at"].apply(lambda value: _friendly_date(value) or "Not dated")
    view = view.rename(
        columns={
            "company_name": "Company",
            "cin": "CIN",
            "action_types": "Action Type",
            "dominant_legal_section": "Major Section",
            "company_status": "Status",
            "incorporation_date": "Incorporated",
            "last_balance_sheet_date": "Last Balance Sheet",
            "official_source_count": "Official Sources",
            "record_count": "Records",
            "latest_published_at": "Latest Publication",
        }
    )
    columns = [
        column
        for column in [
            "Company",
            "CIN",
            "Major Section",
            "Status",
            "Action Type",
            "Incorporated",
            "Last Balance Sheet",
            "Official Sources",
            "Records",
            "Latest Publication",
        ]
        if column in view.columns
    ]
    return view[columns]


def _display_supporting_records(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    view = df.copy()
    if "legal_sections" in view.columns:
        view["legal_sections"] = view["legal_sections"].apply(lambda value: _clean_scalar(value, blank="Not mapped"))
    if "published_at" in view.columns:
        view["published_at"] = view["published_at"].apply(lambda value: _friendly_date(value) or "Not dated")
    view = view.rename(
        columns={
            "title": "Title",
            "source_name": "Source",
            "violation_type": "Action",
            "legal_sections": "Legal Sections",
            "published_at": "Published",
            "url": "URL",
        }
    )
    columns = [column for column in ["Title", "Source", "Action", "Legal Sections", "Published", "URL"] if column in view.columns]
    return view[columns].fillna("")


def _display_assistant_citations(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    view = df.copy()
    for column in ["published_at", "fetched_at"]:
        if column in view.columns:
            view[column] = view[column].apply(lambda value: _friendly_date(value) or "Not dated")
    view = view.rename(
        columns={
            "title": "Record",
            "source_name": "Source",
            "published_at": "Published",
            "fetched_at": "Fetched",
            "url": "URL",
        }
    )
    columns = [column for column in ["Record", "Source", "Published", "Fetched", "URL"] if column in view.columns]
    return view[columns].fillna("")


def _display_source_register(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    view = df.copy()
    for column in ["published_at", "fetched_at"]:
        if column in view.columns:
            view[column] = view[column].apply(lambda value: _friendly_date(value) or _clean_scalar(value, blank="Not dated"))
    view = view.rename(
        columns={
            "title": "Title",
            "source_name": "Source",
            "reliability_label": "Reliability",
            "published_at": "Published",
            "fetched_at": "Fetched",
            "url": "URL",
        }
    )
    columns = [column for column in ["Title", "Source", "Reliability", "Published", "Fetched", "URL"] if column in view.columns]
    return view[columns].fillna("")


def _display_master_records(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    view = df.copy()
    for column in ["published_at", "fetched_at"]:
        if column in view.columns:
            view[column] = view[column].apply(lambda value: _friendly_date(value) or _clean_scalar(value, blank="Not dated"))
    for column in ["company_name", "district", "legal_sections", "violation_type"]:
        if column in view.columns:
            view[column] = view[column].apply(lambda value: _clean_scalar(value, blank=""))
    view = view.rename(
        columns={
            "title": "Title",
            "company_name": "Company",
            "district": "District",
            "legal_sections": "Legal Sections",
            "violation_type": "Action",
            "source_name": "Source",
            "published_at": "Published",
            "url": "URL",
        }
    )
    columns = [column for column in ["Title", "Company", "District", "Legal Sections", "Action", "Source", "Published", "URL"] if column in view.columns]
    return view[columns].fillna("")


def render_hero(metadata: dict, company_reports: pd.DataFrame) -> None:
    mode = str(metadata.get("mode", "unknown")).lower()
    source_count = metadata.get("source_count", 0)
    company_count = len(company_reports)
    official_company_count = (
        int((company_reports.get("official_source_count", pd.Series(dtype=int)).fillna(0).astype(int) > 0).sum())
        if not company_reports.empty
        else 0
    )

    status = "Live Data" if mode == "live" else "Demo Data" if mode == "demo" else "Unknown Mode"
    st.markdown(
        f"""
        <div class="hero-card">
          <div class="metric-chip">Hybrid Legal Intelligence Workspace</div>
          <div class="metric-chip">Snapshot: {escape(status)}</div>
          <h1 style="margin:0.55rem 0 0.5rem; color:#0d3470;">West Bengal Corporate Fraud and Regulatory Intelligence Console</h1>
          <p style="font-size:1.06rem; line-height:1.6; margin:0 0 0.85rem;">
            A more interactive analyst workspace for official company actions, adjudicatory references,
            source-backed evidence review, and explainable company-level evaluation.
          </p>
          <div class="metric-chip">Source Rows: {source_count}</div>
          <div class="metric-chip">Company Reports: {company_count}</div>
          <div class="metric-chip">Official Company Reports: {official_company_count}</div>
          <div class="metric-chip">Generated: {escape(_friendly_timestamp(metadata.get("generated_at", "unknown")))}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def prepare_company_reports(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    prepared = df.copy()
    prepared["action_types_list"] = prepared["action_types"].apply(split_pipe_values)
    prepared["legal_sections_list"] = prepared["legal_sections"].apply(split_pipe_values)
    prepared["districts_list"] = prepared["districts"].apply(split_pipe_values)
    prepared["source_names_list"] = prepared["source_names"].apply(split_pipe_values)
    prepared["profile_sources_list"] = prepared.get("profile_sources", pd.Series(dtype=object)).apply(split_pipe_values)
    prepared["official_source_count"] = prepared["official_source_count"].fillna(0).astype(int)
    prepared["record_count"] = prepared["record_count"].fillna(0).astype(int)
    completeness_columns = [
        "cin",
        "company_status",
        "incorporation_date",
        "last_agm_date",
        "last_balance_sheet_date",
        "registered_address",
        "authorized_capital",
        "paid_up_capital",
        "roc_office",
    ]
    prepared["profile_completeness_score"] = 0
    for column in completeness_columns:
        if column in prepared.columns:
            prepared["profile_completeness_score"] += (
                prepared[column].fillna("").astype(str).str.strip().ne("").astype(int)
            )
    prepared["has_profile"] = prepared["profile_sources_list"].apply(bool).astype(int)
    prepared = prepared.sort_values(
        by=["has_profile", "profile_completeness_score", "official_source_count", "record_count", "company_name"],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    return prepared


def prepare_master(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    prepared = df.copy()
    for column in ["published_at", "fetched_at"]:
        if column in prepared.columns:
            prepared[f"{column}_parsed"] = pd.to_datetime(prepared[column], errors="coerce", utc=True)
    return prepared


def action_type_chart(company_reports: pd.DataFrame) -> go.Figure:
    if company_reports.empty:
        return go.Figure()
    exploded = company_reports.explode("action_types_list")
    exploded["action_types_list"] = exploded["action_types_list"].replace("", pd.NA)
    exploded = exploded.dropna(subset=["action_types_list"])
    if exploded.empty:
        return go.Figure()
    counts = exploded["action_types_list"].value_counts().reset_index()
    counts.columns = ["action_type", "count"]
    fig = px.bar(
        counts,
        x="count",
        y="action_type",
        orientation="h",
        color="count",
        color_continuous_scale=BLUE_SCALE,
    )
    fig.update_layout(
        title="Action-Type Distribution",
        font=dict(family="Times New Roman", color="#173965"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#ffffff",
        margin=dict(l=20, r=20, t=60, b=20),
        coloraxis_showscale=False,
        yaxis_title="",
        xaxis_title="Company reports",
    )
    return fig


def source_mix_chart(source_log: pd.DataFrame) -> go.Figure:
    if source_log.empty:
        return go.Figure()
    counts = (
        source_log.groupby(["source_name", "reliability_label"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(14)
    )
    fig = px.bar(
        counts,
        x="count",
        y="source_name",
        color="reliability_label",
        orientation="h",
        color_discrete_map={
            "official": "#123f8c",
            "regulated": "#3f71be",
            "registry_aggregator": "#6f96d8",
            "reputed_media": "#7fa9e6",
            "open_web": "#d5e4fb",
        },
    )
    fig.update_layout(
        title="Top Sources by Volume",
        barmode="stack",
        font=dict(family="Times New Roman", color="#173965"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#ffffff",
        margin=dict(l=20, r=20, t=60, b=20),
        yaxis_title="",
        xaxis_title="Rows logged",
        legend_title_text="Reliability",
    )
    return fig


def timeline_chart(source_log: pd.DataFrame) -> go.Figure:
    if source_log.empty:
        return go.Figure()
    history_frames = [source_log.copy()]
    for path in sorted(OUTPUT_DIR.glob("source_log_*.csv")):
        if path.name == "source_log_latest.csv":
            continue
        try:
            history_frames.append(pd.read_csv(path))
        except Exception:
            continue
    prepared = pd.concat(history_frames, ignore_index=True, sort=False)
    dedupe_columns = [column for column in ["title", "url", "source_name", "published_at", "fetched_at"] if column in prepared.columns]
    if dedupe_columns:
        prepared = prepared.drop_duplicates(subset=dedupe_columns)
    published = pd.to_datetime(prepared.get("published_at"), errors="coerce", utc=True)
    fetched = pd.to_datetime(prepared.get("fetched_at"), errors="coerce", utc=True)
    published = published.dropna()
    fetched = fetched.dropna()
    if published.empty and fetched.empty:
        return go.Figure()

    all_dates = pd.concat([published, fetched], ignore_index=True)
    span_days = max(1, int((all_dates.max() - all_dates.min()).days))
    bucket = "M" if span_days > 365 else "D"

    timeline_frames: list[pd.DataFrame] = []
    for label, series in [("Published", published), ("Fetched", fetched)]:
        if series.empty:
            continue
        series = series.dt.tz_localize(None)
        counts = (
            series.dt.to_period(bucket)
            .astype(str)
            .value_counts()
            .sort_index()
            .rename_axis("period")
            .reset_index(name="count")
        )
        counts["timeline_type"] = label
        timeline_frames.append(counts)
    if not timeline_frames:
        return go.Figure()

    counts = pd.concat(timeline_frames, ignore_index=True)
    fig = px.line(
        counts,
        x="period",
        y="count",
        color="timeline_type",
        markers=True,
        color_discrete_map={"Published": "#123f8c", "Fetched": "#7fa9e6"},
    )
    fig.update_layout(
        title="Published vs Fetched Timeline",
        font=dict(family="Times New Roman", color="#173965"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#ffffff",
        margin=dict(l=20, r=20, t=60, b=20),
        xaxis_title="Month" if bucket == "M" else "Date",
        yaxis_title="Rows",
        legend_title_text="Event Type",
    )
    fig.update_traces(line=dict(width=3))
    return fig


def company_treemap(company_reports: pd.DataFrame) -> go.Figure:
    if company_reports.empty:
        return go.Figure()
    exploded = company_reports.explode("action_types_list").copy()
    exploded["action_types_list"] = exploded["action_types_list"].apply(
        lambda value: value if value else "other"
    )
    top = exploded.sort_values(["official_source_count", "record_count"], ascending=False).head(50)
    fig = px.treemap(
        top,
        path=["action_types_list", "company_name"],
        values="record_count",
        color="official_source_count",
        color_continuous_scale=BLUE_SCALE,
    )
    fig.update_layout(
        title="Company Action Treemap",
        font=dict(family="Times New Roman", color="#173965"),
        margin=dict(l=10, r=10, t=60, b=10),
    )
    return fig


def risk_scatter(risks: pd.DataFrame) -> go.Figure:
    if risks.empty:
        return go.Figure()
    filtered = risks.loc[risks["entity_type"].fillna("") == "company"].copy()
    if filtered.empty:
        return go.Figure()
    filtered["mention_count"] = filtered["mention_count"].fillna(0).astype(int)
    filtered["distinct_sources"] = filtered["distinct_sources"].fillna(0).astype(int)
    filtered["network_risk_score"] = filtered["network_risk_score"].fillna(0.0).astype(float)
    fig = px.scatter(
        filtered,
        x="distinct_sources",
        y="network_risk_score",
        size="mention_count",
        color="risk_band",
        hover_name="entity_name",
        color_discrete_map={"Low": "#bfd6fb", "Medium": "#5d8cd3", "High": "#123f8c"},
        size_max=32,
    )
    fig.update_layout(
        title="Risk vs Source Diversity",
        font=dict(family="Times New Roman", color="#173965"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#ffffff",
        margin=dict(l=20, r=20, t=60, b=20),
        xaxis_title="Distinct sources",
        yaxis_title="Risk score",
        legend_title_text="Risk band",
    )
    return fig


def company_source_breakdown(mentions: pd.DataFrame) -> go.Figure:
    if mentions.empty:
        return go.Figure()
    counts = (
        mentions.groupby("source_reliability_label", dropna=False)
        .size()
        .reset_index(name="count")
        .rename(columns={"source_reliability_label": "reliability"})
    )
    counts["reliability"] = counts["reliability"].fillna("unknown")
    fig = px.pie(
        counts,
        names="reliability",
        values="count",
        hole=0.55,
        color="reliability",
        color_discrete_map={
            "official": "#123f8c",
            "regulated": "#4b7dc6",
            "registry_aggregator": "#6f96d8",
            "reputed_media": "#8cb2ef",
            "open_web": "#dce9ff",
            "unknown": "#d0dbec",
        },
    )
    fig.update_layout(
        title="Source Reliability Mix",
        font=dict(family="Times New Roman", color="#173965"),
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=60, b=20),
        legend_title_text="Source type",
    )
    return fig


def company_mentions_timeline(mentions: pd.DataFrame) -> go.Figure:
    if mentions.empty:
        return go.Figure()
    timeline = mentions.copy()
    if "published_at_parsed" in timeline.columns:
        timeline["event_date"] = timeline["published_at_parsed"]
    else:
        timeline["event_date"] = pd.NaT
    if timeline["event_date"].isna().all() and "fetched_at_parsed" in timeline.columns:
        timeline["event_date"] = timeline["fetched_at_parsed"]
    timeline = timeline.dropna(subset=["event_date"])
    if timeline.empty:
        return go.Figure()
    grouped = (
        timeline.assign(event_day=timeline["event_date"].dt.date.astype(str))
        .groupby("event_day", dropna=False)
        .size()
        .reset_index(name="count")
    )
    fig = px.bar(
        grouped,
        x="event_day",
        y="count",
        color="count",
        color_continuous_scale=BLUE_SCALE,
    )
    fig.update_layout(
        title="Record Timeline",
        font=dict(family="Times New Roman", color="#173965"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#ffffff",
        margin=dict(l=20, r=20, t=60, b=20),
        coloraxis_showscale=False,
        xaxis_title="Date",
        yaxis_title="Records",
    )
    return fig


def filter_company_reports(
    company_reports: pd.DataFrame,
    selected_actions: list[str],
    official_only: bool,
    search_text: str,
) -> pd.DataFrame:
    filtered = company_reports.copy()
    if official_only:
        filtered = filtered.loc[filtered["official_source_count"].fillna(0).astype(int) > 0]
    if selected_actions:
        filtered = filtered.loc[
            filtered["action_types_list"].apply(lambda values: bool(set(values) & set(selected_actions)))
        ]
    if search_text.strip():
        needle = search_text.strip().lower()
        filtered = filtered.loc[
            filtered["company_name"].fillna("").astype(str).str.lower().str.contains(needle, regex=False)
            | filtered["cin"].fillna("").astype(str).str.lower().str.contains(needle, regex=False)
        ]
    return filtered.sort_values(["official_source_count", "record_count", "company_name"], ascending=[False, False, True])


def company_detail_panel(selected_company: str, company_reports: pd.DataFrame, master: pd.DataFrame) -> None:
    if not selected_company or company_reports.empty:
        st.info("Choose a company from the filtered list to inspect its dossier.")
        return

    row = company_reports.loc[company_reports["company_name"] == selected_company]
    if row.empty:
        st.info("The selected company is not available in the current filtered view.")
        return
    row = row.iloc[0]

    st.markdown(
        f"""
        <div class="dossier-note">
          <strong>{escape(str(row['company_name']))}</strong><br/>
          {escape(str(row.get('dossier_summary', 'No dossier summary available.')))}
        </div>
        """,
        unsafe_allow_html=True,
    )

    dominant_section = _friendly_section(row.get("dominant_legal_section"))
    dominant_act = _clean_scalar(row.get("dominant_legal_act"), blank="")
    dominant_meaning = _clean_scalar(row.get("dominant_legal_meaning"), blank="")
    if dominant_section:
        statutory_note = f"{dominant_act + ' - ' if dominant_act else ''}Section {dominant_section}"
        if dominant_meaning:
            statutory_note += f" ({dominant_meaning})"
        st.markdown(
            f"""
            <div class="insight-panel">
              <div class="panel-label">Major Statutory Signal</div>
              <div><strong>{escape(statutory_note)}</strong></div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    render_stat_cards(
        [
            ("CIN", _clean_scalar(row.get("cin"), blank="Not extracted")),
            ("Records", int(row.get("record_count", 0))),
            ("Sources", int(row.get("source_count", 0))),
            ("Official Sources", int(row.get("official_source_count", 0))),
        ]
    )

    mentions = master.loc[master["company_name"].fillna("") == selected_company].copy()
    if "published_at_parsed" in mentions.columns:
        mentions = mentions.sort_values(["published_at_parsed", "fetched_at_parsed"], ascending=False)

    detail_left, detail_right = st.columns([1, 1])
    with detail_left:
        render_chip_group("Action Types", split_pipe_values(row.get("action_types")))
        render_chip_group("Districts", split_pipe_values(row.get("districts")))
        render_chip_group("Industry", split_pipe_values(row.get("industry_descriptions")), blank="Not available")
    with detail_right:
        render_chip_group("Legal Sections", split_pipe_values(row.get("legal_sections")), blank="Not mapped")
        render_chip_group("Source Footprint", split_pipe_values(row.get("source_names")), blank="Not available")
        render_chip_group("Profile Sources", split_pipe_values(row.get("profile_sources")), blank="Not available")

    render_stat_cards(
        [
            ("Major Act", _clean_scalar(row.get("dominant_legal_act"), blank="Not mapped")),
            ("Major Section", f"Section {_clean_scalar(row.get('dominant_legal_section'), blank='Not mapped')}" if dominant_section else "Not mapped"),
            ("Section Meaning", _clean_scalar(row.get("dominant_legal_meaning"), blank="Not mapped")),
            ("Action Types", _clean_scalar(row.get("action_types"), blank="Other")),
        ]
    )

    render_stat_cards(
        [
            ("Status", _clean_scalar(row.get("company_status"), blank="Not available")),
            ("Incorporated", _friendly_date(row.get("incorporation_date")) or "Not dated"),
            ("Last AGM", _friendly_date(row.get("last_agm_date")) or "Not dated"),
            ("Last Balance Sheet", _friendly_date(row.get("last_balance_sheet_date")) or "Not dated"),
        ]
    )

    render_stat_cards(
        [
            ("Authorized Capital", _clean_scalar(row.get("authorized_capital"), blank="Not available")),
            ("Paid Up Capital", _clean_scalar(row.get("paid_up_capital"), blank="Not available")),
            ("Listed Status", _clean_scalar(row.get("listed_status"), blank="Not available")),
            ("ROC", _clean_scalar(row.get("roc_office"), blank="Not available")),
        ]
    )

    st.markdown(
        f"""
        <div class="insight-panel">
          <div class="panel-label">Registered Address</div>
          <div>{escape(_clean_scalar(row.get("registered_address"), blank="Not available"))}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    chart_left, chart_right = st.columns([1, 1])
    with chart_left:
        st.plotly_chart(company_source_breakdown(mentions), use_container_width=True)
    with chart_right:
        st.plotly_chart(company_mentions_timeline(mentions), use_container_width=True)

    st.markdown("#### Supporting Records")
    if mentions.empty:
        st.write("No supporting record rows are currently available for this company.")
    else:
        view_columns = [
            column
            for column in [
                "title",
                "source_name",
                "violation_type",
                "legal_sections",
                "published_at",
                "url",
            ]
            if column in mentions.columns
        ]
        render_html_table(
            _display_supporting_records(mentions[view_columns]),
            link_columns={"URL"},
            height=340,
            max_rows=40,
        )

    st.download_button(
        "Download Current Company Reports CSV",
        data=company_reports.to_csv(index=False).encode("utf-8"),
        file_name="company_reports_filtered.csv",
        mime="text/csv",
        use_container_width=True,
    )


def build_sidebar(company_reports: pd.DataFrame) -> tuple[list[str], bool, str]:
    st.sidebar.markdown("## Filters")
    all_actions = sorted(
        {
            action
            for values in company_reports.get("action_types_list", pd.Series(dtype=object))
            for action in (values if isinstance(values, list) else [])
        }
    )
    selected_actions = st.sidebar.multiselect("Action Types", options=all_actions)
    official_only = st.sidebar.toggle("Only official-backed company reports", value=True)
    search_text = st.sidebar.text_input("Search company or CIN")
    st.sidebar.caption("The Streamlit workspace is the recommended interface for deeper filtering and evidence review.")
    return selected_actions, official_only, search_text


def _assistant_history() -> list[dict]:
    if "assistant_history" not in st.session_state:
        st.session_state["assistant_history"] = [
            {
                "role": "assistant",
                "content": (
                    "Ask the workspace about a company, CIN, legal section, district, source, recent activity, "
                    "or risk trend. I answer only from the current processed snapshot and cite the supporting rows."
                ),
                "intent": "welcome",
                "citations": [],
                "matched_company": "",
                "stat_cards": [],
                "follow_ups": ASSISTANT_QUICK_PROMPTS[:4],
            }
        ]
    return st.session_state["assistant_history"]


def _append_assistant_exchange(prompt: str, datasets: dict[str, pd.DataFrame]) -> None:
    history = _assistant_history()
    history.append({"role": "user", "content": prompt})
    response = answer_question_detailed(prompt, datasets)
    history.append(
        {
            "role": "assistant",
            "content": response.answer_markdown,
            "intent": response.intent,
            "citations": response.citations.fillna("").to_dict("records"),
            "matched_company": response.matched_company,
            "stat_cards": [[label, value] for label, value in response.stat_cards],
            "follow_ups": response.follow_ups,
        }
    )


def _assistant_quick_prompt_buttons(prompts: list[str], key_prefix: str) -> str:
    selected = ""
    columns = st.columns(3)
    for idx, prompt in enumerate(prompts):
        if columns[idx % 3].button(prompt, key=f"{key_prefix}-{idx}", use_container_width=True):
            selected = prompt
    return selected


def render_assistant_tab(
    datasets: dict[str, pd.DataFrame],
    company_reports: pd.DataFrame,
    master: pd.DataFrame,
) -> None:
    st.markdown(
        """
        <div class="section-card">
          <strong>Navigation Assistant</strong><br/>
          Use the processed snapshot like a guided research workspace. Ask questions in plain language and review the
          linked source rows immediately.
        </div>
        """,
        unsafe_allow_html=True,
    )

    top_left, top_right = st.columns([1.2, 0.8])
    with top_left:
        prompt = _assistant_quick_prompt_buttons(ASSISTANT_QUICK_PROMPTS, "assistant-quick")
    with top_right:
        if st.button("Clear conversation", use_container_width=True):
            st.session_state.pop("assistant_history", None)
            st.rerun()

    history = _assistant_history()
    latest_assistant_message: dict | None = None
    for message in history:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if message["role"] != "assistant":
                continue
            latest_assistant_message = message
            stat_cards = [(label, value) for label, value in message.get("stat_cards", [])]
            if stat_cards:
                render_stat_cards(stat_cards)
            citations = pd.DataFrame(message.get("citations", []))
            if not citations.empty:
                st.markdown("##### Supporting Citations")
                render_html_table(
                    _display_assistant_citations(citations),
                    link_columns={"URL"},
                    height=240,
                    max_rows=8,
                )

    follow_up_prompt = ""
    if latest_assistant_message and latest_assistant_message.get("follow_ups"):
        st.markdown("#### Try Next")
        follow_up_prompt = _assistant_quick_prompt_buttons(
            latest_assistant_message.get("follow_ups", []),
            "assistant-follow-up",
        )

    matched_company = _clean_scalar((latest_assistant_message or {}).get("matched_company"), blank="")
    if matched_company and not company_reports.empty and matched_company in company_reports["company_name"].astype(str).tolist():
        action_left, action_right = st.columns([0.6, 1.4])
        with action_left:
            if st.button(f"Load {matched_company} in Company Explorer", use_container_width=True):
                st.session_state["company_selectbox"] = matched_company
                st.success(f"{matched_company} is ready in the Company Explorer tab.")
        with action_right:
            with st.expander(f"Preview dossier for {matched_company}", expanded=False):
                company_detail_panel(matched_company, company_reports, master)

    user_prompt = st.chat_input("Ask the workspace about a company, section, source, district, or risk trend")
    submitted_prompt = user_prompt or follow_up_prompt or prompt
    if submitted_prompt:
        _append_assistant_exchange(submitted_prompt, datasets)
        st.rerun()


def main() -> None:
    apply_page_style()
    datasets, metadata = load_dashboard_state()

    master = prepare_master(datasets.get("master_dataset", pd.DataFrame()))
    company_reports = prepare_company_reports(datasets.get("company_reports", pd.DataFrame()))
    risk_scores = datasets.get("risk_scores", pd.DataFrame())
    source_log = datasets.get("source_log", pd.DataFrame())
    clusters = datasets.get("cluster_summary", pd.DataFrame())

    render_hero(metadata, company_reports)
    selected_actions, official_only, search_text = build_sidebar(company_reports)
    filtered_company_reports = filter_company_reports(company_reports, selected_actions, official_only, search_text)

    total_company_reports = len(filtered_company_reports)
    official_company_reports = (
        int((filtered_company_reports["official_source_count"].fillna(0).astype(int) > 0).sum())
        if not filtered_company_reports.empty
        else 0
    )
    official_source_rows = (
        int(source_log["reliability_label"].fillna("").isin(["official", "regulated"]).sum())
        if not source_log.empty
        else 0
    )
    usable_records = int(master["usable_for_analytics"].fillna(False).sum()) if not master.empty else 0

    render_stat_cards(
        [
            ("Filtered Company Reports", total_company_reports),
            ("Official Company Reports", official_company_reports),
            ("Official / Regulated Source Rows", official_source_rows),
            ("Usable Records", usable_records),
        ]
    )

    executive_tab, assistant_tab, explorer_tab, network_tab, evidence_tab = st.tabs(
        ["Executive View", "Assistant", "Company Explorer", "Source Network", "Evidence Register"]
    )

    with executive_tab:
        top_left, top_right = st.columns([1, 1])
        with top_left:
            st.plotly_chart(action_type_chart(filtered_company_reports), use_container_width=True)
        with top_right:
            st.plotly_chart(risk_scatter(risk_scores), use_container_width=True)

        bottom_left, bottom_right = st.columns([1.05, 0.95])
        with bottom_left:
            st.plotly_chart(company_treemap(filtered_company_reports), use_container_width=True)
        with bottom_right:
            st.plotly_chart(timeline_chart(source_log), use_container_width=True)
            st.caption(
                "Published reflects the date carried by the source record. Fetched reflects when this workspace ingested the row. "
                "With only one or two pipeline runs on record, fetched activity will naturally cluster around those run dates."
            )

    with assistant_tab:
        render_assistant_tab(datasets, company_reports, master)

    with explorer_tab:
        left_col, right_col = st.columns([0.95, 1.35])
        with left_col:
            st.markdown("### Company Selection")
            options = filtered_company_reports["company_name"].dropna().astype(str).tolist()
            if options:
                if st.session_state.get("company_selectbox") not in options:
                    st.session_state["company_selectbox"] = options[0]
                selected_company = st.selectbox(
                    "Choose a company dossier",
                    options=options,
                    key="company_selectbox",
                    placeholder="Pick a company from the filtered results",
                )
            else:
                selected_company = ""
                st.info("No companies match the current filter set.")
            view_columns = [
                column
                for column in [
                    "company_name",
                    "cin",
                    "company_status",
                    "action_types",
                    "incorporation_date",
                    "last_balance_sheet_date",
                    "official_source_count",
                    "record_count",
                    "latest_published_at",
                ]
                if column in filtered_company_reports.columns
            ]
            render_html_table(
                _display_company_reports_table(filtered_company_reports[view_columns]),
                height=520,
                max_rows=None,
            )
        with right_col:
            st.markdown("### Company Dossier")
            company_detail_panel(selected_company, filtered_company_reports, master)

    with network_tab:
        left_col, right_col = st.columns([1.1, 0.9])
        with left_col:
            st.plotly_chart(source_mix_chart(source_log), use_container_width=True)
        with right_col:
            st.markdown("### Cluster Summary")
            render_html_table(clusters, height=480, max_rows=50)

    with evidence_tab:
        st.markdown("### Source Register")
        render_html_table(_display_source_register(source_log), link_columns={"URL"}, height=380, max_rows=80)
        st.download_button(
            "Download Current Source Register CSV",
            data=source_log.to_csv(index=False).encode("utf-8"),
            file_name="source_log_latest.csv",
            mime="text/csv",
            use_container_width=True,
        )

        st.markdown("### Master Records")
        render_html_table(_display_master_records(master), link_columns={"URL"}, height=420, max_rows=80)
        st.download_button(
            "Download Current Master Dataset CSV",
            data=master.to_csv(index=False).encode("utf-8"),
            file_name="master_dataset_latest.csv",
            mime="text/csv",
            use_container_width=True,
        )


if __name__ == "__main__":
    main()
