from __future__ import annotations

from dataclasses import dataclass, field
import re

import pandas as pd

from hybrid_legal_dashboard.config import LEGAL_SECTION_MAP, WEST_BENGAL_DISTRICTS


CIN_PATTERN = re.compile(r"\b[LUF]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}\b", re.IGNORECASE)
ACTION_KEYWORDS = {
    "liquidation": ["liquidation", "winding up", "dissolution"],
    "insolvency": ["insolvency", "ibc", "resolution", "corporate debtor"],
    "fraud": ["fraud", "section 447", "false statement"],
    "struck_off": ["strike off", "struck off", "section 248"],
    "director_disqualification": ["disqualification", "section 164", "director"],
    "dormant_status": ["dormant", "section 455"],
}
RELIABILITY_RANK = {
    "official": 0,
    "regulated": 1,
    "registry_aggregator": 2,
    "reputed_media": 3,
    "open_web": 4,
}


@dataclass
class ChatResponse:
    answer_markdown: str
    citations: pd.DataFrame = field(default_factory=pd.DataFrame)
    matched_company: str = ""
    intent: str = "general"
    stat_cards: list[tuple[str, str]] = field(default_factory=list)
    follow_ups: list[str] = field(default_factory=list)


def _tokenize(text: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9]+", (text or "").lower()) if len(token) > 2}


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "nat"}:
        return ""
    return text


def _pipe_values(value: object) -> list[str]:
    return [item.strip() for item in _safe_text(value).split("|") if item.strip()]


def _normalize_section_value(value: object) -> str:
    text = _safe_text(value).upper()
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def _citations(records: pd.DataFrame, limit: int = 6) -> pd.DataFrame:
    if records.empty:
        return pd.DataFrame(columns=["title", "source_name", "published_at", "fetched_at", "url"])
    working = records.copy()
    reliability_column = ""
    if "source_reliability_label" in working.columns:
        reliability_column = "source_reliability_label"
    elif "reliability_label" in working.columns:
        reliability_column = "reliability_label"
    if reliability_column:
        working["_reliability_rank"] = (
            working[reliability_column].fillna("").astype(str).str.lower().map(RELIABILITY_RANK).fillna(99)
        )
    else:
        working["_reliability_rank"] = 99
    for column in ["published_at", "fetched_at"]:
        if column in working.columns:
            working[f"_{column}_sort"] = pd.to_datetime(working[column], errors="coerce", utc=True, dayfirst=True)
        else:
            working[f"_{column}_sort"] = pd.NaT
    working = working.sort_values(
        ["_reliability_rank", "_published_at_sort", "_fetched_at_sort"],
        ascending=[True, False, False],
        na_position="last",
    )
    columns = [column for column in ["title", "source_name", "published_at", "fetched_at", "url"] if column in working.columns]
    return working[columns].head(limit).reset_index(drop=True)


def _real_company_citations(
    company_reports: pd.DataFrame,
    master: pd.DataFrame,
    limit: int = 6,
    action_filter: str = "",
    district_filter: str = "",
) -> pd.DataFrame:
    if company_reports.empty or master.empty or "company_name" not in company_reports.columns or "company_name" not in master.columns:
        return _citations(master, limit=limit)

    working_master = master.copy()
    if action_filter and "violation_type" in working_master.columns:
        working_master = working_master.loc[
            working_master["violation_type"].fillna("").astype(str).str.lower().str.contains(action_filter.lower(), regex=False)
        ]
    if district_filter and "district" in working_master.columns:
        working_master = working_master.loc[
            working_master["district"].fillna("").astype(str).str.lower() == district_filter.lower()
        ]
    if working_master.empty:
        return pd.DataFrame(columns=["title", "source_name", "published_at", "fetched_at", "url"])

    company_names = [
        _safe_text(name)
        for name in company_reports["company_name"].dropna().astype(str).tolist()
        if _safe_text(name)
    ]
    if not company_names:
        return _citations(working_master, limit=limit)

    working_master["_company_name_key"] = working_master["company_name"].fillna("").astype(str).str.lower()
    citations: list[pd.Series] = []
    seen_urls: set[str] = set()
    for company_name in company_names:
        matches = working_master.loc[working_master["_company_name_key"] == company_name.lower()]
        if matches.empty:
            continue
        citation_rows = _citations(matches, limit=1)
        if citation_rows.empty:
            continue
        row = citation_rows.iloc[0]
        url = _safe_text(row.get("url"))
        if url and url in seen_urls:
            continue
        if url:
            seen_urls.add(url)
        citations.append(row)
        if len(citations) >= limit:
            break
    if citations:
        return pd.DataFrame(citations).reset_index(drop=True)
    return _citations(working_master, limit=limit)


def _real_entity_citations(risks: pd.DataFrame, master: pd.DataFrame, limit: int = 6) -> pd.DataFrame:
    if risks.empty or master.empty:
        return _citations(master, limit=limit)
    working_master = master.copy()
    citations: list[pd.Series] = []
    seen_urls: set[str] = set()
    company_column = working_master["company_name"].fillna("").astype(str).str.lower() if "company_name" in working_master.columns else pd.Series(dtype=str)
    subject_column = working_master["subject_label"].fillna("").astype(str).str.lower() if "subject_label" in working_master.columns else pd.Series(dtype=str)
    title_column = working_master["title"].fillna("").astype(str).str.lower() if "title" in working_master.columns else pd.Series(dtype=str)
    for _, risk_row in risks.iterrows():
        entity_name = _safe_text(risk_row.get("entity_name"))
        if not entity_name:
            continue
        entity_key = entity_name.lower()
        mask = pd.Series(False, index=working_master.index)
        if not company_column.empty:
            mask = mask | (company_column == entity_key)
        if not subject_column.empty:
            mask = mask | (subject_column == entity_key)
        if not title_column.empty:
            mask = mask | title_column.str.contains(entity_key, regex=False)
        matches = working_master.loc[mask]
        if matches.empty:
            continue
        citation_rows = _citations(matches, limit=1)
        if citation_rows.empty:
            continue
        row = citation_rows.iloc[0]
        url = _safe_text(row.get("url"))
        if url and url in seen_urls:
            continue
        if url:
            seen_urls.add(url)
        citations.append(row)
        if len(citations) >= limit:
            break
    if citations:
        return pd.DataFrame(citations).reset_index(drop=True)
    return _citations(working_master, limit=limit)


def _follow_ups(*items: str) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        text = _safe_text(item)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        ordered.append(text)
    return ordered


def _extract_section(question: str) -> str:
    match = re.search(r"(?:section|sec\.?)\s*(\d{3,4}[A-Z]?)", question, re.IGNORECASE)
    return match.group(1).upper() if match else ""


def _extract_cin(question: str) -> str:
    match = CIN_PATTERN.search(question or "")
    return match.group(0).upper() if match else ""


def _company_match(question: str, company_reports: pd.DataFrame) -> str:
    if company_reports.empty or "company_name" not in company_reports.columns:
        return ""

    lowered = question.lower()
    cin = _extract_cin(question)
    if cin and "cin" in company_reports.columns:
        exact_cin = company_reports.loc[company_reports["cin"].fillna("").astype(str).str.upper() == cin]
        if not exact_cin.empty:
            return _safe_text(exact_cin.iloc[0]["company_name"])

    names = [name for name in company_reports["company_name"].dropna().astype(str).tolist() if name.strip()]
    exact = [name for name in names if name.lower() in lowered]
    if exact:
        return max(exact, key=len)

    question_tokens = _tokenize(question)
    best_name = ""
    best_score = 0.0
    for name in names:
        name_tokens = _tokenize(name)
        if not name_tokens:
            continue
        overlap = len(question_tokens & name_tokens)
        if overlap == 0:
            continue
        score = overlap / max(1, len(name_tokens))
        if overlap >= 2:
            score += 0.5
        if score > best_score:
            best_name = name
            best_score = score
    return best_name if best_score >= 0.6 else ""


def _company_response(company_name: str, datasets: dict[str, pd.DataFrame]) -> ChatResponse:
    company_reports = datasets.get("company_reports", pd.DataFrame())
    master = datasets.get("master_dataset", pd.DataFrame())
    risks = datasets.get("risk_scores", pd.DataFrame())

    row = company_reports.loc[company_reports["company_name"].fillna("") == company_name]
    if row.empty:
        return ChatResponse(
            answer_markdown=f"I found **{company_name}** in the snapshot context, but its company report row is not currently available.",
            intent="company",
            matched_company=company_name,
        )
    row = row.iloc[0]

    related = master.loc[master["company_name"].fillna("") == company_name].copy() if "company_name" in master.columns else master
    risk_row = pd.DataFrame()
    if not risks.empty and "entity_name" in risks.columns:
        risk_row = risks.loc[risks["entity_name"].fillna("").astype(str).str.lower() == company_name.lower()]

    answer_parts = [
        f"**{company_name}** has **{int(row.get('record_count', 0))}** record(s) across **{int(row.get('source_count', 0))}** source(s).",
        f"Official-backed sources: **{int(row.get('official_source_count', 0))}**.",
        f"Primary action types: **{_safe_text(row.get('action_types')) or 'other'}**.",
    ]
    dominant_section = _normalize_section_value(row.get("dominant_legal_section"))
    dominant_act = _safe_text(row.get("dominant_legal_act"))
    dominant_meaning = _safe_text(row.get("dominant_legal_meaning"))
    if dominant_section:
        statutory_reference = f"Section {dominant_section}"
        if dominant_act:
            statutory_reference = f"{dominant_act} - {statutory_reference}"
        if dominant_meaning:
            statutory_reference = f"{statutory_reference} ({dominant_meaning})"
        answer_parts.append(f"Major statutory signal: **{statutory_reference}**.")
    if _safe_text(row.get("company_status")):
        answer_parts.append(f"Status: **{_safe_text(row.get('company_status'))}**.")
    if _safe_text(row.get("cin")):
        answer_parts.append(f"CIN: **{_safe_text(row.get('cin'))}**.")
    if _safe_text(row.get("incorporation_date")):
        answer_parts.append(f"Incorporated: **{_safe_text(row.get('incorporation_date'))}**.")
    if not risk_row.empty:
        risk = risk_row.iloc[0]
        answer_parts.append(
            f"Current company risk band: **{_safe_text(risk.get('risk_band')) or 'Not scored'}** "
            f"(score **{_safe_text(risk.get('network_risk_score')) or 'n/a'}**)."
        )

    stat_cards = [
        ("Records", str(int(row.get("record_count", 0)))),
        ("Sources", str(int(row.get("source_count", 0)))),
        ("Official Sources", str(int(row.get("official_source_count", 0)))),
        ("CIN", _safe_text(row.get("cin")) or "Not extracted"),
    ]
    if dominant_section:
        stat_cards.append(("Major Section", f"Section {dominant_section}"))
    if dominant_act:
        stat_cards.append(("Major Act", dominant_act))
    if not risk_row.empty:
        risk = risk_row.iloc[0]
        stat_cards.append(("Risk Band", _safe_text(risk.get("risk_band")) or "Not scored"))
        stat_cards.append(("Risk Score", _safe_text(risk.get("network_risk_score")) or "n/a"))

    return ChatResponse(
        answer_markdown=" ".join(answer_parts),
        citations=_citations(related),
        matched_company=company_name,
        intent="company",
        stat_cards=stat_cards[:6],
        follow_ups=_follow_ups(
            f"What sources mention {company_name}?",
            f"What legal sections are linked to {company_name}?",
            f"Show the risk score for {company_name}",
        ),
    )


def _top_risk_response(datasets: dict[str, pd.DataFrame]) -> ChatResponse:
    risks = datasets.get("risk_scores", pd.DataFrame())
    master = datasets.get("master_dataset", pd.DataFrame())
    if risks.empty:
        return ChatResponse(
            answer_markdown="No risk-score rows are available in the current snapshot yet.",
            intent="risk",
        )
    ranked = risks.loc[risks["entity_type"].fillna("").astype(str).str.lower() == "company"].copy()
    if ranked.empty:
        ranked = risks.copy()
    ranked = ranked.sort_values("network_risk_score", ascending=False).head(5)
    summary = "; ".join(
        f"**{_safe_text(row.get('entity_name'))}** ({_safe_text(row.get('risk_band')) or 'Unbanded'} / {_safe_text(row.get('network_risk_score')) or 'n/a'})"
        for _, row in ranked.iterrows()
    )
    return ChatResponse(
        answer_markdown=f"The highest-risk entities in the current snapshot are {summary}.",
        citations=_real_entity_citations(ranked, master),
        intent="risk",
        stat_cards=[
            ("Top Risk Rows", str(len(ranked))),
            ("Highest Score", _safe_text(ranked.iloc[0].get("network_risk_score")) or "n/a"),
        ],
        follow_ups=_follow_ups("Show official-backed high-risk companies", "Which sources drive those risk scores?"),
    )


def _section_response(question: str, datasets: dict[str, pd.DataFrame]) -> ChatResponse:
    master = datasets.get("master_dataset", pd.DataFrame())
    if master.empty or "legal_sections" not in master.columns:
        return ChatResponse(answer_markdown="No legal-section data is available in the current snapshot.", intent="section")

    requested = _extract_section(question)
    section_counts: dict[str, int] = {}
    for raw_sections in master["legal_sections"].fillna(""):
        for section in _pipe_values(raw_sections):
            normalized = _normalize_section_value(section)
            if not normalized:
                continue
            section_counts[normalized] = section_counts.get(normalized, 0) + 1

    if not section_counts:
        return ChatResponse(answer_markdown="I could not find mapped legal sections in the current snapshot.", intent="section")

    if requested:
        count = section_counts.get(requested, 0)
        related = master.loc[master["legal_sections"].fillna("").astype(str).str.contains(requested, regex=False)]
        meaning = LEGAL_SECTION_MAP.get(requested, "Section meaning not mapped")
        return ChatResponse(
            answer_markdown=(
                f"**Section {requested}** appears in **{count}** record(s) in the current snapshot. "
                f"Mapped meaning: **{meaning}**."
            ),
            citations=_citations(related),
            intent="section",
            stat_cards=[("Section", requested), ("Records", str(count))],
            follow_ups=_follow_ups(f"Which companies are linked to Section {requested}?", "Which section appears most often?"),
        )

    top_section, count = max(section_counts.items(), key=lambda item: item[1])
    related = master.loc[master["legal_sections"].fillna("").astype(str).str.contains(top_section, regex=False)]
    meaning = LEGAL_SECTION_MAP.get(top_section, "Section meaning not mapped")
    return ChatResponse(
        answer_markdown=(
            f"The most visible legal section in the current snapshot is **Section {top_section}**, "
            f"appearing in **{count}** record(s). Mapped meaning: **{meaning}**."
        ),
        citations=_citations(related),
        intent="section",
        stat_cards=[("Top Section", top_section), ("Records", str(count))],
        follow_ups=_follow_ups(f"Which companies are linked to Section {top_section}?", "Show the top risk companies"),
    )


def _source_response(datasets: dict[str, pd.DataFrame]) -> ChatResponse:
    source_log = datasets.get("source_log", pd.DataFrame())
    if source_log.empty or "source_name" not in source_log.columns:
        return ChatResponse(answer_markdown="No source log is available in the current snapshot.", intent="source")

    top_sources = source_log["source_name"].fillna("").astype(str).value_counts()
    if top_sources.empty:
        return ChatResponse(answer_markdown="I could not find a populated source register in the current snapshot.", intent="source")

    source_name = top_sources.index[0]
    count = int(top_sources.iloc[0])
    filtered = source_log.loc[source_log["source_name"] == source_name]
    return ChatResponse(
        answer_markdown=f"The most active source in the current snapshot is **{source_name}** with **{count}** logged item(s).",
        citations=_citations(filtered),
        intent="source",
        stat_cards=[("Top Source", source_name), ("Rows", str(count))],
        follow_ups=_follow_ups("Which official sources are most active?", "Show recent source rows"),
    )


def _district_response(question: str, datasets: dict[str, pd.DataFrame]) -> ChatResponse:
    company_reports = datasets.get("company_reports", pd.DataFrame())
    master = datasets.get("master_dataset", pd.DataFrame())
    lowered = question.lower()
    district = next((item for item in WEST_BENGAL_DISTRICTS if item.lower() in lowered), "")
    if not district:
        return ChatResponse(answer_markdown="Mention a West Bengal district and I can summarize the related companies and records.", intent="district")

    company_matches = company_reports.loc[
        company_reports.get("districts", pd.Series(dtype=object)).fillna("").astype(str).str.contains(district, regex=False)
    ].copy()
    record_matches = master.loc[master.get("district", pd.Series(dtype=object)).fillna("").astype(str).str.lower() == district.lower()].copy()
    return ChatResponse(
        answer_markdown=(
            f"**{district}** appears in **{len(record_matches)}** master record(s) and **{len(company_matches)}** company report(s) "
            f"in the current snapshot."
        ),
        citations=_real_company_citations(company_matches, master, district_filter=district) if not company_matches.empty else _citations(record_matches),
        intent="district",
        stat_cards=[("District", district), ("Records", str(len(record_matches))), ("Companies", str(len(company_matches)))],
        follow_ups=_follow_ups(f"Which sources mention {district}?", f"Which companies in {district} have official backing?"),
    )


def _action_response(question: str, datasets: dict[str, pd.DataFrame]) -> ChatResponse:
    company_reports = datasets.get("company_reports", pd.DataFrame())
    lowered = question.lower()
    matched_action = ""
    for action, keywords in ACTION_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            matched_action = action
            break
    if not matched_action:
        return ChatResponse(answer_markdown="", intent="action")
    if company_reports.empty or "action_types" not in company_reports.columns:
        return ChatResponse(answer_markdown="No company action summaries are available in the current snapshot.", intent="action")

    filtered = company_reports.loc[
        company_reports["action_types"].fillna("").astype(str).str.lower().str.contains(matched_action.lower(), regex=False)
    ].copy()
    if filtered.empty:
        return ChatResponse(
            answer_markdown=f"I could not find any company reports tagged as **{matched_action}** in the current snapshot.",
            intent="action",
        )

    top = filtered.sort_values(["official_source_count", "record_count"], ascending=False).head(5)
    top_names = ", ".join(f"**{_safe_text(row.get('company_name'))}**" for _, row in top.iterrows())
    return ChatResponse(
        answer_markdown=(
            f"There are **{len(filtered)}** company report(s) tagged as **{matched_action}**. "
            f"Some of the strongest current matches are {top_names}."
        ),
        citations=_real_company_citations(top, datasets.get("master_dataset", pd.DataFrame()), action_filter=matched_action),
        intent="action",
        stat_cards=[("Action Type", matched_action), ("Company Reports", str(len(filtered)))],
        follow_ups=_follow_ups(f"Show official-backed {matched_action} companies", f"Which sources discuss {matched_action}?"),
    )


def _count_response(datasets: dict[str, pd.DataFrame]) -> ChatResponse:
    company_reports = datasets.get("company_reports", pd.DataFrame())
    master = datasets.get("master_dataset", pd.DataFrame())
    source_log = datasets.get("source_log", pd.DataFrame())
    official_company_reports = (
        int((company_reports["official_source_count"].fillna(0).astype(int) > 0).sum())
        if not company_reports.empty and "official_source_count" in company_reports.columns
        else 0
    )
    return ChatResponse(
        answer_markdown=(
            f"The current snapshot contains **{len(company_reports)}** company report(s), **{len(master)}** master record(s), "
            f"and **{len(source_log)}** source-log row(s)."
        ),
        citations=_real_company_citations(
            company_reports.sort_values(["official_source_count", "record_count"], ascending=False).head(8),
            master,
        ),
        intent="count",
        stat_cards=[
            ("Company Reports", str(len(company_reports))),
            ("Master Records", str(len(master))),
            ("Source Rows", str(len(source_log))),
            ("Official Company Reports", str(official_company_reports)),
        ],
        follow_ups=_follow_ups("Show the highest-risk companies", "Which sources are most active?"),
    )


def _recent_response(datasets: dict[str, pd.DataFrame], official_only: bool = False) -> ChatResponse:
    master = datasets.get("master_dataset", pd.DataFrame())
    if master.empty:
        return ChatResponse(answer_markdown="No master records are available in the current snapshot.", intent="recent")

    working = master.copy()
    if official_only and "source_reliability_label" in working.columns:
        working = working.loc[working["source_reliability_label"].fillna("").isin(["official", "regulated"])]
    if "published_at" in working.columns:
        working["_published_at_sort"] = pd.to_datetime(working["published_at"], errors="coerce", utc=True, dayfirst=True)
    else:
        working["_published_at_sort"] = pd.NaT
    if "fetched_at" in working.columns:
        working["_fetched_at_sort"] = pd.to_datetime(working["fetched_at"], errors="coerce", utc=True, dayfirst=True)
    else:
        working["_fetched_at_sort"] = pd.NaT
    working = working.sort_values(
        ["_published_at_sort", "_fetched_at_sort"],
        ascending=[False, False],
        na_position="last",
    ).head(6)
    qualifier = "official or regulated " if official_only else ""
    return ChatResponse(
        answer_markdown=f"These are the most recent {qualifier}records I found in the current snapshot.",
        citations=_citations(working),
        intent="recent",
        stat_cards=[("Recent Rows", str(len(working)))],
        follow_ups=_follow_ups("Which sources are driving recent activity?", "Show recent high-risk companies"),
    )


def _semantic_response(question: str, datasets: dict[str, pd.DataFrame]) -> ChatResponse:
    master = datasets.get("master_dataset", pd.DataFrame())
    company_reports = datasets.get("company_reports", pd.DataFrame())
    question_tokens = _tokenize(question)

    scored_rows = []
    if question_tokens and not master.empty:
        for _, row in master.iterrows():
            row_text = " ".join(
                str(row.get(column, ""))
                for column in ["title", "snippet", "company_name", "district", "legal_sections", "source_name"]
            )
            overlap = len(question_tokens & _tokenize(row_text))
            if overlap:
                scored_rows.append((overlap, row))

    if scored_rows:
        scored_rows.sort(key=lambda item: item[0], reverse=True)
        best_rows = pd.DataFrame([row for _, row in scored_rows[:6]])
        top_row = best_rows.iloc[0]
        company = _safe_text(top_row.get("company_name")) or _safe_text(top_row.get("subject_label")) or "the matched subject"
        matched_company = ""
        if not company_reports.empty and "company_name" in company_reports.columns:
            company_names = set(company_reports["company_name"].dropna().astype(str))
            if company in company_names:
                matched_company = company
        return ChatResponse(
            answer_markdown=(
                f"The closest match in the current snapshot points to **{company}**. "
                f"The top matching record is **{_safe_text(top_row.get('title')) or 'an untitled record'}** "
                f"from **{_safe_text(top_row.get('source_name')) or 'an unknown source'}**."
            ),
            citations=_citations(best_rows),
            matched_company=matched_company,
            intent="semantic",
            follow_ups=_follow_ups(
                f"Summarize {company}",
                f"What sources mention {company}?",
                "Show the highest-risk companies",
            ),
        )

    return ChatResponse(
        answer_markdown=(
            "I could not find a strong match in the current processed snapshot. "
            "Try a company name, CIN, district, source, section, or action type."
        ),
        citations=_citations(master),
        intent="general",
        follow_ups=_follow_ups(
            "How many company reports are in the snapshot?",
            "Which companies have the highest risk?",
            "Which legal sections appear most often?",
        ),
    )


def answer_question_detailed(question: str, datasets: dict[str, pd.DataFrame]) -> ChatResponse:
    company_reports = datasets.get("company_reports", pd.DataFrame())
    lowered = (question or "").strip().lower()
    if not lowered:
        return ChatResponse(
            answer_markdown="Ask about a company, CIN, legal section, district, source, recent activity, or risk trend.",
            intent="general",
            follow_ups=_follow_ups(
                "How many company reports are in the snapshot?",
                "Which companies have the highest risk?",
                "Which legal sections appear most often?",
                "Show recent official records",
            ),
        )

    matched_company = _company_match(question, company_reports)
    if matched_company:
        return _company_response(matched_company, datasets)

    if any(phrase in lowered for phrase in ["highest risk", "top risk", "most risky", "high risk"]):
        return _top_risk_response(datasets)

    if "section" in lowered or _extract_section(question):
        return _section_response(question, datasets)

    if "source" in lowered:
        return _source_response(datasets)

    if any(district.lower() in lowered for district in WEST_BENGAL_DISTRICTS):
        return _district_response(question, datasets)

    action_response = _action_response(question, datasets)
    if action_response.answer_markdown:
        return action_response

    if "how many" in lowered or "count" in lowered or "total" in lowered:
        return _count_response(datasets)

    if "recent" in lowered or "latest" in lowered or "newest" in lowered:
        return _recent_response(datasets, official_only="official" in lowered or "regulated" in lowered)

    return _semantic_response(question, datasets)


def answer_question(question: str, datasets: dict[str, pd.DataFrame]) -> tuple[str, pd.DataFrame]:
    response = answer_question_detailed(question, datasets)
    return response.answer_markdown, response.citations
