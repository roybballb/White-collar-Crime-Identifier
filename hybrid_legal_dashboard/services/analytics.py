from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
import re
from typing import Optional

import networkx as nx

from hybrid_legal_dashboard.config import LEGAL_SECTION_MAP, RISK_WEIGHTS, SECTION_ACT_MAP
from hybrid_legal_dashboard.schemas import ClusterSummary, CompanyReport, EntityRisk, LegalRecord


def _risk_band(score: float) -> str:
    if score >= 70:
        return "High"
    if score >= 45:
        return "Medium"
    return "Low"


def _entity_identity(record: LegalRecord) -> tuple[str, str]:
    if record.company_name:
        return record.company_name.lower(), "company"
    return record.subject_label.lower(), "record_subject"


def build_entity_rollup(records: list[LegalRecord]) -> list[EntityRisk]:
    grouped: dict[str, dict] = defaultdict(
        lambda: {
            "entity_name": "",
            "entity_type": "",
            "records": [],
            "sources": set(),
            "sections": set(),
            "districts": set(),
        }
    )

    for record in records:
        if not record.usable_for_analytics:
            continue

        entity_key, entity_type = _entity_identity(record)
        entry = grouped[entity_key]
        entry["entity_name"] = record.company_name or record.subject_label
        entry["entity_type"] = entity_type
        entry["records"].append(record)
        entry["sources"].add(record.source_name)
        entry["sections"].update(record.legal_sections)
        if record.district:
            entry["districts"].add(record.district)

    entities: list[EntityRisk] = []
    for entity_key, entry in grouped.items():
        records_for_entity: list[LegalRecord] = entry["records"]
        mention_count = len(records_for_entity)
        fraud_linked_mentions = sum(
            record.violation_type in {"fraud", "forgery", "criminal breach of trust", "cheating", "money laundering"}
            for record in records_for_entity
        )
        distinct_sources = len(entry["sources"])
        legal_section_count = len(entry["sections"])
        district_count = len(entry["districts"])
        average_quality = sum(record.record_quality_score for record in records_for_entity) / mention_count
        average_reliability = (
            sum(record.source_reliability_score for record in records_for_entity) / mention_count
        )

        score = (
            mention_count * RISK_WEIGHTS.mention_count
            + fraud_linked_mentions * RISK_WEIGHTS.fraud_mentions
            + distinct_sources * RISK_WEIGHTS.distinct_sources
            + legal_section_count * RISK_WEIGHTS.legal_sections
            + district_count * RISK_WEIGHTS.district_spread
            + average_quality * RISK_WEIGHTS.average_quality
            + average_reliability * RISK_WEIGHTS.average_reliability
        )
        score = min(100.0, round(score, 2))

        entities.append(
            EntityRisk(
                entity_id=entity_key.replace(" ", "-"),
                entity_name=entry["entity_name"],
                entity_type=entry["entity_type"],
                mention_count=mention_count,
                fraud_linked_mentions=fraud_linked_mentions,
                distinct_sources=distinct_sources,
                legal_section_count=legal_section_count,
                district_count=district_count,
                average_quality_score=round(average_quality, 2),
                average_reliability_score=round(average_reliability, 2),
                network_risk_score=score,
                risk_band=_risk_band(score),
                supporting_records=[record.record_id for record in records_for_entity],
                districts=sorted(entry["districts"]),
                legal_sections=sorted(entry["sections"]),
            )
        )

    return sorted(entities, key=lambda entity: entity.network_risk_score, reverse=True)


def build_case_graph(
    records: list[LegalRecord],
    entity_rollup: list[EntityRisk],
) -> nx.Graph:
    graph = nx.Graph()
    risk_lookup = {entity.entity_name: entity for entity in entity_rollup}

    for record in records:
        if not record.usable_for_analytics:
            continue

        entity_name = record.company_name or record.subject_label
        risk_band = risk_lookup.get(entity_name).risk_band if entity_name in risk_lookup else "Low"
        entity_node = f"entity::{entity_name}"
        graph.add_node(entity_node, type="entity", label=entity_name, risk_band=risk_band)

        for section in record.legal_sections:
            section_node = f"section::{section}"
            graph.add_node(section_node, type="section", label=f"Section {section}")
            graph.add_edge(entity_node, section_node)

        if record.district:
            district_node = f"district::{record.district}"
            graph.add_node(district_node, type="district", label=record.district)
            graph.add_edge(entity_node, district_node)

        source_node = f"source::{record.source_name}"
        graph.add_node(source_node, type="source", label=record.source_name)
        graph.add_edge(entity_node, source_node)

    return graph


def build_cluster_summary(
    records: list[LegalRecord],
    entity_rollup: list[EntityRisk],
) -> list[ClusterSummary]:
    graph = build_case_graph(records, entity_rollup)
    summaries: list[ClusterSummary] = []

    risk_rank = {"Low": 1, "Medium": 2, "High": 3}
    entity_risk_lookup = {entity.entity_name: entity.risk_band for entity in entity_rollup}

    for index, component in enumerate(nx.connected_components(graph), start=1):
        subgraph = graph.subgraph(component)
        entities = []
        sections = []
        districts = []
        top_risk_band = "Low"

        for node, attributes in subgraph.nodes(data=True):
            node_type = attributes.get("type")
            label = attributes.get("label", node)
            if node_type == "entity":
                entities.append(label)
                risk_band = entity_risk_lookup.get(label, "Low")
                if risk_rank[risk_band] > risk_rank[top_risk_band]:
                    top_risk_band = risk_band
            elif node_type == "section":
                sections.append(label)
            elif node_type == "district":
                districts.append(label)

        summaries.append(
            ClusterSummary(
                cluster_id=f"cluster-{index:03d}",
                node_count=subgraph.number_of_nodes(),
                edge_count=subgraph.number_of_edges(),
                member_entities=sorted(entities),
                member_sections=sorted(sections),
                member_districts=sorted(districts),
                top_risk_band=top_risk_band,
            )
        )

    return sorted(
        summaries,
        key=lambda summary: (risk_rank[summary.top_risk_band], summary.node_count, summary.edge_count),
        reverse=True,
    )


def build_kpis(records: list[LegalRecord], entity_rollup: list[EntityRisk]) -> dict[str, int]:
    usable_records = sum(record.usable_for_analytics for record in records)
    high_risk_entities = sum(entity.risk_band == "High" for entity in entity_rollup)
    repeated_entities = sum(entity.mention_count > 1 for entity in entity_rollup)
    sections = set()
    for record in records:
        sections.update(record.legal_sections)

    return {
        "sources_tracked": len(records),
        "usable_records": usable_records,
        "repeat_entities": repeated_entities,
        "high_risk_entities": high_risk_entities,
        "distinct_sections": len(sections),
    }


def _parse_date(value: str) -> Optional[datetime]:
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def _latest_date(values: list[str]) -> str:
    dated = [(parsed, value) for value in values if value and (parsed := _parse_date(value))]
    if dated:
        return max(dated, key=lambda item: item[0])[1]
    return next((value for value in values if value), "")


def _earliest_date(values: list[str]) -> str:
    dated = [(parsed, value) for value in values if value and (parsed := _parse_date(value))]
    if dated:
        return min(dated, key=lambda item: item[0])[1]
    return next((value for value in values if value), "")


def _capital_value(value: str) -> float:
    cleaned = re.sub(r"[^0-9.]", "", value or "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _clean_text(value: object) -> str:
    text = str(value or "").strip()
    if text.lower() in {"", "nan", "none", "nat"}:
        return ""
    return text


def _normalize_section_value(value: object) -> str:
    text = _clean_text(value).upper()
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def _section_sort_key(section: str) -> tuple[int, str]:
    match = re.match(r"(\d+)([A-Z]?)", section or "")
    if not match:
        return (0, section or "")
    return (int(match.group(1)), match.group(2))


def build_company_reports(records: list[LegalRecord]) -> list[CompanyReport]:
    grouped: dict[str, dict] = defaultdict(
        lambda: {
            "company_name": "",
            "cin": "",
            "records": [],
            "sources": set(),
            "official_sources": set(),
            "sections": set(),
            "section_counts": Counter(),
            "actions": set(),
            "districts": set(),
            "published": [],
            "statuses": [],
            "incorporation_dates": [],
            "agm_dates": [],
            "balance_sheet_dates": [],
            "addresses": [],
            "authorized_capitals": [],
            "paid_up_capitals": [],
            "profile_sources": set(),
            "industries": set(),
            "listed_statuses": [],
            "roc_offices": [],
        }
    )

    for record in records:
        record_company_name = _clean_text(record.company_name)
        record_subject_label = _clean_text(record.subject_label)
        record_cin_candidates = [_clean_text(item) for item in record.cin_candidates if _clean_text(item)]
        if not record_company_name and not record_cin_candidates:
            continue

        cin = record_cin_candidates[0] if record_cin_candidates else ""
        key = cin or record_company_name.lower()
        entry = grouped[key]
        entry["company_name"] = record_company_name or entry["company_name"]
        entry["cin"] = cin or entry["cin"]
        entry["records"].append(record)
        entry["sources"].add(record.source_name)
        if record.source_reliability_label in {"official", "regulated"}:
            entry["official_sources"].add(record.source_name)
        normalized_sections = [_normalize_section_value(section) for section in record.legal_sections if _normalize_section_value(section)]
        entry["sections"].update(normalized_sections)
        entry["section_counts"].update(normalized_sections)
        if record.violation_type and record.violation_type != "other":
            entry["actions"].add(record.violation_type)
        if record.district:
            entry["districts"].add(record.district)
        if record.published_at:
            entry["published"].append(record.published_at)
        if record.company_status:
            entry["statuses"].append(record.company_status)
        if record.incorporation_date:
            entry["incorporation_dates"].append(record.incorporation_date)
        if record.last_agm_date:
            entry["agm_dates"].append(record.last_agm_date)
        if record.last_balance_sheet_date:
            entry["balance_sheet_dates"].append(record.last_balance_sheet_date)
        if record.registered_address:
            entry["addresses"].append(record.registered_address)
        if record.authorized_capital:
            entry["authorized_capitals"].append(record.authorized_capital)
        if record.paid_up_capital:
            entry["paid_up_capitals"].append(record.paid_up_capital)
        if record.source_provider in {"zauba", "instafinancials"}:
            entry["profile_sources"].add(record.source_name)
        if record.industry_description:
            entry["industries"].add(record.industry_description)
        if record.listed_status:
            entry["listed_statuses"].append(record.listed_status)
        if record.roc_office:
            entry["roc_offices"].append(record.roc_office)

    reports: list[CompanyReport] = []
    for key, entry in grouped.items():
        records_for_company: list[LegalRecord] = entry["records"]
        sorted_records = sorted(
            records_for_company,
            key=lambda record: (record.published_at or "", record.fetched_at or "", record.title),
            reverse=True,
        )
        latest = sorted_records[0]
        profile_records = [
            record
            for record in sorted_records
            if any(
                [
                    record.company_status,
                    record.incorporation_date,
                    record.last_agm_date,
                    record.last_balance_sheet_date,
                    record.registered_address,
                    record.authorized_capital,
                    record.paid_up_capital,
                ]
            )
        ]
        latest_profile = profile_records[0] if profile_records else latest
        published_values = sorted(value for value in entry["published"] if value)
        action_types = sorted(entry["actions"]) or ["other"]
        source_names = sorted(entry["sources"])
        sections = sorted(entry["sections"], key=_section_sort_key)
        districts = sorted(entry["districts"])
        cin = entry["cin"]
        company_name = entry["company_name"] or _clean_text(latest.company_name) or _clean_text(latest.subject_label)
        action_label = ", ".join(action_types)
        source_count = len(entry["sources"])
        official_source_count = len(entry["official_sources"])
        company_status = latest_profile.company_status or (entry["statuses"][0] if entry["statuses"] else "")
        incorporation_date = _earliest_date(entry["incorporation_dates"])
        last_agm_date = _latest_date(entry["agm_dates"])
        last_balance_sheet_date = _latest_date(entry["balance_sheet_dates"])
        registered_address = latest_profile.registered_address or (entry["addresses"][0] if entry["addresses"] else "")
        authorized_capital = (
            max(entry["authorized_capitals"], key=_capital_value)
            if entry["authorized_capitals"]
            else ""
        )
        paid_up_capital = (
            max(entry["paid_up_capitals"], key=_capital_value)
            if entry["paid_up_capitals"]
            else ""
        )
        listed_status = latest_profile.listed_status or (entry["listed_statuses"][0] if entry["listed_statuses"] else "")
        roc_office = latest_profile.roc_office or (entry["roc_offices"][0] if entry["roc_offices"] else "")
        industry_descriptions = sorted(entry["industries"])
        profile_sources = sorted(entry["profile_sources"])
        section_counts: Counter = entry["section_counts"]
        dominant_legal_section = ""
        dominant_legal_act = ""
        dominant_legal_meaning = ""
        if section_counts:
            dominant_legal_section = sorted(
                section_counts.items(),
                key=lambda item: (item[1], _section_sort_key(item[0])),
                reverse=True,
            )[0][0]
            dominant_legal_act = SECTION_ACT_MAP.get(dominant_legal_section, "")
            dominant_legal_meaning = LEGAL_SECTION_MAP.get(dominant_legal_section, "")

        summary_parts = [
            f"{company_name} appears in {len(records_for_company)} record(s) across {source_count} source(s).",
            f"Primary action types: {action_label}.",
            f"CIN: {cin or 'not extracted'}.",
        ]
        if dominant_legal_section:
            reference = f"Section {dominant_legal_section}"
            if dominant_legal_act:
                reference = f"{dominant_legal_act} - {reference}"
            if dominant_legal_meaning:
                reference = f"{reference} ({dominant_legal_meaning})"
            summary_parts.append(f"Major statutory signal: {reference}.")
        if company_status:
            summary_parts.append(f"Status: {company_status}.")
        if incorporation_date:
            summary_parts.append(f"Incorporated: {incorporation_date}.")
        if last_balance_sheet_date:
            summary_parts.append(f"Last balance sheet: {last_balance_sheet_date}.")
        if last_agm_date:
            summary_parts.append(f"Last AGM: {last_agm_date}.")

        reports.append(
            CompanyReport(
                company_key=str(key).replace(" ", "-").lower(),
                company_name=company_name,
                cin=cin,
                source_count=source_count,
                official_source_count=official_source_count,
                record_count=len(records_for_company),
                legal_sections=sections,
                action_types=action_types,
                districts=districts,
                source_names=source_names,
                first_published_at=published_values[0] if published_values else "",
                latest_published_at=published_values[-1] if published_values else "",
                latest_title=latest.title,
                representative_url=latest.url,
                dossier_summary=" ".join(summary_parts),
                dominant_legal_section=dominant_legal_section,
                dominant_legal_act=dominant_legal_act,
                dominant_legal_meaning=dominant_legal_meaning,
                company_status=company_status,
                incorporation_date=incorporation_date,
                last_agm_date=last_agm_date,
                last_balance_sheet_date=last_balance_sheet_date,
                registered_address=registered_address,
                authorized_capital=authorized_capital,
                paid_up_capital=paid_up_capital,
                profile_sources=profile_sources,
                industry_descriptions=industry_descriptions,
                listed_status=listed_status,
                roc_office=roc_office,
            )
        )

    return sorted(
        reports,
        key=lambda report: (report.official_source_count, report.record_count, report.company_name.lower()),
        reverse=True,
    )
