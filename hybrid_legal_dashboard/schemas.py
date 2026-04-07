from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class SourceRecord:
    title: str
    url: str
    source_name: str
    snippet: str = ""
    reliability_label: str = "open_web"
    query_text: str = ""
    fetched_at: str = field(default_factory=utc_now_iso)
    published_at: str = ""
    source_type: str = "web"
    source_provider: str = ""
    company_name: str = ""
    cin: str = ""
    company_status: str = ""
    incorporation_date: str = ""
    last_agm_date: str = ""
    last_balance_sheet_date: str = ""
    registered_address: str = ""
    authorized_capital: str = ""
    paid_up_capital: str = ""
    nic_code: str = ""
    industry_description: str = ""
    roc_office: str = ""
    listed_status: str = ""
    profile_last_updated: str = ""


@dataclass
class CompanyReport:
    company_key: str
    company_name: str
    cin: str = ""
    source_count: int = 0
    official_source_count: int = 0
    record_count: int = 0
    legal_sections: list[str] = field(default_factory=list)
    action_types: list[str] = field(default_factory=list)
    districts: list[str] = field(default_factory=list)
    source_names: list[str] = field(default_factory=list)
    first_published_at: str = ""
    latest_published_at: str = ""
    latest_title: str = ""
    representative_url: str = ""
    dossier_summary: str = ""
    dominant_legal_section: str = ""
    dominant_legal_act: str = ""
    dominant_legal_meaning: str = ""
    company_status: str = ""
    incorporation_date: str = ""
    last_agm_date: str = ""
    last_balance_sheet_date: str = ""
    registered_address: str = ""
    authorized_capital: str = ""
    paid_up_capital: str = ""
    profile_sources: list[str] = field(default_factory=list)
    industry_descriptions: list[str] = field(default_factory=list)
    listed_status: str = ""
    roc_office: str = ""


@dataclass
class LegalRecord:
    record_id: str
    title: str
    url: str
    source_name: str
    source_reliability_label: str
    source_reliability_score: float
    query_text: str
    fetched_at: str
    published_at: str
    company_name: str = ""
    district: str = ""
    legal_sections: list[str] = field(default_factory=list)
    legal_section_meanings: list[str] = field(default_factory=list)
    violation_type: str = ""
    cin_candidates: list[str] = field(default_factory=list)
    subject_label: str = ""
    snippet: str = ""
    has_company_name: bool = False
    has_district: bool = False
    has_legal_section: bool = False
    record_quality_score: float = 0.0
    usable_for_analytics: bool = False
    source_provider: str = ""
    company_status: str = ""
    incorporation_date: str = ""
    last_agm_date: str = ""
    last_balance_sheet_date: str = ""
    registered_address: str = ""
    authorized_capital: str = ""
    paid_up_capital: str = ""
    nic_code: str = ""
    industry_description: str = ""
    roc_office: str = ""
    listed_status: str = ""
    profile_last_updated: str = ""


@dataclass
class EntityRisk:
    entity_id: str
    entity_name: str
    entity_type: str
    mention_count: int
    fraud_linked_mentions: int
    distinct_sources: int
    legal_section_count: int
    district_count: int
    average_quality_score: float
    average_reliability_score: float
    network_risk_score: float
    risk_band: str
    supporting_records: list[str] = field(default_factory=list)
    districts: list[str] = field(default_factory=list)
    legal_sections: list[str] = field(default_factory=list)


@dataclass
class ClusterSummary:
    cluster_id: str
    node_count: int
    edge_count: int
    member_entities: list[str] = field(default_factory=list)
    member_sections: list[str] = field(default_factory=list)
    member_districts: list[str] = field(default_factory=list)
    top_risk_band: str = "Low"
