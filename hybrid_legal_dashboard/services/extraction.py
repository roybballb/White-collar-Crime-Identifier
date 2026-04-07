from __future__ import annotations

from hashlib import sha256
import re

from hybrid_legal_dashboard.config import (
    LEGAL_SECTION_MAP,
    RELIABILITY_SCORES,
    WEST_BENGAL_DISTRICTS,
)
from hybrid_legal_dashboard.schemas import LegalRecord, SourceRecord


COMPANY_PATTERN = re.compile(
    r"\b([A-Z][A-Za-z0-9&.'-]*(?:\s+[A-Z][A-Za-z0-9&.'-]*)*\s+"
    r"(?:Private Limited|Pvt(?:\.|\s)?Ltd|Limited|Ltd|LLP|Corporation|Corp|"
    r"Industries|Enterprises|Group))\b"
)
SECTION_PATTERN = re.compile(r"(?:section|sec\.?)\s*(\d{3,4}[A-Z]?)", re.IGNORECASE)
CIN_PATTERN = re.compile(r"\b[LUF]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}\b")
TITLE_COMPANY_PATTERNS = [
    re.compile(
        r"(?:winding up notification(?: dated .*?)? of|notice for servicing in .*? – )\s+"
        r"(?:M/s\.?\s*)?(.+?)(?:\s+in\s+C\.?\s*P\.?|\s+in\s+CP\b|\s*\(|\s+&\s+ors\.|\s+–\s+reg\.|$)",
        re.IGNORECASE,
    ),
    re.compile(
        r"notification\s*-\s*C\.?P\.?.*?\(\s*(.+?)\s*\)",
        re.IGNORECASE,
    ),
]
VERSUS_PATTERN = re.compile(r"\bvs\.?\b", re.IGNORECASE)
INVALID_COMPANY_PREFIXES = (
    "SFIO",
    "CBI",
    "ED",
    "SEBI",
    "MCA",
    "Police",
    "West Bengal Police",
)
INVALID_COMPANY_TOKENS = {
    "arrested",
    "bust",
    "busted",
    "case",
    "complaint",
    "conduct",
    "files",
    "launched",
    "launches",
    "linked",
    "probe",
    "raids",
    "scam",
    "searches",
}


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_preserve(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        cleaned = normalize_whitespace(value)
        if not cleaned:
            continue
        key = cleaned.upper()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cleaned)
    return deduped


def make_record_id(title: str, url: str) -> str:
    return sha256(f"{title}|{url}".encode("utf-8")).hexdigest()[:16]


def extract_company_name(text: str) -> str:
    if VERSUS_PATTERN.search(text):
        rhs = VERSUS_PATTERN.split(text, maxsplit=1)[-1]
        rhs_match = COMPANY_PATTERN.search(rhs)
        if rhs_match:
            candidate = normalize_whitespace(rhs_match.group(1).strip(" -.,"))
            if candidate and not any(candidate.startswith(prefix) for prefix in INVALID_COMPANY_PREFIXES):
                return candidate

    for pattern in TITLE_COMPANY_PATTERNS:
        match = pattern.search(text)
        if match:
            candidate = normalize_whitespace(match.group(1).strip(" -.,"))
            if candidate and not any(candidate.startswith(prefix) for prefix in INVALID_COMPANY_PREFIXES):
                return candidate

    for match in COMPANY_PATTERN.finditer(text):
        candidate = normalize_whitespace(match.group(1))
        lowered = candidate.lower()
        if any(candidate.startswith(prefix) for prefix in INVALID_COMPANY_PREFIXES):
            continue
        if any(token in lowered.split() for token in INVALID_COMPANY_TOKENS):
            continue
        return candidate
    return ""


def extract_district(text: str) -> str:
    lowered = text.lower()
    for district in sorted(WEST_BENGAL_DISTRICTS, key=len, reverse=True):
        if district.lower() in lowered:
            return district
    return ""


def extract_legal_sections(text: str) -> list[str]:
    sections = []
    for match in SECTION_PATTERN.findall(text):
        section = match.upper()
        if section not in sections:
            sections.append(section)
    return sections


def section_meanings(sections: list[str]) -> list[str]:
    meanings = []
    for section in sections:
        meaning = LEGAL_SECTION_MAP.get(section, "Section meaning not mapped")
        meanings.append(f"Section {section} - {meaning}")
    return meanings


def detect_violation_type(text: str, sections: list[str]) -> str:
    lowered = text.lower()
    section_set = set(sections)
    if "447" in section_set or "fraud" in lowered:
        return "fraud"
    if {"467", "468", "471"} & section_set or "forg" in lowered:
        return "forgery"
    if {"406", "409"} & section_set or "breach of trust" in lowered:
        return "criminal breach of trust"
    if "420" in section_set or "cheating" in lowered:
        return "cheating"
    if "struck off" in lowered or "striking off" in lowered or ("248" in section_set and "company" in lowered):
        return "struck_off"
    if "disqualification" in lowered or "director disqualification" in lowered or "164" in section_set:
        return "director_disqualification"
    if "liquidation" in lowered or "winding up" in lowered or "dissolution" in lowered:
        return "liquidation"
    if "dormant under section 455" in lowered or ("dormant" in lowered and "455" in section_set):
        return "dormant_status"
    if "insolvency" in lowered or "resolution plan" in lowered or "corporate debtor" in lowered:
        return "insolvency"
    if "launder" in lowered:
        return "money laundering"
    return "other"


def score_record_quality(source: SourceRecord, record: LegalRecord) -> tuple[float, bool]:
    score = 0.0
    score += 10.0 if source.title else 0.0
    score += 10.0 if source.url else 0.0
    score += 10.0 if len(source.snippet) >= 80 else 4.0 if source.snippet else 0.0
    score += 18.0 if record.company_name else 0.0
    score += 14.0 if record.cin_candidates else 0.0
    score += 10.0 if record.district else 0.0
    score += 18.0 if record.legal_sections else 0.0
    score += 8.0 if record.violation_type and record.violation_type != "other" else 0.0
    score += 10.0 if source.source_type in {"pdf_row", "official_gateway", "html_listing", "registry_company_profile"} else 0.0
    score += 8.0 if source.company_status else 0.0
    score += 8.0 if source.incorporation_date else 0.0
    score += 6.0 if source.last_balance_sheet_date else 0.0
    score += 4.0 if source.last_agm_date else 0.0
    score += 6.0 if source.registered_address else 0.0
    score += RELIABILITY_SCORES.get(source.reliability_label, 0.3) * 16.0

    usable_threshold = 48.0 if record.company_name and record.cin_candidates and source.reliability_label == "official" else 55.0
    usable = bool(record.record_id and source.title and source.url and score >= usable_threshold)
    return round(score, 2), usable


def source_to_legal_record(source: SourceRecord) -> LegalRecord:
    structured_fields = [
        source.company_name,
        source.cin,
        source.company_status,
        source.incorporation_date,
        source.last_agm_date,
        source.last_balance_sheet_date,
        source.registered_address,
        source.authorized_capital,
        source.paid_up_capital,
        source.nic_code,
        source.industry_description,
        source.roc_office,
        source.listed_status,
    ]
    combined_text = normalize_whitespace(f"{source.title} {source.snippet} {' '.join(value for value in structured_fields if value)}")
    company_name = source.company_name or extract_company_name(combined_text)
    district = extract_district(combined_text)
    legal_sections = extract_legal_sections(combined_text)
    legal_meanings = section_meanings(legal_sections)
    violation_type = detect_violation_type(combined_text, legal_sections)
    cin_candidates = _dedupe_preserve(([source.cin] if source.cin else []) + CIN_PATTERN.findall(combined_text))
    subject_label = company_name or source.title

    record = LegalRecord(
        record_id=make_record_id(source.title, source.url),
        title=normalize_whitespace(source.title),
        url=source.url,
        source_name=source.source_name,
        source_reliability_label=source.reliability_label,
        source_reliability_score=RELIABILITY_SCORES.get(source.reliability_label, 0.3),
        query_text=source.query_text,
        fetched_at=source.fetched_at,
        published_at=source.published_at,
        company_name=company_name,
        district=district,
        legal_sections=legal_sections,
        legal_section_meanings=legal_meanings,
        violation_type=violation_type,
        cin_candidates=cin_candidates,
        subject_label=subject_label,
        snippet=normalize_whitespace(source.snippet),
        has_company_name=bool(company_name),
        has_district=bool(district),
        has_legal_section=bool(legal_sections),
        source_provider=source.source_provider,
        company_status=normalize_whitespace(source.company_status),
        incorporation_date=normalize_whitespace(source.incorporation_date),
        last_agm_date=normalize_whitespace(source.last_agm_date),
        last_balance_sheet_date=normalize_whitespace(source.last_balance_sheet_date),
        registered_address=normalize_whitespace(source.registered_address),
        authorized_capital=normalize_whitespace(source.authorized_capital),
        paid_up_capital=normalize_whitespace(source.paid_up_capital),
        nic_code=normalize_whitespace(source.nic_code),
        industry_description=normalize_whitespace(source.industry_description),
        roc_office=normalize_whitespace(source.roc_office),
        listed_status=normalize_whitespace(source.listed_status),
        profile_last_updated=normalize_whitespace(source.profile_last_updated),
    )
    quality_score, usable = score_record_quality(source, record)
    record.record_quality_score = quality_score
    record.usable_for_analytics = usable
    return record


def build_legal_records(sources: list[SourceRecord]) -> list[LegalRecord]:
    return [source_to_legal_record(source) for source in sources]
