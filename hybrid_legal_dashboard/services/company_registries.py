from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime
import re
import time
from typing import Optional
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup

from hybrid_legal_dashboard.config import (
    DEFAULT_COMPANY_PROFILE_LIMIT,
    DEFAULT_INSTA_DISCOVERY_PAGE_DEPTH,
    DEFAULT_INSTA_ENRICHMENT_LIMIT,
    RAW_DATA_DIR,
)
from hybrid_legal_dashboard.schemas import SourceRecord
from hybrid_legal_dashboard.services.ingestion import DEFAULT_HEADERS, DEFAULT_TIMEOUT


ZAUBA_BASE_URL = "https://www.zaubacorp.com"
INSTA_BASE_URL = "https://www.instafinancials.com"
CIN_PATTERN = re.compile(r"\b[LUF]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}\b")

STATUS_PRIORITY = {
    "under liquidation": 8,
    "liquidated": 8,
    "under process of striking off": 7,
    "under process of strike off": 7,
    "strike off": 6,
    "dissolved": 6,
    "dormant under section 455": 5,
    "converted to llp": 4,
    "converted to llp and dissolved": 4,
    "amalgamated": 3,
    "active": 1,
}

INSTA_SESSION_RESET_INTERVAL = 12
INSTA_REQUEST_PAUSE_SECONDS = 0.2
INSTA_RETRY_PAUSE_SECONDS = 1.0
INSTA_PROFILE_MIN_FIELDS = 3
INSTA_PROFILE_MARKERS = (
    "The current status of the company is",
    "Company Status",
    "Incorp. Date",
    "Corporate Identification Number (CIN) is",
)
INSTA_DIRECTORY_SOURCE_NAME = "InstaFinancials West Bengal A-Z Directory"
INSTA_LOCAL_DIR = RAW_DATA_DIR / "insta"


@dataclass
class RegistryDirectoryConfig:
    name: str
    base_url: str
    query_text: str
    pages: int = 1
    balance_sheet_year: int = 0
    status_hint: str = ""


@dataclass
class RegistryCandidate:
    company_name: str
    cin: str
    company_url: str
    company_status: str = ""
    registered_address: str = ""
    paid_up_capital: str = ""
    discovery_rank: int = 0
    discovery_tags: set[str] = field(default_factory=set)
    discovery_years: set[int] = field(default_factory=set)
    query_texts: set[str] = field(default_factory=set)


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "-", value or "")
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned.lower()


def _normalize_date(value: str) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        return ""

    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d %b %Y", "%d %B %Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return cleaned


def _clean_capital(value: str) -> str:
    cleaned = _clean_text(value).replace("Rs.", "").replace("₹", "").strip()
    if not cleaned:
        return ""
    if re.fullmatch(r"[0-9,]+(?:\.[0-9]+)?", cleaned):
        return f"₹ {cleaned}"
    return cleaned


def _capital_numeric(value: str) -> float:
    cleaned = _clean_capital(value).replace("₹", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _search(pattern: str, text: str, flags: int = re.IGNORECASE) -> str:
    match = re.search(pattern, text, flags | re.DOTALL)
    return _clean_text(match.group(1)) if match else ""


def _directory_page_url(base_url: str, page_number: int) -> str:
    if page_number <= 1:
        return base_url
    return base_url.replace("-company.html", f"/p-{page_number}-company.html")


def build_zauba_directory_configs(start_year: int, end_year: int) -> list[RegistryDirectoryConfig]:
    selected_years = [
        year
        for year in [2023, 2022, 2021, 2020, 2018, 2016, 2014, 2012, 2010, 2008, 2006, 2005, 2004, 2002, 2000]
        if start_year <= year <= end_year
    ]

    directories = [
        RegistryDirectoryConfig(
            name="Zauba RoC Kolkata Base",
            base_url="https://www.zaubacorp.com/companies-list/roc-RoC-Kolkata-company.html",
            query_text="zauba roc kolkata company directory",
            pages=4,
        ),
        RegistryDirectoryConfig(
            name="Zauba RoC Kolkata Strike Off",
            base_url="https://www.zaubacorp.com/companies-list/status-Strike%20Off/roc-RoC-Kolkata-company.html",
            query_text="zauba roc kolkata strike off companies",
            pages=4,
            status_hint="Strike Off",
        ),
        RegistryDirectoryConfig(
            name="Zauba RoC Kolkata Under Liquidation",
            base_url="https://www.zaubacorp.com/companies-list/status-Under%20Liquidation/roc-RoC-Kolkata-company.html",
            query_text="zauba roc kolkata under liquidation companies",
            pages=3,
            status_hint="Under Liquidation",
        ),
        RegistryDirectoryConfig(
            name="Zauba RoC Kolkata Under Process Of Striking Off",
            base_url="https://www.zaubacorp.com/companies-list/status-Under%20Process%20of%20Striking%20Off/roc-RoC-Kolkata-company.html",
            query_text="zauba roc kolkata under process of striking off companies",
            pages=3,
            status_hint="Under Process of Striking Off",
        ),
        RegistryDirectoryConfig(
            name="Zauba RoC Kolkata Dormant Under Section 455",
            base_url="https://www.zaubacorp.com/companies-list/status-Dormant%20under%20section%20455/roc-RoC-Kolkata-company.html",
            query_text="zauba roc kolkata dormant under section 455 companies",
            pages=2,
            status_hint="Dormant under section 455",
        ),
        RegistryDirectoryConfig(
            name="Zauba RoC Kolkata Converted To LLP",
            base_url="https://www.zaubacorp.com/companies-list/status-Converted%20to%20LLP/roc-RoC-Kolkata-company.html",
            query_text="zauba roc kolkata converted to llp companies",
            pages=2,
            status_hint="Converted to LLP",
        ),
    ]

    for year in selected_years:
        directories.append(
            RegistryDirectoryConfig(
                name=f"Zauba RoC Kolkata Balance Sheet {year}",
                base_url=f"https://www.zaubacorp.com/companies-list/balancesheetdate-{year}-03-31/roc-RoC-Kolkata-company.html",
                query_text=f"zauba roc kolkata balance sheet {year}",
                pages=2 if year >= 2020 else 1,
                balance_sheet_year=year,
            )
        )

    return directories


def _insta_company_list_url(letter: str, page_number: int) -> str:
    return f"{INSTA_BASE_URL}/Companies/{letter}/CompanyList_{letter}{page_number}.html"


def _name_from_company_url(url: str) -> str:
    tail = url.rstrip("/").split("/")[-1]
    tail = re.sub(r"-[LUF]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}.*$", "", tail, flags=re.IGNORECASE)
    return _clean_text(tail.replace("-", " ").replace("_", " ")).upper()


def _local_insta_directory_pages() -> list[tuple[int, str, str]]:
    pages: list[tuple[int, str, str]] = []
    for path in sorted(INSTA_LOCAL_DIR.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".html", ".htm"}:
            continue
        match = re.search(r"CompanyList_([A-Z])(\d+)\.html?$", path.name, re.IGNORECASE)
        if not match:
            continue
        letter = match.group(1).upper()
        page_number = int(match.group(2))
        pages.append((page_number, letter, path.read_text(encoding="utf-8", errors="ignore")))
    return sorted(pages, key=lambda item: (item[0], item[1]))


def _extract_cin(value: str) -> str:
    match = CIN_PATTERN.search(value or "")
    return match.group(0).upper() if match else ""


def _normalize_company_url(value: str, company_name: str = "", cin: str = "") -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        urls = _insta_urls(company_name, cin) if company_name and cin else []
        return urls[0] if urls else ""
    if cleaned.startswith(("http://", "https://")):
        return cleaned
    if "/company/" in cleaned.lower():
        return urljoin(INSTA_BASE_URL, cleaned)
    return cleaned


def _store_candidate(merged: dict[str, RegistryCandidate], candidate: RegistryCandidate) -> None:
    key = candidate.cin or candidate.company_url
    if key in merged:
        merged[key] = _merge_candidate(merged[key], candidate)
    else:
        merged[key] = candidate


def _local_insta_csv_candidates() -> list[RegistryCandidate]:
    candidates: list[RegistryCandidate] = []
    rank = 0
    for path in sorted(INSTA_LOCAL_DIR.rglob("*.csv")):
        with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                continue
            for row in reader:
                company_name = _clean_text(
                    row.get("company_name")
                    or row.get("name")
                    or row.get("title")
                    or row.get("legal_name")
                    or ""
                )
                raw_url = row.get("url") or row.get("company_url") or row.get("link") or row.get("profile_url") or ""
                cin = _extract_cin(row.get("cin", "")) or _extract_cin(raw_url) or _extract_cin(company_name)
                if not cin or cin[6:8] != "WB":
                    continue
                url = _normalize_company_url(raw_url, company_name=company_name, cin=cin)
                if not company_name:
                    company_name = _name_from_company_url(url)
                if not company_name:
                    continue

                rank += 1
                candidate = RegistryCandidate(
                    company_name=company_name,
                    cin=cin,
                    company_url=url,
                    company_status=_clean_text(row.get("company_status", "")),
                    registered_address=_clean_text(row.get("registered_address", "")),
                    paid_up_capital=_clean_capital(row.get("paid_up_capital", "")),
                    discovery_rank=rank,
                )
                candidate.discovery_tags.add(f"CSV {path.name}")
                query_text = _clean_text(row.get("query_text", "")) or "local instafinancials company seed csv"
                candidate.query_texts.add(query_text)
                candidates.append(candidate)
    return candidates


def _merge_insta_candidate(
    merged: dict[str, RegistryCandidate],
    *,
    letter: str,
    page_number: int,
    html: str,
    rank: int,
) -> int:
    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "")
        if "/company/" not in href.lower():
            continue
        match = CIN_PATTERN.search(href)
        if not match:
            continue
        cin = match.group(0).upper()
        if cin[6:8] != "WB":
            continue

        full_url = urljoin(INSTA_BASE_URL, href)
        rank += 1
        candidate = RegistryCandidate(
            company_name=_clean_text(anchor.get_text(" ", strip=True)) or _name_from_company_url(full_url),
            cin=cin,
            company_url=full_url,
            discovery_rank=rank,
        )
        candidate.discovery_tags.add(f"Insta {letter}{page_number}")
        candidate.query_texts.add("instafinancials west bengal company directory")
        _store_candidate(merged, candidate)
    return rank


def discover_instafinancials_candidates(
    max_candidate_pool: Optional[int] = None,
    page_depth: int = DEFAULT_INSTA_DISCOVERY_PAGE_DEPTH,
) -> list[RegistryCandidate]:
    merged: dict[str, RegistryCandidate] = {}
    rank = 0

    local_csv_candidates = _local_insta_csv_candidates()
    for candidate in local_csv_candidates:
        rank = max(rank, candidate.discovery_rank)
        _store_candidate(merged, candidate)

    local_pages = _local_insta_directory_pages()
    if local_pages or local_csv_candidates:
        for page_number, letter, html in local_pages:
            rank = _merge_insta_candidate(
                merged,
                letter=letter,
                page_number=page_number,
                html=html,
                rank=rank,
            )
            if max_candidate_pool and len(merged) >= max_candidate_pool:
                break
        sorted_candidates = sorted(merged.values(), key=lambda candidate: candidate.discovery_rank)
        return sorted_candidates[:max_candidate_pool] if max_candidate_pool else sorted_candidates

    session = _insta_session()
    merged = {}
    rank = 0
    page_requests = 0
    active_letters = {letter: True for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"}
    page_number = 1
    page_limit = max(1, page_depth) if page_depth and page_depth > 0 else None

    while any(active_letters.values()):
        page_found_candidates = False
        for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            if not active_letters[letter]:
                continue
            if page_requests and page_requests % INSTA_SESSION_RESET_INTERVAL == 0:
                session = _insta_session()
                time.sleep(INSTA_REQUEST_PAUSE_SECONDS)
            url = _insta_company_list_url(letter, page_number)
            try:
                html = _fetch_html(session, url)
            except requests.RequestException:
                active_letters[letter] = False
                continue
            page_requests += 1

            prior_count = len(merged)
            rank = _merge_insta_candidate(
                merged,
                letter=letter,
                page_number=page_number,
                html=html,
                rank=rank,
            )
            page_candidate_count = len(merged) - prior_count

            if page_candidate_count == 0:
                active_letters[letter] = False
            else:
                page_found_candidates = True

            if max_candidate_pool and len(merged) >= max_candidate_pool:
                break
        if max_candidate_pool and len(merged) >= max_candidate_pool:
            break
        if page_limit is not None and page_number >= page_limit:
            break
        if not page_found_candidates:
            break
        page_number += 1

    return sorted(merged.values(), key=lambda candidate: candidate.discovery_rank)


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def _insta_session() -> requests.Session:
    session = _session()
    session.headers.update(
        {
            "Referer": f"{INSTA_BASE_URL}/",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return session


def _fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    return response.text


def _extract_zauba_listing_candidates(
    *,
    config: RegistryDirectoryConfig,
    page_number: int,
    html: str,
    rank_offset: int,
) -> list[RegistryCandidate]:
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tr")
    candidates: list[RegistryCandidate] = []
    row_rank = rank_offset

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        cin = _clean_text(cells[0].get_text(" ", strip=True))
        if not CIN_PATTERN.search(cin):
            continue

        company_name = _clean_text(cells[1].get_text(" ", strip=True))
        if not company_name:
            continue

        first_link = row.find("a", href=True)
        if first_link is None:
            continue

        row_rank += 1
        candidate = RegistryCandidate(
            company_name=company_name,
            cin=cin,
            company_url=urljoin(ZAUBA_BASE_URL, first_link["href"]),
            company_status=_clean_text(cells[2].get_text(" ", strip=True)),
            paid_up_capital=_clean_capital(cells[3].get_text(" ", strip=True)),
            registered_address=_clean_text(cells[4].get_text(" ", strip=True)),
            discovery_rank=row_rank,
        )
        candidate.discovery_tags.add(config.name)
        candidate.query_texts.add(config.query_text)
        if config.balance_sheet_year:
            candidate.discovery_years.add(config.balance_sheet_year)
        if config.status_hint:
            candidate.discovery_tags.add(config.status_hint)
        candidates.append(candidate)

    return candidates


def _merge_candidate(target: RegistryCandidate, candidate: RegistryCandidate) -> RegistryCandidate:
    if not target.company_status and candidate.company_status:
        target.company_status = candidate.company_status
    if not target.registered_address and candidate.registered_address:
        target.registered_address = candidate.registered_address
    if _capital_numeric(candidate.paid_up_capital) > _capital_numeric(target.paid_up_capital):
        target.paid_up_capital = candidate.paid_up_capital
    target.discovery_rank = min(target.discovery_rank, candidate.discovery_rank)
    target.discovery_tags.update(candidate.discovery_tags)
    target.discovery_years.update(candidate.discovery_years)
    target.query_texts.update(candidate.query_texts)
    return target


def _candidate_priority(candidate: RegistryCandidate) -> tuple[int, int, int]:
    status_weight = STATUS_PRIORITY.get(candidate.company_status.lower(), 0)
    historical_bonus = 4 if any(year and year <= 2010 for year in candidate.discovery_years) else 0
    variety_bonus = min(5, len(candidate.discovery_tags))
    return (status_weight + historical_bonus + variety_bonus, len(candidate.discovery_years), -candidate.discovery_rank)


def discover_zauba_candidates(
    start_year: int,
    end_year: int,
    max_candidate_pool: int,
) -> list[RegistryCandidate]:
    session = _session()
    merged: dict[str, RegistryCandidate] = {}
    rank_offset = 0

    for config in build_zauba_directory_configs(start_year, end_year):
        for page_number in range(1, config.pages + 1):
            url = _directory_page_url(config.base_url, page_number)
            try:
                html = _fetch_html(session, url)
            except requests.RequestException:
                continue

            for candidate in _extract_zauba_listing_candidates(
                config=config,
                page_number=page_number,
                html=html,
                rank_offset=rank_offset,
            ):
                rank_offset = max(rank_offset, candidate.discovery_rank)
                key = candidate.cin or candidate.company_url
                if key in merged:
                    merged[key] = _merge_candidate(merged[key], candidate)
                else:
                    merged[key] = candidate

        if len(merged) >= max_candidate_pool:
            break

    candidates = list(merged.values())
    return sorted(candidates, key=_candidate_priority, reverse=True)


def _parse_zauba_profile(html: str, fallback: RegistryCandidate) -> dict[str, str]:
    text = BeautifulSoup(html, "html.parser").get_text("\n", strip=True)
    company_name = _search(r"#?\s*([A-Z0-9&.,'() \-]+)\s*\|\s*ZaubaCorp", text) or fallback.company_name
    cin = _search(r"\(CIN:\s*([A-Z0-9]+)\)", text) or _search(r"Corporate Identification Number \(CIN\)\s*\|\s*([A-Z0-9]+)", text)
    if not cin:
        cin = fallback.cin
    company_status = (
        _search(r"Current status of .*? is\s*-\s*([A-Za-z0-9 \-]+)\.", text)
        or _search(r"Company Status\s*\|\s*([A-Za-z0-9 \-]+)", text)
        or fallback.company_status
    )
    incorporation_date = (
        _search(r"incorporated on ([0-9]{2} [A-Za-z]{3} [0-9]{4}|[0-9-]{10})", text)
        or _search(r"Date of Incorporation\s*\|\s*([0-9-]{10})", text)
    )
    last_agm_date = (
        _search(r"AGM\) was last held on ([0-9]{2} [A-Za-z]{3} [0-9]{4}|[0-9-]{10})", text)
        or _search(r"Date of Last Annual General Meeting\s*\|\s*([0-9-]{10})", text)
    )
    last_balance_sheet_date = (
        _search(r"balance sheet was last filed on ([0-9]{2} [A-Za-z]{3} [0-9]{4}|[0-9-]{10})", text)
        or _search(r"Date of Last Filed Balance Sheet\s*\|\s*([0-9-]{10})", text)
    )
    registered_address = (
        _search(r"Registered address of .*? is '?([^']+?)'?\.", text)
        or _search(r"Registered address of .*? is (.+?) Current status of", text)
        or fallback.registered_address
    )
    authorized_capital = (
        _search(r"authorized share capital is Rs\.?\s*([0-9,.]+)", text)
        or _search(r"Authorised Share Capital\s*\|\s*([₹0-9,.\s]+)", text)
    )
    paid_up_capital = (
        _search(r"paid up capital is Rs\.?\s*([0-9,.]+)", text)
        or _search(r"Paid-up Share Capital\s*\|\s*([₹0-9,.\s]+)", text)
        or fallback.paid_up_capital
    )
    nic_code = _search(r"NIC code is\s*([0-9A-Za-z]+)", text) or _search(r"NIC Code:\s*([0-9A-Za-z]+)", text)
    industry_description = _search(r"NIC Description:\s*(.+?)\s*(?:Number of Members|###|Date of Last)", text)
    roc_office = _search(r"ROC\s*\|\s*([A-Za-z \-]+)", text) or _search(
        r"registered at Registrar of Companies,\s*([A-Za-z ]+)\.", text
    )
    listed_status = _search(r"Listed on Stock Exchange\s*\|\s*([A-Za-z ]+)", text)
    profile_last_updated = _search(r"As on:\s*([0-9-]{10})", text)

    return {
        "company_name": company_name or fallback.company_name,
        "cin": cin,
        "company_status": company_status,
        "incorporation_date": _normalize_date(incorporation_date),
        "last_agm_date": _normalize_date(last_agm_date),
        "last_balance_sheet_date": _normalize_date(last_balance_sheet_date),
        "registered_address": registered_address,
        "authorized_capital": _clean_capital(authorized_capital),
        "paid_up_capital": _clean_capital(paid_up_capital),
        "nic_code": nic_code,
        "industry_description": industry_description,
        "roc_office": roc_office.replace("Registrar of Companies,", "RoC-").replace(" ", "-") if roc_office.startswith("Registrar") else roc_office,
        "listed_status": listed_status,
        "profile_last_updated": _normalize_date(profile_last_updated),
    }


def _build_profile_summary(profile: dict[str, str]) -> str:
    parts = []
    for label, key in [
        ("CIN", "cin"),
        ("Status", "company_status"),
        ("Incorporation", "incorporation_date"),
        ("Last AGM", "last_agm_date"),
        ("Last Balance Sheet", "last_balance_sheet_date"),
        ("Authorized Capital", "authorized_capital"),
        ("Paid Up Capital", "paid_up_capital"),
        ("ROC", "roc_office"),
        ("Listed Status", "listed_status"),
        ("NIC", "nic_code"),
        ("Industry", "industry_description"),
        ("Address", "registered_address"),
    ]:
        value = _clean_text(profile.get(key, ""))
        if value:
            parts.append(f"{label}: {value}.")
    return " ".join(parts)


def _zauba_profile_record(profile: dict[str, str], candidate: RegistryCandidate) -> Optional[SourceRecord]:
    company_name = profile.get("company_name") or candidate.company_name
    cin = profile.get("cin") or candidate.cin
    if not company_name or not cin:
        return None
    summary = _build_profile_summary(profile)
    return SourceRecord(
        title=company_name,
        url=candidate.company_url,
        source_name="Zauba RoC Kolkata Company Profile",
        snippet=summary,
        reliability_label="registry_aggregator",
        query_text=" | ".join(sorted(candidate.query_texts)),
        published_at=profile.get("profile_last_updated") or profile.get("last_balance_sheet_date") or profile.get("last_agm_date"),
        source_type="registry_company_profile",
        source_provider="zauba",
        company_name=company_name,
        cin=cin,
        company_status=profile.get("company_status", ""),
        incorporation_date=profile.get("incorporation_date", ""),
        last_agm_date=profile.get("last_agm_date", ""),
        last_balance_sheet_date=profile.get("last_balance_sheet_date", ""),
        registered_address=profile.get("registered_address", ""),
        authorized_capital=profile.get("authorized_capital", ""),
        paid_up_capital=profile.get("paid_up_capital", ""),
        nic_code=profile.get("nic_code", ""),
        industry_description=profile.get("industry_description", ""),
        roc_office=profile.get("roc_office", ""),
        listed_status=profile.get("listed_status", ""),
        profile_last_updated=profile.get("profile_last_updated", ""),
    )


def fetch_zauba_company_profiles(
    *,
    start_year: int,
    end_year: int,
    profile_limit: int = DEFAULT_COMPANY_PROFILE_LIMIT,
) -> list[SourceRecord]:
    profile_limit = max(0, profile_limit)
    if profile_limit == 0:
        return []

    session = _session()
    candidate_pool = discover_zauba_candidates(start_year, end_year, max_candidate_pool=max(profile_limit * 4, 400))
    records: list[SourceRecord] = []

    for candidate in candidate_pool[:profile_limit]:
        try:
            html = _fetch_html(session, candidate.company_url)
        except requests.RequestException:
            continue
        profile = _parse_zauba_profile(html, candidate)
        record = _zauba_profile_record(profile, candidate)
        if record is not None:
            records.append(record)

    return records


def _parse_insta_profile(html: str, company_name: str, cin: str) -> dict[str, str]:
    text = BeautifulSoup(html, "html.parser").get_text("\n", strip=True)
    resolved_name = company_name or _search(r"([A-Z0-9&.,'() \-]+)\s*-\s*[A-Z0-9]{21}", text)
    resolved_cin = _search(r"Corporate Identification Number \(CIN\) is\s*([A-Z0-9]+)", text) or cin
    company_status = _search(r"The current status of the company is\s*([A-Za-z0-9 \-]+)\s*\.", text) or _search(
        r"Company Status\s*([A-Za-z0-9 \-]+)\s*As on", text
    )
    incorporation_date = _search(r"incorporated on\s*([0-9]{1,2} [A-Za-z]{3} [0-9]{4})", text) or _search(
        r"Incorp\. Date\s*([0-9-]{10})", text
    )
    last_agm_date = _search(r"AGM\) was last held on\s*([0-9]{1,2} [A-Za-z]{3} [0-9]{4})", text) or _search(
        r"AGM Date\s*([0-9-]{10})", text
    )
    last_balance_sheet_date = _search(
        r"balance sheet was last filed on\s*([0-9]{1,2} [A-Za-z]{3} [0-9]{4})", text
    ) or _search(r"Balance Sheet Date\s*([0-9-]{10})", text)
    registered_address = _search(r"The registered address of .*? is\s*(.+?)\.", text) or _search(
        r"Address\s*(.+?)\s*Previous Company Names and CINs", text
    )
    authorized_capital = _search(r"authorized share capital is\s*₹?([0-9,.\s]+)", text) or _search(
        r"Authorised Capital\s*\([^)]+\)\s*₹?([0-9,.\s]+)", text
    )
    paid_up_capital = _search(r"paid up capital is\s*₹?([0-9,.\s]+)", text) or _search(
        r"Paid up Capital\s*\([^)]+\)\s*₹?([0-9,.\s]+)", text
    )
    nic_code = _search(r"NIC Code\s*([0-9A-Za-z]+)", text)
    industry_description = _search(r"As per MCA the main line of business is\s*(.+?)\.", text) or _search(
        r"Industry\s*(.+?)\s*NIC Code", text
    )
    roc_office = _search(r"It is registered at\s*(ROC [A-Za-z0-9 \-]+)\.", text) or _search(
        r"It is registered at\s*(RoC-[A-Za-z0-9 \-]+)\.", text
    )
    listed_status = _search(r"classified as [A-Za-z ]+(UnListed|Listed)\s+Indian", text) or _search(
        r"Company Class\s*([A-Za-z ]+)\s*Active Compliant", text
    )
    profile_last_updated = _search(r"Last updated:\s*([0-9-]{10})", text)

    return {
        "company_name": resolved_name,
        "cin": resolved_cin,
        "company_status": company_status,
        "incorporation_date": _normalize_date(incorporation_date),
        "last_agm_date": _normalize_date(last_agm_date),
        "last_balance_sheet_date": _normalize_date(last_balance_sheet_date),
        "registered_address": registered_address,
        "authorized_capital": _clean_capital(authorized_capital),
        "paid_up_capital": _clean_capital(paid_up_capital),
        "nic_code": nic_code,
        "industry_description": industry_description,
        "roc_office": roc_office,
        "listed_status": listed_status,
        "profile_last_updated": _normalize_date(profile_last_updated),
    }


def _insta_urls(company_name: str, cin: str) -> list[str]:
    slug = _slugify(company_name)
    cin = cin.upper()
    return [
        f"{INSTA_BASE_URL}/company/{slug}-{cin}",
        f"{INSTA_BASE_URL}/company/{slug}/{cin}",
    ]


def _insta_directory_record(candidate: RegistryCandidate) -> Optional[SourceRecord]:
    company_name = _clean_text(candidate.company_name)
    cin = _clean_text(candidate.cin).upper()
    if not company_name or not cin:
        return None
    return SourceRecord(
        title=company_name,
        url=candidate.company_url,
        source_name=INSTA_DIRECTORY_SOURCE_NAME,
        snippet=f"CIN: {cin}. Discovered from the InstaFinancials West Bengal A-Z company directory.",
        reliability_label="registry_aggregator",
        query_text=" | ".join(sorted(candidate.query_texts)) or "instafinancials west bengal company directory",
        source_type="registry_directory_row",
        source_provider="instafinancials",
        company_name=company_name,
        cin=cin,
        company_status=candidate.company_status,
        registered_address=candidate.registered_address,
        paid_up_capital=candidate.paid_up_capital,
    )


def _has_insta_profile_payload(parsed: dict[str, str], html: str) -> bool:
    populated_fields = sum(
        1
        for key in [
            "company_status",
            "incorporation_date",
            "last_agm_date",
            "last_balance_sheet_date",
            "registered_address",
            "authorized_capital",
            "paid_up_capital",
            "nic_code",
            "industry_description",
            "roc_office",
            "listed_status",
            "profile_last_updated",
        ]
        if parsed.get(key)
    )
    if populated_fields >= INSTA_PROFILE_MIN_FIELDS:
        return True
    return any(marker in html for marker in INSTA_PROFILE_MARKERS)


def _registry_record_score(record: SourceRecord) -> tuple[int, int]:
    structured_fields = [
        record.company_status,
        record.incorporation_date,
        record.last_agm_date,
        record.last_balance_sheet_date,
        record.registered_address,
        record.authorized_capital,
        record.paid_up_capital,
        record.nic_code,
        record.industry_description,
        record.roc_office,
        record.listed_status,
        record.profile_last_updated,
    ]
    return (
        sum(1 for value in structured_fields if _clean_text(value)),
        1 if record.source_type == "registry_company_profile" else 0,
    )


def fetch_instafinancials_profiles(
    candidates: list[RegistryCandidate],
    limit: int = DEFAULT_INSTA_ENRICHMENT_LIMIT,
) -> list[SourceRecord]:
    limit = max(0, limit)
    if limit == 0:
        return []

    session = _insta_session()
    records: list[SourceRecord] = []
    seen_cins: set[str] = set()
    request_count = 0

    for candidate in candidates:
        if len(records) >= limit:
            break
        cin = candidate.cin.upper()
        if not cin or cin in seen_cins:
            continue

        resolved_url = ""
        parsed: Optional[dict[str, str]] = None
        if request_count and request_count % INSTA_SESSION_RESET_INTERVAL == 0:
            session = _insta_session()
            time.sleep(INSTA_RETRY_PAUSE_SECONDS)

        for attempt in range(3):
            if attempt > 0:
                session = _insta_session()
                time.sleep(INSTA_RETRY_PAUSE_SECONDS * attempt)
            for url in _insta_urls(candidate.company_name, cin):
                try:
                    html = _fetch_html(session, url)
                except requests.RequestException:
                    continue
                request_count += 1
                parsed_candidate = _parse_insta_profile(html, candidate.company_name, cin)
                if _has_insta_profile_payload(parsed_candidate, html):
                    parsed = parsed_candidate
                    resolved_url = url
                    break
            if parsed is not None:
                break

        if parsed is None:
            continue

        if not parsed.get("company_name") or not parsed.get("cin"):
            continue

        summary = _build_profile_summary(parsed)
        records.append(
            SourceRecord(
                title=parsed["company_name"],
                url=resolved_url,
                source_name="InstaFinancials Company Profile",
                snippet=summary,
                reliability_label="registry_aggregator",
                query_text=" | ".join(sorted(candidate.query_texts)) or f"instafinancials company profile {cin}",
                published_at=parsed.get("profile_last_updated") or parsed.get("last_balance_sheet_date"),
                source_type="registry_company_profile",
                source_provider="instafinancials",
                company_name=parsed.get("company_name", ""),
                cin=parsed.get("cin", ""),
                company_status=parsed.get("company_status", ""),
                incorporation_date=parsed.get("incorporation_date", ""),
                last_agm_date=parsed.get("last_agm_date", ""),
                last_balance_sheet_date=parsed.get("last_balance_sheet_date", ""),
                registered_address=parsed.get("registered_address", ""),
                authorized_capital=parsed.get("authorized_capital", ""),
                paid_up_capital=parsed.get("paid_up_capital", ""),
                nic_code=parsed.get("nic_code", ""),
                industry_description=parsed.get("industry_description", ""),
                roc_office=parsed.get("roc_office", ""),
                listed_status=parsed.get("listed_status", ""),
                profile_last_updated=parsed.get("profile_last_updated", ""),
            )
        )
        seen_cins.add(cin)
        time.sleep(INSTA_REQUEST_PAUSE_SECONDS)

    return records


def collect_registry_company_sources(
    *,
    start_year: int,
    end_year: int,
    company_profile_limit: int = DEFAULT_COMPANY_PROFILE_LIMIT,
    insta_enrichment_limit: int = DEFAULT_INSTA_ENRICHMENT_LIMIT,
) -> list[SourceRecord]:
    candidate_pool = discover_instafinancials_candidates(
        max_candidate_pool=None,
        page_depth=DEFAULT_INSTA_DISCOVERY_PAGE_DEPTH,
    )
    directory_records = [
        record
        for candidate in candidate_pool
        if (record := _insta_directory_record(candidate)) is not None
    ]
    insta_profiles = fetch_instafinancials_profiles(
        candidate_pool,
        limit=max(company_profile_limit * 2, company_profile_limit),
    )

    filtered_profiles = []
    for profile in insta_profiles:
        years = []
        for value in [profile.incorporation_date, profile.last_balance_sheet_date, profile.last_agm_date, profile.profile_last_updated]:
            if not value:
                continue
            match = re.match(r"(\d{4})", value)
            if match:
                years.append(int(match.group(1)))
        if years and max(years) < start_year:
            continue
        filtered_profiles.append(profile)

    # Keep the enrichment limit for future secondary providers while the collector is Insta-led.
    if insta_enrichment_limit and len(filtered_profiles) > company_profile_limit:
        filtered_profiles = filtered_profiles[:company_profile_limit]

    merged_by_key: dict[str, SourceRecord] = {}
    for record in directory_records + filtered_profiles:
        key = (_clean_text(record.cin).upper() or _clean_text(record.title).lower() or record.url.lower())
        existing = merged_by_key.get(key)
        if existing is None or _registry_record_score(record) > _registry_record_score(existing):
            merged_by_key[key] = record
    return sorted(merged_by_key.values(), key=lambda record: (_clean_text(record.company_name or record.title).lower(), record.url.lower()))
