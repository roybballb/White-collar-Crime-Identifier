from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import re
from typing import Optional

import pandas as pd

from hybrid_legal_dashboard.config import (
    DEFAULT_COMPANY_PROFILE_LIMIT,
    DEFAULT_HISTORICAL_END_YEAR,
    DEFAULT_HISTORICAL_SEARCH_QUERIES,
    DEFAULT_HISTORICAL_START_YEAR,
    DEFAULT_HISTORICAL_WINDOW_YEARS,
    DEFAULT_INSTA_ENRICHMENT_LIMIT,
    DEFAULT_LIVE_SEARCH_QUERIES,
    DEFAULT_SOURCE_LIMIT,
    OUTPUT_DIR,
    RAW_DATA_DIR,
)
from hybrid_legal_dashboard.demo_data import demo_sources
from hybrid_legal_dashboard.schemas import SourceRecord
from hybrid_legal_dashboard.services.analytics import build_cluster_summary, build_company_reports, build_entity_rollup
from hybrid_legal_dashboard.services.company_registries import collect_registry_company_sources
from hybrid_legal_dashboard.services.extraction import build_legal_records
from hybrid_legal_dashboard.services.ingestion import (
    PageConfig,
    RSSFeedConfig,
    StaticSourceConfig,
    build_google_news_rss_url,
    collect_sources,
)
from hybrid_legal_dashboard.services.storage import records_to_frame, write_outputs, write_run_metadata


def _normalize_pdf_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _extract_pdf_date(value: str) -> str:
    raw = value.lower()
    match = re.search(r"(\d{2})(\d{2})(\d{4})", raw)
    if not match:
        match = re.search(r"(\d{4})(\d{2})(\d{2})", raw)
        if not match:
            return ""
        year, month, day = match.groups()
    else:
        day, month, year = match.groups()

    try:
        return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def _source_url_filename(url: str) -> str:
    return url.rsplit("/", 1)[-1].split("?", 1)[0]


def _score_local_pdf_match(path: Path, config: StaticSourceConfig) -> int:
    haystack = _normalize_pdf_key(str(path))
    score = 0

    official_filename = _source_url_filename(config.url)
    official_key = _normalize_pdf_key(Path(official_filename).stem)
    if official_key and official_key in haystack:
        score += 12

    if config.parser_kind == "mca_director_disqualification_pdf":
        if "director" in haystack:
            score += 4
        if "disqual" in haystack or "164" in haystack:
            score += 5
    elif config.parser_kind == "mca_struck_off_pdf":
        if "stk5" in haystack or "stk7" in haystack:
            score += 5
        if "struck" in haystack or "strikeoff" in haystack or "dissolution" in haystack or "248" in haystack:
            score += 4
        if "roc" in haystack or "company" in haystack or "companies" in haystack:
            score += 2

    return score


def _guess_local_mca_pdf_config(path: Path) -> Optional[StaticSourceConfig]:
    normalized = _normalize_pdf_key(str(path))
    if not any(
        token in normalized
        for token in ("mca", "roc", "stk", "director", "kolkata", "westbengal", "company", "companies")
    ):
        return None

    title = f"Local MCA PDF - {path.stem.replace('_', ' ').replace('-', ' ')}"
    source_name = f"Local MCA PDF - {path.stem}"
    snippet = "Locally supplied MCA PDF for West Bengal company or director actions."
    query_text = "local mca pdf west bengal"
    parser_kind = ""
    max_items = 1

    if "director" in normalized and ("disqual" in normalized or "164" in normalized):
        title = "Local MCA PDF - Director Disqualification"
        source_name = "Local MCA - Disqualified Directors"
        snippet = "Locally supplied MCA PDF for director disqualification under section 164."
        query_text = "local mca director disqualification section 164"
        parser_kind = "mca_director_disqualification_pdf"
        max_items = 1200
    elif any(token in normalized for token in ("stk", "struck", "strikeoff", "dissolution", "248", "company", "companies", "list")):
        title = "Local MCA PDF - Company Action List"
        source_name = "Local MCA - Company Action List"
        snippet = "Locally supplied MCA PDF for struck-off, dissolution, or company-action notices."
        query_text = "local mca company action list section 248"
        parser_kind = "mca_struck_off_pdf"
        max_items = 3000

    return StaticSourceConfig(
        title=title,
        url="",
        source_name=source_name,
        snippet=snippet,
        reliability_label="official",
        query_text=query_text,
        published_at=_extract_pdf_date(path.name),
        source_type="pdf_row" if parser_kind else "document",
        parser_kind=parser_kind,
        max_items=max_items,
        local_path=str(path),
    )


def _attach_local_mca_pdfs(static_sources: list[StaticSourceConfig]) -> list[StaticSourceConfig]:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    local_pdfs = sorted(path for path in RAW_DATA_DIR.rglob("*.pdf") if path.is_file())
    if not local_pdfs:
        return static_sources

    matched_paths: set[Path] = set()
    for config in static_sources:
        if not config.parser_kind.startswith("mca_"):
            continue

        best_path = None
        best_score = 0
        for path in local_pdfs:
            if path in matched_paths:
                continue
            score = _score_local_pdf_match(path, config)
            if score > best_score:
                best_score = score
                best_path = path

        if best_path and best_score >= 6:
            config.local_path = str(best_path)
            if config.parser_kind == "mca_director_disqualification_pdf":
                config.max_items = max(config.max_items, 1200)
            elif config.parser_kind == "mca_struck_off_pdf":
                config.max_items = max(config.max_items, 3000)
            matched_paths.add(best_path)

    extra_local_configs: list[StaticSourceConfig] = []
    for path in local_pdfs:
        if path in matched_paths:
            continue
        guessed = _guess_local_mca_pdf_config(path)
        if guessed is not None:
            extra_local_configs.append(guessed)

    return static_sources + extra_local_configs


def _split_env_list(name: str) -> list[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    return [value.strip() for value in raw.split("||") if value.strip()]


def _source_limit() -> int:
    raw = os.getenv("LEGAL_DASHBOARD_SOURCE_LIMIT", str(DEFAULT_SOURCE_LIMIT))
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_SOURCE_LIMIT


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _normalize_year_range(start_year: int, end_year: int) -> tuple[int, int]:
    start = max(1900, start_year)
    end = min(2100, end_year)
    if start > end:
        start, end = end, start
    return start, end


def _year_windows(start_year: int, end_year: int, window_years: int) -> list[tuple[int, int]]:
    start_year, end_year = _normalize_year_range(start_year, end_year)
    size = max(1, window_years)
    windows: list[tuple[int, int]] = []
    current = start_year
    while current <= end_year:
        window_end = min(end_year, current + size - 1)
        windows.append((current, window_end))
        current = window_end + 1
    return windows


def _historical_queries(
    start_year: int,
    end_year: int,
    window_years: int,
) -> list[tuple[str, str]]:
    queries = _split_env_list("LEGAL_DASHBOARD_HISTORICAL_QUERIES") or DEFAULT_HISTORICAL_SEARCH_QUERIES
    feed_queries: list[tuple[str, str]] = []
    for window_start, window_end in _year_windows(start_year, end_year, window_years):
        before_year = window_end + 1
        for base_query in queries:
            query = f"{base_query} after:{window_start}-01-01 before:{before_year}-01-01"
            label = f"{base_query} [{window_start}-{window_end}]"
            feed_queries.append((label, query))
    return feed_queries


def _merge_sources(*source_batches: list[SourceRecord]) -> list[SourceRecord]:
    merged: list[SourceRecord] = []
    seen: set[tuple[str, str]] = set()
    for batch in source_batches:
        for record in batch:
            key = (record.title.strip().lower(), record.url.strip().lower())
            if not record.title or not record.url or key in seen:
                continue
            seen.add(key)
            merged.append(record)
    return merged


def build_mca_static_sources() -> list[StaticSourceConfig]:
    return [
        StaticSourceConfig(
            title="ROC West Bengal disqualified directors under section 164(2)(a)",
            url="https://www.mca.gov.in/Ministry/pdf/directordisqualificationKolkatta_16112017.pdf",
            source_name="MCA ROC Kolkata - Disqualified Directors",
            snippet=(
                "Official MCA PDF relating to disqualified directors associated with ROC Kolkata / West Bengal "
                "under section 164(2)(a)."
            ),
            reliability_label="official",
            query_text="roc kolkata disqualified directors section 164(2)(a)",
            published_at="2017-11-16",
            source_type="pdf_row",
            parser_kind="mca_director_disqualification_pdf",
            max_items=1200,
        ),
        StaticSourceConfig(
            title="ROC West Bengal public notice of striking off under section 248(1)",
            url="https://www.mca.gov.in/Ministry/pdf/STK5RocWB_03072018.pdf",
            source_name="MCA ROC Kolkata - Proposed Struck Off Companies",
            snippet=(
                "Official MCA STK-5 public notice for ROC West Bengal proposing the striking off of companies "
                "under section 248(1) of the Companies Act, 2013."
            ),
            reliability_label="official",
            query_text="roc west bengal stk-5 proposed struck off companies section 248(1)",
            published_at="2018-07-03",
            source_type="pdf_row",
            parser_kind="mca_struck_off_pdf",
            max_items=3000,
        ),
        StaticSourceConfig(
            title="ROC West Bengal notice of striking off and dissolution under section 248",
            url="https://www.mca.gov.in/Ministry/pdf/ListROCKolkata_21072017.pdf",
            source_name="MCA ROC Kolkata - Struck Off Companies",
            snippet=(
                "Official MCA STK-7 notice for ROC West Bengal regarding striking off and dissolution of companies "
                "under section 248 of the Companies Act, 2013."
            ),
            reliability_label="official",
            query_text="roc kolkata struck off companies section 248 stk-7",
            published_at="2017-07-21",
            source_type="pdf_row",
            parser_kind="mca_struck_off_pdf",
            max_items=3000,
        ),
    ]


def _default_live_sources(
    *,
    include_historical: bool = False,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    historical_window_years: Optional[int] = None,
) -> tuple[list[RSSFeedConfig], list[PageConfig], list[StaticSourceConfig]]:
    source_limit = _source_limit()
    queries = _split_env_list("LEGAL_DASHBOARD_SEARCH_QUERIES") or DEFAULT_LIVE_SEARCH_QUERIES
    extra_rss_urls = _split_env_list("LEGAL_DASHBOARD_EXTRA_RSS_URLS")
    include_historical = include_historical or _bool_env("LEGAL_DASHBOARD_INCLUDE_HISTORICAL", False)
    start_year = start_year if start_year is not None else _int_env("LEGAL_DASHBOARD_START_YEAR", DEFAULT_HISTORICAL_START_YEAR)
    end_year = end_year if end_year is not None else _int_env("LEGAL_DASHBOARD_END_YEAR", DEFAULT_HISTORICAL_END_YEAR)
    historical_window_years = (
        historical_window_years
        if historical_window_years is not None
        else _int_env("LEGAL_DASHBOARD_HISTORICAL_WINDOW_YEARS", DEFAULT_HISTORICAL_WINDOW_YEARS)
    )

    rss_feeds = [
        RSSFeedConfig(
            name=f"Google News RSS - {query}",
            url=build_google_news_rss_url(query),
            reliability_label="open_web",
            query_text=query,
            max_items=source_limit,
        )
        for query in queries
    ]

    if include_historical:
        for label, query in _historical_queries(start_year, end_year, historical_window_years):
            rss_feeds.append(
                RSSFeedConfig(
                    name=f"Google News RSS Historical - {label}",
                    url=build_google_news_rss_url(query),
                    reliability_label="open_web",
                    query_text=query,
                    max_items=source_limit,
                )
            )

    for index, url in enumerate(extra_rss_urls, start=1):
        rss_feeds.append(
            RSSFeedConfig(
                name=f"Configured RSS Feed {index}",
                url=url,
                reliability_label="open_web",
                query_text="configured_rss_feed",
                max_items=source_limit,
            )
        )

    pages = [
        PageConfig(
            name="SFIO Homepage",
            url="https://sfio.gov.in/en/",
            reliability_label="official",
            query_text="sfio homepage",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="SFIO What's New",
            url="https://sfio.gov.in/en/notice-category/whats-new/",
            reliability_label="official",
            query_text="sfio whats new",
            container_selector="table tr, tbody tr, .view-content tr",
            title_from_container=True,
            same_domain_only=False,
            allowed_domains=["sfio.gov.in", "cdnbbsr.s3waas.gov.in"],
            url_allow_patterns=[".pdf", "/sites/default/files", "/wp-content/uploads"],
            max_items=source_limit,
            source_type="html_listing",
        ),
        PageConfig(
            name="SFIO Notifications",
            url="https://sfio.gov.in/en/notice-category/notifications/",
            reliability_label="official",
            query_text="sfio notifications",
            container_selector="table tr, tbody tr, .view-content tr",
            title_from_container=True,
            same_domain_only=False,
            allowed_domains=["sfio.gov.in", "cdnbbsr.s3waas.gov.in"],
            url_allow_patterns=[".pdf", "/sites/default/files", "/wp-content/uploads"],
            max_items=source_limit,
            source_type="html_listing",
        ),
        PageConfig(
            name="SFIO Investigations Completed",
            url="https://sfio.gov.in/en/investigation-completed/",
            reliability_label="official",
            query_text="sfio investigations completed",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="SFIO Summons and Notices",
            url="https://sfio.gov.in/en/summons-notices/",
            reliability_label="official",
            query_text="sfio summons notices",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="NCLT Kolkata Bench",
            url="https://nclt.gov.in/kolkata",
            reliability_label="official",
            query_text="nclt kolkata bench",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="NCLT Kolkata Order Date Search",
            url="https://nclt.gov.in/order-date-wise",
            reliability_label="official",
            query_text="nclt kolkata order date search",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="NCLT Kolkata Case Number Search",
            url="https://nclt.gov.in/case-number-wise-search",
            reliability_label="official",
            query_text="nclt kolkata case number order search",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="NCLT Kolkata Petitioner / Respondent Search",
            url="https://nclt.gov.in/order-party-wise",
            reliability_label="official",
            query_text="nclt kolkata petitioner respondent order search",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="NCLT Kolkata Advocate Search",
            url="https://nclt.gov.in/order-advocate-wise",
            reliability_label="official",
            query_text="nclt kolkata advocate order search",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="NCLT Kolkata Judgment Date Search",
            url="https://nclt.gov.in/order-judgement-date-wise",
            reliability_label="official",
            query_text="nclt kolkata judgment date search",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="NCLT Kolkata By Judge Search",
            url="https://nclt.gov.in/order-judge-wise",
            reliability_label="official",
            query_text="nclt kolkata by judge search",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="NCLT Archive Notice Circulars",
            url="https://archive.nclt.gov.in/notice-circular",
            reliability_label="official",
            query_text="nclt archive notice circulars kolkata",
            container_selector="a[href$='.pdf'], a[href*='.pdf?']",
            title_allow_patterns=["kolkata"],
            same_domain_only=False,
            allowed_domains=["archive.nclt.gov.in"],
            url_allow_patterns=[".pdf"],
            max_items=source_limit,
            source_type="html_listing",
        ),
        PageConfig(
            name="NCLT Kolkata Final Orders",
            url="https://archive.nclt.gov.in/content/final-order-kolkata-bench-date-14122017-23012017",
            reliability_label="official",
            query_text="nclt kolkata final orders",
            container_selector="a[href$='.pdf'], a[href*='.pdf?']",
            title_allow_patterns=["m/s.", " pvt", " ltd", " limited"],
            same_domain_only=False,
            allowed_domains=["archive.nclt.gov.in"],
            url_allow_patterns=["/old_interm-final_order/", ".pdf"],
            max_items=source_limit,
            fallback_to_page_record=False,
            source_type="html_listing",
        ),
        PageConfig(
            name="Calcutta High Court Notices",
            url="https://calcuttahighcourt.gov.in/Notices/All",
            reliability_label="official",
            query_text="calcutta high court notices company liquidation fraud",
            container_selector="a[href*='/Notice-Files/']",
            title_allow_patterns=["company", "liquidation", "fraud", "cyber"],
            same_domain_only=True,
            url_allow_patterns=["/Notice-Files/"],
            max_items=source_limit,
            fallback_to_page_record=False,
            source_type="html_listing",
        ),
        PageConfig(
            name="Calcutta High Court eCourts Original Side",
            url="https://hcservices.ecourts.gov.in/ecourtindiaHC/cases/highcourt_causelist.php?state_cd=16&dist_cd=1&court_code=1&stateNm=Calcutta",
            reliability_label="official",
            query_text="calcuttahighcourt ecourts original side cause list",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="Calcutta High Court eCourts Appellate Side",
            url="https://hcservices.ecourts.gov.in/ecourtindiaHC/cases/highcourt_causelist.php?state_cd=16&dist_cd=1&court_code=3&stateNm=Calcutta",
            reliability_label="official",
            query_text="calcuttahighcourt ecourts appellate side cause list",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="Calcutta High Court eCourts Jalpaiguri Bench",
            url="https://hcservices.ecourts.gov.in/ecourtindiaHC/cases/highcourt_causelist.php?state_cd=16&dist_cd=1&court_code=2&stateNm=Calcutta",
            reliability_label="official",
            query_text="calcuttahighcourt ecourts jalpaiguri cause list",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="Calcutta High Court Order Search",
            url="https://calcuttahighcourt.gov.in/highcourt_order_search",
            reliability_label="official",
            query_text="calcuttahighcourt order judgment search",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="eCourts High Court Judgments Search",
            url="https://judgments.ecourts.gov.in",
            reliability_label="official",
            query_text="ecourts high court judgments search portal",
            page_summary_selector="body",
            max_items=1,
            source_type="html_page",
        ),
        PageConfig(
            name="NCLAT Daily Orders Registrar Court",
            url="https://nclat.nic.in/orders/daily-orders-registrar-court",
            reliability_label="official",
            query_text="nclat daily orders registrar west bengal kolkata",
            container_selector="table tr, tbody tr",
            title_selector="td:nth-of-type(4)",
            publish_selector="td:nth-of-type(3)",
            title_allow_patterns=["west bengal", "kolkata", "calcutta"],
            max_items=source_limit,
            fallback_to_page_record=False,
            source_type="html_listing",
        ),
        PageConfig(
            name="IBBI Orders - NCLT",
            url="https://www.ibbi.gov.in/en/orders/nclt",
            reliability_label="regulated",
            query_text="ibbi nclt orders kolkata bench west bengal",
            container_selector="table tr, tbody tr",
            title_selector="td:nth-of-type(3)",
            snippet_selector="td:nth-of-type(4)",
            publish_selector="td:nth-of-type(2)",
            title_allow_patterns=["kb/", "kb)", "kolkata", "west bengal", "calcutta"],
            max_items=source_limit,
            trim_title_at_first_date=False,
            fallback_to_page_record=False,
            source_type="html_listing",
        ),
        PageConfig(
            name="IBBI Orders - NCLAT",
            url="https://www.ibbi.gov.in/en/orders/nclat",
            reliability_label="regulated",
            query_text="ibbi nclat orders west bengal company",
            container_selector="table tr, tbody tr",
            title_selector="td:nth-of-type(3)",
            snippet_selector="td:nth-of-type(4)",
            publish_selector="td:nth-of-type(2)",
            title_allow_patterns=["west bengal", "kolkata", "calcutta"],
            max_items=source_limit,
            trim_title_at_first_date=False,
            fallback_to_page_record=False,
            source_type="html_listing",
        ),
        PageConfig(
            name="Calcutta High Court General Notifications",
            url="https://calcuttahighcourt.gov.in/Notices/general-notice",
            reliability_label="official",
            query_text="calcutta high court general notices cyber fraud company",
            container_selector="a[href*='/Notice-Files/']",
            title_allow_patterns=["company", "liquidation", "fraud", "cyber"],
            same_domain_only=True,
            url_allow_patterns=["/Notice-Files/"],
            max_items=source_limit,
            fallback_to_page_record=False,
            source_type="html_listing",
        ),
        PageConfig(
            name="Calcutta High Court Company Liquidation",
            url="https://calcuttahighcourt.gov.in/Notices/company-liquidation-notice",
            reliability_label="official",
            query_text="calcutta high court company liquidation notices",
            container_selector="a[href*='/Notice-Files/company-liquidation-notice/']",
            title_allow_patterns=["winding up", "company", "liquidation"],
            title_exclude_patterns=["cause list"],
            same_domain_only=True,
            url_allow_patterns=["/Notice-Files/company-liquidation-notice/"],
            max_items=source_limit,
            fallback_to_page_record=False,
            source_type="html_listing",
        ),
    ]

    for page_number in range(1, 13):
        pages.append(
            PageConfig(
                name=f"IBBI Orders - NCLT - Page {page_number}",
                url=f"https://www.ibbi.gov.in/en/orders/nclt?page={page_number}",
                reliability_label="regulated",
                query_text="ibbi nclt orders kolkata bench west bengal",
                container_selector="table tr, tbody tr",
                title_selector="td:nth-of-type(3)",
                snippet_selector="td:nth-of-type(4)",
                publish_selector="td:nth-of-type(2)",
                title_allow_patterns=["kb/", "kb)", "kolkata", "west bengal", "calcutta"],
                max_items=source_limit,
                trim_title_at_first_date=False,
                fallback_to_page_record=False,
                source_type="html_listing",
            )
        )

    for page_number in range(1, 6):
        pages.append(
            PageConfig(
                name=f"IBBI Orders - NCLAT - Page {page_number}",
                url=f"https://www.ibbi.gov.in/en/orders/nclat?page={page_number}",
                reliability_label="regulated",
                query_text="ibbi nclat orders west bengal company",
                container_selector="table tr, tbody tr",
                title_selector="td:nth-of-type(3)",
                snippet_selector="td:nth-of-type(4)",
                publish_selector="td:nth-of-type(2)",
                title_allow_patterns=["west bengal", "kolkata", "calcutta"],
                max_items=source_limit,
                trim_title_at_first_date=False,
                fallback_to_page_record=False,
                source_type="html_listing",
            )
        )

    ibbi_keywords = [
        ("West Bengal", "ibbi liquidation auction west bengal"),
        ("Kolkata", "ibbi liquidation auction kolkata"),
        ("Calcutta", "ibbi liquidation auction calcutta"),
        ("Howrah", "ibbi liquidation auction howrah"),
        ("Hooghly", "ibbi liquidation auction hooghly"),
        ("Medinipur", "ibbi liquidation auction medinipur"),
        ("Parganas", "ibbi liquidation auction parganas"),
        ("Durgapur", "ibbi liquidation auction durgapur"),
        ("Asansol", "ibbi liquidation auction asansol"),
        ("Siliguri", "ibbi liquidation auction siliguri"),
        ("Haldia", "ibbi liquidation auction haldia"),
    ]
    for keyword, query_text in ibbi_keywords:
        encoded_keyword = keyword.replace(" ", "+")
        for page_number in range(1, 5):
            pages.append(
                PageConfig(
                    name=f"IBBI Liquidation Auctions - {keyword} - Page {page_number}",
                    url=(
                        "https://ibbi.gov.in/liquidation-auction-notices/lists"
                        f"?date=&filter_by=all%2F1000&page={page_number}&reserve_price=&title={encoded_keyword}"
                    ),
                    reliability_label="regulated",
                    query_text=query_text,
                    container_selector="table tr, tbody tr",
                    title_selector="td:nth-of-type(3)",
                    snippet_selector="td:nth-of-type(8)",
                    publish_selector="td:nth-of-type(2)",
                    max_items=source_limit,
                    trim_title_at_first_date=False,
                    fallback_to_page_record=False,
                    source_type="html_listing",
                )
            )

    static_sources = build_mca_static_sources() + [
        StaticSourceConfig(
            title="Calcutta High Court eCourts Original Side case-status and order gateway",
            url="https://hcservices.ecourts.gov.in/ecourtindiaHC/cases/index_highcourt.php?state_cd=16&dist_cd=1&court_code=1&stateNm=Calcutta",
            source_name="Calcutta High Court eCourts Original Side Case Status",
            snippet=(
                "Official Calcutta High Court eCourts connector for Original Side case status, filing status, and "
                "order-search workflows."
            ),
            reliability_label="official",
            query_text="calcuttahighcourt ecourts original side case status orders",
            source_type="official_gateway",
        ),
        StaticSourceConfig(
            title="Calcutta High Court eCourts Appellate Side case-status and order gateway",
            url="https://hcservices.ecourts.gov.in/ecourtindiaHC/cases/index_highcourt.php?state_cd=16&dist_cd=1&court_code=3&stateNm=Calcutta",
            source_name="Calcutta High Court eCourts Appellate Side Case Status",
            snippet=(
                "Official Calcutta High Court eCourts connector for Appellate Side case status, filing status, and "
                "order-search workflows."
            ),
            reliability_label="official",
            query_text="calcuttahighcourt ecourts appellate side case status orders",
            source_type="official_gateway",
        ),
        StaticSourceConfig(
            title="Calcutta High Court eCourts Jalpaiguri bench case-status and order gateway",
            url="https://hcservices.ecourts.gov.in/ecourtindiaHC/cases/index_highcourt.php?state_cd=16&dist_cd=1&court_code=2&stateNm=Calcutta",
            source_name="Calcutta High Court eCourts Jalpaiguri Case Status",
            snippet=(
                "Official Calcutta High Court eCourts connector for the Jalpaiguri bench case status, filing "
                "status, and order-search workflows."
            ),
            reliability_label="official",
            query_text="calcuttahighcourt ecourts jalpaiguri case status orders",
            source_type="official_gateway",
        ),
    ]

    static_sources = _attach_local_mca_pdfs(static_sources)

    return rss_feeds, pages, static_sources


def build_datasets(sources: list[SourceRecord]) -> dict[str, pd.DataFrame]:
    legal_records = build_legal_records(sources)
    entity_rollup = build_entity_rollup(legal_records)
    cluster_summary = build_cluster_summary(legal_records, entity_rollup)
    company_reports = build_company_reports(legal_records)

    source_df = records_to_frame(sources)
    master_df = records_to_frame(legal_records)
    company_reports_df = records_to_frame(company_reports)
    risk_df = records_to_frame(entity_rollup)
    cluster_df = records_to_frame(cluster_summary)

    companies_df = risk_df.loc[risk_df["entity_type"] == "company"].copy() if not risk_df.empty else risk_df
    enforcement_cases_df = (
        master_df.loc[
            master_df["usable_for_analytics"].fillna(False)
            & (
                master_df["has_legal_section"].fillna(False)
                | master_df["violation_type"].fillna("").ne("other")
            )
        ].copy()
        if not master_df.empty
        else master_df
    )
    repeat_offenders_df = (
        risk_df.loc[risk_df["mention_count"].fillna(0).astype(int) > 1].copy()
        if not risk_df.empty
        else risk_df
    )

    return {
        "master_dataset": master_df,
        "company_reports": company_reports_df,
        "companies": companies_df,
        "enforcement_cases": enforcement_cases_df,
        "repeat_offenders": repeat_offenders_df,
        "risk_scores": risk_df,
        "cluster_summary": cluster_df,
        "source_log": source_df,
    }


def run_pipeline(
    output_dir: Path = OUTPUT_DIR,
    mode: str = "demo",
    *,
    include_historical: bool = False,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    historical_window_years: Optional[int] = None,
) -> dict[str, pd.DataFrame]:
    effective_include_historical = include_historical or _bool_env("LEGAL_DASHBOARD_INCLUDE_HISTORICAL", False)
    effective_start_year = (
        start_year if start_year is not None else _int_env("LEGAL_DASHBOARD_START_YEAR", DEFAULT_HISTORICAL_START_YEAR)
    )
    effective_end_year = (
        end_year if end_year is not None else _int_env("LEGAL_DASHBOARD_END_YEAR", DEFAULT_HISTORICAL_END_YEAR)
    )
    effective_start_year, effective_end_year = _normalize_year_range(effective_start_year, effective_end_year)
    company_profile_limit = _int_env("LEGAL_DASHBOARD_COMPANY_PROFILE_LIMIT", DEFAULT_COMPANY_PROFILE_LIMIT)
    insta_enrichment_limit = _int_env("LEGAL_DASHBOARD_INSTA_ENRICHMENT_LIMIT", DEFAULT_INSTA_ENRICHMENT_LIMIT)

    if mode == "demo":
        sources = demo_sources()
    else:
        rss_feeds, pages, static_sources = _default_live_sources(
            include_historical=effective_include_historical,
            start_year=effective_start_year,
            end_year=effective_end_year,
            historical_window_years=historical_window_years,
        )
        base_sources = collect_sources(rss_feeds=rss_feeds, pages=pages, static_sources=static_sources)
        registry_sources = collect_registry_company_sources(
            start_year=effective_start_year,
            end_year=effective_end_year,
            company_profile_limit=company_profile_limit,
            insta_enrichment_limit=insta_enrichment_limit,
        )
        sources = _merge_sources(base_sources, registry_sources)
        if not sources:
            raise ValueError(
                "Live mode was selected but no providers returned data. "
                "Add RSSFeedConfig or PageConfig entries to _default_live_sources()."
            )

    datasets = build_datasets(sources)
    write_outputs(datasets, output_dir)
    provider_counts = {"rss_feeds": 0, "pages": 0, "static_sources": 0}
    if mode == "live":
        rss_feeds, pages, static_sources = _default_live_sources(
            include_historical=effective_include_historical,
            start_year=effective_start_year,
            end_year=effective_end_year,
            historical_window_years=historical_window_years,
        )
        provider_counts = {
            "rss_feeds": len(rss_feeds),
            "pages": len(pages),
            "static_sources": len(static_sources),
            "local_static_pdfs": sum(1 for source in static_sources if source.local_path),
            "historical_rss_feeds": sum(1 for feed in rss_feeds if feed.name.startswith("Google News RSS Historical - ")),
            "company_profile_limit": company_profile_limit,
            "insta_enrichment_limit": insta_enrichment_limit,
        }
    write_run_metadata(
        {
            "mode": mode,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source_count": len(sources),
            "providers": provider_counts,
            "historical_range": (
                {"start_year": effective_start_year, "end_year": effective_end_year}
                if mode == "live" and effective_include_historical
                else {}
            ),
            "notes": (
                "Synthetic demo dataset for product walkthroughs."
                if mode == "demo"
                else (
                    "Live provider dataset with historical year-windowed search feeds."
                    if effective_include_historical
                    else "Live provider dataset."
                )
            ),
        },
        output_dir,
    )
    return datasets


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the hybrid legal dashboard datasets.")
    parser.add_argument("--mode", choices=["demo", "live"], default="demo")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    parser.add_argument("--include-historical", action="store_true")
    parser.add_argument("--start-year", type=int, default=None)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--historical-window-years", type=int, default=None)
    args = parser.parse_args()

    run_pipeline(
        output_dir=Path(args.output_dir),
        mode=args.mode,
        include_historical=args.include_historical,
        start_year=args.start_year,
        end_year=args.end_year,
        historical_window_years=args.historical_window_years,
    )
    print(f"Wrote processed outputs to {Path(args.output_dir).resolve()}")


if __name__ == "__main__":
    main()
