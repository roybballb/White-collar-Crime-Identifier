from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import io
from pathlib import Path
import re
from typing import Iterable, Optional
from urllib.parse import quote_plus, urljoin, urlparse
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from hybrid_legal_dashboard.schemas import SourceRecord

try:
    from pypdf import PdfReader
except ImportError:  # pragma: no cover - optional dependency until installed
    PdfReader = None


DEFAULT_TIMEOUT = 20
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}
GENERIC_LINK_TEXT = {"view", "read more", "more", "show all", "view all"}
DATE_PATTERN = re.compile(
    r"\b\d{1,2}/\d{1,2}/\d{4}\b|\b\d{1,2}-\d{1,2}-\d{4}\b|\b\d{1,2}-[A-Za-z]{3}-\d{4}\b|\b\d{1,2}\s+"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec),\s+\d{4}\b|\b"
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},\s+\d{4}\b",
    re.IGNORECASE,
)
FILE_SIZE_PATTERN = re.compile(r"\(\s*\d+(?:\.\d+)?\s*(?:KB|MB|GB)\s*\)", re.IGNORECASE)
UPLOADED_PATTERN = re.compile(r"\bUploaded\s*:\s*.*$", re.IGNORECASE)
CIN_PATTERN = re.compile(r"\b[LUF]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}\b")
DIRECTOR_ROW_PATTERN = re.compile(
    r"(?:^|\n)\s*(\d{1,6})\s*(?:\|\s*|\s+)(\d{5,8})\s*(?:\|\s*|\s+)([^|\n]{3,140}?)\s*(?:\|\s*|\s+)"
    r"([^|\n]{3,220}?)\s*(?:\|\s*|\s+)(" + CIN_PATTERN.pattern[2:-2] + r")(?=\s|$)",
    re.IGNORECASE,
)
STRUCK_OFF_ROW_PATTERN = re.compile(
    r"(?:^|\n)\s*(\d{1,6})\s*(?:\|\s*|\s+)([^|\n]{3,220}?)\s*(?:\|\s*|\s+)(" + CIN_PATTERN.pattern[2:-2] + r")(?=\s|$)",
    re.IGNORECASE,
)
PDF_NOISE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"government of india",
        r"ministry of corporate affairs",
        r"office of registrar of companies",
        r"page \d+ of \d+",
        r"public notice no",
        r"registrar of companies, west bengal",
        r"notice of striking off and dissolution",
        r"list of directors disqualified",
        r"matter of striking off of companies",
        r"annexure",
    ]
]


@dataclass
class RSSFeedConfig:
    name: str
    url: str
    reliability_label: str = "open_web"
    query_text: str = ""
    max_items: int = 20


@dataclass
class PageConfig:
    name: str
    url: str
    reliability_label: str = "open_web"
    query_text: str = ""
    container_selector: str = ""
    link_selector: str = "a"
    title_selector: str = ""
    snippet_selector: str = ""
    publish_selector: str = ""
    page_summary_selector: str = "main, article, body"
    title_from_container: bool = False
    trim_title_at_first_date: bool = True
    same_domain_only: bool = True
    allowed_domains: Optional[list[str]] = None
    url_allow_patterns: Optional[list[str]] = None
    url_exclude_patterns: Optional[list[str]] = None
    title_allow_patterns: Optional[list[str]] = None
    title_exclude_patterns: Optional[list[str]] = None
    max_items: int = 20
    include_page_record: bool = False
    fallback_to_page_record: bool = True
    source_type: str = "html"


@dataclass
class StaticSourceConfig:
    title: str
    url: str
    source_name: str
    snippet: str = ""
    reliability_label: str = "official"
    query_text: str = ""
    published_at: str = ""
    source_type: str = "document"
    parser_kind: str = ""
    max_items: int = 1
    local_path: str = ""


def _fetch_text(url: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    response = requests.get(url, timeout=timeout, headers=DEFAULT_HEADERS)
    response.raise_for_status()
    return response.text


def _fetch_bytes(url: str, timeout: int = DEFAULT_TIMEOUT) -> bytes:
    response = requests.get(url, timeout=timeout, headers=DEFAULT_HEADERS)
    response.raise_for_status()
    return response.content


def build_google_news_rss_url(query: str, hl: str = "en-IN", gl: str = "IN", ceid: str = "IN:en") -> str:
    encoded_query = quote_plus(query)
    return f"https://news.google.com/rss/search?q={encoded_query}&hl={hl}&gl={gl}&ceid={ceid}"


def _find_text(element: ET.Element, names: Iterable[str]) -> str:
    for name in names:
        found = element.find(name)
        if found is not None and found.text:
            return found.text.strip()
    return ""


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _clean_pdf_line(value: str) -> str:
    text = value.replace("\xa0", " ").replace("|", " | ")
    text = _clean_text(text)
    text = re.sub(r"\s+\|\s+", " | ", text)
    return text


def _normalized_domain(url: str) -> str:
    return urlparse(url).netloc.lower()


def _matches_allowed_domain(url: str, config: PageConfig) -> bool:
    domain = _normalized_domain(url)
    if not domain:
        return False

    if config.allowed_domains:
        for allowed in config.allowed_domains:
            allowed = allowed.lower()
            if domain == allowed or domain.endswith(f".{allowed}"):
                return True
        return False

    if config.same_domain_only:
        page_domain = _normalized_domain(config.url)
        return domain == page_domain or domain.endswith(f".{page_domain}")

    return True


def _matches_url_filters(url: str, config: PageConfig) -> bool:
    lowered = url.lower()
    if config.url_allow_patterns and not any(pattern.lower() in lowered for pattern in config.url_allow_patterns):
        return False
    if config.url_exclude_patterns and any(pattern.lower() in lowered for pattern in config.url_exclude_patterns):
        return False
    return True


def _first_matching_text(container: Tag, selector: str) -> str:
    if not selector:
        return ""
    node = container.select_one(selector)
    return _clean_text(node.get_text(" ", strip=True)) if node else ""


def _detect_date(value: str) -> str:
    match = DATE_PATTERN.search(value or "")
    return match.group(0) if match else ""


def _strip_listing_noise(value: str) -> str:
    text = _clean_text(value)
    text = FILE_SIZE_PATTERN.sub("", text)
    text = UPLOADED_PATTERN.sub("", text)
    text = re.sub(r"\bView\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bArchive\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip(" -|")


def _build_title(container: Tag, link_tag: Optional[Tag], config: PageConfig) -> str:
    if config.title_selector:
        title = _first_matching_text(container, config.title_selector)
    elif not config.title_from_container and link_tag is not None:
        title = _clean_text(link_tag.get_text(" ", strip=True))
    else:
        title = _clean_text(container.get_text(" ", strip=True))

    if not title or title.lower() in GENERIC_LINK_TEXT:
        title = _clean_text(container.get_text(" ", strip=True))

    if config.trim_title_at_first_date and (config.title_from_container or title.lower() in GENERIC_LINK_TEXT):
        title = re.split(DATE_PATTERN.pattern, title, maxsplit=1, flags=DATE_PATTERN.flags)[0]
    title = _strip_listing_noise(title)

    return title


def _build_summary(container: Tag, title: str, config: PageConfig) -> str:
    if config.snippet_selector:
        return _first_matching_text(container, config.snippet_selector)

    text = _strip_listing_noise(container.get_text(" ", strip=True))
    if title and text.startswith(title):
        text = text[len(title) :].strip(" -|")
    return text[:480]


def _is_usable_url(url: str) -> bool:
    lowered = (url or "").lower()
    return bool(url) and not lowered.startswith(("javascript:", "mailto:", "#"))


def _candidate_link_url(link_tag: Tag, base_url: str) -> str:
    href = (link_tag.get("href", "") or "").strip()
    if _is_usable_url(href):
        return urljoin(base_url, href)

    onclick = (link_tag.get("onclick", "") or "").strip()
    if onclick:
        match = re.search(r"""['"]([^'"]+\.(?:pdf|html?)[^'"]*)['"]""", onclick, re.IGNORECASE)
        if match:
            return urljoin(base_url, match.group(1).strip())

    data_href = (link_tag.get("data-href", "") or "").strip()
    if _is_usable_url(data_href):
        return urljoin(base_url, data_href)

    return ""


def _matches_title_filters(title: str, config: PageConfig) -> bool:
    lowered = title.lower()
    if config.title_allow_patterns and not any(pattern.lower() in lowered for pattern in config.title_allow_patterns):
        return False
    if config.title_exclude_patterns and any(pattern.lower() in lowered for pattern in config.title_exclude_patterns):
        return False
    return True


def _extract_records_from_listing(config: PageConfig, soup: BeautifulSoup) -> list[SourceRecord]:
    records: list[SourceRecord] = []
    seen: set[tuple[str, str]] = set()
    containers = soup.select(config.container_selector)

    for container in containers:
        if not isinstance(container, Tag):
            continue

        if isinstance(container, Tag) and container.name == "a":
            link_tag = container
        else:
            link_tag = container.select_one(config.link_selector) if config.link_selector else container.find("a")
        if link_tag is None:
            continue

        href = _candidate_link_url(link_tag, config.url)
        if not _is_usable_url(href):
            continue
        if not _matches_allowed_domain(href, config):
            continue
        if not _matches_url_filters(href, config):
            continue

        title = _build_title(container, link_tag, config)
        if not title or len(title) < 8:
            continue
        if not _matches_title_filters(title, config):
            continue

        summary = _build_summary(container, title, config)
        published_at = _first_matching_text(container, config.publish_selector) or _detect_date(
            container.get_text(" ", strip=True)
        )

        dedupe_key = (title.lower(), href.lower())
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        records.append(
            SourceRecord(
                title=title,
                url=href,
                source_name=config.name,
                snippet=summary,
                reliability_label=config.reliability_label,
                query_text=config.query_text,
                fetched_at=datetime.now(timezone.utc).isoformat(),
                published_at=published_at,
                source_type=config.source_type or "html_listing",
            )
        )

        if len(records) >= config.max_items:
            break

    return records


def _page_level_record(config: PageConfig, soup: BeautifulSoup) -> list[SourceRecord]:
    title = soup.title.text.strip() if soup.title and soup.title.text else config.name

    description = ""
    meta = soup.find("meta", attrs={"name": "description"})
    if meta:
        description = _clean_text(meta.get("content", ""))

    if not description:
        summary_node = soup.select_one(config.page_summary_selector) if config.page_summary_selector else None
        if summary_node is not None:
            description = _clean_text(summary_node.get_text(" ", strip=True))[:480]

    return [
        SourceRecord(
            title=title,
            url=config.url,
            source_name=config.name,
            snippet=description,
            reliability_label=config.reliability_label,
            query_text=config.query_text,
            source_type=config.source_type or "html_page",
        )
    ]


def extract_page_records(config: PageConfig, html: str) -> list[SourceRecord]:
    soup = BeautifulSoup(html, "html.parser")

    if config.container_selector:
        records = _extract_records_from_listing(config, soup)
        if records and not config.include_page_record:
            return records
        if not records and not config.fallback_to_page_record:
            return []
        return records + _page_level_record(config, soup)

    return _page_level_record(config, soup)


def fetch_rss_feed(config: RSSFeedConfig) -> list[SourceRecord]:
    xml_text = _fetch_text(config.url)
    root = ET.fromstring(xml_text)
    items = root.findall(".//item")
    if not items:
        items = root.findall(".//{http://www.w3.org/2005/Atom}entry")

    records: list[SourceRecord] = []
    for item in items:
        title = _find_text(item, ["title", "{http://www.w3.org/2005/Atom}title"])
        link = _find_text(item, ["link", "{http://www.w3.org/2005/Atom}link"])
        if not link:
            atom_link = item.find("{http://www.w3.org/2005/Atom}link")
            if atom_link is not None:
                link = atom_link.attrib.get("href", "")
        summary = _find_text(
            item,
            [
                "description",
                "summary",
                "{http://www.w3.org/2005/Atom}summary",
            ],
        )
        published = _find_text(
            item,
            ["pubDate", "published", "{http://www.w3.org/2005/Atom}updated"],
        )
        if title and link:
            records.append(
                SourceRecord(
                    title=_clean_text(title),
                    url=link,
                    source_name=config.name,
                    snippet=_clean_text(summary),
                    reliability_label=config.reliability_label,
                    query_text=config.query_text,
                    published_at=published,
                    fetched_at=datetime.now(timezone.utc).isoformat(),
                    source_type="rss",
                )
            )
        if len(records) >= config.max_items:
            break
    return records


def fetch_html_page(config: PageConfig) -> list[SourceRecord]:
    html = _fetch_text(config.url)
    return extract_page_records(config, html)


def build_static_source(config: StaticSourceConfig) -> SourceRecord:
    snippet = _clean_text(config.snippet)
    if config.local_path:
        local_name = Path(config.local_path).name
        snippet = _clean_text(f"{snippet} Local PDF: {local_name}.")
    return SourceRecord(
        title=_clean_text(config.title),
        url=_static_source_url(config),
        source_name=config.source_name,
        snippet=snippet,
        reliability_label=config.reliability_label,
        query_text=config.query_text,
        published_at=config.published_at,
        fetched_at=datetime.now(timezone.utc).isoformat(),
        source_type=config.source_type,
    )


def _is_pdf_noise(line: str) -> bool:
    lowered = line.lower()
    if not line or len(line) < 6:
        return True
    return any(pattern.search(lowered) for pattern in PDF_NOISE_PATTERNS)


def _pdf_page_texts(pdf_bytes: bytes) -> list[str]:
    if PdfReader is None:
        raise RuntimeError("PDF parsing requires pypdf to be installed.")

    reader = PdfReader(io.BytesIO(pdf_bytes))
    page_texts: list[str] = []
    for page in reader.pages:
        page_texts.append(page.extract_text() or "")
    return page_texts


def _static_source_url(config: StaticSourceConfig) -> str:
    if config.url:
        return config.url
    if config.local_path:
        return Path(config.local_path).expanduser().resolve().as_uri()
    return ""


def _load_static_pdf_bytes(config: StaticSourceConfig) -> bytes:
    if config.local_path:
        return Path(config.local_path).expanduser().read_bytes()
    return _fetch_bytes(config.url)


def _strip_row_prefix(value: str) -> str:
    cleaned = _clean_text(value)
    cleaned = re.sub(r"^\d{1,6}\s*", "", cleaned)
    cleaned = cleaned.strip(" |,-")
    return cleaned


def _normalize_company_name(value: str) -> str:
    cleaned = _strip_row_prefix(value)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    cleaned = re.sub(r"\b(?:din|cin)\b.*$", "", cleaned, flags=re.IGNORECASE).strip(" |,-")
    return cleaned


def _build_pdf_company_record(
    *,
    company_name: str,
    cin: str,
    config: StaticSourceConfig,
    detail: str,
) -> SourceRecord:
    title = _normalize_company_name(company_name)
    detail_text = detail
    if config.local_path:
        detail_text = f"{detail} Local PDF: {Path(config.local_path).name}."
    snippet = _clean_text(f"CIN: {cin}. {detail_text}")
    return SourceRecord(
        title=title,
        url=_static_source_url(config),
        source_name=config.source_name,
        snippet=snippet,
        reliability_label=config.reliability_label,
        query_text=config.query_text,
        fetched_at=datetime.now(timezone.utc).isoformat(),
        published_at=config.published_at,
        source_type=config.source_type or "pdf_row",
    )


def _parse_director_disqualification_pdf(config: StaticSourceConfig) -> list[SourceRecord]:
    records: list[SourceRecord] = []
    seen: set[tuple[str, str]] = set()
    page_texts = _pdf_page_texts(_load_static_pdf_bytes(config))

    for page_text in page_texts:
        cleaned_page = "\n".join(
            line for line in (_clean_pdf_line(raw) for raw in page_text.splitlines()) if not _is_pdf_noise(line)
        )
        for _, din, director_name, company_name, cin in DIRECTOR_ROW_PATTERN.findall(cleaned_page):
            normalized_company = _normalize_company_name(company_name)
            if not normalized_company or not cin:
                continue
            dedupe_key = (normalized_company.lower(), cin.upper())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            records.append(
                _build_pdf_company_record(
                    company_name=normalized_company,
                    cin=cin.upper(),
                    config=config,
                    detail=(
                        f"Director: {_clean_text(director_name)}. DIN: {din}. "
                        "Official ROC West Bengal director disqualification list under section 164(2)(a)."
                    ),
                )
            )
            if len(records) >= config.max_items:
                return records

    return records


def _parse_struck_off_pdf(config: StaticSourceConfig) -> list[SourceRecord]:
    records: list[SourceRecord] = []
    seen: set[tuple[str, str]] = set()
    page_texts = _pdf_page_texts(_load_static_pdf_bytes(config))

    action_detail = (
        "Official ROC West Bengal striking-off notice under section 248."
        if "248" in config.title or "248" in config.query_text
        else "Official ROC West Bengal company action."
    )

    for page_text in page_texts:
        cleaned_page = "\n".join(
            line for line in (_clean_pdf_line(raw) for raw in page_text.splitlines()) if not _is_pdf_noise(line)
        )
        for _, company_name, cin in STRUCK_OFF_ROW_PATTERN.findall(cleaned_page):
            normalized_company = _normalize_company_name(company_name)
            if not normalized_company or not cin:
                continue
            dedupe_key = (normalized_company.lower(), cin.upper())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            records.append(
                _build_pdf_company_record(
                    company_name=normalized_company,
                    cin=cin.upper(),
                    config=config,
                    detail=action_detail,
                )
            )
            if len(records) >= config.max_items:
                return records

    return records


def fetch_static_source(config: StaticSourceConfig) -> list[SourceRecord]:
    if config.parser_kind == "mca_director_disqualification_pdf":
        try:
            records = _parse_director_disqualification_pdf(config)
            if records:
                return records
        except (OSError, requests.RequestException, RuntimeError):
            return [build_static_source(config)]

    if config.parser_kind == "mca_struck_off_pdf":
        try:
            records = _parse_struck_off_pdf(config)
            if records:
                return records
        except (OSError, requests.RequestException, RuntimeError):
            return [build_static_source(config)]

    return [build_static_source(config)]


def collect_sources(
    rss_feeds: Optional[list[RSSFeedConfig]] = None,
    pages: Optional[list[PageConfig]] = None,
    static_sources: Optional[list[StaticSourceConfig]] = None,
) -> list[SourceRecord]:
    collected: list[SourceRecord] = []
    seen: set[tuple[str, str]] = set()

    def add_records(records: list[SourceRecord]) -> None:
        for record in records:
            key = (_clean_text(record.title).lower(), record.url.lower())
            if not record.title or not record.url or key in seen:
                continue
            seen.add(key)
            collected.append(record)

    for feed in rss_feeds or []:
        try:
            add_records(fetch_rss_feed(feed))
        except requests.RequestException:
            continue
        except ET.ParseError:
            continue
    for page in pages or []:
        try:
            add_records(fetch_html_page(page))
        except requests.RequestException:
            continue
    for source in static_sources or []:
        try:
            add_records(fetch_static_source(source))
        except (OSError, requests.RequestException):
            add_records([build_static_source(source)])
    return collected
