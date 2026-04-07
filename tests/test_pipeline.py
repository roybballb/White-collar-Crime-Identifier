from hybrid_legal_dashboard.demo_data import demo_sources
from hybrid_legal_dashboard.pipeline import _default_live_sources, build_datasets
from hybrid_legal_dashboard.services.extraction import build_legal_records
from hybrid_legal_dashboard.services.ingestion import PageConfig, build_google_news_rss_url, extract_page_records


def test_demo_records_are_analytics_ready():
    records = build_legal_records(demo_sources())
    assert any(record.usable_for_analytics for record in records)
    assert any(record.company_name for record in records)
    assert any(record.legal_sections for record in records)


def test_repeat_entity_outputs_exist():
    datasets = build_datasets(demo_sources())
    repeat_offenders = datasets["repeat_offenders"]
    assert not repeat_offenders.empty
    assert "Eastern Meridian Agro Private Limited" in set(repeat_offenders["entity_name"])


def test_live_sources_are_configured():
    rss_feeds, pages, static_sources = _default_live_sources()
    assert rss_feeds
    assert pages
    assert static_sources
    assert any("sfio.gov.in" in page.url for page in pages)
    assert any("nclt.gov.in" in page.url for page in pages)
    assert any("calcuttahighcourt.gov.in" in page.url for page in pages)
    assert any("hcservices.ecourts.gov.in" in page.url for page in pages)
    assert any("judgments.ecourts.gov.in" in page.url for page in pages)
    assert any("final-order-kolkata-bench" in page.url for page in pages)
    assert any("mca.gov.in" in source.url for source in static_sources)
    assert any("news.google.com/rss/search" in feed.url for feed in rss_feeds)


def test_google_news_rss_url_is_query_driven():
    url = build_google_news_rss_url("West Bengal company fraud")
    assert "news.google.com/rss/search" in url
    assert "West+Bengal+company+fraud" in url


def test_extract_page_records_from_listing_rows():
    html = """
    <html>
      <body>
        <table>
          <tr>
            <td>Notice for Alpha Bengal Private Limited under Section 447</td>
            <td>05/04/2026</td>
            <td><a href="/files/alpha-bengal.pdf">View</a></td>
          </tr>
          <tr>
            <td>Notice for Riverfront Projects Limited under Section 420</td>
            <td>04/04/2026</td>
            <td><a href="https://cdn.example.org/riverfront.pdf">View</a></td>
          </tr>
        </table>
      </body>
    </html>
    """
    config = PageConfig(
        name="Sample SFIO Listing",
        url="https://sfio.gov.in/en/notice-category/whats-new/",
        reliability_label="official",
        container_selector="tr",
        title_from_container=True,
        same_domain_only=False,
        allowed_domains=["sfio.gov.in", "cdn.example.org"],
        max_items=10,
        source_type="html_listing",
    )

    records = extract_page_records(config, html)
    assert len(records) == 2
    assert records[0].title == "Notice for Alpha Bengal Private Limited under Section 447"
    assert records[0].published_at == "05/04/2026"
    assert records[0].url == "https://sfio.gov.in/files/alpha-bengal.pdf"


def test_extract_page_records_from_anchor_listing():
    html = """
    <html>
      <body>
        <a href="/Notice-Files/company-liquidation-1">Company liquidation notice for Alpha Bengal Private Limited Uploaded:05-Apr-2026 14:00:00</a>
        <a href="/Notice-Files/cause-list">Daily cause list Uploaded:05-Apr-2026 14:00:00</a>
      </body>
    </html>
    """
    config = PageConfig(
        name="Calcutta High Court Notices",
        url="https://calcuttahighcourt.gov.in/Notices/All",
        reliability_label="official",
        container_selector="a[href*='/Notice-Files/']",
        title_allow_patterns=["company", "liquidation", "fraud"],
        url_allow_patterns=["/Notice-Files/"],
        max_items=10,
        source_type="html_listing",
    )

    records = extract_page_records(config, html)
    assert len(records) == 1
    assert records[0].title.startswith("Company liquidation notice")
    assert records[0].published_at == "05-Apr-2026"


def test_listing_without_matches_can_skip_page_fallback():
    html = "<html><body><a href=\"/Notice-Files/cause-list\">Daily cause list Uploaded:05-Apr-2026 14:00:00</a></body></html>"
    config = PageConfig(
        name="Calcutta High Court Notices",
        url="https://calcuttahighcourt.gov.in/Notices/All",
        reliability_label="official",
        container_selector="a[href*='/Notice-Files/']",
        title_allow_patterns=["company", "liquidation", "fraud"],
        url_allow_patterns=["/Notice-Files/"],
        max_items=10,
        fallback_to_page_record=False,
        source_type="html_listing",
    )

    records = extract_page_records(config, html)
    assert records == []
