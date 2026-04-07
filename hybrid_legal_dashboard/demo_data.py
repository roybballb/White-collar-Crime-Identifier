from __future__ import annotations

from .schemas import SourceRecord


def demo_sources() -> list[SourceRecord]:
    """Synthetic records so the MVP can run without making live allegations."""

    return [
        SourceRecord(
            title=(
                "Sample Official Bulletin: Eastern Meridian Agro Private Limited "
                "reviewed under Section 447 and Section 448 in Kolkata"
            ),
            url="https://example.com/demo/eastern-meridian-official-review",
            source_name="Sample Official Bulletin",
            snippet=(
                "A Kolkata-linked review references Eastern Meridian Agro Private "
                "Limited with discussion of Section 447 fraud and Section 448 false "
                "statement indicators tied to procurement disclosures."
            ),
            reliability_label="official",
            query_text="corporate fraud west bengal section 447",
            published_at="2026-04-04",
            source_type="demo",
        ),
        SourceRecord(
            title=(
                "Regional business report flags Eastern Meridian Agro Private Limited "
                "in North 24 Parganas over Section 420 complaint"
            ),
            url="https://example.com/demo/eastern-meridian-media-report",
            source_name="Sample Business Daily",
            snippet=(
                "The report describes a Section 420 cheating complaint and follow-up "
                "questions around vendor payments in North 24 Parganas."
            ),
            reliability_label="reputed_media",
            query_text="eastern meridian agro private limited section 420",
            published_at="2026-04-03",
            source_type="demo",
        ),
        SourceRecord(
            title=(
                "Riverglow Infrastructure Limited appears in Howrah audit note "
                "referencing Section 406 and Section 447"
            ),
            url="https://example.com/demo/riverglow-audit-note",
            source_name="Sample Audit Circular",
            snippet=(
                "An audit note in Howrah references Riverglow Infrastructure Limited "
                "and lists concerns aligned with criminal breach of trust and fraud."
            ),
            reliability_label="regulated",
            query_text="riverglow infrastructure limited howrah section 406",
            published_at="2026-04-02",
            source_type="demo",
        ),
        SourceRecord(
            title=(
                "Market investigation mentions Riverglow Infrastructure Limited "
                "and Section 471 patterns across Hooghly records"
            ),
            url="https://example.com/demo/riverglow-market-investigation",
            source_name="Sample Investigations Desk",
            snippet=(
                "The article highlights Hooghly references, forged-document concerns, "
                "and repeated entity appearances linked to vendor documentation."
            ),
            reliability_label="reputed_media",
            query_text="riverglow infrastructure limited hooghly section 471",
            published_at="2026-04-01",
            source_type="demo",
        ),
        SourceRecord(
            title=(
                "Sundar Delta Exports LLP tracked in South 24 Parganas with "
                "Section 420 complaint progression"
            ),
            url="https://example.com/demo/sundar-delta-exports-progress",
            source_name="Sample Compliance Watch",
            snippet=(
                "South 24 Parganas records reference Sundar Delta Exports LLP and a "
                "cheating-related complaint connected to contract advances."
            ),
            reliability_label="open_web",
            query_text="sundar delta exports llp section 420 south 24 parganas",
            published_at="2026-03-30",
            source_type="demo",
        ),
        SourceRecord(
            title=(
                "Bengal Apex Micro Systems Private Limited linked to Nadia filing "
                "that cites Section 468 and Section 471"
            ),
            url="https://example.com/demo/bengal-apex-filing",
            source_name="Sample Filing Watch",
            snippet=(
                "A Nadia-linked filing mentions Bengal Apex Micro Systems Private "
                "Limited and possible forgery-for-cheating indicators."
            ),
            reliability_label="regulated",
            query_text="bengal apex micro systems private limited nadia section 468",
            published_at="2026-03-29",
            source_type="demo",
        ),
        SourceRecord(
            title=(
                "Hooghly Basin Developers Private Limited named in Hooghly "
                "compliance review involving Section 447"
            ),
            url="https://example.com/demo/hooghly-basin-review",
            source_name="Sample Official Bulletin",
            snippet=(
                "The compliance review notes Hooghly Basin Developers Private Limited "
                "in Hooghly and references fraud-risk review triggers."
            ),
            reliability_label="official",
            query_text="hooghly basin developers private limited section 447",
            published_at="2026-03-28",
            source_type="demo",
        ),
        SourceRecord(
            title=(
                "Green Crest Supply Chain Limited noted in Kolkata complaint "
                "with Section 409 reference"
            ),
            url="https://example.com/demo/green-crest-complaint",
            source_name="Sample Metro Ledger",
            snippet=(
                "A Kolkata complaint references Green Crest Supply Chain Limited and "
                "possible misuse of entrusted assets under Section 409."
            ),
            reliability_label="reputed_media",
            query_text="green crest supply chain limited kolkata section 409",
            published_at="2026-03-27",
            source_type="demo",
        ),
    ]

