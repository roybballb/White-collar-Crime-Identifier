from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
OUTPUT_DIR = DATA_DIR / "output"
RUN_METADATA_PATH = OUTPUT_DIR / "run_metadata_latest.json"

RELIABILITY_SCORES = {
    "official": 1.0,
    "regulated": 0.85,
    "registry_aggregator": 0.65,
    "reputed_media": 0.7,
    "open_web": 0.4,
}

LEGAL_SECTION_MAP = {
    "164": "Disqualification of directors for non-filing or defaults",
    "248": "Removal of company name from register / striking off",
    "455": "Dormant company status",
    "406": "Criminal breach of trust",
    "409": "Criminal breach of trust by agent or public servant",
    "420": "Cheating and dishonestly inducing delivery of property",
    "447": "Fraud",
    "448": "False statement",
    "449": "False evidence",
    "467": "Forgery of valuable security",
    "468": "Forgery for cheating",
    "471": "Using forged document as genuine",
}

SECTION_ACT_MAP = {
    "164": "Companies Act, 2013",
    "248": "Companies Act, 2013",
    "447": "Companies Act, 2013",
    "448": "Companies Act, 2013",
    "449": "Companies Act, 2013",
    "455": "Companies Act, 2013",
    "406": "Indian Penal Code, 1860",
    "409": "Indian Penal Code, 1860",
    "420": "Indian Penal Code, 1860",
    "467": "Indian Penal Code, 1860",
    "468": "Indian Penal Code, 1860",
    "471": "Indian Penal Code, 1860",
}

WEST_BENGAL_DISTRICTS = [
    "Alipurduar",
    "Bankura",
    "Birbhum",
    "Cooch Behar",
    "Dakshin Dinajpur",
    "Darjeeling",
    "Hooghly",
    "Howrah",
    "Jalpaiguri",
    "Jhargram",
    "Kalimpong",
    "Kolkata",
    "Malda",
    "Murshidabad",
    "Nadia",
    "North 24 Parganas",
    "Paschim Bardhaman",
    "Paschim Medinipur",
    "Purba Bardhaman",
    "Purba Medinipur",
    "Purulia",
    "South 24 Parganas",
    "Uttar Dinajpur",
]


@dataclass(frozen=True)
class RiskWeights:
    mention_count: float = 14.0
    fraud_mentions: float = 10.0
    distinct_sources: float = 7.0
    legal_sections: float = 6.0
    district_spread: float = 4.0
    average_quality: float = 0.18
    average_reliability: float = 10.0


RISK_WEIGHTS = RiskWeights()

EXPORT_FILE_ORDER = [
    "master_dataset",
    "company_reports",
    "companies",
    "enforcement_cases",
    "repeat_offenders",
    "risk_scores",
    "cluster_summary",
    "source_log",
]

DEFAULT_LIVE_SEARCH_QUERIES = [
    "West Bengal company fraud",
    "Kolkata corporate fraud company",
    "SFIO company fraud West Bengal",
    "section 447 company fraud Kolkata",
]

DEFAULT_HISTORICAL_SEARCH_QUERIES = [
    "West Bengal company fraud",
    "Kolkata corporate fraud company",
    "West Bengal company liquidation",
    "Calcutta High Court winding up company",
    "ROC Kolkata struck off company",
    "West Bengal director disqualification company",
    "Kolkata insolvency company",
]

DEFAULT_SOURCE_LIMIT = 15
DEFAULT_HISTORICAL_WINDOW_YEARS = 5
DEFAULT_HISTORICAL_START_YEAR = 2000
DEFAULT_HISTORICAL_END_YEAR = 2025
DEFAULT_COMPANY_PROFILE_LIMIT = 280
DEFAULT_INSTA_ENRICHMENT_LIMIT = 80
DEFAULT_INSTA_DISCOVERY_PAGE_DEPTH = 0
