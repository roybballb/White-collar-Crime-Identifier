from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any

import pandas as pd


def _serialize_value(value: Any) -> Any:
    if isinstance(value, list):
        return " | ".join(str(item) for item in value)
    return value


def records_to_frame(records: list[Any]) -> pd.DataFrame:
    rows = []
    for record in records:
        if is_dataclass(record):
            row = asdict(record)
        elif isinstance(record, dict):
            row = record
        else:
            raise TypeError(f"Unsupported record type: {type(record)!r}")
        rows.append({key: _serialize_value(value) for key, value in row.items()})
    return pd.DataFrame(rows)


def write_outputs(dataframes: dict[str, pd.DataFrame], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    for dataset_name, dataframe in dataframes.items():
        latest_path = output_dir / f"{dataset_name}_latest.csv"
        snapshot_path = output_dir / f"{dataset_name}_{stamp}.csv"
        dataframe.to_csv(latest_path, index=False)
        dataframe.to_csv(snapshot_path, index=False)


def write_run_metadata(metadata: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    latest_path = output_dir / "run_metadata_latest.json"
    stamp = datetime.now().strftime("%Y%m%d")
    snapshot_path = output_dir / f"run_metadata_{stamp}.json"
    payload = json.dumps(metadata, indent=2)
    latest_path.write_text(payload, encoding="utf-8")
    snapshot_path.write_text(payload, encoding="utf-8")


def load_outputs(output_dir: Path) -> dict[str, pd.DataFrame]:
    datasets = {}
    for path in output_dir.glob("*_latest.csv"):
        datasets[path.name.replace("_latest.csv", "")] = pd.read_csv(path)
    return datasets


def load_run_metadata(output_dir: Path) -> dict[str, Any]:
    path = output_dir / "run_metadata_latest.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))
