from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import sys

from hybrid_legal_dashboard.config import RAW_DATA_DIR
from hybrid_legal_dashboard.pipeline import build_mca_static_sources
from hybrid_legal_dashboard.services.ingestion import DEFAULT_HEADERS


MCA_REFERER = "https://www.mca.gov.in/"


def _filename_from_url(url: str) -> str:
    return url.rsplit("/", 1)[-1].split("?", 1)[0]


def _is_pdf_file(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 1024:
        return False
    return path.read_bytes()[:4] == b"%PDF"


def _download_with_curl(url: str, destination: Path, overwrite: bool) -> tuple[bool, str]:
    if destination.exists() and not overwrite and _is_pdf_file(destination):
        return True, f"Already present: {destination}"

    if shutil.which("curl") is None:
        return False, "curl is not available on this machine."

    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(f"{destination.suffix}.part")
    if temp_path.exists():
        temp_path.unlink()

    command = [
        "curl",
        "-L",
        "--fail",
        "--retry",
        "2",
        "-A",
        DEFAULT_HEADERS["User-Agent"],
        "-e",
        MCA_REFERER,
        "-o",
        str(temp_path),
        url,
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        if temp_path.exists():
            temp_path.unlink()
        message = result.stderr.strip() or result.stdout.strip() or f"curl exited with code {result.returncode}"
        return False, message

    if not _is_pdf_file(temp_path):
        preview = ""
        try:
            preview = temp_path.read_text(encoding="utf-8", errors="ignore")[:160].strip()
        except OSError:
            preview = ""
        temp_path.unlink(missing_ok=True)
        return False, preview or "Downloaded file was not a valid PDF."

    temp_path.replace(destination)
    return True, f"Saved {destination}"


def _manual_download_note(url: str, destination: Path) -> str:
    return (
        f"Open {url} in a browser and save the PDF as {destination.name} inside {destination.parent}."
    )


def download_mca_pdfs(destination_dir: Path, overwrite: bool = False) -> int:
    destination_dir.mkdir(parents=True, exist_ok=True)
    failures = 0

    print(f"Downloading MCA PDFs into {destination_dir}")
    for config in build_mca_static_sources():
        filename = _filename_from_url(config.url)
        destination = destination_dir / filename
        ok, message = _download_with_curl(config.url, destination, overwrite=overwrite)
        if ok:
            print(f"[ok] {filename}: {message}")
            continue

        failures += 1
        print(f"[warn] {filename}: {message}")
        print(f"       {_manual_download_note(config.url, destination)}")

    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Download official MCA PDFs into data/raw/ for local parsing.")
    parser.add_argument("--dest-dir", default=str(RAW_DATA_DIR / "mca"))
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    failures = download_mca_pdfs(Path(args.dest_dir), overwrite=args.overwrite)
    if failures:
        print(
            f"Completed with {failures} warning(s). "
            "Any PDFs that were blocked can still be downloaded manually into the same folder."
        )
        sys.exit(1)

    print("All configured MCA PDFs were downloaded successfully.")


if __name__ == "__main__":
    main()
