"""
Post-processing stage for Guncad scraper output.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urljoin, urlparse, unquote

import logging
import requests
from bs4 import BeautifulSoup

import sys
import json

BASE_DIR = Path(__file__).resolve().parent
LBRYTOOLS_DIR = BASE_DIR / "lbrytools"
DEFAULT_LBRY_DOWNLOAD_DIR = BASE_DIR / "lbry_downloads"
LBRY_RPC_URL = "http://localhost:5279"

if LBRYTOOLS_DIR.exists():
    sys.path.insert(0, str(LBRYTOOLS_DIR))

try:
    import lbrytools as lbryt  # type: ignore
except ImportError:
    lbryt = None

logger = logging.getLogger(__name__)


def configure_logging(level: int) -> None:
    logging.getLogger().setLevel(level)
    logger.setLevel(level)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=level, format="%(asctime)s - %(message)s")


def ensure_download_dir() -> Path:
    DEFAULT_LBRY_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    return DEFAULT_LBRY_DOWNLOAD_DIR

@dataclass
class ExternalLink:
    id: int
    link_id: int
    external_url: str
    source_href: str
    link_text: str


def fetch_external_links(db_path: str) -> Iterable[ExternalLink]:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, link_id, external_url, source_href, link_text FROM exturl ORDER BY id"
        )
        for row in cursor.fetchall():
            yield ExternalLink(*row)


def find_download_target(page_url: str, html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    download_selectors = [
        ("a", lambda tag: "download" in (tag.get_text(" ", strip=True) or "").lower()),
        ("button", lambda tag: "download" in (tag.get_text(" ", strip=True) or "").lower()),
        ("a", lambda tag: tag.has_attr("download")),
        ("button", lambda tag: tag.has_attr("data-download")),
    ]

    for selector, predicate in download_selectors:
        for tag in soup.find_all(selector):
            if predicate(tag):
                href = tag.get("href") or tag.get("data-href")
                if href:
                    return urljoin(page_url, href)
    return None


def trigger_download(url: str) -> bool:
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        # We intentionally do nothing with the body; the request itself triggers the server-side download.
        return True
    except requests.RequestException as exc:
        logger.warning("Download trigger failed for %s: %s", url, exc)
        return False


def odysee_to_canonical(uri: str) -> Optional[str]:
    if uri.startswith("lbry://"):
        canonical = uri[len("lbry://") :]
        return canonical.strip("/")

    parsed = urlparse(uri)
    if "odysee.com" not in parsed.netloc.lower():
        return None

    path = unquote(parsed.path.strip("/"))
    if not path:
        return None

    segments = []
    for segment in path.split("/"):
        if ":" in segment and "#" not in segment:
            name, short_id = segment.split(":", 1)
            segment = f"{name}#{short_id}"
        segments.append(segment)

    canonical = "/".join(segments)
    return canonical.strip("/")


def download_with_lbrytools(canonical_uri: str) -> bool:
    if lbryt is None:
        logger.warning("lbrytools is not installed; cannot download LBRY content.")
        return False

    download_dir = ensure_download_dir()
    try:
        result = lbryt.download_single(
            uri=canonical_uri,
            save_file=True,
            ddir=str(download_dir),
        )
    except Exception as exc:  # noqa: BLE001
        return handle_lbry_error(exc, canonical_uri, download_dir)

    if isinstance(result, bool):
        logger.info("lbrytools download_single returned %s for %s", result, canonical_uri)
        if result:
            return True
        return fallback_lbry_get(canonical_uri, download_dir)

    if not isinstance(result, dict):
        logger.warning("Unexpected result type from lbrytools for %s: %s", canonical_uri, type(result))
        return fallback_lbry_get(canonical_uri, download_dir)

    output_path = result.get("download_path") or result.get("file_path") or str(download_dir)
    if output_path:
        logger.info("LBRY download saved to %s", output_path)
    else:
        logger.info("LBRY download triggered for %s", canonical_uri)
    return True


def handle_lbry_error(exc: Exception, canonical_uri: str, download_dir: Path) -> bool:
    message = str(exc)
    if "Cannot establish connection" in message:
        logger.error("Cannot connect to lbrynet daemon. Please run `lbrynet start` before Stage2.")
        return False
    logger.warning("lbrytools download failed for %s: %s", canonical_uri, message)
    return fallback_lbry_get(canonical_uri, download_dir)


def fallback_lbry_get(canonical_uri: str, download_dir: Path) -> bool:
    logger.info("Falling back to direct lbrynet RPC download for %s", canonical_uri)
    rpc_result = call_lbry_rpc(
        "get",
        {
            "uri": f"lbry://{canonical_uri}",
            "save_file": True,
            "download_dir": str(download_dir),
        },
    )
    if not rpc_result:
        logger.warning("lbrynet RPC get call returned no result for %s", canonical_uri)
        return False

    output_path = rpc_result.get("download_path") or rpc_result.get("file_path") or str(download_dir)
    logger.info("lbrynet RPC download saved to %s", output_path)
    return True


def call_lbry_rpc(method: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        response = requests.post(
            LBRY_RPC_URL,
            json={"method": method, "params": params},
            timeout=30,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Failed to call lbrynet RPC %s: %s", method, exc)
        return None

    try:
        data = response.json()
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON from lbrynet RPC %s: %s", method, exc)
        return None

    if "error" in data:
        logger.warning("lbrynet RPC %s error: %s", method, data["error"])
        return None

    result = data.get("result")
    if result is None:
        logger.warning("lbrynet RPC %s returned no result field.", method)
        return None
    return result


def process_external_links(db_path: str, limit: Optional[int] = None) -> None:
    logger.info("Stage 2 download triggering:")
    for idx, ext in enumerate(fetch_external_links(db_path)):
        if limit is not None and idx >= limit:
            break

        canonical = odysee_to_canonical(ext.external_url)
        if canonical:
            logger.info("Processing Odyssey/LBRY link #%s: %s", ext.id, canonical)
            download_with_lbrytools(canonical)
            continue

        logger.info("Visiting external link #%s: %s", ext.id, ext.external_url)
        try:
            resp = requests.get(ext.external_url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Failed to load page %s: %s", ext.external_url, exc)
            continue

        target = find_download_target(ext.external_url, resp.text)
        if not target:
            logger.warning("No download button found at %s", ext.external_url)
            continue

        if trigger_download(target):
            logger.info("Download request sent for %s", target)


def run_stage_two(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 2 now inspects the exturl table, visits each site, and triggers its download button.
    """
    stats = payload.get("stats") or {}
    logger.info("Stage 2 processing summary:")
    for key, value in stats.items():
        logger.info("  - %s: %s", key, value)
    if payload.get("json_file"):
        logger.info("  JSON export: %s", payload["json_file"])
    db_path = payload.get("db_path")
    logger.info("  DB path: %s", db_path)
    if payload.get("failed_details"):
        logger.warning("  Failed detail pages: %s", len(payload["failed_details"]))

    if not db_path:
        logger.warning("No database path provided; skipping download triggers.")
        return payload

    process_external_links(db_path)
    return payload


if __name__ == "__main__":
    sample = {
        "stats": {"total_links": 5, "pages_with_links": 2},
        "json_file": "sample.json",
        "db_path": "guncad.db",
        "failed_details": [],
    }
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
    run_stage_two(sample)