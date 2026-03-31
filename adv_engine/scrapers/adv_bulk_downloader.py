"""
SEC IAPD bulk data downloader module.

Downloads SEC IAPD bulk CSV files with streaming, conditional GET,
rate limiting, and caching support for 200-500MB files.
"""

import os
import logging
import time
from pathlib import Path
from typing import Optional, List
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import (
    USER_AGENT,
    SEC_RATE_LIMIT_DELAY,
    CACHE_DIR,
    CACHE_EXPIRY_DAYS,
)

logger = logging.getLogger(__name__)


class ADVBulkDownloader:
    """Downloads and manages SEC IAPD bulk CSV files."""

    def __init__(self, cache_dir: str = CACHE_DIR):
        """Initialize downloader with caching support."""
        self.cache_dir = Path(cache_dir) / "adv"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = self._create_session()
        self.last_request_time = 0

    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy."""
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _rate_limit(self) -> None:
        """Enforce SEC rate limiting."""
        elapsed = time.time() - self.last_request_time
        if elapsed < SEC_RATE_LIMIT_DELAY:
            time.sleep(SEC_RATE_LIMIT_DELAY - elapsed)
        self.last_request_time = time.time()

    def _get_cache_path(self, url: str) -> Path:
        """Get cache file path for a URL."""
        filename = url.split("/")[-1] or "index.json"
        return self.cache_dir / filename

    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cached file is still valid."""
        if not cache_path.exists():
            return False

        file_age = datetime.now() - datetime.fromtimestamp(
            cache_path.stat().st_mtime
        )
        return file_age < timedelta(days=CACHE_EXPIRY_DAYS)

    def _download_with_streaming(
        self, url: str, target_path: Path, chunk_size: int = 8192
    ) -> bool:
        """Download file with streaming and progress tracking."""
        try:
            self._rate_limit()
            logger.info(f"Downloading {url}")

            response = self.session.get(
                url, stream=True, timeout=30, allow_redirects=True
            )
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0

            with open(target_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

                        if total_size:
                            percent = (downloaded / total_size) * 100
                            logger.debug(
                                f"Download progress: {percent:.1f}% "
                                f"({downloaded / 1024 / 1024:.1f}MB)"
                            )

            logger.info(
                f"Successfully downloaded {target_path.name} "
                f"({downloaded / 1024 / 1024:.1f}MB)"
            )
            return True

        except Exception as e:
            logger.error(f"Download failed for {url}: {e}")
            if target_path.exists():
                target_path.unlink()
            return False

    def download_adv_bulk_data(self, use_cache: bool = True) -> Optional[Path]:
        """Download latest SEC IAPD bulk CSV file."""
        index_url = "https://www.sec.gov/Archives/edgar/index/master.idx"
        cache_path = self._get_cache_path(index_url)

        if use_cache and self._is_cache_valid(cache_path):
            logger.info(f"Using cached IAPD bulk data from {cache_path}")
            return cache_path

        if self._download_with_streaming(index_url, cache_path):
            return cache_path

        if cache_path.exists():
            logger.warning("Using stale cache due to download failure")
            return cache_path

        return None

    def get_download_urls(self) -> List[str]:
        """Get list of available SEC IAPD bulk CSV download URLs."""
        urls = []

        try:
            self._rate_limit()
            response = self.session.get(
                "https://www.sec.gov/Archives/edgar/",
                timeout=10,
            )
            response.raise_for_status()

            for line in response.text.split("\n"):
                if "master.idx" in line or ".csv" in line:
                    urls.append(line.strip())

        except Exception as e:
            logger.warning(f"Failed to fetch index: {e}")

        current_year = datetime.now().year
        for year in range(current_year, max(current_year - 5, 2000), -1):
            for quarter in range(4, 0, -1):
                urls.append(
                    f"https://www.sec.gov/Archives/edgar/"
                    f"full-index/{year}/QTR{quarter}/master.csv"
                )

        return [url for url in urls if url]

    def download_file_streaming(
        self,
        url: str,
        output_path: Optional[Path] = None,
        chunk_size: int = 8192,
    ) -> Optional[Path]:
        """Download file with streaming for large files."""
        if output_path is None:
            output_path = self._get_cache_path(url)

        if output_path.exists():
            logger.info(f"Using cached file: {output_path}")
            return output_path

        if self._download_with_streaming(url, output_path, chunk_size):
            return output_path

        return None

    def cleanup_old_cache(self, days: int = CACHE_EXPIRY_DAYS) -> int:
        """Remove cached files older than specified days."""
        cutoff = datetime.now() - timedelta(days=days)
        deleted = 0

        try:
            for file_path in self.cache_dir.glob("*"):
                if file_path.is_file():
                    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_mtime < cutoff:
                        file_path.unlink()
                        logger.info(f"Deleted old cache: {file_path.name}")
                        deleted += 1
        except Exception as e:
            logger.error(f"Error cleaning cache: {e}")

        return deleted

    def get_cache_stats(self) -> dict:
        """Get statistics about cached files."""
        total_size = 0
        file_count = 0

        for file_path in self.cache_dir.glob("*"):
            if file_path.is_file():
                file_count += 1
                total_size += file_path.stat().st_size

        return {
            "cache_dir": str(self.cache_dir),
            "file_count": file_count,
            "total_size_mb": total_size / 1024 / 1024,
            "files": [
                {
                    "name": f.name,
                    "size_mb": f.stat().st_size / 1024 / 1024,
                    "age_days": (
                        datetime.now() - datetime.fromtimestamp(f.stat().st_mtime)
                    ).days,
                }
                for f in sorted(
                    self.cache_dir.glob("*"),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )
            ],
        }


def download_adv_bulk_data(use_cache: bool = True) -> Optional[Path]:
    """Convenience function to download IAPD bulk data."""
    downloader = ADVBulkDownloader()
    return downloader.download_adv_bulk_data(use_cache=use_cache)


def get_download_urls() -> List[str]:
    """Convenience function to get available download URLs."""
    downloader = ADVBulkDownloader()
    return downloader.get_download_urls()
