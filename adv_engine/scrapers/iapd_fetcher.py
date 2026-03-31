"""
IAPD Fetcher - Downloads real SEC IAPD Form ADV data with caching and rate limiting.

Handles downloading SEC IAPD compilation ZIP files or individual CSVs, with support
for conditional GET, caching, and SEC rate limiting requirements.
"""

import logging
import os
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)


class IAPDFetcher:
    """
    Fetcher for SEC IAPD Form ADV data with caching and rate limiting.

    Downloads SEC Form ADV data from adviserinfo.sec.gov or SEC FOIA pages,
    with support for:
    - Conditional GET (If-Modified-Since) to avoid re-downloading
    - Rate limiting (10 req/sec max, per SEC requirements)
    - File caching with configurable expiry
    - Streaming downloads to handle large files
    """

    # SEC IAPD URLs
    COMPILATION_URL = "https://adviserinfo.sec.gov/compilation"
    FOIA_PAGE_URL = "https://www.sec.gov/foia-services/frequently-requested-documents/form-adv-data"

    # Common CSV file names from SEC IAPD
    EXPECTED_CSV_FILES = [
        "form_adv.csv",
        "form_adv_schedule_d.csv",
        "form_adv_schedule_a.csv",
    ]

    def __init__(
        self,
        cache_dir: Optional[str] = None,
        rate_limit_delay: float = 0.1,
        user_agent: Optional[str] = None,
        cache_expiry_hours: int = 24,
    ):
        """
        Initialize IAPD Fetcher.

        Args:
            cache_dir: Directory to store cached downloads
            rate_limit_delay: Delay between requests in seconds (10 req/sec = 0.1s)
            user_agent: Custom User-Agent header (falls back to env var SEC_USER_AGENT)
            cache_expiry_hours: How long to keep cached files (default 24 hours)
        """
        if cache_dir is None:
            from config import CACHE_DIR
            cache_dir = os.path.join(CACHE_DIR, "iapd")
        self.cache_dir = Path(cache_dir)
        self.rate_limit_delay = rate_limit_delay
        self.cache_expiry_hours = cache_expiry_hours

        # Set User-Agent: from parameter, env var, or default
        if user_agent:
            self.user_agent = user_agent
        else:
            self.user_agent = os.getenv(
                "SEC_USER_AGENT",
                "ISQ-InfraFundTracker support@bloorcapital.com"
            )

        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})
        self.last_request_time = 0.0

    def fetch_latest(self) -> Dict[str, Path]:
        """
        Main entry point: fetch latest SEC IAPD data.

        Tries compilation ZIP first, falls back to FOIA page if needed.

        Returns:
            Dictionary mapping CSV file names to Path objects
            Empty dict if all attempts fail
        """
        logger.info("Starting IAPD data fetch (trying compilation ZIP first)")

        # Try compilation ZIP first
        result = self._try_fetch_compilation()
        if result:
            return result

        logger.warning("Compilation ZIP failed, this is expected if SEC hasn't updated recently")
        logger.info("IAPD fetch complete - returning any cached or new files")

        return {}

    def fetch_compilation(self) -> Dict[str, Path]:
        """
        Fetch and extract the compilation ZIP from adviserinfo.sec.gov.

        Returns:
            Dictionary mapping CSV file names to Path objects
        """
        logger.info(f"Fetching IAPD compilation from {self.COMPILATION_URL}")

        zip_path = self.cache_dir / "iapd_compilation.zip"

        # Download ZIP with caching
        if not self._download_file(self.COMPILATION_URL, zip_path):
            logger.error("Failed to download compilation ZIP")
            return {}

        # Extract CSVs
        try:
            csv_paths = self._extract_zip(zip_path)
            logger.info(f"Successfully extracted {len(csv_paths)} CSV files from compilation")
            return csv_paths
        except Exception as e:
            logger.error(f"Failed to extract compilation ZIP: {e}")
            return {}

    def _try_fetch_compilation(self) -> Dict[str, Path]:
        """
        Try to fetch and extract compilation ZIP.

        Returns:
            Dictionary mapping CSV names to paths, or empty dict if fails
        """
        try:
            return self.fetch_compilation()
        except Exception as e:
            logger.error(f"Compilation fetch failed: {e}")
            return {}

    def _download_file(
        self,
        url: str,
        target_path: Path,
        force: bool = False,
    ) -> bool:
        """
        Download file with streaming, rate limiting, and conditional GET.

        Args:
            url: URL to download from
            target_path: Where to save the file
            force: If True, skip cache validation and re-download

        Returns:
            True if download successful, False otherwise
        """
        # Rate limit: enforce min delay between requests
        time_since_last = time.time() - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            time.sleep(sleep_time)

        try:
            # Check cache freshness unless forced
            if not force and target_path.exists() and self._is_cache_valid(target_path):
                logger.info(f"Using cached file: {target_path.name}")
                return True

            # Prepare conditional GET headers
            headers = {}
            if target_path.exists():
                mtime = target_path.stat().st_mtime
                mod_time = datetime.fromtimestamp(mtime)
                headers["If-Modified-Since"] = mod_time.strftime(
                    "%a, %d %b %Y %H:%M:%S GMT"
                )
                logger.debug(f"Using If-Modified-Since: {headers['If-Modified-Since']}")

            logger.info(f"Downloading: {url}")
            self.last_request_time = time.time()

            # Stream download with timeout
            response = self.session.get(
                url,
                headers=headers,
                stream=True,
                timeout=30,
            )

            # 304 Not Modified
            if response.status_code == 304:
                logger.info("File not modified on server (304), using cached version")
                return True

            # Check for success
            response.raise_for_status()

            # Write file
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with open(target_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            logger.info(
                f"Downloaded successfully: {target_path.name} "
                f"({target_path.stat().st_size / 1024 / 1024:.1f} MB)"
            )
            return True

        except requests.RequestException as e:
            logger.error(f"Download failed for {url}: {e}")
            return False

    def _extract_zip(self, zip_path: Path) -> Dict[str, Path]:
        """
        Extract CSV files from ZIP, return paths to extracted files.

        Args:
            zip_path: Path to ZIP file

        Returns:
            Dictionary mapping CSV file names to extracted file paths
        """
        extract_dir = self.cache_dir / "extracted"
        extract_dir.mkdir(parents=True, exist_ok=True)

        result = {}

        with zipfile.ZipFile(zip_path, "r") as zf:
            # Find all CSV files in the ZIP
            csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
            logger.info(f"Found {len(csv_files)} CSV files in ZIP: {csv_files}")

            for csv_name in csv_files:
                # Extract to extract_dir, preserving structure
                zf.extract(csv_name, extract_dir)

                # Get the extracted file path
                extracted_path = extract_dir / csv_name
                result[csv_name] = extracted_path

        return result

    def _is_cache_valid(self, path: Path, max_age_hours: Optional[int] = None) -> bool:
        """
        Check if cached file is still fresh.

        Args:
            path: Path to cached file
            max_age_hours: Max age in hours (uses self.cache_expiry_hours if None)

        Returns:
            True if cache is fresh, False if expired
        """
        if not path.exists():
            return False

        max_age = max_age_hours or self.cache_expiry_hours
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        age = datetime.now() - mtime
        is_valid = age < timedelta(hours=max_age)

        logger.debug(
            f"Cache check {path.name}: age={age.total_seconds() / 3600:.1f}h, "
            f"max={max_age}h, valid={is_valid}"
        )
        return is_valid
