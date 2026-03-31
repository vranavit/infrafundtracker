"""
IAPD Fetcher - Downloads real SEC IAPD Form ADV data via the public JSON API.

Uses the undocumented but public SEC IAPD search API at
api.adviserinfo.sec.gov to fetch investment adviser firm data.
Supports pagination, rate limiting, and caching.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

import requests

from .adv_parser import FirmRecord

logger = logging.getLogger(__name__)


class IAPDFetcher:
    """
    Fetcher for SEC IAPD Form ADV data via the public JSON search API.

    Uses api.adviserinfo.sec.gov/search/firm to query all SEC-registered
    investment advisers, with pagination to handle the full ~40,000+ universe.

    Features:
    - Paginated fetching (API caps at 10,000 per query, so we split by state)
    - Rate limiting (10 req/sec max, per SEC requirements)
    - JSON caching with configurable expiry
    - Direct conversion to FirmRecord objects (no CSV intermediate)
    """

    # SEC IAPD public search API
    API_BASE = "https://api.adviserinfo.sec.gov/search/firm"

    # Page size for API requests (max the API reliably handles)
    PAGE_SIZE = 100

    # US state codes to iterate through for full coverage
    # (API has 10k result cap, splitting by state keeps each query under limit)
    US_STATES = [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
        "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
        "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
        "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "PR",
        "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
        "WI", "WY",
    ]

    # Family office name patterns
    FAMILY_OFFICE_PATTERNS = [
        r"\bfamily\s+office\b",
        r"\bmfo\b",
        r"\bmulti-family\s+office\b",
        r"\bsingle\s+family\s+office\b",
        r"private\s+family",
        r"family\s+wealth",
        r"family\s+investment",
    ]

    def __init__(
        self,
        cache_dir: Optional[str] = None,
        rate_limit_delay: float = 0.12,
        user_agent: Optional[str] = None,
        cache_expiry_hours: int = 20,
        min_aum: float = 50_000_000,
    ):
        """
        Initialize IAPD Fetcher.

        Args:
            cache_dir: Directory to store cached JSON responses
            rate_limit_delay: Delay between requests (0.12s = ~8 req/sec, under SEC 10/s limit)
            user_agent: Custom User-Agent header
            cache_expiry_hours: How long to keep cached data (default 20 hours)
            min_aum: Minimum AUM to include firms (default $50M)
        """
        if cache_dir is None:
            from config import CACHE_DIR
            cache_dir = os.path.join(CACHE_DIR, "iapd")
        self.cache_dir = Path(cache_dir)
        self.rate_limit_delay = rate_limit_delay
        self.cache_expiry_hours = cache_expiry_hours
        self.min_aum = min_aum

        if user_agent:
            self.user_agent = user_agent
        else:
            self.user_agent = os.getenv(
                "SEC_USER_AGENT",
                "ISQ-InfraFundTracker support@bloorcapital.com"
            )

        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        })
        self.last_request_time = 0.0

        # Stats
        self.stats = {
            "api_calls": 0,
            "total_hits": 0,
            "firms_parsed": 0,
            "firms_filtered_aum": 0,
            "errors": 0,
        }

    def fetch_latest(self) -> List[FirmRecord]:
        """
        Main entry point: fetch all SEC-registered IA firms via the JSON API.

        Iterates through US states to stay under the API's 10k result limit
        per query. Returns FirmRecord objects directly (no CSV intermediate).

        Returns:
            List of FirmRecord instances (filtered by min AUM)
            Empty list if all attempts fail
        """
        # Check cache first
        cache_path = self.cache_dir / "iapd_firms.json"
        if self._is_cache_valid(cache_path):
            cached = self._load_cache(cache_path)
            if cached:
                logger.info(f"Using cached data: {len(cached)} firms")
                return cached

        logger.info("Starting IAPD API fetch across all US states")
        all_firms = {}  # keyed by CRD to deduplicate

        for state in self.US_STATES:
            try:
                state_firms = self._fetch_state(state)
                for firm in state_firms:
                    # Deduplicate by SEC file number
                    all_firms[firm.sec_file_number] = firm
            except Exception as e:
                logger.error(f"Error fetching state {state}: {e}")
                self.stats["errors"] += 1
                continue

        firms = list(all_firms.values())
        logger.info(
            f"IAPD fetch complete: {len(firms)} firms "
            f"(API calls: {self.stats['api_calls']}, "
            f"filtered by AUM: {self.stats['firms_filtered_aum']})"
        )

        # Cache results
        if firms:
            self._save_cache(cache_path, firms)

        return firms

    def _fetch_state(self, state: str) -> List[FirmRecord]:
        """
        Fetch all IA firms in a given state via paginated API calls.

        Args:
            state: 2-letter US state code

        Returns:
            List of FirmRecord for firms in that state meeting AUM threshold
        """
        firms = []
        start = 0

        while True:
            data = self._api_request(state=state, start=start)
            if data is None:
                break

            hits = data.get("hits", {})
            hit_list = hits.get("hits", [])
            total = hits.get("total", 0)

            if not hit_list:
                break

            for hit in hit_list:
                firm = self._parse_hit(hit)
                if firm:
                    firms.append(firm)

            start += len(hit_list)

            # Stop if we've fetched all results
            if start >= total or start >= 10000:
                break

        if firms:
            logger.info(f"  {state}: {len(firms)} firms (of {start} total checked)")

        return firms

    def _api_request(
        self,
        state: Optional[str] = None,
        query: str = "",
        start: int = 0,
    ) -> Optional[Dict]:
        """
        Make a single API request to the SEC IAPD search endpoint.

        Args:
            state: Filter by state code
            query: Search query string
            start: Pagination offset

        Returns:
            Parsed JSON response or None on failure
        """
        # Rate limit
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)

        params = {
            "query": query,
            "nrows": self.PAGE_SIZE,
            "start": start,
            "r": self.PAGE_SIZE,
            "sort": "score+desc",
            "wt": "json",
        }

        if state:
            params["query"] = f"firm_ia_st_cd:{state}"

        try:
            self.last_request_time = time.time()
            self.stats["api_calls"] += 1

            response = self.session.get(
                self.API_BASE,
                params=params,
                timeout=30,
            )

            if response.status_code == 429:
                # Rate limited - back off and retry once
                logger.warning("Rate limited by SEC API, backing off 5s")
                time.sleep(5)
                self.last_request_time = time.time()
                response = self.session.get(
                    self.API_BASE,
                    params=params,
                    timeout=30,
                )

            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse API response as JSON: {e}")
            return None

    def _parse_hit(self, hit: Dict[str, Any]) -> Optional[FirmRecord]:
        """
        Parse a single API search hit into a FirmRecord.

        The API returns hits with a _source object containing firm fields
        like firm_name, firm_ia_aum, firm_ia_st_cd, etc.

        Args:
            hit: Single hit dict from API response

        Returns:
            FirmRecord if firm meets criteria, None otherwise
        """
        try:
            source = hit.get("_source", {})
            if not source:
                return None

            firm_name = source.get("firm_name", "").strip()
            if not firm_name:
                return None

            # CRD number (used as unique ID)
            crd = source.get("firm_source_id", "")
            if not crd:
                return None

            # SEC file number
            sec_num = source.get("firm_ia_full_sec_number", "")
            if not sec_num:
                sec_num = f"CRD-{crd}"  # fallback

            # State
            state = source.get("firm_ia_st_cd", "")

            # AUM - try multiple fields
            aum = 0.0
            aum_str = source.get("firm_ia_aum", "")
            if aum_str:
                aum = self._parse_aum(str(aum_str))

            # Filter by minimum AUM
            if aum < self.min_aum:
                self.stats["firms_filtered_aum"] += 1
                return None

            # Number of accounts/clients
            num_clients = self._safe_int(source.get("firm_ia_num_accts", 0))

            # City
            city = source.get("firm_ia_city", "")

            # Detect family office from name
            is_family_office = self._detect_family_office(firm_name)

            # Other names (may contain custodian hints)
            other_names = source.get("firm_other_names", [])
            if isinstance(other_names, str):
                other_names = [other_names]

            # BD (broker-dealer) indicator
            has_bd = bool(source.get("firm_bd_full_sec_number", ""))

            # Build FirmRecord
            firm = FirmRecord(
                firm_name=firm_name,
                sec_file_number=sec_num,
                cik=crd,
                state=state,
                country="United States",
                aum_total=aum,
                aum_regulatory=aum,
                num_clients=num_clients,
                is_family_office=is_family_office,
                is_registered_representative=has_bd,
                raw_data={
                    "api_source": source,
                    "other_names": other_names,
                    "city": city,
                },
            )

            self.stats["firms_parsed"] += 1
            return firm

        except Exception as e:
            logger.debug(f"Error parsing hit: {e}")
            self.stats["errors"] += 1
            return None

    def _parse_aum(self, value: str) -> float:
        """Parse AUM from API response (may be numeric string or formatted)."""
        if not value:
            return 0.0
        try:
            cleaned = str(value).replace("$", "").replace(",", "").strip()
            if not cleaned:
                return 0.0
            return float(cleaned)
        except (ValueError, TypeError):
            return 0.0

    def _safe_int(self, value) -> int:
        """Safely convert value to int."""
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    def _detect_family_office(self, firm_name: str) -> bool:
        """Detect if firm name suggests a family office."""
        if not firm_name:
            return False
        name_lower = firm_name.lower()
        for pattern in self.FAMILY_OFFICE_PATTERNS:
            if re.search(pattern, name_lower, re.IGNORECASE):
                return True
        return False

    def _is_cache_valid(self, path: Path) -> bool:
        """Check if cached file is still fresh."""
        if not path.exists():
            return False
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        age = datetime.now() - mtime
        return age < timedelta(hours=self.cache_expiry_hours)

    def _save_cache(self, path: Path, firms: List[FirmRecord]) -> None:
        """Save firms to JSON cache."""
        try:
            cache_data = {
                "generated_at": datetime.now().isoformat(),
                "count": len(firms),
                "firms": [
                    {
                        "firm_name": f.firm_name,
                        "sec_file_number": f.sec_file_number,
                        "crd": f.cik,
                        "state": f.state,
                        "aum_total": f.aum_total,
                        "num_clients": f.num_clients,
                        "is_family_office": f.is_family_office,
                        "city": f.raw_data.get("city", ""),
                    }
                    for f in firms
                ],
            }
            with open(path, "w") as fh:
                json.dump(cache_data, fh)
            logger.info(f"Cached {len(firms)} firms to {path}")
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")

    def _load_cache(self, path: Path) -> List[FirmRecord]:
        """Load firms from JSON cache."""
        try:
            with open(path, "r") as fh:
                data = json.load(fh)

            firms = []
            for item in data.get("firms", []):
                firm = FirmRecord(
                    firm_name=item["firm_name"],
                    sec_file_number=item["sec_file_number"],
                    cik=item.get("crd", ""),
                    state=item.get("state", ""),
                    aum_total=item.get("aum_total", 0),
                    num_clients=item.get("num_clients", 0),
                    is_family_office=item.get("is_family_office", False),
                    raw_data={"city": item.get("city", "")},
                )
                firms.append(firm)

            logger.info(f"Loaded {len(firms)} firms from cache")
            return firms
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
            return []
