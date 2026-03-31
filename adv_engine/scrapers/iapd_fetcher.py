"""
IAPD Fetcher - Downloads real SEC IAPD Form ADV data via the public JSON API.

Uses the SEC IAPD search API at api.adviserinfo.sec.gov to fetch investment
adviser firm data using text-based queries and pagination.

The search API returns: firm name, CRD, SEC number, state, active status,
branch count. It does NOT return AUM, client counts, or custodian info.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Set

import requests

from .adv_parser import FirmRecord

logger = logging.getLogger(__name__)


class IAPDFetcher:
    """
    Fetcher for SEC IAPD Form ADV data via the public JSON search API.

    Strategy: search with common RIA industry terms to cover the full
    universe of ~40,000 firms. Deduplicates by CRD number.

    The API caps at ~10,000 results per query, so we use multiple
    search terms to achieve broad coverage.
    """

    # SEC IAPD public search API
    API_BASE = "https://api.adviserinfo.sec.gov/search/firm"

    # Page size for API requests
    PAGE_SIZE = 100

    # Search terms designed to cover the vast majority of RIA firms.
    # Each term is paginated up to 10,000 results.
    # Together these should cover 30,000+ unique firms.
    SEARCH_TERMS = [
        "advisors",
        "advisory",
        "capital",
        "wealth",
        "management",
        "financial",
        "partners",
        "investment",
        "group",
        "associates",
        "consulting",
        "trust",
        "family office",
        "private",
        "asset",
        "fund",
        "securities",
        "planning",
        "retirement",
        "fiduciary",
        "LLC",
        "LP",
        "Inc",
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
    ):
        if cache_dir is None:
            from config import CACHE_DIR
            cache_dir = os.path.join(CACHE_DIR, "iapd")
        self.cache_dir = Path(cache_dir)
        self.rate_limit_delay = rate_limit_delay
        self.cache_expiry_hours = cache_expiry_hours

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
            "firms_inactive": 0,
            "errors": 0,
        }

    def fetch_latest(self) -> List[FirmRecord]:
        """
        Main entry point: fetch SEC-registered IA firms via the JSON API.

        Uses text-search queries with common RIA terms, deduplicates by CRD.

        Returns:
            List of FirmRecord instances (active firms only)
            Empty list if all attempts fail
        """
        # Check cache first
        cache_path = self.cache_dir / "iapd_firms.json"
        if self._is_cache_valid(cache_path):
            cached = self._load_cache(cache_path)
            if cached:
                logger.info(f"Using cached data: {len(cached)} firms")
                return cached

        logger.info(
            f"Starting IAPD API fetch with {len(self.SEARCH_TERMS)} search terms"
        )
        all_firms = {}  # keyed by CRD to deduplicate
        seen_crds: Set[str] = set()

        for term in self.SEARCH_TERMS:
            try:
                new_count = self._fetch_by_term(term, all_firms, seen_crds)
                if new_count > 0:
                    logger.info(
                        f"  '{term}': +{new_count} new firms "
                        f"(total unique: {len(all_firms)})"
                    )
            except Exception as e:
                logger.error(f"Error searching '{term}': {e}")
                self.stats["errors"] += 1
                continue

        firms = list(all_firms.values())
        logger.info(
            f"IAPD fetch complete: {len(firms)} active firms "
            f"(API calls: {self.stats['api_calls']}, "
            f"inactive skipped: {self.stats['firms_inactive']})"
        )

        # Cache results
        if firms:
            self._save_cache(cache_path, firms)

        return firms

    def _fetch_by_term(
        self, term: str, all_firms: Dict[str, FirmRecord], seen_crds: Set[str]
    ) -> int:
        """
        Fetch all firms matching a search term, paginating through results.

        Args:
            term: Search term
            all_firms: Dict to add firms to (keyed by CRD)
            seen_crds: Set of already-seen CRD numbers

        Returns:
            Number of NEW firms added by this term
        """
        new_count = 0
        start = 0
        max_per_term = 10000  # API limit

        while start < max_per_term:
            data = self._api_request(query=term, start=start)
            if data is None:
                break

            hits = data.get("hits", {})
            if not isinstance(hits, dict):
                break

            hit_list = hits.get("hits", [])
            total = hits.get("total", 0)

            if not hit_list:
                break

            for hit in hit_list:
                source = hit.get("_source", hit)
                crd = str(source.get("firm_source_id", ""))

                # Skip if already seen
                if not crd or crd in seen_crds:
                    continue

                seen_crds.add(crd)

                firm = self._parse_hit(source)
                if firm:
                    all_firms[crd] = firm
                    new_count += 1

            start += len(hit_list)

            # Stop if we've fetched all results for this term
            if start >= total:
                break

        return new_count

    def _api_request(
        self, query: str = "", start: int = 0
    ) -> Optional[Dict]:
        """
        Make a single API request to the SEC IAPD search endpoint.
        """
        # Rate limit
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)

        params = {
            "query": query,
            "hl": "true",
            "nrows": self.PAGE_SIZE,
            "start": start,
            "r": self.PAGE_SIZE,
            "sort": "score+desc",
            "wt": "json",
        }

        try:
            self.last_request_time = time.time()
            self.stats["api_calls"] += 1

            response = self.session.get(
                self.API_BASE,
                params=params,
                timeout=30,
            )

            if response.status_code == 429:
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
            logger.error(f"Failed to parse API response: {e}")
            return None

    def _parse_hit(self, source: Dict[str, Any]) -> Optional[FirmRecord]:
        """
        Parse a single API search hit into a FirmRecord.

        The API returns: firm_name, firm_source_id (CRD), firm_ia_scope,
        firm_ia_full_sec_number, firm_ia_address_details (JSON string),
        firm_branches_count, firm_other_names.

        NOTE: AUM is NOT available from the search API.
        """
        try:
            firm_name = source.get("firm_name", "").strip()
            if not firm_name:
                return None

            # Only include ACTIVE investment advisers
            scope = source.get("firm_ia_scope", "")
            if scope != "ACTIVE":
                self.stats["firms_inactive"] += 1
                return None

            crd = str(source.get("firm_source_id", ""))
            if not crd:
                return None

            # SEC file number
            sec_num = source.get("firm_ia_full_sec_number", "")
            if not sec_num:
                sec_num = f"CRD-{crd}"

            # Parse address details (JSON string)
            state = ""
            city = ""
            address_json = source.get("firm_ia_address_details", "")
            if address_json and isinstance(address_json, str):
                try:
                    addr = json.loads(address_json)
                    office = addr.get("officeAddress", {})
                    state = office.get("state", "")
                    city = office.get("city", "")
                except (json.JSONDecodeError, AttributeError):
                    pass

            # Normalize state to 2-letter code
            if state and len(state) > 2:
                state = self._normalize_state(state)

            # Branch count (proxy for firm size)
            branches = source.get("firm_branches_count", 0)
            try:
                branches = int(branches)
            except (ValueError, TypeError):
                branches = 0

            # Detect family office from name
            is_family_office = self._detect_family_office(firm_name)

            # Detect if name suggests alternatives/private funds
            manages_private = self._name_suggests_alts(firm_name)

            # Other names
            other_names = source.get("firm_other_names", [])
            if isinstance(other_names, str):
                other_names = [other_names]

            # Estimate AUM based on branch count (very rough proxy)
            # Firms with more branches tend to be larger
            # This is an approximation since the API doesn't provide AUM
            estimated_aum = self._estimate_aum(branches, is_family_office)

            # Build FirmRecord
            firm = FirmRecord(
                firm_name=firm_name,
                sec_file_number=sec_num,
                cik=crd,
                state=state,
                country="United States",
                aum_total=estimated_aum,
                aum_regulatory=estimated_aum,
                num_clients=max(branches * 20, 10),  # rough estimate
                is_family_office=is_family_office,
                manages_private_funds=manages_private,
                raw_data={
                    "api_source": source,
                    "other_names": other_names,
                    "city": city,
                    "branches": branches,
                    "aum_estimated": True,
                },
            )

            self.stats["firms_parsed"] += 1
            return firm

        except Exception as e:
            logger.debug(f"Error parsing hit: {e}")
            self.stats["errors"] += 1
            return None

    def _estimate_aum(self, branches: int, is_family_office: bool) -> float:
        """
        Rough AUM estimate based on branch count.

        NOTE: This is marked in raw_data as estimated. The dashboard
        should indicate when AUM data is estimated vs actual.

        Average SEC-registered IA has ~$500M AUM. Firms with more
        branches tend to be larger. Family offices tend to be smaller
        but with higher per-client AUM.
        """
        if is_family_office:
            return 200_000_000  # $200M typical family office
        if branches >= 10:
            return 5_000_000_000  # $5B+ for large multi-branch firms
        if branches >= 5:
            return 1_000_000_000  # $1B
        if branches >= 2:
            return 500_000_000  # $500M
        return 200_000_000  # $200M default for single-office

    def _normalize_state(self, state: str) -> str:
        """Normalize state name to 2-letter code."""
        if not state:
            return ""
        state = state.strip().upper()
        if len(state) == 2 and state.isalpha():
            return state

        state_map = {
            "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ",
            "ARKANSAS": "AR", "CALIFORNIA": "CA", "COLORADO": "CO",
            "CONNECTICUT": "CT", "DELAWARE": "DE", "FLORIDA": "FL",
            "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID",
            "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA",
            "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA",
            "MAINE": "ME", "MARYLAND": "MD", "MASSACHUSETTS": "MA",
            "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS",
            "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE",
            "NEVADA": "NV", "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ",
            "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC",
            "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK",
            "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI",
            "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD", "TENNESSEE": "TN",
            "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT",
            "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
            "WISCONSIN": "WI", "WYOMING": "WY",
            "DISTRICT OF COLUMBIA": "DC",
        }
        return state_map.get(state, state[:2] if len(state) >= 2 else "")

    def _detect_family_office(self, firm_name: str) -> bool:
        """Detect if firm name suggests a family office."""
        if not firm_name:
            return False
        name_lower = firm_name.lower()
        for pattern in self.FAMILY_OFFICE_PATTERNS:
            if re.search(pattern, name_lower, re.IGNORECASE):
                return True
        return False

    def _name_suggests_alts(self, firm_name: str) -> bool:
        """Detect if firm name suggests alternatives/private funds focus."""
        if not firm_name:
            return False
        name_lower = firm_name.lower()
        alt_keywords = [
            "alternative", "private equity", "private fund",
            "hedge fund", "real estate", "infrastructure",
            "venture", "private capital", "private credit",
        ]
        return any(kw in name_lower for kw in alt_keywords)

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
                        "manages_private_funds": f.manages_private_funds,
                        "city": f.raw_data.get("city", ""),
                        "branches": f.raw_data.get("branches", 0),
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
                    manages_private_funds=item.get("manages_private_funds", False),
                    raw_data={
                        "city": item.get("city", ""),
                        "branches": item.get("branches", 0),
                        "aum_estimated": True,
                    },
                )
                firms.append(firm)

            logger.info(f"Loaded {len(firms)} firms from cache")
            return firms
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
            return []
