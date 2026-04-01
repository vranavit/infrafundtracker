"""
IAPD Fetcher - Downloads real SEC Form ADV data via sec-api.io

Uses the sec-api.io Form ADV API to fetch complete adviser filings including
AUM, custodian names (Schedule D), client counts, investment types, fee
structure, and historical data.

Requires SEC_API_KEY environment variable (from sec-api.io, $55/month plan).
Falls back to the free SEC IAPD search API if no API key is set.
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
    Fetcher for SEC Form ADV data via sec-api.io.

    The sec-api.io Form ADV API provides complete filing data for 41,000+
    adviser firms including:
    - Real AUM (discretionary and total)
    - Custodian names from Schedule D Section 5.F
    - Client counts by type (HNW, institutional, etc.)
    - Investment types (private funds, real estate, etc.)
    - Fee structure and minimums
    - Registration dates
    - Historical filings for change detection

    API: POST https://api.sec-api.io/form-adv/firm?token=API_KEY
    Auth: token query parameter
    Docs: https://sec-api.io/docs/investment-adviser-and-adv-api
    """

    # sec-api.io endpoints
    SEC_API_FIRM_SEARCH = "https://api.sec-api.io/form-adv/firm"
    SEC_API_SCHEDULE_D = "https://api.sec-api.io/form-adv/schedule-d"

    # Page size for API requests (max 50 per sec-api.io docs)
    PAGE_SIZE = 50

    # How many firms to fetch total (covers SEC-registered advisers)
    MAX_FIRMS = 45000

    # Custodian keyword mapping for platform detection
    CUSTODIAN_KEYWORDS = {
        "schwab": ["schwab", "charles schwab"],
        "fidelity": ["fidelity", "national financial services", "nfs"],
        "pershing": ["pershing", "bny pershing"],
        "lpl": ["lpl financial", "lpl"],
        "icapital": ["icapital"],
        "cais": ["cais"],
        "morgan stanley": ["morgan stanley"],
        "merrill": ["merrill lynch", "merrill"],
        "ubs": ["ubs"],
        "raymond james": ["raymond james"],
    }

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
        rate_limit_delay: float = 0.25,
        cache_expiry_hours: int = 20,
    ):
        if cache_dir is None:
            from config import CACHE_DIR
            cache_dir = os.path.join(CACHE_DIR, "iapd")
        self.cache_dir = Path(cache_dir)
        self.rate_limit_delay = rate_limit_delay
        self.cache_expiry_hours = cache_expiry_hours

        self.api_key = os.environ.get("SEC_API_KEY", "")
        if not self.api_key:
            logger.warning(
                "SEC_API_KEY not set — will fall back to free IAPD search API "
                "(limited data: no AUM, no custodians, no client counts)"
            )

        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self.last_request_time = 0.0

        # Stats
        self.stats = {
            "api_calls": 0,
            "total_hits": 0,
            "firms_parsed": 0,
            "firms_skipped": 0,
            "errors": 0,
        }

    def fetch_latest(self) -> List[FirmRecord]:
        """
        Main entry point: fetch SEC-registered IA firms.

        Uses sec-api.io if API key is available, otherwise falls back to
        the free SEC IAPD search API.

        Returns:
            List of FirmRecord instances (active firms only)
        """
        # Check cache first
        cache_path = self.cache_dir / "secapi_firms.json"
        if self._is_cache_valid(cache_path):
            cached = self._load_cache(cache_path)
            if cached:
                logger.info(f"Using cached data: {len(cached)} firms")
                return cached

        if self.api_key:
            firms = self._fetch_via_sec_api()
        else:
            firms = self._fetch_via_free_api()

        # Cache results
        if firms:
            self._save_cache(cache_path, firms)

        return firms

    # =========================================================================
    # sec-api.io fetcher (full data)
    # =========================================================================

    def _fetch_via_sec_api(self) -> List[FirmRecord]:
        """
        Fetch all SEC-registered advisers via sec-api.io Form ADV API.

        Strategy: Query all adviser filings using broad Lucene queries,
        paginate through results. The API uses field paths like
        Info.FirmCrdNb, Info.FirmName, etc.

        Response format: {"total": N, "filings": [...]}
        """
        logger.info("Fetching firms via sec-api.io Form ADV API...")

        all_firms: List[FirmRecord] = []
        seen_crds: Set[str] = set()
        offset = 0

        # Query: all firms with a CRD number (effectively all registered firms)
        # Using wildcard on CRD to get all filings
        query = "Info.FirmCrdNb:*"

        total_available = None

        while offset < self.MAX_FIRMS:
            try:
                data = self._sec_api_request(query=query, start=offset)
                if data is None:
                    logger.error(f"API returned None at offset {offset}")
                    break

                # sec-api.io returns {"total": N, "filings": [...]}
                if isinstance(data, dict):
                    filings = data.get("filings", [])
                    if total_available is None:
                        total_available = data.get("total", 0)
                        logger.info(
                            f"  sec-api.io reports {total_available} total filings"
                        )
                elif isinstance(data, list):
                    filings = data
                else:
                    logger.error(f"Unexpected response type: {type(data)}")
                    break

                if not filings:
                    logger.info(f"No more results at offset {offset}")
                    break

                for filing in filings:
                    # Deduplicate by CRD
                    crd = str(
                        filing.get("Info", {}).get("FirmCrdNb", "")
                        or filing.get("firmCrdNumber", "")
                        or ""
                    )
                    if crd and crd in seen_crds:
                        continue
                    if crd:
                        seen_crds.add(crd)

                    firm = self._parse_sec_api_filing(filing)
                    if firm:
                        all_firms.append(firm)

                offset += len(filings)

                if offset % 1000 == 0 or offset < 100:
                    logger.info(
                        f"  Fetched {offset} filings, "
                        f"parsed {len(all_firms)} unique firms so far"
                    )

                # If we got fewer than PAGE_SIZE, we're done
                if len(filings) < self.PAGE_SIZE:
                    break

                # Stop if we've passed total available
                if total_available and offset >= total_available:
                    break

            except Exception as e:
                logger.error(f"Error at offset {offset}: {e}")
                self.stats["errors"] += 1
                # Try to continue from next page
                offset += self.PAGE_SIZE
                if self.stats["errors"] > 10:
                    logger.error("Too many errors, stopping fetch")
                    break

        logger.info(
            f"sec-api.io fetch complete: {len(all_firms)} active firms "
            f"(API calls: {self.stats['api_calls']}, "
            f"skipped: {self.stats['firms_skipped']}, "
            f"errors: {self.stats['errors']})"
        )

        return all_firms

    def _sec_api_request(
        self, query: str, start: int = 0
    ) -> Optional[Any]:
        """
        Make a single POST request to sec-api.io Form ADV firm search.

        API: POST https://api.sec-api.io/form-adv/firm?token=API_KEY
        Body: {"query": "...", "from": 0, "size": 50}
        """
        # Rate limit
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)

        url = f"{self.SEC_API_FIRM_SEARCH}?token={self.api_key}"

        payload = {
            "query": query,
            "from": start,
            "size": self.PAGE_SIZE,
        }

        try:
            self.last_request_time = time.time()
            self.stats["api_calls"] += 1

            response = self.session.post(
                url,
                json=payload,
                timeout=60,
            )

            if response.status_code == 429:
                logger.warning("Rate limited by sec-api.io, backing off 10s")
                time.sleep(10)
                self.last_request_time = time.time()
                response = self.session.post(url, json=payload, timeout=60)

            if response.status_code == 401:
                logger.error(
                    "sec-api.io returned 401 Unauthorized — check SEC_API_KEY"
                )
                return None

            if response.status_code == 402:
                logger.error(
                    "sec-api.io returned 402 — API quota exceeded. "
                    "Upgrade plan or wait for reset."
                )
                return None

            response.raise_for_status()
            result = response.json()

            # Debug: log the structure of the first response so we can
            # verify field paths are correct
            if self.stats["api_calls"] <= 1:
                if isinstance(result, dict):
                    logger.info(
                        f"  API response keys: {list(result.keys())}"
                    )
                    filings = result.get("filings", result.get("data", []))
                    if filings and isinstance(filings, list) and len(filings) > 0:
                        first = filings[0]
                        logger.info(
                            f"  First filing top-level keys: "
                            f"{list(first.keys())[:20]}"
                        )
                        # Log Info sub-keys if present
                        if "Info" in first:
                            logger.info(
                                f"  Info sub-keys: "
                                f"{list(first['Info'].keys())[:20]}"
                            )
                elif isinstance(result, list) and len(result) > 0:
                    logger.info(f"  Response is list, len={len(result)}")
                    logger.info(
                        f"  First item keys: "
                        f"{list(result[0].keys())[:20]}"
                    )

            return result

        except requests.RequestException as e:
            logger.error(f"sec-api.io request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse sec-api.io response: {e}")
            return None

    def _parse_sec_api_filing(self, filing: Dict[str, Any]) -> Optional[FirmRecord]:
        """
        Parse a sec-api.io Form ADV filing into a FirmRecord.

        sec-api.io may return filings in different structures depending on
        the endpoint. We handle both:
        - Top-level "Info" structure: Info.FirmCrdNb, Info.FirmName, etc.
        - Nested "FormInfo" structure: FormInfo.Part1A.Item1, etc.

        This parser tries the top-level Info fields first, then falls back
        to the nested FormInfo structure.
        """
        try:
            # ---------- Top-level "Info" fields (primary) ----------
            info = filing.get("Info", {})

            # Navigate to nested form data
            form_info = filing.get("FormInfo", {})
            part1a = form_info.get("Part1A", {})

            # ---------- Core identification ----------
            firm_name = (
                info.get("FirmName", "")
                or info.get("firmName", "")
                or part1a.get("Item1", {}).get("Q1A", "")
                or filing.get("firmName", "")
                or ""
            ).strip()

            if not firm_name:
                self.stats["firms_skipped"] += 1
                return None

            # CRD number
            crd = str(
                info.get("FirmCrdNb", "")
                or filing.get("firmCrdNumber", "")
                or part1a.get("Item1", {}).get("Q1F", "")
                or ""
            )

            # SEC file number
            sec_num = str(
                info.get("SECRgnCD", "")
                or info.get("SecFileNb", "")
                or filing.get("secFileNumber", "")
                or ""
            )
            if not sec_num:
                sec_num = f"CRD-{crd}" if crd else ""

            # ---------- Address ----------
            # Try Info-level address
            state = (
                info.get("MainAddr", {}).get("State", "")
                or info.get("BusAddr", {}).get("State", "")
                or part1a.get("Item1", {}).get("Q1I", {}).get("state", "")
                or filing.get("firmAddress", {}).get("state", "")
                or ""
            )
            city = (
                info.get("MainAddr", {}).get("City", "")
                or info.get("BusAddr", {}).get("City", "")
                or part1a.get("Item1", {}).get("Q1I", {}).get("city", "")
                or filing.get("firmAddress", {}).get("city", "")
                or ""
            )
            country = "United States"

            if state and len(state) > 2:
                state = self._normalize_state(state)

            # ---------- AUM ----------
            # Try multiple paths for AUM data
            item5 = part1a.get("Item5", {})
            aum_total = self._parse_number(
                info.get("TtlRgltryAUM", 0)
                or info.get("TtlGrssAUM", 0)
                or item5.get("Q5F2C", 0)
            )
            aum_discretionary = self._parse_number(
                info.get("DscrtnryAUM", 0)
                or item5.get("Q5F2A", 0)
            )
            if aum_total == 0:
                aum_total = aum_discretionary

            # ---------- Client counts ----------
            num_clients = self._parse_int(
                info.get("TtlClntCnt", 0)
                or item5.get("Q5D1", 0)
            )

            hnw_clients = self._parse_int(
                info.get("HghNtWrthCnt", 0)
                or item5.get("Q5D2B", 0)
            )
            institutional_clients = self._parse_int(
                info.get("InstnlCnt", 0)
                or (
                    self._parse_int(item5.get("Q5D2G", 0))
                    + self._parse_int(item5.get("Q5D2I", 0))
                    + self._parse_int(item5.get("Q5D2L", 0))
                    + self._parse_int(item5.get("Q5D2K", 0))
                )
            )

            # ---------- Investment types ----------
            item5b = item5.get("Q5B", {})
            if not isinstance(item5b, dict):
                item5b = {}
            item8 = part1a.get("Item8", {})

            manages_private_funds = (
                self._is_yes(info.get("PrvtFndFlg", ""))
                or self._is_yes(item5b.get("Q5B2", ""))
                or self._is_yes(item8.get("Q8B1", ""))
            )
            manages_real_estate = self._is_yes(
                item8.get("Q8C", "")
            )
            manages_hedge_funds = self._is_yes(
                item8.get("Q8D", "")
            )
            manages_public_securities = self._is_yes(
                item8.get("Q8A", "")
            )

            # ---------- Custodians ----------
            custodian_names = self._extract_custodians(filing)

            # ---------- Fee structure ----------
            item6 = part1a.get("Item6", {})
            fee_structure = self._determine_fee_structure(item5, item6)

            # ---------- Minimum account ----------
            minimum_account = self._parse_number(item5.get("Q5C", 0))

            # ---------- Registration date ----------
            reg_date_str = (
                info.get("DtSECEffective", "")
                or filing.get("filedAt", "")
                or filing.get("registrationDate", "")
                or ""
            )
            registration_date = None
            if reg_date_str:
                for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y",
                            "%Y-%m-%dT%H:%M:%S.%fZ"]:
                    try:
                        registration_date = datetime.strptime(
                            reg_date_str[:26].rstrip("Z"), fmt.rstrip("Z")
                        )
                        break
                    except ValueError:
                        continue

            # ---------- Firm classification ----------
            is_family_office = (
                self._detect_family_office(firm_name)
                or self._is_yes(info.get("FmlyOffcFlg", ""))
            )
            is_multi_family_office = (
                "multi-family" in firm_name.lower()
                or "multi family" in firm_name.lower()
                or "mfo" in firm_name.lower()
            )

            # ---------- Principals ----------
            principal_names = []
            principals = filing.get("principals", [])
            if isinstance(principals, list):
                for p in principals[:5]:
                    name = p.get("name", "") if isinstance(p, dict) else str(p)
                    if name:
                        principal_names.append(name)

            # ---------- Website ----------
            website = (
                info.get("Website", "")
                or part1a.get("Item1", {}).get("Q1J", "")
                or ""
            )

            # ---------- Build FirmRecord ----------
            firm = FirmRecord(
                firm_name=firm_name,
                sec_file_number=sec_num,
                cik=crd,
                state=state,
                country=country,
                registration_date=registration_date,
                aum_total=aum_total,
                aum_regulatory=aum_discretionary,
                num_clients=max(num_clients, 1),
                hnw_clients=hnw_clients,
                institutional_clients=institutional_clients,
                manages_public_securities=manages_public_securities,
                manages_private_funds=manages_private_funds,
                manages_real_estate=manages_real_estate,
                manages_hedge_funds=manages_hedge_funds,
                custodian_names=custodian_names,
                fee_structure=fee_structure,
                minimum_account_size=minimum_account,
                is_family_office=is_family_office,
                is_multi_family_office=is_multi_family_office,
                principal_names=principal_names,
                website=website,
                raw_data={
                    "city": city,
                    "crd": crd,
                    "aum_estimated": False,
                    "data_source": "sec-api.io",
                    "filing_date": reg_date_str,
                },
            )

            self.stats["firms_parsed"] += 1
            return firm

        except Exception as e:
            logger.debug(f"Error parsing filing: {e}")
            self.stats["errors"] += 1
            return None

    def _extract_custodians(self, filing: Dict[str, Any]) -> List[str]:
        """
        Extract custodian names from Schedule D Section 5.F.

        Schedule D 5.F lists the firm's custodians (where they hold client
        assets). This is the key data for platform detection.
        """
        custodians = []

        # Try Schedule D data in the filing
        schedule_d = filing.get("ScheduleD", {})
        if not schedule_d:
            schedule_d = filing.get("scheduleD", {})

        # Section 5.F — Custody
        section_5f = schedule_d.get("Section5F", [])
        if not section_5f:
            section_5f = schedule_d.get("section5F", [])
        if not section_5f:
            # Try nested FormInfo path
            form_info = filing.get("FormInfo", {})
            schedule_d_inner = form_info.get("ScheduleD", {})
            section_5f = schedule_d_inner.get("Section5F", [])

        if isinstance(section_5f, list):
            for entry in section_5f:
                if isinstance(entry, dict):
                    name = (
                        entry.get("custodianName", "")
                        or entry.get("Q5F1", "")
                        or entry.get("name", "")
                        or ""
                    ).strip()
                    if name and name not in custodians:
                        custodians.append(name)
                elif isinstance(entry, str) and entry.strip():
                    if entry.strip() not in custodians:
                        custodians.append(entry.strip())

        # Also check Part1A Item 5.F for simpler custodian references
        form_info = filing.get("FormInfo", {})
        part1a = form_info.get("Part1A", {})
        item5 = part1a.get("Item5", {})

        # Q5F1 sometimes has primary custodian
        primary_custodian = item5.get("Q5F1", "")
        if isinstance(primary_custodian, str) and primary_custodian.strip():
            if primary_custodian.strip() not in custodians:
                custodians.append(primary_custodian.strip())

        return custodians

    def _determine_fee_structure(
        self, item5: Dict, item6: Dict
    ) -> str:
        """Determine fee structure from Item 5.E and Item 6."""
        # Item 6 — compensation arrangement
        # Q6A = Percentage of AUM
        # Q6B = Hourly charges
        # Q6C = Subscription fees
        # Q6D = Fixed fees
        # Q6E = Commissions
        # Q6F = Performance-based fees

        aum_based = self._is_yes(item6.get("Q6A", ""))
        commission = self._is_yes(item6.get("Q6E", ""))
        performance = self._is_yes(item6.get("Q6F", ""))

        if aum_based and commission:
            return "Hybrid"
        elif aum_based:
            if performance:
                return "Fee-Based"
            return "Assets Under Management"
        elif commission:
            return "Commission"
        else:
            return "Fee-Based"

    # =========================================================================
    # Free IAPD API fallback (limited data)
    # =========================================================================

    def _fetch_via_free_api(self) -> List[FirmRecord]:
        """
        Fallback: fetch firms via the free SEC IAPD search API.

        This API only returns: firm name, CRD, SEC number, state, branch count.
        AUM, custodians, and client data are NOT available.
        """
        logger.info(
            "Falling back to free SEC IAPD search API "
            "(no SEC_API_KEY set — data will be limited)"
        )

        FREE_API_BASE = "https://api.adviserinfo.sec.gov/search/firm"
        SEARCH_TERMS = [
            "advisors", "advisory", "capital", "wealth", "management",
            "financial", "partners", "investment", "group", "associates",
            "consulting", "trust", "family office", "private", "asset",
            "fund", "securities", "planning", "retirement", "fiduciary",
            "LLC", "LP", "Inc",
        ]

        # Set up headers for the free API
        user_agent = os.getenv(
            "SEC_USER_AGENT",
            "ISQ-InfraFundTracker support@bloorcapital.com"
        )
        free_session = requests.Session()
        free_session.headers.update({
            "User-Agent": user_agent,
            "Accept": "application/json",
        })

        all_firms = {}
        seen_crds: Set[str] = set()

        for term in SEARCH_TERMS:
            try:
                start = 0
                while start < 10000:
                    elapsed = time.time() - self.last_request_time
                    if elapsed < 0.12:
                        time.sleep(0.12 - elapsed)

                    params = {
                        "query": term,
                        "hl": "true",
                        "nrows": 100,
                        "start": start,
                        "r": 100,
                        "sort": "score+desc",
                        "wt": "json",
                    }
                    self.last_request_time = time.time()
                    self.stats["api_calls"] += 1

                    resp = free_session.get(
                        FREE_API_BASE, params=params, timeout=30
                    )
                    if resp.status_code == 429:
                        time.sleep(5)
                        resp = free_session.get(
                            FREE_API_BASE, params=params, timeout=30
                        )
                    resp.raise_for_status()
                    data = resp.json()

                    hits = data.get("hits", {})
                    hit_list = hits.get("hits", [])
                    total = hits.get("total", 0)

                    if not hit_list:
                        break

                    new_this_page = 0
                    for hit in hit_list:
                        source = hit.get("_source", hit)
                        crd = str(source.get("firm_source_id", ""))
                        if not crd or crd in seen_crds:
                            continue
                        seen_crds.add(crd)

                        # Only active IAs
                        if source.get("firm_ia_scope", "") != "ACTIVE":
                            continue

                        firm_name = source.get("firm_name", "").strip()
                        if not firm_name:
                            continue

                        sec_num = source.get("firm_ia_full_sec_number", "")
                        if not sec_num:
                            sec_num = f"CRD-{crd}"

                        # Parse address
                        f_state = ""
                        f_city = ""
                        addr_json = source.get("firm_ia_address_details", "")
                        if addr_json and isinstance(addr_json, str):
                            try:
                                addr = json.loads(addr_json)
                                office = addr.get("officeAddress", {})
                                f_state = office.get("state", "")
                                f_city = office.get("city", "")
                            except (json.JSONDecodeError, AttributeError):
                                pass

                        if f_state and len(f_state) > 2:
                            f_state = self._normalize_state(f_state)

                        branches = 0
                        try:
                            branches = int(source.get("firm_branches_count", 0))
                        except (ValueError, TypeError):
                            pass

                        is_fo = self._detect_family_office(firm_name)

                        firm = FirmRecord(
                            firm_name=firm_name,
                            sec_file_number=sec_num,
                            cik=crd,
                            state=f_state,
                            country="United States",
                            aum_total=self._estimate_aum(branches, is_fo),
                            aum_regulatory=0,
                            num_clients=max(branches * 20, 10),
                            is_family_office=is_fo,
                            manages_private_funds=self._name_suggests_alts(firm_name),
                            raw_data={
                                "city": f_city,
                                "branches": branches,
                                "aum_estimated": True,
                                "data_source": "free_iapd_api",
                            },
                        )
                        all_firms[crd] = firm
                        new_this_page += 1

                    start += len(hit_list)
                    if start >= total:
                        break

                if new_this_page > 0:
                    logger.info(
                        f"  '{term}': found firms (total unique: {len(all_firms)})"
                    )

            except Exception as e:
                logger.error(f"Error searching '{term}': {e}")
                continue

        firms = list(all_firms.values())
        logger.info(f"Free API fetch complete: {len(firms)} active firms")
        return firms

    # =========================================================================
    # Helpers
    # =========================================================================

    def _parse_number(self, value: Any) -> float:
        """Parse a number from various formats."""
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # Remove commas, dollar signs, spaces
            cleaned = re.sub(r"[,$\s]", "", value)
            if not cleaned:
                return 0.0
            try:
                return float(cleaned)
            except ValueError:
                return 0.0
        return 0.0

    def _parse_int(self, value: Any) -> int:
        """Parse an integer from various formats."""
        return int(self._parse_number(value))

    def _is_yes(self, value: Any) -> bool:
        """Check if a Form ADV field value means 'Yes'."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().upper() in ("Y", "YES", "TRUE", "1")
        return False

    def _estimate_aum(self, branches: int, is_family_office: bool) -> float:
        """Rough AUM estimate for fallback mode (free API)."""
        if is_family_office:
            return 200_000_000
        if branches >= 10:
            return 5_000_000_000
        if branches >= 5:
            return 1_000_000_000
        if branches >= 2:
            return 500_000_000
        return 200_000_000

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
        """Detect if firm name suggests alternatives/private funds."""
        if not firm_name:
            return False
        name_lower = firm_name.lower()
        alt_keywords = [
            "alternative", "private equity", "private fund",
            "hedge fund", "real estate", "infrastructure",
            "venture", "private capital", "private credit",
        ]
        return any(kw in name_lower for kw in alt_keywords)

    # =========================================================================
    # Caching
    # =========================================================================

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
                "data_source": "sec-api.io" if self.api_key else "free_iapd_api",
                "firms": [
                    {
                        "firm_name": f.firm_name,
                        "sec_file_number": f.sec_file_number,
                        "crd": f.cik,
                        "state": f.state,
                        "aum_total": f.aum_total,
                        "aum_regulatory": f.aum_regulatory,
                        "num_clients": f.num_clients,
                        "hnw_clients": f.hnw_clients,
                        "institutional_clients": f.institutional_clients,
                        "custodian_names": f.custodian_names,
                        "manages_private_funds": f.manages_private_funds,
                        "manages_real_estate": f.manages_real_estate,
                        "manages_hedge_funds": f.manages_hedge_funds,
                        "manages_public_securities": f.manages_public_securities,
                        "fee_structure": f.fee_structure,
                        "minimum_account_size": f.minimum_account_size,
                        "is_family_office": f.is_family_office,
                        "is_multi_family_office": f.is_multi_family_office,
                        "registration_date": (
                            f.registration_date.isoformat()
                            if f.registration_date else None
                        ),
                        "website": f.website,
                        "city": f.raw_data.get("city", ""),
                        "aum_estimated": f.raw_data.get("aum_estimated", True),
                        "data_source": f.raw_data.get("data_source", ""),
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
                reg_date = None
                if item.get("registration_date"):
                    try:
                        reg_date = datetime.fromisoformat(
                            item["registration_date"]
                        )
                    except (ValueError, TypeError):
                        pass

                firm = FirmRecord(
                    firm_name=item["firm_name"],
                    sec_file_number=item["sec_file_number"],
                    cik=item.get("crd", ""),
                    state=item.get("state", ""),
                    aum_total=item.get("aum_total", 0),
                    aum_regulatory=item.get("aum_regulatory", 0),
                    num_clients=item.get("num_clients", 0),
                    hnw_clients=item.get("hnw_clients", 0),
                    institutional_clients=item.get("institutional_clients", 0),
                    custodian_names=item.get("custodian_names", []),
                    manages_private_funds=item.get("manages_private_funds", False),
                    manages_real_estate=item.get("manages_real_estate", False),
                    manages_hedge_funds=item.get("manages_hedge_funds", False),
                    manages_public_securities=item.get("manages_public_securities", False),
                    fee_structure=item.get("fee_structure", "Commission"),
                    minimum_account_size=item.get("minimum_account_size", 0),
                    is_family_office=item.get("is_family_office", False),
                    is_multi_family_office=item.get("is_multi_family_office", False),
                    registration_date=reg_date,
                    website=item.get("website", ""),
                    raw_data={
                        "city": item.get("city", ""),
                        "aum_estimated": item.get("aum_estimated", True),
                        "data_source": item.get("data_source", ""),
                    },
                )
                firms.append(firm)

            logger.info(f"Loaded {len(firms)} firms from cache")
            return firms
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
            return []
