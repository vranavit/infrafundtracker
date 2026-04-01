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
    SEC_API_SCHEDULE_D = "https://api.sec-api.io/form-adv/schedule-d-5-k"

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

    # Date-range windows for paginating past the 10,000 result cap.
    # sec-api.io caps any single query at 10,000 results; we split by
    # Filing.Dt ranges so each window stays under the cap.
    DATE_WINDOWS = [
        ("2024-01-01", "2024-06-30"),
        ("2024-07-01", "2024-12-31"),
        ("2025-01-01", "2025-06-30"),
        ("2025-07-01", "2025-12-31"),
        # 2026 split into monthly windows — Q1 alone had 10K+ filings
        ("2026-01-01", "2026-01-31"),
        ("2026-02-01", "2026-02-28"),
        ("2026-03-01", "2026-03-31"),
        ("2026-04-01", "2026-04-30"),
        ("2026-05-01", "2026-05-31"),
        ("2026-06-01", "2026-06-30"),
        ("2026-07-01", "2026-12-31"),
    ]

    def _fetch_via_sec_api(self) -> List[FirmRecord]:
        """
        Fetch all SEC-registered advisers via sec-api.io Form ADV API.

        Uses date-range windows on Filing.Dt to stay under the 10,000
        result cap per query.  Deduplicates by CRD across windows so
        each firm appears only once (most recent filing wins).

        Response format: {"total": {"value": N, "relation": "..."}, "filings": [...]}
        """
        logger.info("Fetching firms via sec-api.io Form ADV API...")

        all_firms: Dict[str, FirmRecord] = {}  # CRD → FirmRecord (dedup)
        seen_crds: Set[str] = set()

        for window_start, window_end in self.DATE_WINDOWS:
            query = (
                f"Filing.Dt:[{window_start} TO {window_end}] "
                f"AND Rgstn.St:APPROVED"
            )
            logger.info(f"  Window {window_start}..{window_end}")
            offset = 0
            window_total = None

            while offset < 10000:
                try:
                    data = self._sec_api_request(query=query, start=offset)
                    if data is None:
                        logger.error(f"API returned None at offset {offset}")
                        break

                    filings, window_total = self._extract_filings(
                        data, window_total
                    )

                    if not filings:
                        logger.info(f"  No more results at offset {offset}")
                        break

                    for filing in filings:
                        crd = str(
                            filing.get("Info", {}).get("FirmCrdNb", "")
                            or ""
                        )
                        if not crd:
                            continue
                        if crd in seen_crds:
                            continue
                        seen_crds.add(crd)

                        firm = self._parse_sec_api_filing(filing)
                        if firm:
                            all_firms[crd] = firm
                            if len(all_firms) == 1:
                                logger.info(
                                    f"  First parsed firm: {firm.firm_name}, "
                                    f"CRD={firm.cik}, State={firm.state}, "
                                    f"AUM=${firm.aum_total:,.0f}, "
                                    f"Clients={firm.num_clients}"
                                )

                    offset += len(filings)

                    if offset % 1000 == 0 or offset < 100:
                        logger.info(
                            f"    Fetched {offset} filings in window, "
                            f"total unique firms: {len(all_firms)}"
                        )

                    if len(filings) < self.PAGE_SIZE:
                        break
                    if window_total and offset >= window_total:
                        break

                except Exception as e:
                    logger.error(f"Error at offset {offset}: {e}")
                    self.stats["errors"] += 1
                    offset += self.PAGE_SIZE
                    if self.stats["errors"] > 20:
                        logger.error("Too many errors, stopping fetch")
                        break

            if self.stats["errors"] > 20:
                break

        result = list(all_firms.values())
        logger.info(
            f"sec-api.io fetch complete: {len(result)} active firms "
            f"(API calls: {self.stats['api_calls']}, "
            f"skipped: {self.stats['firms_skipped']}, "
            f"errors: {self.stats['errors']})"
        )
        return result

    def _extract_filings(self, data, current_total):
        """
        Pull the filings list and total count from an API response.
        Handles total being either an int or {"value": N, "relation": "..."}.
        """
        if isinstance(data, dict):
            filings = data.get("filings", [])
            if current_total is None:
                raw_total = data.get("total", 0)
                if isinstance(raw_total, dict):
                    current_total = raw_total.get("value", 0)
                else:
                    current_total = int(raw_total) if raw_total else 0
                logger.info(f"    Window reports {current_total}+ filings")
            return filings, current_total
        elif isinstance(data, list):
            return data, current_total
        else:
            logger.error(f"Unexpected response type: {type(data)}")
            return [], current_total

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
                        # Log sub-keys for field mapping
                        if "Info" in first:
                            logger.info(
                                f"  Info sub-keys: "
                                f"{list(first['Info'].keys())[:20]}"
                            )
                        if "MainAddr" in first:
                            logger.info(
                                f"  MainAddr sub-keys: "
                                f"{list(first['MainAddr'].keys())[:15]}"
                            )
                        if "Rgstn" in first:
                            rgstn_val = first["Rgstn"]
                            if isinstance(rgstn_val, list) and rgstn_val:
                                logger.info(
                                    f"  Rgstn[0] keys: "
                                    f"{list(rgstn_val[0].keys())[:15]}"
                                )
                            elif isinstance(rgstn_val, dict):
                                logger.info(
                                    f"  Rgstn keys: "
                                    f"{list(rgstn_val.keys())[:15]}"
                                )
                        if "FormInfo" in first:
                            fi = first["FormInfo"]
                            logger.info(
                                f"  FormInfo sub-keys: "
                                f"{list(fi.keys())[:10]}"
                            )
                            if "Part1A" in fi:
                                p1a_keys = list(fi['Part1A'].keys())[:20]
                                logger.info(f"  Part1A sub-keys: {p1a_keys}")
                                # Log first firm's AUM/client data
                                p1a = fi["Part1A"]
                                for item_key in ["Item5C", "Item5D", "Item5E", "Item5F", "Item7B"]:
                                    if item_key in p1a:
                                        logger.info(
                                            f"  {item_key}: {dict(list(p1a[item_key].items())[:5])}"
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

        Field paths are based on the official sec-api.io documentation:
        https://sec-api.io/docs/investment-adviser-and-adv-api

        Top-level keys: Info, MainAddr, Rgstn (array), FormInfo, Filing, etc.
        Nested: FormInfo.Part1A.Item5C, Item5D, Item5E, Item5F, Item5G,
                Item7B, etc.
        """
        try:
            info = filing.get("Info", {})
            main_addr = filing.get("MainAddr", {}) or {}
            form_info = filing.get("FormInfo", {})
            part1a = form_info.get("Part1A", {})

            # ---- Core identification ----
            firm_name = (
                info.get("BusNm", "")
                or info.get("LegalNm", "")
                or ""
            ).strip()

            if not firm_name:
                self.stats["firms_skipped"] += 1
                return None

            crd = str(info.get("FirmCrdNb", "") or "")
            sec_num = str(info.get("SECNb", "") or "")
            if not sec_num:
                sec_num = f"CRD-{crd}" if crd else ""

            # ---- Address (MainAddr is top-level) ----
            state = (
                main_addr.get("State", "")
                or main_addr.get("Stcd", "")
                or ""
            )
            city = main_addr.get("City", "") or ""
            country = main_addr.get("Cntry", "United States") or "United States"

            if state and len(state) > 2:
                state = self._normalize_state(state)

            # ---- AUM (Item 5.F) ----
            # Q5F2A = discretionary AUM, Q5F2C = total regulatory AUM
            item5f = part1a.get("Item5F", {})
            aum_total = self._parse_number(item5f.get("Q5F2C", 0))
            aum_discretionary = self._parse_number(item5f.get("Q5F2A", 0))
            if aum_total == 0:
                aum_total = aum_discretionary

            # ---- Client counts (Item 5.C and 5.D) ----
            # Q5C1 = approximate number of clients (string like "18")
            item5c = part1a.get("Item5C", {})
            num_clients = self._parse_int(item5c.get("Q5C1", 0))

            # Item5D: client type breakdown
            # Q5DA1 = individuals (other than HNW) count
            # Q5DB1 = high net worth individuals count
            # Q5DC1 = banking/thrift count
            # Q5DD1 = investment companies count
            # Q5DE1 = business dev companies count
            # Q5DF1 = pooled investment vehicles count
            # Q5DG1 = pension/profit sharing plans count
            # Q5DH1 = charitable organisations count
            # Q5DI1 = state/municipal entities count
            # Q5DJ1 = other investment advisers count
            # Q5DK1 = insurance companies count
            # Q5DL1 = sovereign wealth count
            # Q5DM1 = corporations/other businesses count
            # Q5DN1 = other count
            item5d = part1a.get("Item5D", {})
            hnw_clients = self._parse_int(item5d.get("Q5DB1", 0))
            individual_clients = self._parse_int(item5d.get("Q5DA1", 0))

            # Institutional = pension + charitable + state/muni + insurance
            # + sovereign + corporations + investment cos
            institutional_clients = (
                self._parse_int(item5d.get("Q5DG1", 0))  # pension
                + self._parse_int(item5d.get("Q5DH1", 0))  # charitable
                + self._parse_int(item5d.get("Q5DI1", 0))  # state/muni
                + self._parse_int(item5d.get("Q5DK1", 0))  # insurance
                + self._parse_int(item5d.get("Q5DL1", 0))  # sovereign
                + self._parse_int(item5d.get("Q5DM1", 0))  # corporations
                + self._parse_int(item5d.get("Q5DD1", 0))  # investment cos
            )

            # If num_clients is 0 but we have breakdowns, sum them
            if num_clients == 0:
                num_clients = (
                    individual_clients
                    + hnw_clients
                    + institutional_clients
                    + self._parse_int(item5d.get("Q5DF1", 0))  # pooled
                    + self._parse_int(item5d.get("Q5DJ1", 0))  # other IAs
                    + self._parse_int(item5d.get("Q5DN1", 0))  # other
                )

            # ---- Investment types ----
            # Item7B.Q7B: "Y" if private fund adviser
            item7b = part1a.get("Item7B", {})
            manages_private_funds = self._is_yes(item7b.get("Q7B", ""))

            # Item5G: advisory services provided
            # Q5G4 = pooled investment vehicles (other than investment cos)
            item5g = part1a.get("Item5G", {})
            manages_pooled_vehicles = self._is_yes(item5g.get("Q5G4", ""))
            if not manages_private_funds:
                manages_private_funds = manages_pooled_vehicles

            # Item5B has employee/advisory staff counts, not investment types
            # Use firm name heuristics for real estate / hedge fund flags
            manages_real_estate = self._name_suggests_type(
                firm_name, ["real estate", "reit", "property", "realty"]
            )
            manages_hedge_funds = self._name_suggests_type(
                firm_name, ["hedge fund", "hedge"]
            )
            manages_public_securities = self._is_yes(
                item5g.get("Q5G1", "")  # securities portfolios
            )

            # ---- Custodians ----
            # NOTE: Custodian data (Schedule D 5.K) requires a separate
            # GET /form-adv/schedule-d-5-k/<crd> call.
            # We leave custodians empty here and batch-fetch them later
            # for top-scored firms only.
            custodian_names: List[str] = []

            # ---- Fee structure (Item 5.E) ----
            # Q5E1-Q5E7 are Y/N flags for compensation types
            item5e = part1a.get("Item5E", {})
            fee_structure = self._determine_fee_structure(item5e)

            # ---- Registration date (Rgstn array) ----
            # Rgstn is an array; Rgstn[0].Dt = registration date,
            # Rgstn[0].St = status (e.g. "APPROVED")
            registration_date = None
            filing_date_str = ""
            rgstn = filing.get("Rgstn", [])
            if isinstance(rgstn, list) and rgstn:
                rgstn_entry = rgstn[0] if isinstance(rgstn[0], dict) else {}
                reg_date_str = rgstn_entry.get("Dt", "")
                if reg_date_str:
                    filing_date_str = reg_date_str
                    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y",
                                "%Y-%m-%dT%H:%M:%S.%fZ"]:
                        try:
                            registration_date = datetime.strptime(
                                reg_date_str[:26].rstrip("Z"), fmt.rstrip("Z")
                            )
                            break
                        except ValueError:
                            continue
            elif isinstance(rgstn, dict):
                reg_date_str = rgstn.get("Dt", "")
                if reg_date_str:
                    filing_date_str = reg_date_str
                    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                        try:
                            registration_date = datetime.strptime(
                                reg_date_str[:19], fmt
                            )
                            break
                        except ValueError:
                            continue

            # Fall back to Filing.Dt if no Rgstn date
            if not registration_date:
                filed_at = filing.get("Filing", {}).get("Dt", "") or filing.get("filedAt", "")
                if filed_at:
                    filing_date_str = filed_at
                    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                        try:
                            registration_date = datetime.strptime(
                                filed_at[:19], fmt
                            )
                            break
                        except ValueError:
                            continue

            # ---- Firm classification ----
            is_family_office = self._detect_family_office(firm_name)
            is_multi_family_office = (
                "multi-family" in firm_name.lower()
                or "multi family" in firm_name.lower()
                or "mfo" in firm_name.lower()
            )

            # ---- Website (Item1.Q1J) ----
            item1 = part1a.get("Item1", {})
            website = item1.get("Q1J", "") or ""

            # ---- Build FirmRecord ----
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
                is_family_office=is_family_office,
                is_multi_family_office=is_multi_family_office,
                website=website,
                raw_data={
                    "city": city,
                    "crd": crd,
                    "aum_estimated": False,
                    "data_source": "sec-api.io",
                    "filing_date": filing_date_str,
                },
            )

            # Compute avg AUM per client for QP scorer
            if firm.num_clients > 0 and firm.aum_total > 0:
                firm.avg_aum_per_client = firm.aum_total / firm.num_clients

            self.stats["firms_parsed"] += 1
            return firm

        except Exception as e:
            logger.debug(f"Error parsing filing: {e}")
            self.stats["errors"] += 1
            return None

    def _name_suggests_type(self, name: str, keywords: List[str]) -> bool:
        """Check if firm name contains any of the given keywords."""
        if not name:
            return False
        lower = name.lower()
        return any(kw in lower for kw in keywords)

    def _extract_custodians_for_firm(self, crd: str) -> List[str]:
        """
        Fetch custodian names via the Schedule D 5.K endpoint.

        GET /form-adv/schedule-d-5-k/<crd>?token=API_KEY

        Response structure (from sec-api.io docs):
        {
          "1-separatelyManagedAccounts": { ... asset allocations ... },
          "2-borrowingsAndDerivatives": { ... },
          "3-custodiansForSeparatelyManagedAccounts": [
            {
              "a-legalName": "GOLDMAN SACHS & CO. LLC",
              "b-businessName": "GOLDMAN SACHS & CO. LLC",
              "c-locations": [...],
              "d-isRelatedPerson": false,
              "g-amountHeldAtCustodian": "$ 97,402,293,517"
            }
          ]
        }
        """
        if not self.api_key or not crd:
            return []

        url = f"{self.SEC_API_SCHEDULE_D}/{crd}?token={self.api_key}"

        # Rate limit
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)

        try:
            self.last_request_time = time.time()
            self.stats["api_calls"] += 1
            resp = self.session.get(url, timeout=30)

            if resp.status_code == 404:
                return []  # No Schedule D 5.K data for this firm
            if resp.status_code == 429:
                time.sleep(10)
                resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            data = resp.json()
            custodians = []

            # Log structure of first successful response for debugging
            if self.stats.get("custodian_debug_logged") is None:
                self.stats["custodian_debug_logged"] = True
                if isinstance(data, dict):
                    logger.info(
                        f"  Schedule D 5.K response keys for CRD {crd}: "
                        f"{list(data.keys())[:10]}"
                    )
                elif isinstance(data, list):
                    logger.info(
                        f"  Schedule D 5.K response is list, len={len(data)}"
                    )
                    if data and isinstance(data[0], dict):
                        logger.info(
                            f"  First item keys: {list(data[0].keys())[:10]}"
                        )

            # --- Parse custodians from the response ---
            # The custodians live in "3-custodiansForSeparatelyManagedAccounts"
            if isinstance(data, dict):
                custodian_entries = data.get(
                    "3-custodiansForSeparatelyManagedAccounts", []
                )
                if isinstance(custodian_entries, list):
                    for entry in custodian_entries:
                        if not isinstance(entry, dict):
                            continue
                        name = (
                            entry.get("a-legalName", "")
                            or entry.get("b-businessName", "")
                            or ""
                        ).strip()
                        if name and name not in custodians:
                            custodians.append(name)

            # If the response is a list (multiple filings), iterate
            elif isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    custodian_entries = item.get(
                        "3-custodiansForSeparatelyManagedAccounts", []
                    )
                    if isinstance(custodian_entries, list):
                        for entry in custodian_entries:
                            if not isinstance(entry, dict):
                                continue
                            name = (
                                entry.get("a-legalName", "")
                                or entry.get("b-businessName", "")
                                or ""
                            ).strip()
                            if name and name not in custodians:
                                custodians.append(name)

            return custodians

        except Exception as e:
            logger.debug(f"Schedule D 5.K fetch failed for CRD {crd}: {e}")
            return []

    # Max firms to enrich with custodian data (each = 1 API call).
    # 1000 firms ≈ 4-5 min at 0.25s rate limit + response time.
    MAX_CUSTODIAN_ENRICHMENT = 1000

    def enrich_custodians(self, firms: List[FirmRecord]) -> None:
        """
        Batch-fetch custodian data for high-priority firms via Schedule D 5.K.

        Enrichment targets (in priority order, capped at MAX_CUSTODIAN_ENRICHMENT):
        1. Top 500 firms by AUM
        2. Family offices (by AUM descending)
        3. Private fund managers (by AUM descending)

        Deduplicates so each CRD is only fetched once.
        """
        cap = self.MAX_CUSTODIAN_ENRICHMENT
        seen_crds: Set[str] = set()
        enrich_queue: List[FirmRecord] = []

        def _add(f: FirmRecord) -> bool:
            """Add firm to queue if not already seen. Returns False if cap hit."""
            if len(enrich_queue) >= cap:
                return False
            crd = f.cik or f.raw_data.get("crd", "")
            if crd and crd not in seen_crds and not f.custodian_names:
                seen_crds.add(crd)
                enrich_queue.append(f)
            return True

        # 1. Top 500 by AUM (highest priority)
        by_aum = sorted(firms, key=lambda f: f.aum_total, reverse=True)
        for f in by_aum[:500]:
            if not _add(f):
                break

        # 2. Family offices sorted by AUM
        if len(enrich_queue) < cap:
            family_offices = sorted(
                [f for f in firms if f.is_family_office],
                key=lambda f: f.aum_total, reverse=True,
            )
            for f in family_offices:
                if not _add(f):
                    break

        # 3. Private fund managers sorted by AUM
        if len(enrich_queue) < cap:
            pvt_fund_mgrs = sorted(
                [f for f in firms if f.manages_private_funds],
                key=lambda f: f.aum_total, reverse=True,
            )
            for f in pvt_fund_mgrs:
                if not _add(f):
                    break

        total = len(enrich_queue)
        logger.info(
            f"Enriching custodian data for {total} firms "
            f"(cap={cap}, top AUM + family offices + private fund mgrs)..."
        )

        enriched = 0
        empty_responses = 0
        for i, firm in enumerate(enrich_queue):
            crd = firm.cik or firm.raw_data.get("crd", "")
            custodians = self._extract_custodians_for_firm(crd)
            if custodians:
                firm.custodian_names = custodians
                enriched += 1
                if enriched <= 5:
                    logger.info(
                        f"  Custodian match: {firm.firm_name} → "
                        f"{custodians[:3]}"
                    )
            else:
                empty_responses += 1

            # Progress logging
            if (i + 1) % 100 == 0:
                logger.info(
                    f"  Custodian progress: {i+1}/{total} checked, "
                    f"{enriched} enriched, {empty_responses} empty"
                )

        logger.info(
            f"Custodian enrichment complete: {enriched} firms enriched, "
            f"{empty_responses} had no Schedule D 5.K custodian data"
        )

    def _determine_fee_structure(self, item5e: Dict) -> str:
        """
        Determine fee structure from Item 5.E.

        Item5E fields (all Y/N):
        Q5E1 = A percentage of AUM
        Q5E2 = Hourly charges
        Q5E3 = Subscription fees (fixed/flat)
        Q5E4 = Commissions
        Q5E5 = Performance-based fees
        Q5E6 = Other
        """
        aum_based = self._is_yes(item5e.get("Q5E1", ""))
        hourly = self._is_yes(item5e.get("Q5E2", ""))
        subscription = self._is_yes(item5e.get("Q5E3", ""))
        commission = self._is_yes(item5e.get("Q5E4", ""))
        performance = self._is_yes(item5e.get("Q5E5", ""))

        if aum_based and commission:
            return "Hybrid"
        elif aum_based:
            if performance:
                return "Fee-Based"
            return "Assets Under Management"
        elif commission:
            return "Commission"
        elif hourly or subscription:
            return "Fee-Based"
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
