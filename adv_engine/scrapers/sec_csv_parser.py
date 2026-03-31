"""
SEC CSV Parser - Parses SEC Form ADV CSVs into FirmRecord objects.

Handles multiple CSV column name formats, missing values, and filtering.
Converts SEC IAPD data into standardized FirmRecord dataclass instances.
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .adv_parser import FirmRecord

logger = logging.getLogger(__name__)


class SECCSVParser:
    """
    Parser for SEC Form ADV CSV files into FirmRecord dataclass instances.

    Handles:
    - Multiple column name formats (different SEC export versions)
    - Missing/empty values gracefully
    - AUM normalization (strings like "$1,234,567" or "1.2B")
    - State code normalization
    - Family office detection from firm name patterns
    - Filtering by AUM threshold and US registration
    """

    # Minimum AUM to include firm (ISQ minimum)
    MIN_AUM_THRESHOLD = 50_000_000  # $50M

    # Family office name patterns
    FAMILY_OFFICE_PATTERNS = [
        r"\bfamily\s+office\b",
        r"\bmfo\b",
        r"\bmulti-family\s+office\b",
        r"\bmulti\s+family\s+office\b",
        r"\bsingle\s+family\s+office\b",
        r"\bSFO\b",
        r"private\s+family",
        r"family\s+wealth",
        r"family\s+investment",
    ]

    # Possible column names for each field (in priority order)
    COLUMN_ALIASES = {
        "crd": ["Organization CRD#", "CRD Number", "CRD#"],
        "sec_file": ["SEC#", "SEC File Number", "SEC File#"],
        "firm_name": ["Primary Business Name", "Business Name", "Firm Name"],
        "legal_name": ["Legal Name", "Legal Entity Name"],
        "city": ["Main Office City", "Office City", "City"],
        "state": ["Main Office State", "State"],
        "country": ["Main Office Country", "Country"],
        "aum": [
            "Total Regulatory Assets Under Management",
            "Regulatory AUM",
            "Total AUM",
            "Assets Under Management",
            "AUM",
        ],
        "num_accounts": [
            "Total Number of Accounts",
            "Number of Accounts",
            "Client Count",
            "Accounts",
        ],
        "private_fund_aum": [
            "Total Gross Assets of Private Funds",
            "Private Fund AUM",
            "Gross Assets Private Funds",
        ],
        # Investment type flags (Y/N columns)
        "public_sec": [
            "Manages Public Securities",
            "Public Securities",
            "Stocks and Bonds",
        ],
        "private_funds": [
            "Manages Private Funds",
            "Private Funds",
            "Private Fund Management",
        ],
        "real_estate": ["Manages Real Estate", "Real Estate"],
        "commodities": ["Manages Commodities", "Commodities"],
        "hedge_funds": ["Manages Hedge Funds", "Hedge Funds"],
        "other_alts": ["Manages Other Alternatives", "Other Alternatives"],
    }

    def __init__(self, min_aum: Optional[float] = None):
        """
        Initialize parser.

        Args:
            min_aum: Minimum AUM threshold to include firms (default: $50M)
        """
        self.min_aum = min_aum or self.MIN_AUM_THRESHOLD
        self.stats = {
            "total_rows": 0,
            "valid_firms": 0,
            "filtered_too_small": 0,
            "filtered_non_us": 0,
            "parse_errors": 0,
        }

    def parse_firms(self, csv_paths: Dict[str, Path]) -> List[FirmRecord]:
        """
        Main entry point: parse all SEC CSVs into FirmRecord objects.

        Args:
            csv_paths: Dictionary mapping CSV names to file paths

        Returns:
            List of FirmRecord instances
        """
        logger.info(f"Starting parse of {len(csv_paths)} CSV files")

        # Reset stats
        self.stats = {
            "total_rows": 0,
            "valid_firms": 0,
            "filtered_too_small": 0,
            "filtered_non_us": 0,
            "parse_errors": 0,
        }

        # Find and parse main Form ADV CSV
        base_csv = self._find_csv(csv_paths, "form_adv")
        if not base_csv:
            logger.error("Could not find form_adv.csv in provided CSV paths")
            return []

        # Parse base firm data
        firms = self._parse_base_csv(base_csv)
        logger.info(f"Parsed {len(firms)} firms from base CSV")

        # Enrich with Schedule D (custodian info)
        schedule_d = self._find_csv(csv_paths, "schedule_d")
        if schedule_d:
            self._parse_schedule_d(schedule_d, firms)
            logger.info("Enriched firms with Schedule D custodian data")

        # Log statistics
        logger.info(
            f"Parse complete - Total: {self.stats['total_rows']}, "
            f"Valid: {self.stats['valid_firms']}, "
            f"Filtered (small): {self.stats['filtered_too_small']}, "
            f"Filtered (non-US): {self.stats['filtered_non_us']}, "
            f"Errors: {self.stats['parse_errors']}"
        )

        return list(firms.values())

    def _find_csv(self, csv_paths: Dict[str, Path], keyword: str) -> Optional[Path]:
        """
        Find a CSV file by keyword in the path.

        Args:
            csv_paths: Dictionary of CSV paths
            keyword: Keyword to search for in filename

        Returns:
            Path object or None
        """
        for name, path in csv_paths.items():
            if keyword.lower() in name.lower():
                return path
        return None

    def _parse_base_csv(self, csv_path: Path) -> Dict[str, FirmRecord]:
        """
        Parse main Form ADV CSV into FirmRecord dictionary.

        Args:
            csv_path: Path to form_adv.csv

        Returns:
            Dictionary mapping SEC file number to FirmRecord
        """
        firms = {}

        try:
            import csv

            with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)

                if not reader.fieldnames:
                    logger.error(f"CSV has no header row: {csv_path}")
                    return firms

                for row_num, row in enumerate(reader, start=2):
                    self.stats["total_rows"] += 1

                    try:
                        firm = self._parse_row(row)
                        if firm:
                            firms[firm.sec_file_number] = firm
                            self.stats["valid_firms"] += 1
                    except Exception as e:
                        logger.debug(f"Error parsing row {row_num}: {e}")
                        self.stats["parse_errors"] += 1

        except Exception as e:
            logger.error(f"Failed to read CSV {csv_path}: {e}")

        return firms

    def _parse_row(self, row: Dict[str, str]) -> Optional[FirmRecord]:
        """
        Parse a single CSV row into a FirmRecord.

        Args:
            row: Dictionary of column name -> value from CSV

        Returns:
            FirmRecord if valid, None if filtered out
        """
        # Extract fields with column name flexibility
        sec_file = self._get_field(row, "sec_file", "").strip()
        firm_name = self._get_field(row, "firm_name", "").strip()
        state = self._get_field(row, "state", "").strip()
        country = self._get_field(row, "country", "").strip()

        # Must have SEC file number and firm name
        if not sec_file or not firm_name:
            return None

        # Filter: US only
        if country and country.upper() not in ("USA", "UNITED STATES", "US"):
            self.stats["filtered_non_us"] += 1
            return None

        # Parse AUM
        aum_str = self._get_field(row, "aum", "")
        aum = self._normalize_aum(aum_str)

        # Filter: minimum AUM threshold
        if aum < self.min_aum:
            self.stats["filtered_too_small"] += 1
            return None

        # Parse number of accounts
        num_accounts_str = self._get_field(row, "num_accounts", "")
        num_accounts = self._parse_int(num_accounts_str)

        # Parse investment types (Y/N fields)
        manages_public = self._is_yes(self._get_field(row, "public_sec", ""))
        manages_private = self._is_yes(self._get_field(row, "private_funds", ""))
        manages_real_estate = self._is_yes(self._get_field(row, "real_estate", ""))
        manages_commodities = self._is_yes(self._get_field(row, "commodities", ""))
        manages_hedge = self._is_yes(self._get_field(row, "hedge_funds", ""))
        manages_other_alts = self._is_yes(self._get_field(row, "other_alts", ""))

        # Detect family office
        is_family_office = self._detect_family_office(firm_name)

        # Create FirmRecord
        firm = FirmRecord(
            firm_name=firm_name,
            sec_file_number=sec_file,
            state=self._normalize_state(state),
            country=country or "United States",
            aum_total=aum,
            aum_regulatory=aum,
            num_clients=num_accounts,
            manages_public_securities=manages_public,
            manages_private_funds=manages_private,
            manages_real_estate=manages_real_estate,
            manages_commodities=manages_commodities,
            manages_hedge_funds=manages_hedge,
            manages_other_alts=manages_other_alts,
            is_family_office=is_family_office,
            raw_data={"csv_row": row},
        )

        return firm

    def _parse_schedule_d(self, schedule_d_path: Path, firms: Dict[str, FirmRecord]):
        """
        Enrich firms with Schedule D data (custodian information).

        Args:
            schedule_d_path: Path to schedule_d.csv
            firms: Dictionary of FirmRecord to update in-place
        """
        try:
            import csv

            with open(schedule_d_path, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)

                if not reader.fieldnames:
                    logger.warning(f"Schedule D CSV has no header: {schedule_d_path}")
                    return

                for row in reader:
                    sec_file = self._get_field(row, "sec_file", "").strip()

                    if sec_file not in firms:
                        continue

                    firm = firms[sec_file]

                    # Try to extract custodian name
                    custodian = self._extract_custodian(row)
                    if custodian and custodian not in firm.custodian_names:
                        firm.custodian_names.append(custodian)

        except Exception as e:
            logger.warning(f"Failed to parse Schedule D {schedule_d_path}: {e}")

    def _normalize_aum(self, value: str) -> float:
        """
        Normalize AUM string to float.

        Handles formats like:
        - "$1,234,567"
        - "1.2B" or "1.2b"
        - "500M"
        - "12345"

        Args:
            value: AUM string from CSV

        Returns:
            AUM as float, 0 if unparseable
        """
        if not value or not isinstance(value, str):
            return 0.0

        value = value.strip()
        if not value:
            return 0.0

        # Remove $ and commas
        value = value.replace("$", "").replace(",", "")

        # Handle B (billions), M (millions), K (thousands)
        multipliers = {"B": 1e9, "b": 1e9, "M": 1e6, "m": 1e6, "K": 1e3, "k": 1e3}

        for suffix, multiplier in multipliers.items():
            if value.endswith(suffix):
                try:
                    num = float(value[:-1])
                    return num * multiplier
                except ValueError:
                    return 0.0

        # Try direct float conversion
        try:
            return float(value)
        except ValueError:
            return 0.0

    def _normalize_state(self, state: str) -> str:
        """
        Normalize state code/name.

        Args:
            state: State code or name

        Returns:
            Normalized state code (e.g., "CA", "NY")
        """
        if not state:
            return ""

        state = state.strip().upper()

        # Already a 2-letter code
        if len(state) == 2 and state.isalpha():
            return state

        # Common state name abbreviations
        state_map = {
            "CALIFORNIA": "CA",
            "TEXAS": "TX",
            "NEW YORK": "NY",
            "FLORIDA": "FL",
        }

        return state_map.get(state, state[:2].upper())

    def _detect_family_office(self, firm_name: str) -> bool:
        """
        Detect if firm name suggests a family office.

        Args:
            firm_name: Firm name string

        Returns:
            True if likely a family office
        """
        if not firm_name:
            return False

        name_lower = firm_name.lower()

        for pattern in self.FAMILY_OFFICE_PATTERNS:
            if re.search(pattern, name_lower, re.IGNORECASE):
                return True

        return False

    def _get_field(self, row: Dict[str, str], field_key: str, default: str = "") -> str:
        """
        Get field value with column name alias resolution.

        Tries multiple possible column names in priority order.

        Args:
            row: CSV row dictionary
            field_key: Key in COLUMN_ALIASES
            default: Default value if not found

        Returns:
            Field value or default
        """
        if field_key not in self.COLUMN_ALIASES:
            return default

        for col_name in self.COLUMN_ALIASES[field_key]:
            if col_name in row:
                return row[col_name] or default

        return default

    def _extract_custodian(self, row: Dict[str, str]) -> Optional[str]:
        """
        Extract custodian name from Schedule D row.

        Args:
            row: CSV row from Schedule D

        Returns:
            Custodian name or None
        """
        # Try common Schedule D custodian column names
        custodian_columns = [
            "Custodian",
            "Custodian Name",
            "Primary Custodian",
            "Broker/Custodian",
        ]

        for col in custodian_columns:
            if col in row and row[col]:
                return row[col].strip()

        return None

    def _parse_int(self, value: str) -> int:
        """
        Parse integer value.

        Args:
            value: String value

        Returns:
            Integer or 0 if unparseable
        """
        if not value:
            return 0

        try:
            # Remove commas
            cleaned = value.replace(",", "").strip()
            return int(float(cleaned))
        except (ValueError, TypeError):
            return 0

    def _is_yes(self, value: str) -> bool:
        """
        Check if value represents 'yes'.

        Args:
            value: String value (Y, Yes, True, etc.)

        Returns:
            True if value is affirmative
        """
        if not value:
            return False

        return value.strip().upper() in ("Y", "YES", "TRUE", "1")
