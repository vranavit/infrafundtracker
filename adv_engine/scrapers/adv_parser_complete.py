"""
SEC IAPD bulk CSV parser module.

Parses SEC bulk CSV files into structured FirmRecord dataclass objects
with comprehensive firm information, AUM data, and client composition.
"""

import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict, Any, Generator
import re

logger = logging.getLogger(__name__)


@dataclass
class FirmRecord:
    """Complete firm record from SEC IAPD data."""

    # Identity
    sec_id: str
    crd_id: Optional[str] = None
    firm_name: str = ""
    hq_state: Optional[str] = None
    hq_country: Optional[str] = None
    website: Optional[str] = None
    primary_email: Optional[str] = None
    phone: Optional[str] = None

    # Registration & Regulatory
    registration_date: Optional[str] = None
    regulatory_status: str = "ACTIVE"
    filing_date: Optional[str] = None

    # AUM Data
    total_aum: float = 0.0
    discretionary_aum: float = 0.0
    non_discretionary_aum: float = 0.0

    # Client Composition
    number_of_clients: int = 0
    percent_hnw_clients: float = 0.0
    percent_institutional_clients: float = 0.0
    number_of_employees: Optional[int] = None

    # Investment Types
    investment_types: List[str] = field(default_factory=list)
    has_private_equity: bool = False
    has_private_debt: bool = False
    has_real_estate: bool = False
    has_infrastructure: bool = False
    has_hedge_funds: bool = False
    has_commodities: bool = False

    # Compensation
    compensation_methods: List[str] = field(default_factory=list)
    has_fee_based: bool = False
    has_aum_based: bool = False
    has_hourly: bool = False
    has_transaction_based: bool = False

    # Custodians & Platforms
    custodians: List[str] = field(default_factory=list)
    primary_custodian: Optional[str] = None
    platforms: List[str] = field(default_factory=list)

    # Computed/Derived Scores
    avg_aum_per_client: float = 0.0
    qp_score: float = 0.0
    signal_score: int = 0

    @property
    def qualified_purchaser(self) -> bool:
        """Check if firm meets qualified purchaser ($5M AUM) threshold."""
        return self.total_aum >= 5000000.0

    @property
    def min_investment_compatible(self) -> bool:
        """Check if avg AUM per client exceeds $25K minimum."""
        return self.avg_aum_per_client >= 25000.0

    def add_investment_type(self, inv_type: str) -> None:
        """Add investment type if not already present."""
        normalized = inv_type.strip().title()
        if normalized not in self.investment_types:
            self.investment_types.append(normalized)

    def add_compensation_method(self, method: str) -> None:
        """Add compensation method if not already present."""
        normalized = method.strip().lower()
        if normalized not in self.compensation_methods:
            self.compensation_methods.append(normalized)

    def add_custodian(self, custodian: str) -> None:
        """Add custodian if not already present."""
        normalized = custodian.strip()
        if normalized not in self.custodians:
            self.custodians.append(normalized)

    def calculate_avg_aum_per_client(self) -> None:
        """Calculate average AUM per client."""
        if self.number_of_clients > 0:
            self.avg_aum_per_client = self.total_aum / self.number_of_clients
        else:
            self.avg_aum_per_client = 0.0

    def finalize(self) -> None:
        """Finalize record by calculating derived fields."""
        self.calculate_avg_aum_per_client()

        # Update boolean flags based on lists
        self.has_fee_based = "fee-based" in self.compensation_methods
        self.has_aum_based = "aum-based" in self.compensation_methods
        self.has_hourly = "hourly" in self.compensation_methods
        self.has_transaction_based = "transaction-based" in self.compensation_methods

        # Set primary custodian
        if self.custodians:
            self.primary_custodian = self.custodians[0]

        # Check investment types
        self.has_private_equity = any(
            "private equity" in t.lower() for t in self.investment_types
        )
        self.has_private_debt = any(
            "private debt" in t.lower() for t in self.investment_types
        )
        self.has_real_estate = any(
            "real estate" in t.lower() for t in self.investment_types
        )
        self.has_infrastructure = any(
            "infrastructure" in t.lower() for t in self.investment_types
        )
        self.has_hedge_funds = any(
            "hedge fund" in t.lower() for t in self.investment_types
        )
        self.has_commodities = any(
            "commodi" in t.lower() for t in self.investment_types
        )


class ADVParser:
    """Parser for SEC IAPD bulk CSV files."""

    # Common field mappings for different SEC CSV formats
    FIELD_MAPPINGS = {
        "sec_cik": ["cik", "SEC_CIK", "cik_number"],
        "crd_number": ["crd", "CRD", "crd_number"],
        "name": ["name", "firm_name", "firm name"],
        "state": ["state", "HQ_STATE", "headquarters_state"],
        "country": ["country", "HQ_COUNTRY"],
        "website": ["website", "website_address"],
        "email": ["email", "primary_email"],
        "phone": ["phone", "phone_number"],
        "aum": ["aum", "total_aum", "assets_under_management"],
        "discretionary_aum": ["discretionary_aum", "disc_aum"],
        "non_discretionary_aum": ["non_discretionary_aum", "non_disc_aum"],
        "clients": ["num_clients", "number_of_clients", "client_count"],
        "hnw_percent": ["percent_hnw", "hnw_percent"],
        "institutional_percent": ["percent_institutional"],
        "employees": ["num_employees", "employee_count"],
        "registration_date": ["registration_date", "date_registered"],
        "filing_date": ["filing_date", "date_filed"],
        "status": ["status", "regulatory_status"],
        "custodian": ["custodian", "custodians"],
        "types": ["types_of_clients", "business_types"],
    }

    def __init__(self):
        """Initialize parser."""
        pass

    def _find_field_index(
        self, headers: List[str], field_aliases: List[str]
    ) -> Optional[int]:
        """Find column index using field aliases."""
        headers_lower = [h.lower().strip() for h in headers]
        for alias in field_aliases:
            alias_lower = alias.lower().strip()
            try:
                return headers_lower.index(alias_lower)
            except ValueError:
                continue
        return None

    def _parse_aum(self, value: Any) -> float:
        """Parse AUM value from string to float."""
        if not value:
            return 0.0

        value_str = str(value).strip()

        if value_str.lower() in ["n/a", "unknown", ""]:
            return 0.0

        cleaned = re.sub(r"[$,\s]", "", value_str)

        if cleaned.lower().endswith("m"):
            cleaned = cleaned[:-1]
            try:
                return float(cleaned) * 1_000_000
            except ValueError:
                return 0.0

        if cleaned.lower().endswith("b"):
            cleaned = cleaned[:-1]
            try:
                return float(cleaned) * 1_000_000_000
            except ValueError:
                return 0.0

        try:
            return float(cleaned)
        except ValueError:
            logger.warning(f"Could not parse AUM value: {value}")
            return 0.0

    def _parse_investment_types(self, value: Any) -> List[str]:
        """Parse investment types from comma-separated or pipe-separated string."""
        if not value:
            return []

        value_str = str(value).strip()

        types = []
        for delimiter in [",", "|", ";"]:
            if delimiter in value_str:
                types = [t.strip() for t in value_str.split(delimiter)]
                break

        if not types:
            types = [value_str]

        return [t for t in types if t]

    def _parse_compensation(self, value: Any) -> List[str]:
        """Parse compensation methods."""
        if not value:
            return []

        value_str = str(value).lower().strip()
        compensation = []

        if "fee" in value_str or "bps" in value_str:
            compensation.append("fee-based")
        if "aum" in value_str or "assets under" in value_str:
            compensation.append("aum-based")
        if "hourly" in value_str:
            compensation.append("hourly")
        if "transaction" in value_str or "commission" in value_str:
            compensation.append("transaction-based")

        return compensation

    def parse_firm_record(
        self, row: Dict[str, str], headers: List[str]
    ) -> Optional[FirmRecord]:
        """Parse a single CSV row into a FirmRecord."""
        try:
            sec_id_idx = self._find_field_index(
                headers, self.FIELD_MAPPINGS["sec_cik"]
            )
            if sec_id_idx is None:
                return None

            sec_id = row[headers[sec_id_idx]].strip()
            if not sec_id:
                return None

            record = FirmRecord(sec_id=sec_id)

            field_map = {
                "crd_id": "crd_number",
                "firm_name": "name",
                "hq_state": "state",
                "hq_country": "country",
                "website": "website",
                "primary_email": "email",
                "phone": "phone",
                "registration_date": "registration_date",
                "filing_date": "filing_date",
                "regulatory_status": "status",
            }

            for record_field, csv_field in field_map.items():
                idx = self._find_field_index(headers, self.FIELD_MAPPINGS[csv_field])
                if idx is not None and headers[idx] in row:
                    value = row[headers[idx]].strip()
                    if value:
                        setattr(record, record_field, value)

            for csv_field, record_field in [
                ("aum", "total_aum"),
                ("discretionary_aum", "discretionary_aum"),
                ("non_discretionary_aum", "non_discretionary_aum"),
                ("clients", "number_of_clients"),
                ("employees", "number_of_employees"),
            ]:
                idx = self._find_field_index(headers, self.FIELD_MAPPINGS[csv_field])
                if idx is not None and headers[idx] in row:
                    value = row[headers[idx]]
                    if csv_field in ["aum", "discretionary_aum", "non_discretionary_aum"]:
                        setattr(record, record_field, self._parse_aum(value))
                    else:
                        try:
                            setattr(record, record_field, int(float(value)))
                        except (ValueError, TypeError):
                            pass

            for csv_field, record_field in [
                ("hnw_percent", "percent_hnw_clients"),
                ("institutional_percent", "percent_institutional_clients"),
            ]:
                idx = self._find_field_index(headers, self.FIELD_MAPPINGS[csv_field])
                if idx is not None and headers[idx] in row:
                    try:
                        value = float(row[headers[idx]].rstrip("%"))
                        setattr(record, record_field, value)
                    except (ValueError, TypeError):
                        pass

            idx = self._find_field_index(headers, self.FIELD_MAPPINGS["types"])
            if idx is not None and headers[idx] in row:
                types = self._parse_investment_types(row[headers[idx]])
                record.investment_types = types

            idx = self._find_field_index(headers, self.FIELD_MAPPINGS.get("compensation", []))
            if idx is not None and headers[idx] in row:
                comp = self._parse_compensation(row[headers[idx]])
                record.compensation_methods = comp

            idx = self._find_field_index(headers, self.FIELD_MAPPINGS["custodian"])
            if idx is not None and headers[idx] in row:
                custodians = self._parse_investment_types(row[headers[idx]])
                record.custodians = custodians

            record.finalize()
            return record

        except Exception as e:
            logger.error(f"Error parsing firm record: {e}")
            return None

    def parse_bulk_file(self, file_path: Path) -> Generator[FirmRecord, None, None]:
        """Parse bulk CSV file and yield FirmRecord objects."""
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                reader = csv.DictReader(f)

                if reader.fieldnames is None:
                    logger.error(f"Empty or invalid CSV file: {file_path}")
                    return

                headers = list(reader.fieldnames)
                logger.info(
                    f"Parsing CSV with {len(headers)} fields: {headers[:5]}..."
                )

                for row_num, row in enumerate(reader, start=2):
                    record = self.parse_firm_record(row, headers)
                    if record:
                        yield record
                    else:
                        logger.debug(f"Skipped invalid row {row_num}")

        except Exception as e:
            logger.error(f"Error parsing bulk file {file_path}: {e}")

    def extract_investment_types(
        self, record: FirmRecord
    ) -> Dict[str, bool]:
        """Extract investment type flags from a firm record."""
        return {
            "has_private_equity": record.has_private_equity,
            "has_private_debt": record.has_private_debt,
            "has_real_estate": record.has_real_estate,
            "has_infrastructure": record.has_infrastructure,
            "has_hedge_funds": record.has_hedge_funds,
            "has_commodities": record.has_commodities,
        }


_default_parser = None


def get_parser() -> ADVParser:
    """Get default parser instance."""
    global _default_parser
    if _default_parser is None:
        _default_parser = ADVParser()
    return _default_parser


def parse_bulk_file(file_path: Path) -> Generator[FirmRecord, None, None]:
    """Convenience function to parse bulk file."""
    parser = get_parser()
    return parser.parse_bulk_file(file_path)
