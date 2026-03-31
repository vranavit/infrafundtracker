"""
ADV Parser - Converts SEC Form ADV data into FirmRecord dataclass
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


@dataclass
class FirmRecord:
    """
    Complete SEC Form ADV firm record with parsed metrics and classifications.

    Represents a single RIA/investment adviser firm with comprehensive data
    for signal detection and scoring.
    """
    # Core identification
    firm_name: str
    sec_file_number: str
    cik: Optional[str] = None
    state: str = ""
    country: str = "United States"
    registration_date: Optional[datetime] = None

    # AUM and client metrics
    aum_total: float = 0.0  # Total assets under management
    aum_regulatory: float = 0.0  # Regulatory AUM if different
    num_clients: int = 0
    avg_aum_per_client: float = 0.0

    # Client composition
    hnw_clients: int = 0  # High net worth client count
    institutional_clients: int = 0
    client_type_breakdown: Dict[str, int] = field(default_factory=dict)  # e.g., {"HNW": 10, "Institutional": 5}

    # Investment types
    manages_public_securities: bool = False
    manages_private_funds: bool = False
    manages_real_estate: bool = False
    manages_commodities: bool = False
    manages_hedge_funds: bool = False
    manages_other_alts: bool = False

    # Platform/custody information
    custodian_names: List[str] = field(default_factory=list)  # Primary custodians
    clearing_agent: Optional[str] = None
    prime_broker: Optional[str] = None

    # Fee structure and minimums
    fee_structure: str = "Commission"  # "Fee-Based", "Commission", "Hybrid", "Assets Under Management"
    minimum_account_size: float = 0.0

    # Firm classification
    firm_type: str = "Independent"  # "Independent", "Wirehouse", "IBD", "Bank", "Insurance"
    is_family_office: bool = False
    is_multi_family_office: bool = False
    is_registered_representative: bool = False

    # Breakaway indicators
    years_in_business: int = 0
    wirehouse_background: bool = False

    # Personnel (parsed from form)
    principal_names: List[str] = field(default_factory=list)
    has_cio: bool = False
    cio_name: Optional[str] = None

    # Recent additions/changes
    new_custodians_this_period: List[str] = field(default_factory=list)
    new_funds_this_period: List[str] = field(default_factory=list)
    recent_personnel_changes: List[str] = field(default_factory=list)

    # Filing metadata
    adv_amendment_date: Optional[datetime] = None
    adv_text_length: int = 0
    last_updated: datetime = field(default_factory=datetime.now)

    # Additional context
    website: Optional[str] = None
    notes: str = ""
    raw_data: Dict[str, Any] = field(default_factory=dict)  # Store raw ADV data for audit trail

    def __post_init__(self):
        """Validate and compute derived fields after initialization."""
        if self.num_clients > 0 and self.aum_total > 0:
            self.avg_aum_per_client = self.aum_total / self.num_clients

    def is_qualified_purchaser_candidate(self) -> bool:
        """Quick check if firm meets basic qualified purchaser criteria."""
        # Typical QP candidates have $5M+ AUM per client or are family offices
        if self.is_family_office or self.is_multi_family_office:
            return True
        return self.avg_aum_per_client >= 5_000_000

    def get_investment_profile(self) -> List[str]:
        """Return list of investment types managed."""
        types = []
        if self.manages_public_securities:
            types.append("Public Securities")
        if self.manages_private_funds:
            types.append("Private Funds")
        if self.manages_real_estate:
            types.append("Real Estate")
        if self.manages_commodities:
            types.append("Commodities")
        if self.manages_hedge_funds:
            types.append("Hedge Funds")
        if self.manages_other_alts:
            types.append("Other Alternatives")
        return types or ["Not Specified"]

    def has_modern_custody_infrastructure(self) -> bool:
        """Check if firm uses Tier 1/2 custodians."""
        from config import PLATFORMS_ACCESSIBILITY

        tier_1_2_keywords = []
        for platform, info in PLATFORMS_ACCESSIBILITY.items():
            if info["tier"] in [1, 2]:
                tier_1_2_keywords.extend(info["keywords"])

        custodian_lower = [c.lower() for c in self.custodian_names]
        for custodian in custodian_lower:
            for keyword in tier_1_2_keywords:
                if keyword.lower() in custodian:
                    return True
        return False

    def __repr__(self) -> str:
        """Human-readable representation."""
        return (
            f"FirmRecord("
            f"firm_name={self.firm_name!r}, "
            f"aum_total=${self.aum_total:,.0f}, "
            f"num_clients={self.num_clients}, "
            f"custodians={self.custodian_names}"
            f")"
        )
