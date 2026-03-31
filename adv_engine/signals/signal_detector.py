"""
Signal Detector - Core signal detection engine for ADV Buying Signal Engine

Detects all signals defined in config.SIGNALS by comparing current firm state
to previous state and matching against defined patterns.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum

from config import SIGNALS, PLATFORMS_ACCESSIBILITY
from scrapers.adv_parser import FirmRecord

logger = logging.getLogger(__name__)


class SignalCategory(str, Enum):
    """Signal category enumeration."""
    PLATFORM = "platform"
    INVESTMENT_TYPE = "investment_type"
    AUM_GROWTH = "aum_growth"
    CLIENT_COMPOSITION = "client_composition"
    PERSONNEL = "personnel"
    BREAKAWAY = "breakaway"
    FEE_STRUCTURE = "fee_structure"


@dataclass
class Signal:
    """
    Single detected buying signal with evidence and context.

    Represents one fired signal with all necessary context for scoring,
    lead generation, and reporting.
    """
    name: str
    weight: float
    evidence: str
    talking_point: str
    is_new: bool  # True if this is new vs. existing signal
    category: str
    detected_at: datetime = field(default_factory=datetime.now)

    # Optional context fields
    platform: Optional[str] = None
    platform_tier: Optional[int] = None
    raw_data: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"Signal(name={self.name!r}, weight={self.weight}, is_new={self.is_new})"


class SignalDetector:
    """
    Detects buying signals by comparing current and previous firm records.

    Implements complete signal detection logic for all signal types:
    - Platform adoption (Schwab, Fidelity, etc.)
    - Investment type expansion (private funds, real estate)
    - AUM growth (percentage and threshold crossing)
    - Client composition changes (HNW growth, institutional)
    - Personnel changes (new CIO, alternatives hire)
    - Breakaway indicators (registration date, wirehouse background)
    - Fee/minimum changes
    """

    # Threshold constants
    AUM_GROWTH_THRESHOLD_25PCT = 0.25
    AUM_GROWTH_THRESHOLD_50PCT = 0.50
    AUM_THRESHOLD_1B = 1_000_000_000
    AUM_THRESHOLD_500M = 500_000_000
    HNW_GROWTH_THRESHOLD = 0.20  # 20%
    BREAKAWAY_REGISTRATION_DAYS = 548  # 18 months

    def __init__(self):
        """Initialize signal detector."""
        logger.info("Initialized SignalDetector")
        self.signals_config = SIGNALS
        self.platforms_config = PLATFORMS_ACCESSIBILITY

    def detect_signals(
        self,
        current: FirmRecord,
        previous: Optional[FirmRecord] = None
    ) -> List[Signal]:
        """
        Detect all signals for a firm by comparing current to previous state.

        Args:
            current: Current firm record
            previous: Previous firm record (if available)

        Returns:
            List of detected Signal objects
        """
        signals: List[Signal] = []

        # All signal detection methods
        signals.extend(self._detect_platform_signals(current, previous))
        signals.extend(self._detect_investment_type_signals(current, previous))
        signals.extend(self._detect_aum_growth_signals(current, previous))
        signals.extend(self._detect_client_composition_signals(current, previous))
        signals.extend(self._detect_personnel_signals(current, previous))
        signals.extend(self._detect_breakaway_signals(current, previous))
        signals.extend(self._detect_fee_structure_signals(current, previous))

        if signals:
            logger.info(
                f"Detected {len(signals)} signals for {current.firm_name} "
                f"(new: {sum(1 for s in signals if s.is_new)})"
            )
        return signals

    def _detect_platform_signals(
        self,
        current: FirmRecord,
        previous: Optional[FirmRecord] = None
    ) -> List[Signal]:
        """Detect platform adoption signals (Schwab, Fidelity, etc.)."""
        signals: List[Signal] = []

        if not current.custodian_names:
            return signals

        previous_custodians = set(
            [c.lower() for c in (previous.custodian_names if previous else [])]
        )
        current_custodians = set([c.lower() for c in current.custodian_names])

        # Map platform names to signal keys
        platform_signals_map = {
            "schwab": ("schwab_adoption", 8),
            "fidelity": ("fidelity_adoption", 7),
            "pershing": ("pershing_adoption", 6),
            "broadridge": ("broadridge_adoption", 6),
            "dss": ("dss_adoption", 6),
        }

        for platform, (signal_key, weight) in platform_signals_map.items():
            platform_keywords = self.platforms_config.get(platform, {}).get("keywords", [])

            # Check if platform appears in current custodians
            for custodian in current_custodians:
                for keyword in platform_keywords:
                    if keyword.lower() in custodian:
                        is_new = custodian not in previous_custodians

                        if signal_key in self.signals_config:
                            config = self.signals_config[signal_key]
                            talking_point = config["talking_point_template"].format(
                                firm_name=current.firm_name,
                                custodian=custodian
                            )

                            signals.append(
                                Signal(
                                    name=config["name"],
                                    weight=weight,
                                    evidence=f"Custodian: {custodian}",
                                    talking_point=talking_point,
                                    is_new=is_new,
                                    category=config["category"],
                                    platform=platform,
                                    platform_tier=self.platforms_config[platform]["tier"],
                                    raw_data={"custodian": custodian}
                                )
                            )
                        break

        return signals

    def _detect_investment_type_signals(
        self,
        current: FirmRecord,
        previous: Optional[FirmRecord] = None
    ) -> List[Signal]:
        """Detect investment type expansion signals."""
        signals: List[Signal] = []

        # New private funds
        if current.manages_private_funds and (
            previous is None or not previous.manages_private_funds
        ):
            if "new_private_funds" in self.signals_config:
                config = self.signals_config["new_private_funds"]
                signals.append(
                    Signal(
                        name=config["name"],
                        weight=config["weight"],
                        evidence="Firm began managing private funds",
                        talking_point=config["talking_point_template"].format(
                            firm_name=current.firm_name
                        ),
                        is_new=True,
                        category=config["category"],
                        raw_data={"manages_private_funds": True}
                    )
                )

        # Existing private funds without modern infrastructure
        if (
            current.manages_private_funds
            and not current.has_modern_custody_infrastructure()
        ):
            if "existing_private_funds_no_infra" in self.signals_config:
                config = self.signals_config["existing_private_funds_no_infra"]
                signals.append(
                    Signal(
                        name=config["name"],
                        weight=config["weight"],
                        evidence="Manages private funds without Tier 1/2 custody",
                        talking_point=config["talking_point_template"].format(
                            firm_name=current.firm_name
                        ),
                        is_new=False,
                        category=config["category"],
                        raw_data={
                            "manages_private_funds": True,
                            "custody_tiers": current.custodian_names
                        }
                    )
                )

        # New real estate added
        if current.manages_real_estate and (
            previous is None or not previous.manages_real_estate
        ):
            if "new_real_estate_added" in self.signals_config:
                config = self.signals_config["new_real_estate_added"]
                signals.append(
                    Signal(
                        name=config["name"],
                        weight=config["weight"],
                        evidence="Firm began managing real estate",
                        talking_point=config["talking_point_template"].format(
                            firm_name=current.firm_name
                        ),
                        is_new=True,
                        category=config["category"],
                        raw_data={"manages_real_estate": True}
                    )
                )

        return signals

    def _detect_aum_growth_signals(
        self,
        current: FirmRecord,
        previous: Optional[FirmRecord] = None
    ) -> List[Signal]:
        """Detect AUM growth signals (percentage and threshold crossings)."""
        signals: List[Signal] = []

        if previous is None or previous.aum_total == 0:
            return signals

        aum_previous = previous.aum_total
        aum_current = current.aum_total
        aum_growth = (aum_current - aum_previous) / aum_previous

        # 50%+ growth (check first as it's more significant)
        if aum_growth >= self.AUM_GROWTH_THRESHOLD_50PCT:
            if "aum_growth_50pct" in self.signals_config:
                config = self.signals_config["aum_growth_50pct"]
                signals.append(
                    Signal(
                        name=config["name"],
                        weight=config["weight"],
                        evidence=f"AUM grew {aum_growth*100:.1f}%",
                        talking_point=config["talking_point_template"].format(
                            firm_name=current.firm_name,
                            aum_previous=aum_previous,
                            aum_current=aum_current
                        ),
                        is_new=True,
                        category=config["category"],
                        raw_data={
                            "aum_previous": aum_previous,
                            "aum_current": aum_current,
                            "growth_pct": aum_growth
                        }
                    )
                )
        # 25%+ growth
        elif aum_growth >= self.AUM_GROWTH_THRESHOLD_25PCT:
            if "aum_growth_25pct" in self.signals_config:
                config = self.signals_config["aum_growth_25pct"]
                signals.append(
                    Signal(
                        name=config["name"],
                        weight=config["weight"],
                        evidence=f"AUM grew {aum_growth*100:.1f}%",
                        talking_point=config["talking_point_template"].format(
                            firm_name=current.firm_name,
                            aum_previous=aum_previous,
                            aum_current=aum_current
                        ),
                        is_new=True,
                        category=config["category"],
                        raw_data={
                            "aum_previous": aum_previous,
                            "aum_current": aum_current,
                            "growth_pct": aum_growth
                        }
                    )
                )

        # Threshold crossings
        if aum_previous < self.AUM_THRESHOLD_1B <= aum_current:
            if "aum_crossed_1b" in self.signals_config:
                config = self.signals_config["aum_crossed_1b"]
                signals.append(
                    Signal(
                        name=config["name"],
                        weight=config["weight"],
                        evidence=f"AUM crossed $1B threshold (now ${aum_current:,.0f})",
                        talking_point=config["talking_point_template"].format(
                            firm_name=current.firm_name
                        ),
                        is_new=True,
                        category=config["category"],
                        raw_data={"threshold": 1_000_000_000, "aum_current": aum_current}
                    )
                )

        if aum_previous < self.AUM_THRESHOLD_500M <= aum_current:
            if "aum_crossed_500m" in self.signals_config:
                config = self.signals_config["aum_crossed_500m"]
                signals.append(
                    Signal(
                        name=config["name"],
                        weight=config["weight"],
                        evidence=f"AUM crossed $500M threshold (now ${aum_current:,.0f})",
                        talking_point=config["talking_point_template"].format(
                            firm_name=current.firm_name
                        ),
                        is_new=True,
                        category=config["category"],
                        raw_data={"threshold": 500_000_000, "aum_current": aum_current}
                    )
                )

        return signals

    def _detect_client_composition_signals(
        self,
        current: FirmRecord,
        previous: Optional[FirmRecord] = None
    ) -> List[Signal]:
        """Detect client composition change signals."""
        signals: List[Signal] = []

        # HNW client growth > 20%
        if previous and previous.hnw_clients > 0:
            hnw_growth = (current.hnw_clients - previous.hnw_clients) / previous.hnw_clients
            if hnw_growth > self.HNW_GROWTH_THRESHOLD:
                if "hnw_client_growth_20pct" in self.signals_config:
                    config = self.signals_config["hnw_client_growth_20pct"]
                    signals.append(
                        Signal(
                            name=config["name"],
                            weight=config["weight"],
                            evidence=f"HNW clients grew {hnw_growth*100:.1f}% ({previous.hnw_clients} to {current.hnw_clients})",
                            talking_point=config["talking_point_template"].format(
                                firm_name=current.firm_name
                            ),
                            is_new=True,
                            category=config["category"],
                            raw_data={
                                "hnw_previous": previous.hnw_clients,
                                "hnw_current": current.hnw_clients,
                                "growth_pct": hnw_growth
                            }
                        )
                    )

        # Institutional clients newly added
        if current.institutional_clients > 0 and (
            previous is None or previous.institutional_clients == 0
        ):
            if "institutional_clients_added" in self.signals_config:
                config = self.signals_config["institutional_clients_added"]
                signals.append(
                    Signal(
                        name=config["name"],
                        weight=config["weight"],
                        evidence=f"Firm began serving institutional clients ({current.institutional_clients})",
                        talking_point=config["talking_point_template"].format(
                            firm_name=current.firm_name
                        ),
                        is_new=True,
                        category=config["category"],
                        raw_data={"institutional_clients": current.institutional_clients}
                    )
                )

        # Family office detection
        if (current.is_family_office or current.is_multi_family_office) and (
            previous is None
            or (not previous.is_family_office and not previous.is_multi_family_office)
        ):
            if "family_office_detected" in self.signals_config:
                config = self.signals_config["family_office_detected"]
                fo_type = "Multi-Family Office" if current.is_multi_family_office else "Family Office"
                signals.append(
                    Signal(
                        name=config["name"],
                        weight=config["weight"],
                        evidence=f"Firm identified as {fo_type}",
                        talking_point=config["talking_point_template"].format(
                            firm_name=current.firm_name
                        ),
                        is_new=True,
                        category=config["category"],
                        raw_data={
                            "is_family_office": current.is_family_office,
                            "is_multi_family_office": current.is_multi_family_office
                        }
                    )
                )

        return signals

    def _detect_personnel_signals(
        self,
        current: FirmRecord,
        previous: Optional[FirmRecord] = None
    ) -> List[Signal]:
        """Detect personnel change signals."""
        signals: List[Signal] = []

        # New CIO hired (from firm_type changes or cio_name changes)
        if current.has_cio and (previous is None or not previous.has_cio):
            if "new_cio_hired" in self.signals_config:
                config = self.signals_config["new_cio_hired"]
                cio_info = f" ({current.cio_name})" if current.cio_name else ""
                signals.append(
                    Signal(
                        name=config["name"],
                        weight=config["weight"],
                        evidence=f"New CIO hired{cio_info}",
                        talking_point=config["talking_point_template"].format(
                            firm_name=current.firm_name
                        ),
                        is_new=True,
                        category=config["category"],
                        raw_data={"cio_name": current.cio_name or "Unknown"}
                    )
                )

        # Alternatives hire detection
        if current.recent_personnel_changes:
            for change in current.recent_personnel_changes:
                if "alternative" in change.lower():
                    if "alternatives_hire" in self.signals_config:
                        config = self.signals_config["alternatives_hire"]
                        signals.append(
                            Signal(
                                name=config["name"],
                                weight=config["weight"],
                                evidence=f"Alternatives specialist hired: {change}",
                                talking_point=config["talking_point_template"].format(
                                    firm_name=current.firm_name
                                ),
                                is_new=True,
                                category=config["category"],
                                raw_data={"hire": change}
                            )
                        )
                    break

        return signals

    def _detect_breakaway_signals(
        self,
        current: FirmRecord,
        previous: Optional[FirmRecord] = None
    ) -> List[Signal]:
        """Detect breakaway/new firm signals."""
        signals: List[Signal] = []

        # Recently registered (within 548 days / 18 months)
        if current.registration_date:
            days_since_registration = (
                datetime.now() - current.registration_date
            ).days
            if days_since_registration <= self.BREAKAWAY_REGISTRATION_DAYS:
                if "recently_registered" in self.signals_config:
                    config = self.signals_config["recently_registered"]
                    signals.append(
                        Signal(
                            name=config["name"],
                            weight=config["weight"],
                            evidence=f"Registered {days_since_registration} days ago",
                            talking_point=config["talking_point_template"].format(
                                firm_name=current.firm_name,
                                registration_days_ago=days_since_registration
                            ),
                            is_new=False,
                            category=config["category"],
                            raw_data={
                                "registration_date": current.registration_date.isoformat(),
                                "days_ago": days_since_registration
                            }
                        )
                    )

        # Wirehouse breakaway detection
        if current.firm_type == "Wirehouse" or (
            current.wirehouse_background
            and current.firm_type == "Independent"
        ):
            if "wirehouse_breakaway" in self.signals_config:
                config = self.signals_config["wirehouse_breakaway"]
                signals.append(
                    Signal(
                        name=config["name"],
                        weight=config["weight"],
                        evidence="Wirehouse background detected",
                        talking_point=config["talking_point_template"].format(
                            firm_name=current.firm_name
                        ),
                        is_new=False,
                        category=config["category"],
                        raw_data={
                            "firm_type": current.firm_type,
                            "wirehouse_background": current.wirehouse_background
                        }
                    )
                )

        return signals

    def _detect_fee_structure_signals(
        self,
        current: FirmRecord,
        previous: Optional[FirmRecord] = None
    ) -> List[Signal]:
        """Detect fee structure and minimum signals."""
        signals: List[Signal] = []

        # Fee-based compensation model
        if "Fee-Based" in current.fee_structure and (
            previous is None or "Fee-Based" not in previous.fee_structure
        ):
            if "fee_based_compensation" in self.signals_config:
                config = self.signals_config["fee_based_compensation"]
                signals.append(
                    Signal(
                        name=config["name"],
                        weight=config["weight"],
                        evidence=f"Fee structure: {current.fee_structure}",
                        talking_point=config["talking_point_template"].format(
                            firm_name=current.firm_name
                        ),
                        is_new=True,
                        category=config["category"],
                        raw_data={"fee_structure": current.fee_structure}
                    )
                )

        # Minimum account decreased
        if previous and previous.minimum_account_size > 0:
            if current.minimum_account_size < previous.minimum_account_size:
                reduction = previous.minimum_account_size - current.minimum_account_size
                reduction_pct = reduction / previous.minimum_account_size
                if reduction_pct > 0.10:  # More than 10% reduction
                    if "min_account_decreased" in self.signals_config:
                        config = self.signals_config["min_account_decreased"]
                        signals.append(
                            Signal(
                                name=config["name"],
                                weight=config["weight"],
                                evidence=f"Minimum reduced {reduction_pct*100:.1f}%",
                                talking_point=config["talking_point_template"].format(
                                    firm_name=current.firm_name,
                                    min_previous=previous.minimum_account_size,
                                    min_current=current.minimum_account_size
                                ),
                                is_new=True,
                                category=config["category"],
                                raw_data={
                                    "min_previous": previous.minimum_account_size,
                                    "min_current": current.minimum_account_size,
                                    "reduction_pct": reduction_pct
                                }
                            )
                        )

        return signals
