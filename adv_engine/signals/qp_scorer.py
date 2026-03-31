"""
QP Scorer - Qualified Purchaser Probability Scoring

Determines likelihood that a firm qualifies as an accredited/qualified investor
based on AUM per client, family office status, and HNW client composition.
"""

import logging
from dataclasses import dataclass
from typing import Tuple

from config import QP_SCORING
from scrapers.adv_parser import FirmRecord

logger = logging.getLogger(__name__)


@dataclass
class QPScore:
    """QP probability score with reasoning."""
    score: float  # 0-10
    explanation: str
    method_used: str
    confidence: float  # 0-1


class QPScorer:
    """
    Scores firm's likelihood of being a qualified purchaser.

    Uses three methods in priority order:
    1. AUM per client (5M+→10, 2-5M→7, 1-2M→4, below→1)
    2. Family office override (FO/MFO→10)
    3. HNW percentage (80%+→8, 50%+→6)
    4. Default: 3 (insufficient data)
    """

    def __init__(self):
        """Initialize QP scorer."""
        self.qp_config = QP_SCORING
        logger.info("Initialized QPScorer")

    def score_qp_probability(self, firm: FirmRecord) -> QPScore:
        """
        Score firm's qualified purchaser probability.

        Args:
            firm: FirmRecord to score

        Returns:
            QPScore with score (0-10), explanation, and method used
        """
        # Method 1: AUM per client
        if firm.avg_aum_per_client > 0:
            return self._score_by_aum_per_client(firm)

        # Method 2: Family office override
        if firm.is_family_office or firm.is_multi_family_office:
            return self._score_family_office_override(firm)

        # Method 3: HNW percentage
        if firm.num_clients > 0 and firm.hnw_clients > 0:
            return self._score_by_hnw_percentage(firm)

        # Method 4: Default
        return self._score_default(firm)

    def _score_by_aum_per_client(self, firm: FirmRecord) -> QPScore:
        """
        Score using average AUM per client.

        Thresholds:
        - $5M+: score 10 (very high probability)
        - $2-5M: score 7 (high probability)
        - $1-2M: score 4 (moderate probability)
        - Below $1M: score 1 (low probability)
        """
        aum_per_client = firm.avg_aum_per_client
        thresholds = self.qp_config["aum_per_client_thresholds"]

        if aum_per_client >= thresholds["5_million_plus"]["min"]:
            score = thresholds["5_million_plus"]["score"]
            explanation = (
                f"Firm avg AUM per client (${aum_per_client:,.0f}) exceeds "
                f"${thresholds['5_million_plus']['min']:,.0f} threshold"
            )
            confidence = 0.95
        elif (
            thresholds["2_to_5_million"]["min"]
            <= aum_per_client
            < thresholds["2_to_5_million"]["max"]
        ):
            score = thresholds["2_to_5_million"]["score"]
            explanation = (
                f"Firm avg AUM per client (${aum_per_client:,.0f}) in "
                f"${thresholds['2_to_5_million']['min']:,.0f}-"
                f"${thresholds['2_to_5_million']['max']:,.0f} range"
            )
            confidence = 0.85
        elif (
            thresholds["1_to_2_million"]["min"]
            <= aum_per_client
            < thresholds["1_to_2_million"]["max"]
        ):
            score = thresholds["1_to_2_million"]["score"]
            explanation = (
                f"Firm avg AUM per client (${aum_per_client:,.0f}) in "
                f"${thresholds['1_to_2_million']['min']:,.0f}-"
                f"${thresholds['1_to_2_million']['max']:,.0f} range"
            )
            confidence = 0.70
        else:
            score = thresholds["below_1_million"]["score"]
            explanation = (
                f"Firm avg AUM per client (${aum_per_client:,.0f}) below "
                f"${thresholds['below_1_million']['max']:,.0f} threshold"
            )
            confidence = 0.50

        logger.debug(
            f"{firm.firm_name}: QP score {score}/10 (AUM/client method) "
            f"- ${aum_per_client:,.0f}"
        )

        return QPScore(
            score=score,
            explanation=explanation,
            method_used="aum_per_client",
            confidence=confidence
        )

    def _score_family_office_override(self, firm: FirmRecord) -> QPScore:
        """
        Score using family office status.

        Family offices and multi-family offices are assumed to have high
        qualified purchaser probability regardless of AUM metrics.
        """
        score = self.qp_config["family_office_override_score"]
        fo_type = (
            "Multi-Family Office"
            if firm.is_multi_family_office
            else "Family Office"
        )
        explanation = (
            f"Firm identified as {fo_type}, indicating qualified purchaser status"
        )
        confidence = 0.90

        logger.debug(
            f"{firm.firm_name}: QP score {score}/10 (family office override) "
            f"- {fo_type}"
        )

        return QPScore(
            score=score,
            explanation=explanation,
            method_used="family_office_override",
            confidence=confidence
        )

    def _score_by_hnw_percentage(self, firm: FirmRecord) -> QPScore:
        """
        Score using HNW client percentage.

        Thresholds:
        - 80%+: score 8
        - 50-80%: score 6
        - Below 50%: score 3
        """
        hnw_pct = firm.hnw_clients / firm.num_clients
        thresholds = self.qp_config["hnw_percentage_thresholds"]

        if hnw_pct >= thresholds["80_plus"]["min"]:
            score = thresholds["80_plus"]["score"]
            explanation = (
                f"Firm has {hnw_pct*100:.1f}% HNW clients, indicating "
                f"qualified purchaser base"
            )
            confidence = 0.85
        elif (
            thresholds["50_to_80"]["min"]
            <= hnw_pct
            < thresholds["50_to_80"]["max"]
        ):
            score = thresholds["50_to_80"]["score"]
            explanation = (
                f"Firm has {hnw_pct*100:.1f}% HNW clients, good QP indicator"
            )
            confidence = 0.70
        else:
            score = thresholds["below_50"]["score"]
            explanation = (
                f"Firm has {hnw_pct*100:.1f}% HNW clients, limited QP indication"
            )
            confidence = 0.50

        logger.debug(
            f"{firm.firm_name}: QP score {score}/10 (HNW percentage method) "
            f"- {hnw_pct*100:.1f}% HNW"
        )

        return QPScore(
            score=score,
            explanation=explanation,
            method_used="hnw_percentage",
            confidence=confidence
        )

    def _score_default(self, firm: FirmRecord) -> QPScore:
        """
        Default score when insufficient data.

        Used when firm has no AUM/client data, is not a family office,
        and has insufficient HNW client data.
        """
        score = self.qp_config["default_score"]
        explanation = (
            f"Insufficient client composition data for {firm.firm_name}; "
            f"assigning default QP score"
        )
        confidence = 0.30

        logger.debug(
            f"{firm.firm_name}: QP score {score}/10 (default) "
            f"- insufficient data"
        )

        return QPScore(
            score=score,
            explanation=explanation,
            method_used="default",
            confidence=confidence
        )
