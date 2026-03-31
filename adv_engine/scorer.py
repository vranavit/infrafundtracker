"""
Scoring module for ADV Buying Signal Engine
Assigns tier scores and ratings to firms
"""

import logging
from typing import Tuple

from models import FirmRecord, Tier, Platform
from signal_engine import SignalDetector

logger = logging.getLogger(__name__)


class FirmScorer:
    """Scores and tiers firms"""

    # Tier boundaries (score ranges)
    TIER_1_MIN = 80.0
    TIER_2_MIN = 60.0
    TIER_3_MIN = 40.0

    @staticmethod
    def calculate_score(firm: FirmRecord) -> float:
        """
        Calculate composite score for a firm
        Ranges from 0 to 100
        """
        score = 0.0

        # Platform access (must have for high tiers)
        if firm.platform == Platform.INACCESSIBLE:
            return 0.0

        if firm.platform != Platform.UNKNOWN:
            score += 30.0  # Base platform score
        
        # Detect signals
        signals = SignalDetector.detect_all_signals(firm)
        signal_count = len(signals)
        score += signal_count * 10.0  # 10 points per signal
        
        # AUM component (up to 20 points)
        aum_score = min(20.0, (firm.aum / 1_000_000_000) * 5)
        score += aum_score
        
        # Growth component (up to 15 points)
        if firm.aum_growth_percent > 0:
            growth_score = min(15.0, (firm.aum_growth_percent / 50.0) * 15)
            score += growth_score
        
        # Specialist indicators
        if firm.is_family_office:
            score += 5.0
        if firm.has_breakaway:
            score += 5.0
        
        # Cap at 100
        return min(100.0, score)

    @staticmethod
    def assign_tier(firm: FirmRecord) -> Tier:
        """
        Assign tier based on score
        Tier 1: Platform access required + high signals
        Tier 2: Platform access required + some signals
        Tier 3: Lower bar
        Not Qualified: No platform or very low signals
        """
        
        # No platform access = max Tier 3
        if firm.platform == Platform.INACCESSIBLE or firm.platform == Platform.UNKNOWN:
            if firm.score >= FirmScorer.TIER_3_MIN:
                return Tier.TIER_3
            return Tier.NOT_QUALIFIED
        
        # Has platform - check for signals
        signals = SignalDetector.detect_all_signals(firm)
        
        # Tier 1: Platform + 2+ signals + score >= 80
        if len(signals) >= 2 and firm.score >= FirmScorer.TIER_1_MIN:
            return Tier.TIER_1
        
        # Tier 2: Platform + 1+ signal + score >= 60
        if len(signals) >= 1 and firm.score >= FirmScorer.TIER_2_MIN:
            return Tier.TIER_2
        
        # Tier 3: Platform + score >= 40
        if firm.score >= FirmScorer.TIER_3_MIN:
            return Tier.TIER_3
        
        return Tier.NOT_QUALIFIED

    @staticmethod
    def score_and_tier(firm: FirmRecord) -> FirmRecord:
        """Calculate score and assign tier"""
        firm.score = FirmScorer.calculate_score(firm)
        firm.tier = FirmScorer.assign_tier(firm)
        firm.signals = SignalDetector.detect_all_signals(firm)
        return firm
