"""
Unit tests for firm scoring and tiering
Tests scoring logic and tier assignment
"""

import sys
import os
import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models import FirmRecord, Platform, Tier
from scorer import FirmScorer
from conftest import make_firm


class TestScoreCalculation:
    """Tests for score calculation"""

    def test_inaccessible_platform_zero_score(self):
        """Test that inaccessible platforms always score 0"""
        firm = make_firm(
            platform=Platform.INACCESSIBLE,
            aum=5_000_000_000
        )
        score = FirmScorer.calculate_score(firm)
        assert score == 0.0

    def test_unknown_platform_reduced_score(self):
        """Test that unknown platform gets no platform bonus"""
        firm = make_firm(
            platform=Platform.UNKNOWN,
            has_private_funds=True,
            aum_growth_percent=20.0
        )
        score = FirmScorer.calculate_score(firm)
        assert score < 50.0  # Should be lower than accessible platform

    def test_platform_access_bonus(self):
        """Test that platform access gives score boost"""
        firm1 = make_firm(platform=Platform.ICAPITAL)
        score1 = FirmScorer.calculate_score(firm1)
        
        firm2 = make_firm(platform=Platform.UNKNOWN)
        score2 = FirmScorer.calculate_score(firm2)
        
        assert score1 > score2

    def test_signal_points_accumulate(self):
        """Test that signals add to score"""
        firm_no_signals = make_firm(
            platform=Platform.ICAPITAL,
            has_private_funds=False,
            has_breakaway=False,
            is_family_office=False,
            aum_growth_percent=0.0,
            investment_types=["equities"]
        )
        score_no_signals = FirmScorer.calculate_score(firm_no_signals)
        
        firm_signals = make_firm(
            platform=Platform.ICAPITAL,
            has_private_funds=True,
            has_breakaway=True,
            is_family_office=False,
            aum_growth_percent=0.0,
            investment_types=["equities"]
        )
        score_signals = FirmScorer.calculate_score(firm_signals)
        
        assert score_signals > score_no_signals

    def test_score_capped_at_100(self):
        """Test that score is capped at 100"""
        firm = make_firm(
            platform=Platform.ICAPITAL,
            has_private_funds=True,
            has_breakaway=True,
            is_family_office=True,
            aum=10_000_000_000,
            aum_growth_percent=50.0,
            investment_types=["equities", "fixed income", "alternatives"]
        )
        score = FirmScorer.calculate_score(firm)
        assert score <= 100.0


class TestTierAssignment:
    """Tests for tier assignment logic"""

    def test_platform_gate_no_platform(self):
        """Test that no platform caps at Tier 3"""
        firm = make_firm(
            platform=Platform.UNKNOWN,
            score=85.0  # Would be Tier 1 with platform
        )
        tier = FirmScorer.assign_tier(firm)
        assert tier != Tier.TIER_1
        assert tier != Tier.TIER_2

    def test_tier1_requires_platform_and_signals(self):
        """Test Tier 1 requires both platform AND signals"""
        # No signals
        firm_no_signals = make_firm(
            platform=Platform.ICAPITAL,
            has_private_funds=False,
            score=85.0
        )
        assert FirmScorer.assign_tier(firm_no_signals) != Tier.TIER_1
        
        # With signals
        firm_with_signals = make_firm(
            platform=Platform.ICAPITAL,
            has_private_funds=True,
            has_breakaway=True,
            score=85.0
        )
        assert FirmScorer.assign_tier(firm_with_signals) == Tier.TIER_1

    def test_qp_multiplier_effect(self):
        """Test that QP count influences scoring"""
        firm_low_advisors = make_firm(
            platform=Platform.ICAPITAL,
            num_advisors=1,
            aum=100_000_000
        )
        
        firm_high_advisors = make_firm(
            platform=Platform.ICAPITAL,
            num_advisors=50,
            aum=100_000_000
        )
        
        # Both should have same structural score, but high advisors = more scaling potential
        score1 = FirmScorer.calculate_score(firm_low_advisors)
        score2 = FirmScorer.calculate_score(firm_high_advisors)
        
        # Scores should be similar (same AUM/signals)
        assert abs(score1 - score2) < 10.0

    def test_tier_boundaries(self):
        """Test tier boundary conditions"""
        # Just below Tier 3 threshold
        firm_below_t3 = make_firm(platform=Platform.ICAPITAL, score=35.0)
        assert FirmScorer.assign_tier(firm_below_t3) == Tier.NOT_QUALIFIED
        
        # Just at Tier 3 threshold
        firm_at_t3 = make_firm(platform=Platform.ICAPITAL, score=40.0)
        assert FirmScorer.assign_tier(firm_at_t3) == Tier.TIER_3
        
        # Just below Tier 2 threshold
        firm_below_t2 = make_firm(
            platform=Platform.ICAPITAL,
            score=55.0,
            has_private_funds=False  # No signals
        )
        assert FirmScorer.assign_tier(firm_below_t2) != Tier.TIER_2
        
        # Just at Tier 2 threshold with signal
        firm_at_t2 = make_firm(
            platform=Platform.ICAPITAL,
            score=60.0,
            has_private_funds=True  # 1 signal
        )
        assert FirmScorer.assign_tier(firm_at_t2) == Tier.TIER_2


class TestScoreAndTierIntegration:
    """Integration tests for scoring and tiering"""

    def test_score_and_tier_updates_firm(self):
        """Test that score_and_tier updates FirmRecord correctly"""
        firm = make_firm(
            platform=Platform.ICAPITAL,
            has_private_funds=True,
            has_breakaway=True,
            score=0.0,
            tier=Tier.NOT_QUALIFIED,
            signals=[]
        )
        
        updated = FirmScorer.score_and_tier(firm)
        
        assert updated.score > 0
        assert updated.tier != Tier.NOT_QUALIFIED
        assert len(updated.signals) > 0

    def test_tier_1_scoring_example(self):
        """Example of a firm that achieves Tier 1"""
        firm = make_firm(
            platform=Platform.ICAPITAL,
            has_private_funds=True,
            has_breakaway=True,
            aum=2_000_000_000,
            aum_growth_percent=20.0,
            num_advisors=25,
            investment_types=["equities", "fixed income", "alternatives"]
        )
        
        FirmScorer.score_and_tier(firm)
        
        assert firm.tier == Tier.TIER_1
        assert firm.score >= FirmScorer.TIER_1_MIN
        assert len(firm.signals) >= 2

    def test_tier_2_scoring_example(self):
        """Example of a firm that achieves Tier 2"""
        firm = make_firm(
            platform=Platform.PERSHING,
            has_private_funds=True,
            has_breakaway=False,
            aum=1_000_000_000,
            aum_growth_percent=20.0,
            num_advisors=15
        )
        
        FirmScorer.score_and_tier(firm)
        
        assert firm.tier == Tier.TIER_2
        assert firm.score >= FirmScorer.TIER_2_MIN

    def test_tier_3_scoring_example(self):
        """Example of a firm that achieves Tier 3"""
        firm = make_firm(
            platform=Platform.SCHWAB,
            has_private_funds=False,
            aum=500_000_000,
            aum_growth_percent=5.0,
            num_advisors=10
        )
        
        FirmScorer.score_and_tier(firm)
        
        assert firm.tier == Tier.TIER_3
        assert firm.score >= FirmScorer.TIER_3_MIN
