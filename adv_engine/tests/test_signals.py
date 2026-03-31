"""
Unit tests for signal detection
Tests all signal detection logic
"""

import sys
import os
import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models import FirmRecord, Platform, Tier
from signal_engine import SignalDetector
from conftest import make_firm


class TestNewPrivateFundsSignal:
    """Tests for new private funds signal detection"""

    def test_new_private_funds_signal(self):
        """Test detection of new private funds"""
        firm = make_firm(has_private_funds=True)
        assert SignalDetector.detect_new_private_funds(firm) == True

    def test_no_private_funds_signal(self):
        """Test no signal when no private funds"""
        firm = make_firm(has_private_funds=False)
        assert SignalDetector.detect_new_private_funds(firm) == False


class TestAUMGrowthSignal:
    """Tests for AUM growth signal detection"""

    def test_aum_growth_signal_correct_threshold(self):
        """Test AUM growth detection at correct threshold"""
        # Growth at threshold
        firm = make_firm(aum_growth_percent=15.0)
        assert SignalDetector.detect_aum_growth(firm, threshold=15.0) == True
        
        # Growth above threshold
        firm = make_firm(aum_growth_percent=20.0)
        assert SignalDetector.detect_aum_growth(firm, threshold=15.0) == True

    def test_aum_growth_signal_below_threshold(self):
        """Test no signal when growth below threshold"""
        firm = make_firm(aum_growth_percent=10.0)
        assert SignalDetector.detect_aum_growth(firm, threshold=15.0) == False

    def test_aum_growth_signal_negative_growth(self):
        """Test no signal with negative growth"""
        firm = make_firm(aum_growth_percent=-5.0)
        assert SignalDetector.detect_aum_growth(firm, threshold=15.0) == False

    def test_aum_growth_signal_custom_threshold(self):
        """Test AUM growth with custom threshold"""
        firm = make_firm(aum_growth_percent=25.0)
        assert SignalDetector.detect_aum_growth(firm, threshold=20.0) == True
        assert SignalDetector.detect_aum_growth(firm, threshold=30.0) == False


class TestBreakawaySignal:
    """Tests for breakaway group signal"""

    def test_breakaway_detection(self):
        """Test detection of breakaway groups"""
        firm = make_firm(has_breakaway=True)
        assert SignalDetector.detect_breakaway(firm) == True

    def test_no_breakaway(self):
        """Test no signal for non-breakaway"""
        firm = make_firm(has_breakaway=False)
        assert SignalDetector.detect_breakaway(firm) == False


class TestFamilyOfficeSignal:
    """Tests for family office signal"""

    def test_family_office_detection(self):
        """Test detection of family offices"""
        firm = make_firm(is_family_office=True)
        assert SignalDetector.detect_family_office(firm) == True

    def test_no_family_office(self):
        """Test no signal for non-family office"""
        firm = make_firm(is_family_office=False)
        assert SignalDetector.detect_family_office(firm) == False


class TestPlatformExpansionSignal:
    """Tests for platform expansion opportunity signal"""

    def test_platform_signal_detection(self):
        """Test detection of platform expansion opportunity"""
        firm = make_firm(investment_types=["equities", "fixed income", "alternatives"])
        assert SignalDetector.detect_platform_expansion_opportunity(firm) == True

    def test_platform_signal_single_type(self):
        """Test no signal with single investment type"""
        firm = make_firm(investment_types=["equities"])
        assert SignalDetector.detect_platform_expansion_opportunity(firm) == False

    def test_platform_signal_two_types(self):
        """Test signal with exactly two investment types"""
        firm = make_firm(investment_types=["equities", "fixed income"])
        assert SignalDetector.detect_platform_expansion_opportunity(firm) == True


class TestMultipleSignalDetection:
    """Tests for detecting all signals on a firm"""

    def test_no_false_positive_stable_firm(self):
        """Test no signals for stable firm with no special characteristics"""
        firm = make_firm(
            has_private_funds=False,
            has_breakaway=False,
            is_family_office=False,
            aum_growth_percent=0.0,
            investment_types=["equities"]
        )
        signals = SignalDetector.detect_all_signals(firm)
        assert len(signals) == 0

    def test_multiple_signals(self):
        """Test detection of multiple signals on one firm"""
        firm = make_firm(
            has_private_funds=True,
            has_breakaway=True,
            is_family_office=True,
            aum_growth_percent=20.0,
            investment_types=["equities", "fixed income"]
        )
        signals = SignalDetector.detect_all_signals(firm)
        assert len(signals) >= 4
        assert "new_private_funds" in signals
        assert "breakaway_group" in signals
        assert "family_office" in signals
        assert "aum_growth" in signals

    def test_single_signal_detection(self):
        """Test detection of single signal"""
        firm = make_firm(
            has_private_funds=True,
            has_breakaway=False,
            is_family_office=False,
            aum_growth_percent=0.0,
            investment_types=["equities"]
        )
        signals = SignalDetector.detect_all_signals(firm)
        assert len(signals) == 1
        assert "new_private_funds" in signals
