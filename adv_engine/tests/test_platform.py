"""
Unit tests for platform detection and mapping
"""

import sys
import os
import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models import Platform
from parser import FirmDataParser
from scorer import FirmScorer
from conftest import make_firm


class TestPlatformDetection:
    """Tests for platform custodian detection"""

    def test_icapital_detection(self):
        """Test detection of iCapital platform"""
        assert FirmDataParser.extract_custodian("iCapital") == Platform.ICAPITAL
        assert FirmDataParser.extract_custodian("iCapital Inc") == Platform.ICAPITAL
        assert FirmDataParser.extract_custodian("i capital") == Platform.ICAPITAL

    def test_pershing_detection(self):
        """Test detection of Pershing platform"""
        assert FirmDataParser.extract_custodian("Pershing") == Platform.PERSHING
        assert FirmDataParser.extract_custodian("pershing") == Platform.PERSHING
        assert FirmDataParser.extract_custodian("Pershing NCT") == Platform.PERSHING

    def test_schwab_detection(self):
        """Test detection of Schwab platform"""
        assert FirmDataParser.extract_custodian("Schwab") == Platform.SCHWAB
        assert FirmDataParser.extract_custodian("Charles Schwab") == Platform.SCHWAB
        assert FirmDataParser.extract_custodian("schwab") == Platform.SCHWAB

    def test_inaccessible_custodian_blocked(self):
        """Test that inaccessible platforms are properly blocked"""
        result = FirmDataParser.extract_custodian("Inaccessible")
        assert result == Platform.INACCESSIBLE
        
        result = FirmDataParser.extract_custodian("Not Available")
        assert result == Platform.INACCESSIBLE

    def test_no_platform_returns_zero_score(self):
        """Test that no platform capability limits scoring"""
        firm = make_firm(
            platform=Platform.UNKNOWN,
            aum=5_000_000_000,
            has_private_funds=True,
            has_breakaway=True
        )
        
        score = FirmScorer.calculate_score(firm)
        
        # Even with high AUM and signals, no platform limits score
        assert score < 70.0


class TestPlatformPriority:
    """Tests for platform priority and exclusivity"""

    def test_platform_exclusivity(self):
        """Test that platform detection is exclusive"""
        platforms = [
            (Platform.ICAPITAL, "iCapital"),
            (Platform.PERSHING, "Pershing"),
            (Platform.SCHWAB, "Schwab"),
        ]
        
        for platform, name in platforms:
            detected = FirmDataParser.extract_custodian(name)
            assert detected == platform

    def test_unknown_platform(self):
        """Test unknown custodian returns UNKNOWN"""
        assert FirmDataParser.extract_custodian("Random Custodian") == Platform.UNKNOWN
        assert FirmDataParser.extract_custodian("Custom Bank") == Platform.UNKNOWN

    def test_empty_custodian_string(self):
        """Test empty custodian returns UNKNOWN"""
        assert FirmDataParser.extract_custodian("") == Platform.UNKNOWN
        assert FirmDataParser.extract_custodian(None) == Platform.UNKNOWN
        assert FirmDataParser.extract_custodian("   ") == Platform.UNKNOWN
