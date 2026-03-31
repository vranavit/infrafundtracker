"""
Integration tests for the ADV Buying Signal Engine
Tests end-to-end workflows and data consistency
"""

import sys
import os
import json
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pytest
from models import FirmRecord, Tier, Platform, SignalDetection
from database import ADVDatabase
from scorer import FirmScorer
from signal_engine import SignalDetector
from parser import FirmDataParser
from conftest import make_firm, temp_db


class TestFullPipeline:
    """Tests for full pipeline end-to-end"""

    def test_full_pipeline_with_sample_data(self, temp_db):
        """Test full pipeline with synthetic data"""
        
        # Create synthetic firms
        firms = [
            make_firm(
                sec_file_number="001",
                firm_name="iCapital Leader",
                state="NY",
                aum=3_000_000_000,
                num_advisors=30,
                custodian="iCapital",
                platform=Platform.ICAPITAL,
                has_private_funds=True,
                has_breakaway=True,
                aum_growth_percent=20.0,
                investment_types=["equities", "fixed income", "alternatives"]
            ),
            make_firm(
                sec_file_number="002",
                firm_name="Pershing Partner",
                state="CA",
                aum=1_500_000_000,
                num_advisors=20,
                custodian="Pershing",
                platform=Platform.PERSHING,
                has_private_funds=True,
                aum_growth_percent=12.0,
                investment_types=["equities", "fixed income"]
            ),
            make_firm(
                sec_file_number="003",
                firm_name="Schwab Client",
                state="TX",
                aum=800_000_000,
                num_advisors=15,
                custodian="Schwab",
                platform=Platform.SCHWAB,
                has_private_funds=False,
                investment_types=["equities"]
            ),
        ]
        
        # Score and tier each firm
        for firm in firms:
            FirmScorer.score_and_tier(firm)
            temp_db.upsert_firm(firm)
        
        # Verify Tier 1 leads have platform access
        tier1 = temp_db.get_firms_by_tier(Tier.TIER_1)
        for firm in tier1:
            assert firm.platform != Platform.UNKNOWN
            assert firm.platform != Platform.INACCESSIBLE
        
        # Verify all firms stored
        all_firms = temp_db.get_all_firms()
        assert len(all_firms) >= 3

    def test_idempotency(self, temp_db):
        """Test that running pipeline twice produces same results"""
        
        firm = make_firm(
            sec_file_number="TEST001",
            firm_name="Test Firm",
            platform=Platform.ICAPITAL,
            has_private_funds=True
        )
        
        FirmScorer.score_and_tier(firm)
        score1 = firm.score
        tier1 = firm.tier
        
        # Store and retrieve
        temp_db.upsert_firm(firm)
        retrieved = temp_db.get_firm("TEST001")
        
        # Score again
        FirmScorer.score_and_tier(retrieved)
        
        assert retrieved.score == score1
        assert retrieved.tier == tier1

    def test_api_endpoints_return_valid_json(self, temp_db):
        """Test that mock API responses are valid JSON"""
        
        firm = make_firm(sec_file_number="API001", firm_name="API Test Firm")
        FirmScorer.score_and_tier(firm)
        temp_db.upsert_firm(firm)
        
        # Test firm dict conversion
        firm_dict = firm.to_dict()
        
        # Verify it's JSON serializable
        json_str = json.dumps(firm_dict)
        assert json_str is not None
        
        # Verify it can be deserialized
        deserialized = json.loads(json_str)
        assert deserialized['sec_file_number'] == "API001"

    def test_no_tier1_without_platform_access(self, temp_db):
        """Test that Tier 1 requires platform access"""
        
        # Create high-scoring firm WITHOUT platform access
        firm = make_firm(
            sec_file_number="NOPLATFORM",
            firm_name="No Platform Firm",
            platform=Platform.UNKNOWN,
            aum=5_000_000_000,
            has_private_funds=True,
            has_breakaway=True,
            is_family_office=True,
            aum_growth_percent=30.0
        )
        
        FirmScorer.score_and_tier(firm)
        temp_db.upsert_firm(firm)
        
        retrieved = temp_db.get_firm("NOPLATFORM")
        
        # Should NOT be Tier 1
        assert retrieved.tier != Tier.TIER_1

    def test_talking_points_are_personalized(self):
        """Test that talking points reflect firm characteristics"""
        
        firm1 = make_firm(
            firm_name="Private Fund Specialist",
            has_private_funds=True
        )
        
        firm2 = make_firm(
            firm_name="Family Office Provider",
            is_family_office=True
        )
        
        # In production, generate talking points based on signals
        signals1 = SignalDetector.detect_all_signals(firm1)
        signals2 = SignalDetector.detect_all_signals(firm2)
        
        assert signals1 != signals2


class TestDataConsistency:
    """Tests for data consistency and integrity"""

    def test_firm_roundtrip(self, temp_db):
        """Test firm data survives database roundtrip"""
        
        original = make_firm(
            sec_file_number="ROUND001",
            firm_name="Roundtrip Test",
            state="CA",
            aum=1_234_567_890,
            custodian="iCapital",
            platform=Platform.ICAPITAL,
            investment_types=["equities", "fixed income", "alternatives"]
        )
        
        temp_db.upsert_firm(original)
        retrieved = temp_db.get_firm("ROUND001")
        
        assert retrieved.firm_name == original.firm_name
        assert retrieved.aum == original.aum
        assert retrieved.state == original.state
        assert retrieved.investment_types == original.investment_types
        assert retrieved.platform == original.platform

    def test_signal_recording(self, temp_db):
        """Test that signals are properly recorded"""
        
        signal = SignalDetection(
            signal_name="test_signal",
            firm_id="SIG001",
            firm_name="Test Firm",
            detected_date=datetime.now(),
            description="Test signal description",
            confidence=0.95
        )
        
        temp_db.add_signal(signal)
        signals = temp_db.get_recent_signals(days=1)
        
        assert len(signals) > 0
        assert signals[0].signal_name == "test_signal"
        assert signals[0].firm_id == "SIG001"

    def test_filtering_accuracy(self, temp_db):
        """Test that filtering returns correct results"""
        
        # Create firms with different characteristics
        firms = [
            make_firm(sec_file_number="F1", state="CA", aum=1_000_000_000),
            make_firm(sec_file_number="F2", state="CA", aum=500_000_000),
            make_firm(sec_file_number="F3", state="NY", aum=2_000_000_000),
        ]
        
        for firm in firms:
            FirmScorer.score_and_tier(firm)
            temp_db.upsert_firm(firm)
        
        # Filter by state
        ca_leads, ca_count = temp_db.get_filtered_leads(state="CA")
        assert ca_count == 2
        assert all(lead.state == "CA" for lead in ca_leads)
        
        # Filter by AUM
        high_aum, high_count = temp_db.get_filtered_leads(min_aum=1_000_000_000)
        assert high_count >= 2


class TestScenarios:
    """Real-world scenario tests"""

    def test_breakaway_group_scenario(self, temp_db):
        """Test scoring of a breakaway group"""
        
        breakaway = make_firm(
            firm_name="Breakaway Advisors",
            platform=Platform.PERSHING,
            has_breakaway=True,
            aum=600_000_000,
            num_advisors=8,
            aum_growth_percent=18.0
        )
        
        FirmScorer.score_and_tier(breakaway)
        
        assert "breakaway_group" in breakaway.signals
        assert breakaway.tier in [Tier.TIER_1, Tier.TIER_2]

    def test_family_office_scenario(self, temp_db):
        """Test scoring of a family office"""
        
        family_office = make_firm(
            firm_name="Multi-Family Office",
            platform=Platform.ICAPITAL,
            is_family_office=True,
            aum=4_000_000_000,
            num_advisors=40,
            aum_growth_percent=25.0,
            has_private_funds=True
        )
        
        FirmScorer.score_and_tier(family_office)
        
        assert "family_office" in family_office.signals
        assert family_office.tier == Tier.TIER_1

    def test_high_growth_firm_scenario(self, temp_db):
        """Test scoring of rapidly growing firm"""
        
        growth_firm = make_firm(
            firm_name="High Growth Advisory",
            platform=Platform.ICAPITAL,
            aum=750_000_000,
            aum_growth_percent=35.0,
            has_private_funds=True,
            investment_types=["equities", "fixed income", "alternatives"]
        )
        
        FirmScorer.score_and_tier(growth_firm)
        
        assert "aum_growth" in growth_firm.signals
        assert growth_firm.score >= 70.0
