"""
Unit tests for FirmDataParser
Tests custodian extraction, investment type parsing, AUM parsing, etc.
"""

import pytest
import sys
import os

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from parser import FirmDataParser
from models import Platform

# Import from conftest after path is set
from conftest import make_firm


class TestCustodianExtraction:
    """Tests for custodian platform extraction"""

    def test_custodian_extraction_icapital(self):
        """Test iCapital custodian detection"""
        assert FirmDataParser.extract_custodian("iCapital") == Platform.ICAPITAL
        assert FirmDataParser.extract_custodian("icapital") == Platform.ICAPITAL
        assert FirmDataParser.extract_custodian("i capital") == Platform.ICAPITAL
        assert FirmDataParser.extract_custodian("iCapital Inc.") == Platform.ICAPITAL

    def test_custodian_extraction_pershing(self):
        """Test Pershing custodian detection"""
        assert FirmDataParser.extract_custodian("Pershing") == Platform.PERSHING
        assert FirmDataParser.extract_custodian("pershing") == Platform.PERSHING
        assert FirmDataParser.extract_custodian("Pershing NCT") == Platform.PERSHING
        assert FirmDataParser.extract_custodian("pershingNCT") == Platform.PERSHING

    def test_custodian_extraction_schwab(self):
        """Test Schwab custodian detection"""
        assert FirmDataParser.extract_custodian("Schwab") == Platform.SCHWAB
        assert FirmDataParser.extract_custodian("schwab") == Platform.SCHWAB
        assert FirmDataParser.extract_custodian("Charles Schwab") == Platform.SCHWAB
        assert FirmDataParser.extract_custodian("institutional custodial") == Platform.SCHWAB

    def test_inaccessible_custodian_not_detected(self):
        """Test that inaccessible custodians don't match accessible platforms"""
        assert FirmDataParser.extract_custodian("Inaccessible") == Platform.INACCESSIBLE
        assert FirmDataParser.extract_custodian("Not Available") == Platform.INACCESSIBLE
        assert FirmDataParser.extract_custodian("inaccessible custodian") == Platform.INACCESSIBLE

    def test_unknown_custodian(self):
        """Test unknown custodian returns UNKNOWN"""
        assert FirmDataParser.extract_custodian("Random Bank") == Platform.UNKNOWN
        assert FirmDataParser.extract_custodian("") == Platform.UNKNOWN
        assert FirmDataParser.extract_custodian(None) == Platform.UNKNOWN


class TestInvestmentTypeParsing:
    """Tests for investment type parsing"""

    def test_investment_type_parsing(self):
        """Test parsing investment types from strings"""
        types = FirmDataParser.parse_investment_types("equities, fixed income, hedge funds")
        assert "equities" in types
        assert "fixed income" in types
        assert "hedge funds" in types

    def test_investment_type_semicolon_delimiter(self):
        """Test parsing with semicolon delimiter"""
        types = FirmDataParser.parse_investment_types("stocks; bonds; derivatives")
        assert "stocks" in types
        assert "bonds" in types

    def test_investment_type_mixed_delimiters(self):
        """Test parsing with mixed delimiters"""
        types = FirmDataParser.parse_investment_types("equities, fixed income; alternatives")
        assert len(types) >= 3

    def test_investment_type_empty(self):
        """Test parsing empty strings"""
        assert FirmDataParser.parse_investment_types("") == []
        assert FirmDataParser.parse_investment_types(None) == []


class TestAUMParsing:
    """Tests for AUM parsing"""

    def test_aum_parsing_billions(self):
        """Test parsing AUM in billions"""
        assert FirmDataParser.parse_aum("$1.5B") == 1_500_000_000
        assert FirmDataParser.parse_aum("1.5B") == 1_500_000_000
        assert FirmDataParser.parse_aum("2B") == 2_000_000_000

    def test_aum_parsing_millions(self):
        """Test parsing AUM in millions"""
        assert FirmDataParser.parse_aum("$500M") == 500_000_000
        assert FirmDataParser.parse_aum("500M") == 500_000_000
        assert FirmDataParser.parse_aum("1.25M") == 1_250_000

    def test_aum_parsing_thousands(self):
        """Test parsing AUM in thousands"""
        assert FirmDataParser.parse_aum("$100K") == 100_000
        assert FirmDataParser.parse_aum("100K") == 100_000

    def test_aum_parsing_with_commas(self):
        """Test parsing AUM with thousands separators"""
        assert FirmDataParser.parse_aum("$1,500,000,000") == 1_500_000_000
        assert FirmDataParser.parse_aum("1,500,000,000") == 1_500_000_000

    def test_aum_parsing_plain_number(self):
        """Test parsing plain numeric AUM"""
        assert FirmDataParser.parse_aum("1500000000") == 1_500_000_000
        assert FirmDataParser.parse_aum("500000") == 500_000

    def test_aum_parsing_invalid(self):
        """Test parsing invalid AUM returns 0"""
        assert FirmDataParser.parse_aum("invalid") == 0
        assert FirmDataParser.parse_aum("") == 0
        assert FirmDataParser.parse_aum(None) == 0


class TestQPCalculation:
    """Tests for Qualified Person calculation"""

    def test_qp_calculation_from_advisors(self):
        """Test QP calculation from advisor count"""
        firm = make_firm(num_advisors=25)
        qp = FirmDataParser.calculate_qualified_persons(firm)
        assert qp == 25

    def test_qp_calculation_from_aum(self):
        """Test QP calculation from AUM when advisors not provided"""
        firm = make_firm(num_advisors=0, aum=500_000_000)
        qp = FirmDataParser.calculate_qualified_persons(firm)
        assert qp == 10

    def test_qp_calculation_minimum_one(self):
        """Test QP calculation returns at least 1"""
        firm = make_firm(num_advisors=0, aum=10_000_000)
        qp = FirmDataParser.calculate_qualified_persons(firm)
        assert qp >= 1


class TestPrivateFundsDetection:
    """Tests for private funds detection"""

    def test_detect_private_funds(self):
        """Test detection of private funds"""
        assert FirmDataParser.detect_private_funds("manages private funds") == True
        assert FirmDataParser.detect_private_funds("hedge fund strategy") == True
        assert FirmDataParser.detect_private_funds("private equity focus") == True

    def test_detect_private_funds_negative(self):
        """Test non-detection when no private funds"""
        assert FirmDataParser.detect_private_funds("public stocks only") == False
        assert FirmDataParser.detect_private_funds("index funds") == False
        assert FirmDataParser.detect_private_funds("") == False
        assert FirmDataParser.detect_private_funds(None) == False


class TestBreakawayDetection:
    """Tests for breakaway group detection"""

    def test_detect_breakaway(self):
        """Test detection of breakaway groups"""
        assert FirmDataParser.detect_breakaway("breakaway team") == True
        assert FirmDataParser.detect_breakaway("left Morgan Stanley to start firm") == True
        assert FirmDataParser.detect_breakaway("formed team of advisors") == True

    def test_detect_breakaway_negative(self):
        """Test non-detection of non-breakaway firms"""
        assert FirmDataParser.detect_breakaway("established in 1995") == False
        assert FirmDataParser.detect_breakaway("") == False
        assert FirmDataParser.detect_breakaway(None) == False


class TestFamilyOfficeDetection:
    """Tests for family office detection"""

    def test_detect_family_office(self):
        """Test detection of family offices"""
        assert FirmDataParser.detect_family_office("multi-family office") == True
        assert FirmDataParser.detect_family_office("family office services") == True
        assert FirmDataParser.detect_family_office("single family office") == True
        assert FirmDataParser.detect_family_office("MFO provider") == True

    def test_detect_family_office_negative(self):
        """Test non-detection of non-family offices"""
        assert FirmDataParser.detect_family_office("traditional RIA") == False
        assert FirmDataParser.detect_family_office("") == False
        assert FirmDataParser.detect_family_office(None) == False


class TestAUMGrowthParsing:
    """Tests for AUM growth percentage parsing"""

    def test_aum_growth_parsing(self):
        """Test parsing AUM growth percentages"""
        assert FirmDataParser.parse_aum_growth_percent("15.5%") == 15.5
        assert FirmDataParser.parse_aum_growth_percent("25") == 25.0
        assert FirmDataParser.parse_aum_growth_percent("-5.2%") == -5.2

    def test_aum_growth_parsing_invalid(self):
        """Test parsing invalid growth returns 0"""
        assert FirmDataParser.parse_aum_growth_percent("invalid") == 0.0
        assert FirmDataParser.parse_aum_growth_percent("") == 0.0
        assert FirmDataParser.parse_aum_growth_percent(None) == 0.0
