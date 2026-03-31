"""
Parser module for extracting and normalizing firm data
"""

import re
import logging
from typing import Optional, List
from models import FirmRecord, Platform

logger = logging.getLogger(__name__)


class FirmDataParser:
    """Parses and normalizes firm data from various sources"""

    # Custodian detection patterns
    ICAPITAL_PATTERNS = [
        r'icapital', r'i\s*capital', r'iCapital'
    ]
    
    PERSHING_PATTERNS = [
        r'pershing', r'pershingnct'
    ]
    
    SCHWAB_PATTERNS = [
        r'schwab', r'charles\s*schwab', r'institutional\s*custodial'
    ]

    @staticmethod
    def extract_custodian(custodian_str: Optional[str]) -> Platform:
        """Extract custodian platform from string"""
        if not custodian_str:
            return Platform.UNKNOWN
        
        custodian_lower = custodian_str.lower().strip()
        
        # Check for inaccessible custodian
        if 'inaccessible' in custodian_lower or 'not available' in custodian_lower:
            return Platform.INACCESSIBLE
        
        # Check iCapital
        for pattern in FirmDataParser.ICAPITAL_PATTERNS:
            if re.search(pattern, custodian_lower, re.IGNORECASE):
                return Platform.ICAPITAL
        
        # Check Pershing
        for pattern in FirmDataParser.PERSHING_PATTERNS:
            if re.search(pattern, custodian_lower, re.IGNORECASE):
                return Platform.PERSHING
        
        # Check Schwab
        for pattern in FirmDataParser.SCHWAB_PATTERNS:
            if re.search(pattern, custodian_lower, re.IGNORECASE):
                return Platform.SCHWAB
        
        return Platform.UNKNOWN

    @staticmethod
    def parse_aum(aum_str: Optional[str]) -> int:
        """Parse AUM from various formats"""
        if not aum_str:
            return 0
        
        # Remove common formatting
        aum_clean = aum_str.strip().upper()
        
        # Handle various formats: "$1.5B", "1500000000", "$1,500,000,000", etc.
        # Remove dollar signs and commas
        aum_clean = aum_clean.replace('$', '').replace(',', '').strip()
        
        # Extract number part
        match = re.match(r'([\d.]+)\s*([KMB])', aum_clean)
        if match:
            number = float(match.group(1))
            unit = match.group(2)
            
            if unit == 'K':
                return int(number * 1_000)
            elif unit == 'M':
                return int(number * 1_000_000)
            elif unit == 'B':
                return int(number * 1_000_000_000)
        
        # Try parsing as plain number
        try:
            return int(float(aum_clean))
        except (ValueError, AttributeError):
            return 0

    @staticmethod
    def parse_investment_types(types_str: Optional[str]) -> List[str]:
        """Parse investment types from string"""
        if not types_str:
            return []
        
        # Split by comma or semicolon
        types = re.split(r'[,;]', str(types_str))
        return [t.strip().lower() for t in types if t.strip()]

    @staticmethod
    def detect_private_funds(text: Optional[str]) -> bool:
        """Detect if firm has private funds"""
        if not text:
            return False
        
        text_lower = str(text).lower()
        patterns = [
            r'private\s+fund', r'hedge\s+fund', r'private\s+equity',
            r'vc\s+fund', r'venture\s+capital'
        ]
        
        return any(re.search(p, text_lower) for p in patterns)

    @staticmethod
    def detect_breakaway(text: Optional[str]) -> bool:
        """Detect if firm is a breakaway group"""
        if not text:
            return False
        
        text_lower = str(text).lower()
        patterns = [
            r'breakaway', r'break\s*away', r'left\s+.+?\s+to\s',
            r'formed\s+team'
        ]
        
        return any(re.search(p, text_lower) for p in patterns)

    @staticmethod
    def detect_family_office(text: Optional[str]) -> bool:
        """Detect if firm is family office"""
        if not text:
            return False
        
        text_lower = str(text).lower()
        patterns = [
            r'family\s+office', r'multi.family\s+office', r'mfo',
            r'single\s+family\s+office'
        ]
        
        return any(re.search(p, text_lower) for p in patterns)

    @staticmethod
    def parse_aum_growth_percent(growth_str: Optional[str]) -> float:
        """Parse AUM growth percentage"""
        if not growth_str:
            return 0.0
        
        growth_clean = str(growth_str).strip().replace('%', '').strip()
        
        try:
            return float(growth_clean)
        except (ValueError, AttributeError):
            return 0.0

    @staticmethod
    def calculate_qualified_persons(firm: FirmRecord) -> int:
        """Calculate number of qualified persons (simplified)"""
        # In real implementation, would parse from detailed records
        # For now, estimate based on AUM and advisor count
        if firm.num_advisors > 0:
            return firm.num_advisors
        
        # Rough estimate: 1 QP per $50M AUM
        return max(1, firm.aum // 50_000_000)
