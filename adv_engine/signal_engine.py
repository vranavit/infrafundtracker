"""
Signal detection module for ADV Buying Signal Engine
Detects various signals indicating investment advisory firm buying interest
"""

import logging
from datetime import datetime
from typing import List

from models import FirmRecord, SignalDetection

logger = logging.getLogger(__name__)


class SignalDetector:
    """Detects buying signals in firm data"""

    @staticmethod
    def detect_new_private_funds(firm: FirmRecord) -> bool:
        """Check if firm recently launched private funds"""
        return firm.has_private_funds

    @staticmethod
    def detect_aum_growth(firm: FirmRecord, threshold: float = 15.0) -> bool:
        """Check if firm has significant AUM growth"""
        return firm.aum_growth_percent >= threshold

    @staticmethod
    def detect_breakaway(firm: FirmRecord) -> bool:
        """Check if firm is a breakaway group"""
        return firm.has_breakaway

    @staticmethod
    def detect_family_office(firm: FirmRecord) -> bool:
        """Check if firm is family office or multi-family office"""
        return firm.is_family_office

    @staticmethod
    def detect_platform_expansion_opportunity(firm: FirmRecord) -> bool:
        """Check if firm has multiple investment types (platform expansion signal)"""
        return len(firm.investment_types) >= 2

    @staticmethod
    def detect_all_signals(firm: FirmRecord) -> List[str]:
        """Detect all applicable signals for a firm"""
        signals = []
        
        if SignalDetector.detect_new_private_funds(firm):
            signals.append("new_private_funds")
        
        if SignalDetector.detect_aum_growth(firm):
            signals.append("aum_growth")
        
        if SignalDetector.detect_breakaway(firm):
            signals.append("breakaway_group")
        
        if SignalDetector.detect_family_office(firm):
            signals.append("family_office")
        
        if SignalDetector.detect_platform_expansion_opportunity(firm):
            signals.append("platform_expansion")
        
        return signals
