"""
Data models for ADV Buying Signal Engine
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum


class Tier(Enum):
    """Firm scoring tier"""
    TIER_1 = 1
    TIER_2 = 2
    TIER_3 = 3
    NOT_QUALIFIED = 4


class Platform(Enum):
    """Custodian platform types"""
    ICAPITAL = "iCapital"
    PERSHING = "Pershing"
    SCHWAB = "Schwab"
    UNKNOWN = "Unknown"
    INACCESSIBLE = "Inaccessible"


@dataclass
class FirmRecord:
    """Complete firm record for analysis"""
    sec_file_number: str
    firm_name: str
    state: str
    aum: int  # in dollars
    num_advisors: int = 0
    custodian: str = ""
    investment_types: List[str] = field(default_factory=list)
    has_private_funds: bool = False
    has_breakaway: bool = False
    is_family_office: bool = False
    aum_growth_percent: float = 0.0
    platform: Platform = Platform.UNKNOWN
    tier: Tier = Tier.NOT_QUALIFIED
    score: float = 0.0
    signals: List[str] = field(default_factory=list)
    talking_points: List[str] = field(default_factory=list)
    last_updated: Optional[datetime] = None
    data_source: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        d = asdict(self)
        d['platform'] = self.platform.value
        d['tier'] = self.tier.name
        d['last_updated'] = self.last_updated.isoformat() if self.last_updated else None
        return d

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'FirmRecord':
        """Create from dictionary"""
        d = data.copy()
        if isinstance(d.get('platform'), str):
            try:
                d['platform'] = Platform[d['platform']]
            except (KeyError, ValueError):
                d['platform'] = Platform.UNKNOWN
        if isinstance(d.get('tier'), str):
            try:
                d['tier'] = Tier[d['tier']]
            except (KeyError, ValueError):
                d['tier'] = Tier.NOT_QUALIFIED
        if d.get('last_updated') and isinstance(d['last_updated'], str):
            d['last_updated'] = datetime.fromisoformat(d['last_updated'])
        return FirmRecord(**d)


@dataclass
class DailyBrief:
    """Daily summary brief"""
    date: datetime
    tier1_leads: List[FirmRecord] = field(default_factory=list)
    tier2_leads: List[FirmRecord] = field(default_factory=list)
    new_signals: List[FirmRecord] = field(default_factory=list)
    summary_stats: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'date': self.date.isoformat(),
            'tier1_leads': [lead.to_dict() for lead in self.tier1_leads],
            'tier2_leads': [lead.to_dict() for lead in self.tier2_leads],
            'new_signals': [lead.to_dict() for lead in self.new_signals],
            'summary_stats': self.summary_stats,
        }


@dataclass
class SignalDetection:
    """Signal detection result"""
    signal_name: str
    firm_id: str
    firm_name: str
    detected_date: datetime
    description: str
    confidence: float  # 0.0 to 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'signal_name': self.signal_name,
            'firm_id': self.firm_id,
            'firm_name': self.firm_name,
            'detected_date': self.detected_date.isoformat(),
            'description': self.description,
            'confidence': self.confidence,
        }
