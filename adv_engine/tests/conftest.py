"""
Shared test fixtures and helper functions for ADV engine tests
"""

import sys
import os
import pytest
import sqlite3
import tempfile
import json
from datetime import datetime
from contextlib import contextmanager

# Add parent directory to path so we can import adv_engine modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import FirmRecord, Platform, Tier


class TestDatabase:
    """Simple test database implementation"""

    def __init__(self, db_path: str = ":memory:"):
        """Initialize test database"""
        self.db_path = db_path
        self.init_db()

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections"""
        if not hasattr(self, '_conn') or self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        try:
            yield self._conn
        except Exception:
            raise

    def init_db(self):
        """Initialize database schema"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS firms (
                    sec_file_number TEXT PRIMARY KEY,
                    firm_name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    aum INTEGER NOT NULL,
                    num_advisors INTEGER DEFAULT 0,
                    custodian TEXT DEFAULT '',
                    investment_types TEXT DEFAULT '[]',
                    has_private_funds BOOLEAN DEFAULT 0,
                    has_breakaway BOOLEAN DEFAULT 0,
                    is_family_office BOOLEAN DEFAULT 0,
                    aum_growth_percent REAL DEFAULT 0.0,
                    platform TEXT DEFAULT 'Unknown',
                    tier TEXT DEFAULT 'NOT_QUALIFIED',
                    score REAL DEFAULT 0.0,
                    signals TEXT DEFAULT '[]',
                    talking_points TEXT DEFAULT '[]',
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_source TEXT DEFAULT ''
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_name TEXT NOT NULL,
                    firm_id TEXT NOT NULL,
                    firm_name TEXT NOT NULL,
                    detected_date TIMESTAMP NOT NULL,
                    description TEXT NOT NULL,
                    confidence REAL DEFAULT 1.0,
                    FOREIGN KEY (firm_id) REFERENCES firms(sec_file_number)
                )
            ''')
            
            conn.commit()

    def upsert_firm(self, firm: FirmRecord) -> bool:
        """Insert or update a firm record"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                firm.last_updated = datetime.now()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO firms (
                        sec_file_number, firm_name, state, aum, num_advisors,
                        custodian, investment_types, has_private_funds, has_breakaway,
                        is_family_office, aum_growth_percent, platform, tier, score,
                        signals, talking_points, last_updated, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    firm.sec_file_number, firm.firm_name, firm.state, firm.aum,
                    firm.num_advisors, firm.custodian, json.dumps(firm.investment_types),
                    firm.has_private_funds, firm.has_breakaway, firm.is_family_office,
                    firm.aum_growth_percent, firm.platform.value, firm.tier.name,
                    firm.score, json.dumps(firm.signals), json.dumps(firm.talking_points),
                    firm.last_updated, firm.data_source
                ))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error upserting firm: {e}")
            return False

    def get_firm(self, sec_file_number: str):
        """Retrieve a single firm by SEC file number"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM firms WHERE sec_file_number = ?', (sec_file_number,))
                row = cursor.fetchone()
                if not row:
                    return None
                return self._row_to_firm(row)
        except Exception as e:
            print(f"Error retrieving firm: {e}")
            return None

    def get_all_firms(self):
        """Retrieve all firms"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM firms ORDER BY score DESC')
                rows = cursor.fetchall()
                return [self._row_to_firm(row) for row in rows]
        except Exception as e:
            print(f"Error retrieving all firms: {e}")
            return []

    def get_firms_by_tier(self, tier: Tier):
        """Retrieve all firms of a specific tier"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT * FROM firms WHERE tier = ? ORDER BY score DESC',
                    (tier.name,)
                )
                rows = cursor.fetchall()
                return [self._row_to_firm(row) for row in rows]
        except Exception as e:
            print(f"Error retrieving firms by tier: {e}")
            return []

    def get_filtered_leads(self, tier=None, state=None, min_aum=None, max_aum=None, limit=100, offset=0):
        """Get filtered leads with pagination"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                where_clauses = []
                params = []
                
                if tier is not None:
                    where_clauses.append('tier = ?')
                    params.append(f'TIER_{tier}')
                
                if state:
                    where_clauses.append('state = ?')
                    params.append(state)
                
                if min_aum is not None:
                    where_clauses.append('aum >= ?')
                    params.append(min_aum)
                
                if max_aum is not None:
                    where_clauses.append('aum <= ?')
                    params.append(max_aum)
                
                where_clause = ' AND '.join(where_clauses) if where_clauses else '1=1'
                
                cursor.execute(f'SELECT COUNT(*) FROM firms WHERE {where_clause}', params)
                total = cursor.fetchone()[0]
                
                query = f'SELECT * FROM firms WHERE {where_clause} ORDER BY score DESC LIMIT ? OFFSET ?'
                cursor.execute(query, params + [limit, offset])
                rows = cursor.fetchall()
                
                return [self._row_to_firm(row) for row in rows], total
        except Exception as e:
            print(f"Error retrieving filtered leads: {e}")
            return [], 0

    def add_signal(self, signal):
        """Record a detected signal"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO signals (signal_name, firm_id, firm_name, detected_date, description, confidence)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    signal.signal_name, signal.firm_id, signal.firm_name,
                    signal.detected_date, signal.description, signal.confidence
                ))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error adding signal: {e}")
            return False

    def get_recent_signals(self, days=7):
        """Get signals detected in the last N days"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM signals ORDER BY detected_date DESC
                ''')
                rows = cursor.fetchall()
                return [self._row_to_signal(row) for row in rows]
        except Exception as e:
            print(f"Error retrieving signals: {e}")
            return []

    @staticmethod
    def _row_to_firm(row) -> FirmRecord:
        """Convert database row to FirmRecord"""
        return FirmRecord(
            sec_file_number=row['sec_file_number'],
            firm_name=row['firm_name'],
            state=row['state'],
            aum=row['aum'],
            num_advisors=row['num_advisors'],
            custodian=row['custodian'],
            investment_types=json.loads(row['investment_types']),
            has_private_funds=bool(row['has_private_funds']),
            has_breakaway=bool(row['has_breakaway']),
            is_family_office=bool(row['is_family_office']),
            aum_growth_percent=row['aum_growth_percent'],
            platform=next((p for p in Platform if p.value == row['platform']), Platform.UNKNOWN),
            tier=Tier[row['tier']] if row['tier'] in Tier.__members__ else Tier.NOT_QUALIFIED,
            score=row['score'],
            signals=json.loads(row['signals']),
            talking_points=json.loads(row['talking_points']),
            last_updated=datetime.fromisoformat(row['last_updated']) if row['last_updated'] else None,
            data_source=row['data_source']
        )

    @staticmethod
    def _row_to_signal(row):
        """Convert database row to SignalDetection"""
        from models import SignalDetection
        return SignalDetection(
            signal_name=row['signal_name'],
            firm_id=row['firm_id'],
            firm_name=row['firm_name'],
            detected_date=datetime.fromisoformat(row['detected_date']),
            description=row['description'],
            confidence=row['confidence']
        )


@pytest.fixture
def temp_db():
    """Create a temporary in-memory database for testing"""
    db = TestDatabase()
    yield db


def make_firm(
    sec_file_number: str = "12345",
    firm_name: str = "Test Firm LLC",
    state: str = "CA",
    aum: int = 500_000_000,
    num_advisors: int = 10,
    custodian: str = "iCapital",
    investment_types: list = None,
    has_private_funds: bool = False,
    has_breakaway: bool = False,
    is_family_office: bool = False,
    aum_growth_percent: float = 0.0,
    platform: Platform = Platform.ICAPITAL,
    tier: Tier = Tier.NOT_QUALIFIED,
    score: float = 0.0,
    signals: list = None,
    talking_points: list = None,
    last_updated: datetime = None,
    data_source: str = "test"
) -> FirmRecord:
    """
    Factory function to create a FirmRecord with sensible defaults.
    Makes it easy to create test fixtures with minimal boilerplate.
    """
    if investment_types is None:
        investment_types = ["equities", "fixed income"]
    if signals is None:
        signals = []
    if talking_points is None:
        talking_points = []
    if last_updated is None:
        last_updated = datetime.now()
    
    return FirmRecord(
        sec_file_number=sec_file_number,
        firm_name=firm_name,
        state=state,
        aum=aum,
        num_advisors=num_advisors,
        custodian=custodian,
        investment_types=investment_types,
        has_private_funds=has_private_funds,
        has_breakaway=has_breakaway,
        is_family_office=is_family_office,
        aum_growth_percent=aum_growth_percent,
        platform=platform,
        tier=tier,
        score=score,
        signals=signals,
        talking_points=talking_points,
        last_updated=last_updated,
        data_source=data_source
    )


@pytest.fixture
def sample_db(temp_db):
    """Create a sample database with test data"""
    
    # Add some test firms
    firm1 = make_firm(
        sec_file_number="001",
        firm_name="iCapital Advisory",
        state="NY",
        aum=2_000_000_000,
        custodian="iCapital",
        platform=Platform.ICAPITAL
    )
    
    firm2 = make_firm(
        sec_file_number="002",
        firm_name="Pershing Partners",
        state="CA",
        aum=1_500_000_000,
        custodian="Pershing",
        platform=Platform.PERSHING
    )
    
    firm3 = make_firm(
        sec_file_number="003",
        firm_name="No Platform Firm",
        state="TX",
        aum=800_000_000,
        custodian="Unknown",
        platform=Platform.UNKNOWN
    )
    
    firm4 = make_firm(
        sec_file_number="004",
        firm_name="Inaccessible Firm",
        state="FL",
        aum=1_000_000_000,
        custodian="Inaccessible",
        platform=Platform.INACCESSIBLE
    )
    
    temp_db.upsert_firm(firm1)
    temp_db.upsert_firm(firm2)
    temp_db.upsert_firm(firm3)
    temp_db.upsert_firm(firm4)
    
    return temp_db


@pytest.fixture
def sample_leads():
    """Create sample lead records for testing"""
    return [
        make_firm(sec_file_number="L001", firm_name="Lead 1"),
        make_firm(sec_file_number="L002", firm_name="Lead 2"),
        make_firm(sec_file_number="L003", firm_name="Lead 3"),
    ]
