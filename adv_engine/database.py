"""
Database module for the ADV Buying Signal Engine.

Manages SQLite database operations for firms, snapshots, leads, platform detections,
run logs, and signal history. All operations use parameterized queries for security.
"""

import sqlite3
import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

from config import DATABASE_PATH, DATABASE_TIMEOUT

logger = logging.getLogger(__name__)


class ADVDatabase:
    """SQLite database manager for the ADV Buying Signal Engine."""

    def __init__(self, db_path: str = DATABASE_PATH):
        """Initialize database connection."""
        self.db_path = db_path
        self._ensure_db_path()

    def _ensure_db_path(self) -> None:
        """Ensure database directory exists."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections with proper cleanup."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=DATABASE_TIMEOUT)
            conn.row_factory = sqlite3.Row
            yield conn
            conn.commit()
        except sqlite3.Error as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def init_db(self) -> None:
        """Initialize database schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS firms (
                    firm_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sec_id TEXT UNIQUE NOT NULL,
                    crd_id TEXT,
                    firm_name TEXT NOT NULL,
                    hq_state TEXT,
                    hq_country TEXT,
                    website TEXT,
                    primary_email TEXT,
                    phone TEXT,
                    registration_date TEXT,
                    regulatory_status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS adv_snapshots (
                    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    firm_id INTEGER NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    total_aum REAL,
                    discretionary_aum REAL,
                    non_discretionary_aum REAL,
                    number_of_clients INTEGER,
                    avg_aum_per_client REAL,
                    aum_growth_pct REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (firm_id) REFERENCES firms(firm_id) ON DELETE CASCADE,
                    UNIQUE(firm_id, snapshot_date)
                )
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_leads (
                    lead_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    firm_id INTEGER NOT NULL,
                    lead_date TEXT NOT NULL,
                    signal_score INTEGER,
                    qp_score REAL,
                    tier INTEGER,
                    primary_signal TEXT,
                    secondary_signals TEXT,
                    platform_recommendations TEXT,
                    contacted BOOLEAN DEFAULT 0,
                    contacted_date TEXT,
                    outcome TEXT,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (firm_id) REFERENCES firms(firm_id) ON DELETE CASCADE
                )
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS platform_detections (
                    detection_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    firm_id INTEGER NOT NULL,
                    detection_date TEXT NOT NULL,
                    custodian_name TEXT,
                    platform_name TEXT,
                    confidence_score REAL,
                    evidence_text TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (firm_id) REFERENCES firms(firm_id) ON DELETE CASCADE
                )
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS run_log (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_date TEXT NOT NULL,
                    run_type TEXT NOT NULL,
                    records_processed INTEGER,
                    records_created INTEGER,
                    records_updated INTEGER,
                    errors_count INTEGER,
                    status TEXT,
                    error_messages TEXT,
                    duration_seconds REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS signal_history (
                    signal_history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    firm_id INTEGER NOT NULL,
                    signal_name TEXT NOT NULL,
                    signal_date TEXT NOT NULL,
                    signal_score INTEGER,
                    evidence_text TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (firm_id) REFERENCES firms(firm_id) ON DELETE CASCADE
                )
            """
            )

            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_firms_sec_id ON firms(sec_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_adv_snapshots_firm_date ON adv_snapshots(firm_id, snapshot_date)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_daily_leads_firm_date ON daily_leads(firm_id, lead_date)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_daily_leads_tier ON daily_leads(tier, lead_date)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_platform_detections_firm ON platform_detections(firm_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_signal_history_firm ON signal_history(firm_id)"
            )

            logger.info("Database initialized successfully")

    def insert_firm(
        self,
        sec_id: str,
        firm_name: str,
        crd_id: Optional[str] = None,
        hq_state: Optional[str] = None,
        hq_country: Optional[str] = None,
        website: Optional[str] = None,
        primary_email: Optional[str] = None,
        phone: Optional[str] = None,
        registration_date: Optional[str] = None,
        regulatory_status: Optional[str] = None,
    ) -> int:
        """Insert a new firm or return existing firm_id."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT firm_id FROM firms WHERE sec_id = ?", (sec_id,))
            result = cursor.fetchone()
            if result:
                return result[0]

            cursor.execute(
                """
                INSERT INTO firms (
                    sec_id, crd_id, firm_name, hq_state, hq_country,
                    website, primary_email, phone, registration_date, regulatory_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    sec_id,
                    crd_id,
                    firm_name,
                    hq_state,
                    hq_country,
                    website,
                    primary_email,
                    phone,
                    registration_date,
                    regulatory_status,
                ),
            )

            return cursor.lastrowid

    def insert_snapshot(
        self,
        firm_id: int,
        snapshot_date: str,
        total_aum: Optional[float] = None,
        discretionary_aum: Optional[float] = None,
        non_discretionary_aum: Optional[float] = None,
        number_of_clients: Optional[int] = None,
        avg_aum_per_client: Optional[float] = None,
        aum_growth_pct: Optional[float] = None,
    ) -> int:
        """Insert or update AUM snapshot."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                "SELECT snapshot_id FROM adv_snapshots WHERE firm_id = ? AND snapshot_date = ?",
                (firm_id, snapshot_date),
            )
            existing = cursor.fetchone()

            if existing:
                cursor.execute(
                    """
                    UPDATE adv_snapshots SET
                        total_aum = ?, discretionary_aum = ?, non_discretionary_aum = ?,
                        number_of_clients = ?, avg_aum_per_client = ?, aum_growth_pct = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE snapshot_id = ?
                """,
                    (
                        total_aum,
                        discretionary_aum,
                        non_discretionary_aum,
                        number_of_clients,
                        avg_aum_per_client,
                        aum_growth_pct,
                        existing[0],
                    ),
                )
                return existing[0]

            cursor.execute(
                """
                INSERT INTO adv_snapshots (
                    firm_id, snapshot_date, total_aum, discretionary_aum,
                    non_discretionary_aum, number_of_clients, avg_aum_per_client, aum_growth_pct
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    firm_id,
                    snapshot_date,
                    total_aum,
                    discretionary_aum,
                    non_discretionary_aum,
                    number_of_clients,
                    avg_aum_per_client,
                    aum_growth_pct,
                ),
            )

            return cursor.lastrowid

    def insert_lead(
        self,
        firm_id: int,
        lead_date: str,
        signal_score: int,
        qp_score: float,
        tier: int,
        primary_signal: str,
        secondary_signals: Optional[str] = None,
        platform_recommendations: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> int:
        """Insert a new lead."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO daily_leads (
                    firm_id, lead_date, signal_score, qp_score, tier,
                    primary_signal, secondary_signals, platform_recommendations, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    firm_id,
                    lead_date,
                    signal_score,
                    qp_score,
                    tier,
                    primary_signal,
                    secondary_signals,
                    platform_recommendations,
                    notes,
                ),
            )

            return cursor.lastrowid

    def get_leads_by_tier(
        self, tier: int, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get leads for a specific tier, sorted by signal score."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT
                    dl.lead_id, dl.firm_id, f.firm_name, f.sec_id,
                    dl.lead_date, dl.signal_score, dl.qp_score, dl.tier,
                    dl.primary_signal, dl.secondary_signals, dl.platform_recommendations,
                    dl.contacted, dl.outcome, dl.notes
                FROM daily_leads dl
                JOIN firms f ON dl.firm_id = f.firm_id
                WHERE dl.tier = ?
                ORDER BY dl.signal_score DESC
                LIMIT ? OFFSET ?
            """,
                (tier, limit, offset),
            )

            return [dict(row) for row in cursor.fetchall()]

    def get_daily_brief(self, brief_date: str) -> Dict[str, Any]:
        """Get summary of leads generated on a specific date."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT
                    tier,
                    COUNT(*) as count,
                    AVG(signal_score) as avg_score,
                    MAX(signal_score) as max_score
                FROM daily_leads
                WHERE lead_date = ?
                GROUP BY tier
                ORDER BY tier
            """,
                (brief_date,),
            )

            tier_summary = {row["tier"]: dict(row) for row in cursor.fetchall()}

            cursor.execute(
                """
                SELECT
                    COUNT(DISTINCT firm_id) as total_firms,
                    COUNT(*) as total_leads,
                    AVG(signal_score) as avg_score
                FROM daily_leads
                WHERE lead_date = ?
            """,
                (brief_date,),
            )

            overall = dict(cursor.fetchone() or {})

            return {"date": brief_date, "overall": overall, "by_tier": tier_summary}

    def get_platform_summary(self, date_str: Optional[str] = None) -> Dict[str, Any]:
        """Get summary of detected platforms and their prevalence."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            if not date_str:
                date_str = datetime.now().strftime("%Y-%m-%d")

            cursor.execute(
                """
                SELECT
                    platform_name,
                    COUNT(*) as firm_count,
                    AVG(confidence_score) as avg_confidence
                FROM platform_detections
                WHERE detection_date = ? AND is_active = 1
                GROUP BY platform_name
                ORDER BY firm_count DESC
            """,
                (date_str,),
            )

            return {
                "date": date_str,
                "platforms": [dict(row) for row in cursor.fetchall()],
            }

    def get_geography_heat(self, brief_date: str) -> Dict[str, Any]:
        """Get geographic distribution of leads by state."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT
                    f.hq_state,
                    COUNT(dl.lead_id) as lead_count,
                    COUNT(DISTINCT dl.firm_id) as firm_count,
                    AVG(dl.signal_score) as avg_score
                FROM daily_leads dl
                JOIN firms f ON dl.firm_id = f.firm_id
                WHERE dl.lead_date = ?
                GROUP BY f.hq_state
                ORDER BY lead_count DESC
            """,
                (brief_date,),
            )

            return {
                "date": brief_date,
                "by_state": [dict(row) for row in cursor.fetchall()],
            }

    def get_stats(self) -> Dict[str, Any]:
        """Get overall database statistics."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) as count FROM firms")
            total_firms = cursor.fetchone()["count"]

            cursor.execute("SELECT COUNT(DISTINCT firm_id) as count FROM adv_snapshots")
            firms_with_snapshots = cursor.fetchone()["count"]

            cursor.execute("SELECT COUNT(*) as count FROM daily_leads")
            total_leads = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT tier, COUNT(*) as count
                FROM daily_leads
                GROUP BY tier
                ORDER BY tier
            """
            )
            leads_by_tier = {row["tier"]: row["count"] for row in cursor.fetchall()}

            cursor.execute(
                "SELECT COUNT(DISTINCT signal_name) as count FROM signal_history"
            )
            unique_signals = cursor.fetchone()["count"]

            return {
                "total_firms": total_firms,
                "firms_with_snapshots": firms_with_snapshots,
                "total_leads": total_leads,
                "leads_by_tier": leads_by_tier,
                "unique_signals_detected": unique_signals,
            }

    def get_lead_detail(self, lead_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific lead."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT
                    dl.lead_id, dl.firm_id, f.firm_name, f.sec_id, f.hq_state,
                    f.website, f.primary_email, dl.lead_date, dl.signal_score,
                    dl.qp_score, dl.tier, dl.primary_signal, dl.secondary_signals,
                    dl.platform_recommendations, dl.contacted, dl.contacted_date,
                    dl.outcome, dl.notes
                FROM daily_leads dl
                JOIN firms f ON dl.firm_id = f.firm_id
                WHERE dl.lead_id = ?
            """,
                (lead_id,),
            )

            row = cursor.fetchone()
            if not row:
                return None

            result = dict(row)

            cursor.execute(
                """
                SELECT * FROM adv_snapshots
                WHERE firm_id = ?
                ORDER BY snapshot_date DESC
                LIMIT 1
            """,
                (result["firm_id"],),
            )
            latest_snapshot = cursor.fetchone()
            result["latest_snapshot"] = dict(latest_snapshot) if latest_snapshot else None

            cursor.execute(
                """
                SELECT platform_name, custodian_name, confidence_score
                FROM platform_detections
                WHERE firm_id = ? AND is_active = 1
                ORDER BY confidence_score DESC
            """,
                (result["firm_id"],),
            )
            result["platforms"] = [dict(row) for row in cursor.fetchall()]

            cursor.execute(
                """
                SELECT signal_name, signal_score, signal_date, evidence_text
                FROM signal_history
                WHERE firm_id = ?
                ORDER BY signal_date DESC
                LIMIT 10
            """,
                (result["firm_id"],),
            )
            result["recent_signals"] = [dict(row) for row in cursor.fetchall()]

            return result

    def insert_run_log(
        self,
        run_type: str,
        records_processed: int,
        records_created: int,
        records_updated: int,
        errors_count: int = 0,
        status: str = "success",
        error_messages: Optional[str] = None,
        duration_seconds: Optional[float] = None,
    ) -> int:
        """Log a data collection/processing run."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO run_log (
                    run_date, run_type, records_processed, records_created,
                    records_updated, errors_count, status, error_messages, duration_seconds
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    run_type,
                    records_processed,
                    records_created,
                    records_updated,
                    errors_count,
                    status,
                    error_messages,
                    duration_seconds,
                ),
            )

            return cursor.lastrowid

    def insert_signal(
        self,
        firm_id: int,
        signal_name: str,
        signal_score: int,
        evidence_text: Optional[str] = None,
        signal_date: Optional[str] = None,
    ) -> int:
        """Record a detected signal."""
        if not signal_date:
            signal_date = datetime.now().strftime("%Y-%m-%d")

        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO signal_history (
                    firm_id, signal_name, signal_date, signal_score, evidence_text
                ) VALUES (?, ?, ?, ?, ?)
            """,
                (firm_id, signal_name, signal_date, signal_score, evidence_text),
            )

            return cursor.lastrowid

    def insert_platform_detection(
        self,
        firm_id: int,
        platform_name: str,
        custodian_name: Optional[str] = None,
        confidence_score: float = 0.5,
        evidence_text: Optional[str] = None,
        detection_date: Optional[str] = None,
    ) -> int:
        """Record a detected platform or custodian."""
        if not detection_date:
            detection_date = datetime.now().strftime("%Y-%m-%d")

        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO platform_detections (
                    firm_id, platform_name, custodian_name, confidence_score,
                    evidence_text, detection_date
                ) VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    firm_id,
                    platform_name,
                    custodian_name,
                    confidence_score,
                    evidence_text,
                    detection_date,
                ),
            )

            return cursor.lastrowid


_default_db = None


def get_db() -> ADVDatabase:
    """Get default database instance."""
    global _default_db
    if _default_db is None:
        _default_db = ADVDatabase()
    return _default_db
