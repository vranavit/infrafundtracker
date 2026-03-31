"""
Backfill - Historical Baseline Builder

Downloads and processes historical firm data to establish baseline records
for future signal detection. Used for initial setup and development.
"""

import logging
import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional, Any

from config import DB_CONFIG, PIPELINE_CONFIG
from scrapers.adv_parser import FirmRecord
from daily_runner import DailyRunner

logger = logging.getLogger(__name__)


class BackfillRunner:
    """
    Builds historical baseline of firm records for signal detection.

    Backfill process:
    1. Download historical firm data from SEC
    2. Parse into FirmRecord objects
    3. Store in database as baseline/historical records
    4. Generate initial statistics and summaries

    This is typically run once for initial setup or periodically
    to refresh the baseline with updated historical data.
    """

    def __init__(self):
        """Initialize backfill runner."""
        self.daily_runner = DailyRunner(dry_run=True)
        self.db_config = DB_CONFIG
        logger.info("Initialized BackfillRunner")

    def backfill_from_bulk(self, sample_size: int = 1000) -> Dict[str, Any]:
        """
        Download and process a sample of firms for baseline.

        Args:
            sample_size: Number of firms to backfill (default 1000)

        Returns:
            Results dict with statistics
        """
        logger.info(f"Starting backfill with sample_size={sample_size}...")

        backfill_start = datetime.now()
        results = {
            "start_date": backfill_start,
            "sample_size": sample_size,
            "firms_processed": 0,
            "firms_stored": 0,
            "errors": []
        }

        try:
            # Step 1: Download and parse firms
            logger.info("Step 1: Downloading firm data...")
            firms = self._download_bulk_firms(sample_size)
            results["firms_processed"] = len(firms)
            logger.info(f"Downloaded {len(firms)} firms")

            # Step 2: Store in database
            logger.info("Step 2: Storing baseline records...")
            stored = self._store_baseline_records(firms, backfill_start)
            results["firms_stored"] = stored
            logger.info(f"Stored {stored} firm records")

            # Step 3: Generate statistics
            logger.info("Step 3: Generating statistics...")
            stats = self._generate_baseline_stats()
            results["baseline_stats"] = stats

            duration = (datetime.now() - backfill_start).total_seconds()
            logger.info(
                f"Backfill completed in {duration:.1f}s - "
                f"Processed {results['firms_processed']}, "
                f"stored {results['firms_stored']}"
            )
            results["duration_seconds"] = duration

        except Exception as e:
            logger.exception("Backfill failed with error")
            results["errors"].append(str(e))

        return results

    def _download_bulk_firms(self, sample_size: int) -> List[FirmRecord]:
        """
        Download bulk firm data from SEC.

        In production, this would call SEC EDGAR API to fetch multiple
        Form ADV filings. For development, returns sample data.

        Args:
            sample_size: Number of firms to download

        Returns:
            List of FirmRecord objects
        """
        logger.info(f"Downloading {sample_size} firms from SEC...")

        # In production, implement SEC EDGAR API calls here
        # For now, return expanded sample data for testing
        firms = self._get_sample_firms_expanded(sample_size)

        logger.info(f"Downloaded {len(firms)} firms")
        return firms

    def _store_baseline_records(
        self, firms: List[FirmRecord], timestamp: datetime
    ) -> int:
        """
        Store firm records as baseline in database.

        Args:
            firms: List of FirmRecords to store
            timestamp: Timestamp for baseline

        Returns:
            Number of firms stored
        """
        try:
            conn = sqlite3.connect(self.db_config["path"])
            cursor = conn.cursor()

            # Create baseline table if needed
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS baseline_records (
                    sec_file_number TEXT PRIMARY KEY,
                    firm_name TEXT,
                    firm_data TEXT,
                    state TEXT,
                    aum_total REAL,
                    num_clients INTEGER,
                    backfill_date TIMESTAMP
                )
                """
            )

            stored_count = 0

            for firm in firms:
                try:
                    # Serialize firm data to JSON
                    firm_json = self._serialize_firm(firm)

                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO baseline_records
                        (sec_file_number, firm_name, firm_data, state, aum_total, num_clients, backfill_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            firm.sec_file_number,
                            firm.firm_name,
                            firm_json,
                            firm.state,
                            firm.aum_total,
                            firm.num_clients,
                            timestamp
                        )
                    )
                    stored_count += 1

                except Exception as e:
                    logger.warning(f"Could not store {firm.firm_name}: {e}")
                    continue

            conn.commit()
            conn.close()
            logger.info(f"Stored {stored_count}/{len(firms)} baseline records")
            return stored_count

        except Exception as e:
            logger.error(f"Error storing baseline records: {e}")
            return 0

    def _generate_baseline_stats(self) -> Dict[str, Any]:
        """
        Generate statistics from baseline records.

        Args:
            None

        Returns:
            Statistics dict
        """
        try:
            conn = sqlite3.connect(self.db_config["path"])
            cursor = conn.cursor()

            # Count records
            cursor.execute("SELECT COUNT(*) FROM baseline_records")
            total_count = cursor.fetchone()[0]

            # AUM statistics
            cursor.execute(
                """
                SELECT
                    COUNT(*),
                    SUM(aum_total),
                    AVG(aum_total),
                    MIN(aum_total),
                    MAX(aum_total)
                FROM baseline_records
                WHERE aum_total > 0
                """
            )
            aum_row = cursor.fetchone()

            # State distribution
            cursor.execute(
                """
                SELECT state, COUNT(*) as count
                FROM baseline_records
                WHERE state != ''
                GROUP BY state
                ORDER BY count DESC
                LIMIT 10
                """
            )
            state_dist = {row[0]: row[1] for row in cursor.fetchall()}

            # AUM size distribution
            cursor.execute(
                """
                SELECT
                    CASE
                        WHEN aum_total >= 1000000000 THEN '$1B+'
                        WHEN aum_total >= 500000000 THEN '$500M-$1B'
                        WHEN aum_total >= 100000000 THEN '$100M-$500M'
                        WHEN aum_total >= 10000000 THEN '$10M-$100M'
                        ELSE '<$10M'
                    END as size_bucket,
                    COUNT(*) as count
                FROM baseline_records
                WHERE aum_total > 0
                GROUP BY size_bucket
                ORDER BY aum_total DESC
                """
            )
            aum_dist = {row[0]: row[1] for row in cursor.fetchall()}

            conn.close()

            stats = {
                "total_records": total_count,
                "aum_statistics": {
                    "count": aum_row[0] if aum_row else 0,
                    "total_aum": aum_row[1] if aum_row and aum_row[1] else 0,
                    "avg_aum": aum_row[2] if aum_row and aum_row[2] else 0,
                    "min_aum": aum_row[3] if aum_row and aum_row[3] else 0,
                    "max_aum": aum_row[4] if aum_row and aum_row[4] else 0,
                },
                "state_distribution": state_dist,
                "aum_size_distribution": aum_dist
            }

            logger.info(f"Baseline stats: {total_count} firms, "
                       f"${stats['aum_statistics']['total_aum']:,.0f} total AUM")

            return stats

        except Exception as e:
            logger.error(f"Error generating baseline stats: {e}")
            return {}

    def _serialize_firm(self, firm: FirmRecord) -> str:
        """
        Serialize FirmRecord to JSON for database storage.

        Args:
            firm: FirmRecord to serialize

        Returns:
            JSON string
        """
        firm_dict = {
            "firm_name": firm.firm_name,
            "sec_file_number": firm.sec_file_number,
            "cik": firm.cik,
            "state": firm.state,
            "country": firm.country,
            "registration_date": firm.registration_date.isoformat() if firm.registration_date else None,
            "aum_total": firm.aum_total,
            "aum_regulatory": firm.aum_regulatory,
            "num_clients": firm.num_clients,
            "avg_aum_per_client": firm.avg_aum_per_client,
            "hnw_clients": firm.hnw_clients,
            "institutional_clients": firm.institutional_clients,
            "manages_public_securities": firm.manages_public_securities,
            "manages_private_funds": firm.manages_private_funds,
            "manages_real_estate": firm.manages_real_estate,
            "manages_commodities": firm.manages_commodities,
            "manages_hedge_funds": firm.manages_hedge_funds,
            "manages_other_alts": firm.manages_other_alts,
            "custodian_names": firm.custodian_names,
            "fee_structure": firm.fee_structure,
            "minimum_account_size": firm.minimum_account_size,
            "firm_type": firm.firm_type,
            "is_family_office": firm.is_family_office,
            "is_multi_family_office": firm.is_multi_family_office,
            "wirehouse_background": firm.wirehouse_background,
            "website": firm.website,
        }
        return json.dumps(firm_dict, default=str)

    def _get_sample_firms_expanded(self, count: int) -> List[FirmRecord]:
        """
        Generate expanded sample firm data for backfill testing.

        Args:
            count: Number of firms to generate

        Returns:
            List of FirmRecord objects
        """
        base_firms = self.daily_runner._get_sample_firms()

        # Expand to requested count by cloning and varying
        expanded = base_firms.copy()

        for i in range(len(base_firms), count):
            base = base_firms[i % len(base_firms)].copy()
            base.firm_name = f"{base.firm_name} #{i}"
            base.sec_file_number = f"801-{1000 + i}"
            base.aum_total *= (1.0 + (i % 10) * 0.1)  # Vary AUM
            expanded.append(base)

        return expanded[:count]
