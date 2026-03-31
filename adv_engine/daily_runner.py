"""
Daily Runner - Orchestrates ADV Engine Pipeline

Runs complete daily pipeline: data download → parsing → signal detection →
scoring → database storage → report generation
"""

import logging
import sqlite3
from datetime import datetime
from typing import List, Optional, Dict, Any

from config import PIPELINE_CONFIG, DB_CONFIG
from scrapers.adv_parser import FirmRecord
from scrapers.iapd_fetcher import IAPDFetcher
from signals.signal_detector import SignalDetector, Signal
from signals.qp_scorer import QPScorer
from signals.platform_scorer import PlatformScorer
from signals.signal_scorer import SignalScorer
from alert_generator import AlertGenerator

logger = logging.getLogger(__name__)


class DailyRunner:
    """
    Orchestrates complete ADV buying signal engine pipeline.

    Pipeline steps:
    1. Download firm data from SEC/sources
    2. Parse into FirmRecord objects
    3. Detect signals (comparing to previous records)
    4. Score QP probability
    5. Score platform accessibility
    6. Compute overall buying signal score
    7. Store results in database
    8. Generate daily brief report
    """

    def __init__(self, dry_run: bool = False):
        """
        Initialize daily runner.

        Args:
            dry_run: If True, print stats without writing to DB
        """
        self.dry_run = dry_run
        self.signal_detector = SignalDetector()
        self.qp_scorer = QPScorer()
        self.platform_scorer = PlatformScorer()
        self.signal_scorer = SignalScorer()
        self.alert_generator = AlertGenerator()
        self.pipeline_config = PIPELINE_CONFIG
        self.db_config = DB_CONFIG

        logger.info(f"Initialized DailyRunner (dry_run={dry_run})")

    def run_daily_pipeline(
        self,
        sample_size: Optional[int] = None,
        dry_run: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Run complete daily pipeline.

        Args:
            sample_size: Limit to N firms (None = all)
            dry_run: Override dry_run setting (None = use instance setting)

        Returns:
            Pipeline results dict with stats and report
        """
        if dry_run is not None:
            self.dry_run = dry_run

        logger.info(
            f"Starting daily pipeline run "
            f"(sample_size={sample_size}, dry_run={self.dry_run})"
        )

        run_start = datetime.now()
        results = {
            "run_date": run_start,
            "dry_run": self.dry_run,
            "total_firms_processed": 0,
            "total_signals_fired": 0,
            "firms_with_signals": 0,
            "all_results": [],
            "errors": []
        }

        try:
            # Step 1: Download and parse firm data
            logger.info("Step 1: Downloading and parsing firm data...")
            firms = self._download_and_parse_firms(sample_size)
            logger.info(f"Parsed {len(firms)} firms")

            # Step 2: Load previous records from database
            logger.info("Step 2: Loading previous firm records...")
            previous_records = self._load_previous_records(firms)

            # Step 3: Process each firm through signal detection and scoring
            logger.info("Step 3: Processing signals and scoring...")
            processed_results = self._process_firms(
                firms, previous_records
            )

            results["all_results"] = processed_results
            results["total_firms_processed"] = len(firms)
            results["total_signals_fired"] = sum(
                len(r.get("signals", [])) for r in processed_results
            )
            results["firms_with_signals"] = sum(
                1 for r in processed_results if r.get("signals")
            )

            # Step 4: Store results in database
            if not self.dry_run:
                logger.info("Step 4: Storing results in database...")
                self._store_results_in_db(processed_results, run_start)
            else:
                logger.info("Step 4: SKIPPED (dry run mode)")

            # Step 5: Generate report
            logger.info("Step 5: Generating daily brief...")
            brief = self.alert_generator.generate_daily_brief(
                run_start, processed_results
            )
            results["brief"] = brief
            results["markdown_brief"] = (
                self.alert_generator.generate_markdown_brief(brief)
            )

            run_duration = (datetime.now() - run_start).total_seconds()
            logger.info(
                f"Pipeline completed successfully in {run_duration:.1f}s - "
                f"Processed {results['total_firms_processed']} firms, "
                f"fired {results['total_signals_fired']} signals"
            )
            results["duration_seconds"] = run_duration

        except Exception as e:
            logger.exception("Pipeline failed with error")
            results["errors"].append(str(e))

        return results

    def _download_and_parse_firms(
        self, sample_size: Optional[int] = None
    ) -> List[FirmRecord]:
        """
        Download firm data from SEC IAPD and parse into FirmRecord objects.

        Pipeline:
        1. IAPDFetcher downloads bulk CSV data from SEC
        2. SECCSVParser converts CSV rows into FirmRecord objects
        3. Filter and limit as needed

        Falls back to sample data if SEC download fails.

        Args:
            sample_size: Limit to N firms

        Returns:
            List of FirmRecord objects
        """
        logger.info(f"Downloading firm data (sample_size={sample_size})...")

        try:
            # Step 1: Fetch firms directly from SEC IAPD JSON API
            fetcher = IAPDFetcher()
            firms = fetcher.fetch_latest()

            if firms:
                logger.info(f"Fetched {len(firms)} firms from SEC IAPD API")

                if sample_size:
                    firms = firms[:sample_size]

                return firms

            logger.warning(
                "SEC IAPD API returned no data, falling back to sample"
            )
        except Exception as e:
            logger.warning(f"SEC IAPD fetch failed: {e}, falling back to sample")

        # Fallback to sample data
        firms = self._get_sample_firms()
        if sample_size:
            firms = firms[:sample_size]

        logger.info(f"Using {len(firms)} sample firms (fallback)")
        return firms

    def _load_previous_records(
        self, current_firms: List[FirmRecord]
    ) -> Dict[str, FirmRecord]:
        """
        Load previous firm records from database for comparison.

        Args:
            current_firms: List of current FirmRecords

        Returns:
            Dict mapping firm_name -> previous FirmRecord
        """
        previous = {}

        if self.dry_run:
            logger.info("Skipping previous record load (dry run)")
            return previous

        try:
            conn = sqlite3.connect(self.db_config["path"])
            cursor = conn.cursor()

            # Get file numbers to look up
            file_numbers = [f.sec_file_number for f in current_firms]

            if file_numbers:
                placeholders = ",".join(["?"] * len(file_numbers))
                cursor.execute(
                    f"SELECT sec_file_number, firm_data FROM firm_records "
                    f"WHERE sec_file_number IN ({placeholders}) "
                    f"ORDER BY run_date DESC LIMIT 1",
                    file_numbers
                )
                # In real implementation, deserialize firm_data JSON
                # For now, just load the structure

            conn.close()
        except Exception as e:
            logger.warning(f"Could not load previous records: {e}")

        return previous

    def _process_firms(
        self,
        firms: List[FirmRecord],
        previous_records: Dict[str, FirmRecord]
    ) -> List[Dict[str, Any]]:
        """
        Process firms through signal detection and scoring pipeline.

        Args:
            firms: List of current FirmRecord objects
            previous_records: Dict of previous FirmRecords for comparison

        Returns:
            List of result dicts with all scores and signals
        """
        results = []

        for i, firm in enumerate(firms):
            if (i + 1) % 100 == 0:
                logger.info(f"Processed {i + 1}/{len(firms)} firms...")

            try:
                # Get previous record if available
                previous = previous_records.get(firm.sec_file_number)

                # Detect signals
                signals = self.signal_detector.detect_signals(firm, previous)

                # Score QP probability
                qp_result = self.qp_scorer.score_qp_probability(firm)
                qp_score = qp_result.score

                # Score platform accessibility
                platform_result = self.platform_scorer.score_platform_accessibility(firm)
                platform_score = platform_result.score
                platform_tier = platform_result.best_tier

                # Compute overall score
                overall_result = self.signal_scorer.compute_overall_score(
                    signals, qp_score, platform_score, platform_tier, firm
                )

                # Include firms with signals, good platform access,
                # family offices, or private fund managers
                include = (
                    signals
                    or platform_tier in [1, 2]
                    or firm.is_family_office
                    or firm.manages_private_funds
                    or qp_score >= 8
                )
                if include:
                    results.append({
                        "firm": firm,
                        "signals": signals,
                        "qp_score": qp_score,
                        "platform_score": platform_score,
                        "platform_tier": platform_tier,
                        "score": overall_result.score,
                        "tier": overall_result.tier,
                        "label": overall_result.label,
                        "qp_explanation": qp_result.explanation,
                        "platforms": platform_result.platforms_detected,
                    })

            except Exception as e:
                logger.error(f"Error processing {firm.firm_name}: {e}")
                continue

        logger.info(f"Completed processing {len(firms)} firms, {len(results)} with signals")
        return results

    def _store_results_in_db(
        self, results: List[Dict[str, Any]], run_date: datetime
    ) -> None:
        """
        Store pipeline results in database.

        Args:
            results: List of result dicts from processing
            run_date: Date of the run
        """
        try:
            conn = sqlite3.connect(self.db_config["path"])
            cursor = conn.cursor()

            # Create tables if needed
            self._create_tables(cursor)

            # Store run metadata
            cursor.execute(
                "INSERT INTO runs (run_date, total_firms, total_signals) VALUES (?, ?, ?)",
                (run_date, len(results), sum(len(r.get("signals", [])) for r in results))
            )
            run_id = cursor.lastrowid

            # Store firm results
            import json
            for result in results:
                firm = result["firm"]
                signals_json = json.dumps([
                    {
                        "name": s.name,
                        "weight": s.weight,
                        "evidence": s.evidence,
                        "talking_point": s.talking_point,
                        "is_new": s.is_new,
                        "category": s.category
                    }
                    for s in result.get("signals", [])
                ])

                cursor.execute(
                    "INSERT OR REPLACE INTO firm_scores "
                    "(run_id, sec_file_number, firm_name, score, tier, "
                    "qp_score, platform_tier, signals) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        firm.sec_file_number,
                        firm.firm_name,
                        result["score"],
                        result["tier"],
                        result["qp_score"],
                        result["platform_tier"],
                        signals_json
                    )
                )

            conn.commit()
            conn.close()
            logger.info(f"Stored {len(results)} results in database (run_id={run_id})")

        except Exception as e:
            logger.error(f"Error storing results in database: {e}")

    def _create_tables(self, cursor: sqlite3.Cursor) -> None:
        """Create database tables if they don't exist."""
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id INTEGER PRIMARY KEY,
                run_date TIMESTAMP,
                total_firms INTEGER,
                total_signals INTEGER
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS firm_scores (
                run_id INTEGER,
                sec_file_number TEXT,
                firm_name TEXT,
                score REAL,
                tier TEXT,
                qp_score REAL,
                platform_tier INTEGER,
                signals TEXT,
                PRIMARY KEY (run_id, sec_file_number),
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS firm_records (
                sec_file_number TEXT PRIMARY KEY,
                run_date TIMESTAMP,
                firm_data TEXT
            )
            """
        )

    def _get_sample_firms(self) -> List[FirmRecord]:
        """
        Get sample firm data for testing.

        In production, this would be replaced by SEC EDGAR API calls.
        """
        firms = []

        # Sample firm 1: Strong prospect
        firms.append(FirmRecord(
            firm_name="Wealth Management Partners LLC",
            sec_file_number="801-1234",
            state="NY",
            aum_total=750_000_000,
            num_clients=45,
            hnw_clients=35,
            manages_private_funds=True,
            manages_real_estate=True,
            custodian_names=["Charles Schwab"],
            minimum_account_size=250_000,
            fee_structure="Fee-Based",
            is_family_office=True,
            firm_type="Independent",
            registration_date=datetime(2023, 1, 15)
        ))

        # Sample firm 2: Emerging prospect
        firms.append(FirmRecord(
            firm_name="Alternative Investors Group",
            sec_file_number="801-5678",
            state="CA",
            aum_total=450_000_000,
            num_clients=120,
            hnw_clients=60,
            manages_private_funds=False,
            manages_real_estate=False,
            custodian_names=["Fidelity"],
            minimum_account_size=500_000,
            fee_structure="Assets Under Management",
            firm_type="Independent",
            registration_date=datetime(2023, 6, 1)
        ))

        # Sample firm 3: Breakaway
        firms.append(FirmRecord(
            firm_name="Morgan Stanley Breakaway Group",
            sec_file_number="801-9101",
            state="TX",
            aum_total=250_000_000,
            num_clients=80,
            hnw_clients=40,
            manages_private_funds=True,
            custodian_names=["Pershing"],
            firm_type="Independent",
            wirehouse_background=True,
            registration_date=datetime(2024, 2, 1)
        ))

        return firms
