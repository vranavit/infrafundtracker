"""
Scheduler - APScheduler Configuration for Daily Pipeline Execution

Runs the ADV buying signal engine pipeline daily at 07:00 ET.
Handles job scheduling, error logging, and graceful failure handling.
"""

import logging
import sys
from datetime import datetime
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
from pytz import timezone

from config import SCHEDULER_CONFIG
from daily_runner import DailyRunner

logger = logging.getLogger(__name__)


class PipelineScheduler:
    """
    Manages daily execution of ADV buying signal engine pipeline.

    Configuration:
    - Runs daily at 07:00 Eastern Time (configurable)
    - Automatic job coalescing (prevents multiple concurrent runs)
    - Graceful failure handling with 15-minute misfire grace time
    - Comprehensive error logging and alerting
    """

    def __init__(self):
        """Initialize scheduler."""
        self.config = SCHEDULER_CONFIG
        self.timezone = timezone(self.config["timezone"])
        self.scheduler = None
        self.daily_runner = DailyRunner(dry_run=False)

        logger.info(
            f"Initialized PipelineScheduler "
            f"(run_time={self.config['daily_run_time']}, "
            f"timezone={self.config['timezone']})"
        )

    def start(self) -> None:
        """
        Start the scheduler.

        Creates scheduler instance, configures job, and starts background execution.
        """
        try:
            # Create scheduler with thread pool executor
            executors = {
                "default": ThreadPoolExecutor(max_workers=1)
            }

            self.scheduler = BackgroundScheduler(
                executors=executors,
                timezone=self.timezone
            )

            # Configure job defaults
            job_defaults = self.config["job_defaults"]
            self.scheduler.configure(job_defaults=job_defaults)

            # Schedule daily job
            self._schedule_daily_job()

            # Start scheduler
            self.scheduler.start()
            logger.info("Scheduler started successfully")

        except Exception as e:
            logger.exception("Failed to start scheduler")
            raise

    def stop(self) -> None:
        """Stop the scheduler."""
        if self.scheduler:
            self.scheduler.shutdown(wait=True)
            logger.info("Scheduler stopped")

    def _schedule_daily_job(self) -> None:
        """Schedule the daily pipeline job."""
        # Parse run time (format: "HH:MM")
        run_time = self.config["daily_run_time"]
        hour, minute = map(int, run_time.split(":"))

        # Create cron trigger
        trigger = CronTrigger(
            hour=hour,
            minute=minute,
            timezone=self.timezone
        )

        # Add job
        self.scheduler.add_job(
            self._run_pipeline_job,
            trigger=trigger,
            id="adv_daily_pipeline",
            name="ADV Daily Pipeline",
            replace_existing=True
        )

        logger.info(
            f"Scheduled daily pipeline job at {run_time} {self.config['timezone']}"
        )

    def _run_pipeline_job(self) -> None:
        """
        Execute the daily pipeline job.

        Wrapped in try/except to ensure errors don't kill scheduler.
        """
        job_start = datetime.now()
        logger.info("=" * 60)
        logger.info(f"Starting scheduled pipeline run at {job_start.isoformat()}")
        logger.info("=" * 60)

        try:
            # Run pipeline
            results = self.daily_runner.run_daily_pipeline()

            # Log summary
            job_duration = (datetime.now() - job_start).total_seconds()
            logger.info("=" * 60)
            logger.info("PIPELINE RUN COMPLETE")
            logger.info(f"Duration: {job_duration:.1f} seconds")
            logger.info(f"Firms processed: {results['total_firms_processed']}")
            logger.info(f"Signals fired: {results['total_signals_fired']}")
            logger.info(f"Firms with signals: {results['firms_with_signals']}")
            logger.info("=" * 60)

            # Handle report generation
            if "markdown_brief" in results:
                self._handle_report_output(results["markdown_brief"])

        except Exception as e:
            logger.exception("Pipeline job failed with error")
            self._handle_job_error(e, job_start)

    def _handle_report_output(self, markdown_brief: str) -> None:
        """
        Handle daily brief output (email, Slack, file storage).

        In production, this would send to email, Slack, or store report.

        Args:
            markdown_brief: Markdown-formatted brief text
        """
        try:
            # Log to file
            report_path = "/tmp/adv_engine_daily_brief.md"
            with open(report_path, "w") as f:
                f.write(markdown_brief)
            logger.info(f"Report saved to {report_path}")

            # In production, implement:
            # - Email delivery (SendGrid, AWS SES)
            # - Slack webhook posting
            # - Dashboard update
            # - Database archive

        except Exception as e:
            logger.error(f"Error handling report output: {e}")

    def _handle_job_error(self, error: Exception, job_start: datetime) -> None:
        """
        Handle job failures with alerting.

        Args:
            error: Exception that occurred
            job_start: When the job started
        """
        job_duration = (datetime.now() - job_start).total_seconds()

        error_message = (
            f"PIPELINE JOB FAILED\n"
            f"Time: {datetime.now().isoformat()}\n"
            f"Duration: {job_duration:.1f}s\n"
            f"Error: {str(error)}\n"
        )

        logger.error(error_message)

        # In production, send alerting:
        # - PagerDuty alert
        # - Email notification
        # - Slack alert
        # - CloudWatch metric
        try:
            self._send_alert(error_message)
        except Exception as alert_error:
            logger.error(f"Failed to send alert: {alert_error}")

    def _send_alert(self, message: str) -> None:
        """
        Send alert about job failure.

        In production, implement actual alerting (PagerDuty, email, Slack).

        Args:
            message: Alert message
        """
        # Placeholder for production alerting
        logger.warning(f"Alert (placeholder): {message}")

    def get_next_run_time(self) -> Optional[datetime]:
        """
        Get next scheduled run time.

        Returns:
            Next run datetime or None if scheduler not running
        """
        if not self.scheduler or not self.scheduler.running:
            return None

        job = self.scheduler.get_job("adv_daily_pipeline")
        if job:
            return job.next_run_time
        return None

    def get_last_run_time(self) -> Optional[datetime]:
        """
        Get last job run time.

        Returns:
            Last run datetime or None
        """
        if not self.scheduler:
            return None

        job = self.scheduler.get_job("adv_daily_pipeline")
        if job:
            # Would need to track separately; APScheduler doesn't provide this
            # In production, query database for last successful run
            return None
        return None


def create_and_start_scheduler() -> PipelineScheduler:
    """
    Factory function to create and start scheduler.

    Returns:
        Started PipelineScheduler instance
    """
    scheduler = PipelineScheduler()
    scheduler.start()
    return scheduler


if __name__ == "__main__":
    """Run scheduler in foreground (for testing/development)."""
    import logging.config
    from config import LOGGING_CONFIG

    # Configure logging
    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger(__name__)

    logger.info("Starting ADV Engine Scheduler in foreground mode...")

    try:
        scheduler = create_and_start_scheduler()

        logger.info(f"Scheduler running. Next run: {scheduler.get_next_run_time()}")
        logger.info("Press Ctrl+C to stop.")

        # Keep running
        while True:
            import time
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
        scheduler.stop()
        logger.info("Scheduler stopped")
        sys.exit(0)

    except Exception as e:
        logger.exception("Fatal error in scheduler")
        sys.exit(1)
