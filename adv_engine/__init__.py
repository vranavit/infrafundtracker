"""
ADV Buying Signal Engine - Complete Buying Signal Detection and Scoring System

A production-grade platform for detecting buying signals from SEC Form ADV filings,
scoring firm prospects for alternative investment distribution.

Main Components:
- models: Core data structures
- database: SQLite persistence layer
- signals: Signal detection logic
- scorer: Scoring and tiering engine
- parser: Data parsing utilities
- api: Flask REST API
"""

__version__ = "1.0.0"
__author__ = "ISQ Capital"

# Lazy import to avoid config dependency issues in test environments
try:
    import logging.config
    from config import LOGGING_CONFIG
    logging.config.dictConfig(LOGGING_CONFIG)
except (ImportError, FileNotFoundError):
    import logging
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
logger.info(f"ADV Engine v{__version__} initialized")
