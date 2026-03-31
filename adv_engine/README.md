# ADV Buying Signal Engine

A production-grade platform for detecting buying signals from SEC Form ADV filings and scoring investment adviser firms for alternative investment distribution opportunities.

## Overview

The ADV Buying Signal Engine automatically processes SEC Form ADV filings for thousands of registered investment advisers, detects meaningful signals that indicate readiness for alternative investment adoption, and ranks prospects by their fit and opportunity level.

### Key Capabilities

- **Signal Detection**: Identifies 20+ buying signals across 7 categories
- **Multi-Factor Scoring**: Combines signal weights, QP probability, and platform accessibility
- **Daily Pipeline**: Automated processing of 40,000+ firms
- **Report Generation**: Daily briefs with leads organized by tier
- **Historical Baseline**: Tracks firm changes over time for trend detection

## Architecture

### Core Components

```
config.py
├── Signal definitions (20 signals, 7 categories)
├── Platform tier configuration
├── QP scoring thresholds
├── Tier assignments
└── ISQ fund constants

scrapers/adv_parser.py
├── FirmRecord dataclass (complete firm profile)
└── 50+ firm attributes

signals/signal_detector.py
├── SignalDetector class
├── Signal dataclass
└── 7 signal detection methods

signals/qp_scorer.py
├── QPScorer class
├── 4 QP scoring methods
└── Confidence tracking

signals/platform_scorer.py
├── PlatformScorer class
└── Tier-based platform scoring (1-10)

signals/signal_scorer.py
├── SignalScorer class
├── Scoring formula with multipliers
└── Platform gating rules

alert_generator.py
├── AlertGenerator class
├── LeadCard dataclass
├── Markdown report generation

daily_runner.py
├── DailyRunner (orchestration)
├── Pipeline steps 1-5
└── Batch processing

database.py
├── DatabaseManager class
├── Schema management
└── Query utilities

scheduler.py
├── PipelineScheduler class
├── APScheduler integration
└── Error handling/alerting

backfill.py
├── BackfillRunner class
└── Baseline establishment
```

## Signal Types

### 1. Platform Signals (6)
- Schwab Adoption (weight: 8)
- Fidelity Adoption (weight: 7)
- Pershing Adoption (weight: 6)
- Broadridge Adoption (weight: 6)
- DST/SS&C Adoption (weight: 6)

**Rationale**: Platform adoption indicates infrastructure modernization and distribution capability.

### 2. Investment Type Signals (3)
- New Private Funds (weight: 9)
- Existing Private Funds (No Infrastructure) (weight: 5)
- New Real Estate Added (weight: 7)

**Rationale**: Alternative asset management signals need for sophisticated custody and distribution.

### 3. AUM Growth Signals (4)
- 25%+ AUM Growth (weight: 4)
- 50%+ AUM Growth (weight: 6)
- AUM Crossed $1B (weight: 5)
- AUM Crossed $500M (weight: 4)

**Rationale**: Growth signals capacity to take on new asset classes and distribution.

### 4. Client Composition Signals (3)
- HNW Client Growth >20% (weight: 5)
- Institutional Clients Added (weight: 6)
- Family Office Detection (weight: 7)

**Rationale**: Client quality indicates qualified purchaser base and institutional relationships.

### 5. Personnel Signals (2)
- New CIO Hired (weight: 5)
- Alternatives Hire (weight: 4)

**Rationale**: Key personnel changes precede strategic shifts and new initiatives.

### 6. Breakaway Signals (2)
- Recently Registered (weight: 8)
- Wirehouse Breakaway (weight: 9)

**Rationale**: New entrants and breakaways need independent platforms and distribution partners.

### 7. Fee/Minimum Signals (2)
- Fee-Based Compensation (weight: 3)
- Minimum Account Decreased (weight: 4)

**Rationale**: Fee structure and accessibility changes indicate distribution strategy shifts.

## Scoring Formula

### Overall Score Calculation

```
1. Base Score = Sum of signal weights
2. New Signal Bonus = +3 per new signal
3. QP Multiplier = 0.5 + (qp_score / 10.0)
4. Subtotal = (base_score + new_signal_bonus) * qp_multiplier
5. Platform Gate = Apply tier-based caps
6. Normalized Score = subtotal * 0.5 (max 100)
7. Tier Assignment = Based on normalized score (0-100)
```

### Platform Gating Rules

| Platform Tier | Score Cap | Rationale |
|---|---|---|
| Tier 1 (Schwab, Fidelity) | None | Top-tier platforms enable all scores |
| Tier 2 (Pershing, Broadridge, etc.) | 24 | Good platforms but limited to Tier 3 max |
| Tier 3 | 14 | Basic platforms, limited distribution |
| None | 14 | No modern platform, significant upgrade needed |

### QP Probability Scoring (0-10)

**Method 1: AUM per Client** (Priority)
- $5M+: 10 (very likely)
- $2-5M: 7 (likely)
- $1-2M: 4 (possible)
- <$1M: 1 (unlikely)

**Method 2: Family Office Override**
- Family Office or Multi-Family Office: 10 (automatic)

**Method 3: HNW Percentage**
- 80%+: 8
- 50-80%: 6
- <50%: 3

**Method 4: Default**
- Insufficient data: 3

### Tier Assignment

| Tier | Score Range | Label | Description |
|---|---|---|---|
| Tier 1 | 75-100 | Priority Prospect | Highest quality signals and platform fit |
| Tier 2 | 50-74 | Strong Prospect | Good signals with strong growth trajectory |
| Tier 3 | 25-49 | Monitor | Emerging signals, early-stage growth |
| Tier 4 | 0-24 | Watch List | Limited signals or poor platform fit |

## Pipeline Execution

### Daily Pipeline Steps

```
1. Download and Parse
   └─ Fetch firm data from SEC EDGAR
   └─ Parse into FirmRecord objects
   └─ 40,000+ firms in batch mode

2. Load Previous Records
   └─ Retrieve baseline from database
   └─ Enable change detection

3. Process Firms
   └─ Detect signals (compare current to previous)
   └─ Score QP probability
   └─ Score platform accessibility
   └─ Compute overall buying signal score
   └─ Filter: include if signals OR Tier 1/2 platform

4. Store Results
   └─ Write to SQLite database
   └─ Maintains historical record
   └─ Enables trend analysis

5. Generate Report
   └─ Create daily brief
   └─ Organize by tier
   └─ Generate markdown for distribution
```

### Execution Schedule

- **Frequency**: Daily at 07:00 ET
- **Duration**: ~30-60 seconds for 40,000 firms (batch processing)
- **Error Handling**: Graceful failures with 15-minute misfire grace time
- **Alerting**: PagerDuty/email on job failure

## Database Schema

### Tables

```
runs
├─ run_id (PK)
├─ run_date
├─ total_firms
├─ total_signals
└─ duration_seconds

firm_scores
├─ score_id (PK)
├─ run_id (FK)
├─ sec_file_number
├─ firm_name
├─ aum_total
├─ num_clients
├─ score (0-100)
├─ tier
├─ qp_score
├─ platform_tier
└─ signals (JSON)

signals_log
├─ signal_id (PK)
├─ run_id (FK)
├─ sec_file_number
├─ signal_name
├─ signal_weight
├─ evidence
├─ talking_point
└─ is_new (boolean)

firm_records (baseline)
├─ record_id (PK)
├─ sec_file_number
├─ firm_name
├─ firm_data (JSON)
└─ record_date
```

## Usage

### Quick Start

```python
from daily_runner import DailyRunner

# Run daily pipeline
runner = DailyRunner(dry_run=False)
results = runner.run_daily_pipeline()

# Access results
print(f"Tier 1 prospects: {len(results['brief'].tier_1_leads)}")
print(f"Total signals: {results['total_signals_fired']}")
```

### Run with Sample Size

```python
runner = DailyRunner()
results = runner.run_daily_pipeline(sample_size=1000)
```

### Dry Run (No Database Writes)

```python
runner = DailyRunner(dry_run=True)
results = runner.run_daily_pipeline()
```

### Generate Markdown Report

```python
from alert_generator import AlertGenerator

gen = AlertGenerator()
brief = gen.generate_daily_brief(run_date, results)
markdown = gen.generate_markdown_brief(brief)
print(markdown)
```

### Historical Backfill

```python
from backfill import BackfillRunner

backfill = BackfillRunner()
results = backfill.backfill_from_bulk(sample_size=10000)
```

### Start Scheduler

```python
from scheduler import create_and_start_scheduler

scheduler = create_and_start_scheduler()
# Runs daily at 07:00 ET in background
```

## Configuration

All configuration is in `config.py`:

```python
# Signal definitions and weights
SIGNALS = {...}

# Platform tiers and keywords
PLATFORMS_ACCESSIBILITY = {...}

# QP scoring thresholds
QP_SCORING = {...}

# Tier ranges and labels
TIERS = {...}

# Database paths
DB_CONFIG = {...}

# Scheduler settings
SCHEDULER_CONFIG = {...}

# Pipeline parameters
PIPELINE_CONFIG = {...}
```

## Performance Characteristics

- **Processing Speed**: 40,000 firms in ~30-60 seconds
- **Database Size**: ~100MB per year of historical data
- **Memory**: ~500MB for full 40K firm batch
- **Batch Size**: 100 firms per batch (configurable)
- **Retry Logic**: 3 attempts with 5-second delays

## Error Handling

- **Dry Run Mode**: Test without database writes
- **Graceful Degradation**: Continue on per-firm errors
- **Misfire Grace Time**: 15 minutes for job rescheduling
- **Logging**: Comprehensive debug and error logging
- **Alerting**: PagerDuty/email on critical failures

## Extensibility

### Add New Signal Type

1. Add signal definition to `config.py` SIGNALS dict
2. Implement detection method in `SignalDetector`
3. Add to appropriate detection group method
4. Update talking point template

### Add New Platform

1. Add to `config.py` PLATFORMS_ACCESSIBILITY
2. Set tier (1, 2, or 3)
3. Add keywords for matching
4. Platform scorer automatically picks up

### Modify Scoring Formula

1. Edit `signal_scorer.py` constants
2. Update platform gate caps
3. Adjust QP multiplier formula
4. Modify tier ranges in config.py

## Testing

Run the test suite:

```bash
pytest tests/
pytest tests/test_signals.py -v
pytest tests/test_scorer.py -v
pytest tests/test_integration.py
```

## Dependencies

- Python 3.8+
- sqlite3 (built-in)
- apscheduler (for scheduler)
- pytz (for timezone handling)
- requests (for SEC EDGAR API calls, future)

## Files Summary

| File | Purpose | Key Classes |
|---|---|---|
| config.py | Configuration constants | (dataclass-free config) |
| scrapers/adv_parser.py | Firm data structures | FirmRecord |
| signals/signal_detector.py | Signal detection | SignalDetector, Signal |
| signals/qp_scorer.py | QP probability scoring | QPScorer, QPScore |
| signals/platform_scorer.py | Platform accessibility | PlatformScorer, PlatformScore |
| signals/signal_scorer.py | Overall score computation | SignalScorer, OverallScore |
| alert_generator.py | Report generation | AlertGenerator, LeadCard, DailyBrief |
| daily_runner.py | Pipeline orchestration | DailyRunner |
| database.py | Database management | DatabaseManager |
| scheduler.py | Daily job scheduling | PipelineScheduler |
| backfill.py | Historical baseline | BackfillRunner |

## Future Enhancements

1. **API Layer**: REST API for dashboard queries
2. **ML Enhancement**: Train QP probability model on historical data
3. **Real-time Updates**: Stream processing for intraday updates
4. **Distribution Integration**: Direct API connections to platforms
5. **Advanced Analytics**: Cohort analysis, propensity modeling
6. **Multi-fund Support**: Track signals across multiple ISQ funds

## License

Internal use - ISQ Capital

## Contact

For questions or issues, contact the data engineering team.
