"""
Configuration module for the ADV Buying Signal Engine.

Contains all constants for ISQ fund details, platform accessibility,
signal definitions, QP scoring weights, and tier system.
"""

import os
from typing import Dict, List, Tuple

# =============================================================================
# ISQ Fund Configuration
# =============================================================================

ISQ_FUND = {
    "name": "ISQ OpenInfra Company",
    "cik": "2059924",
    "min_investment": 25000,
    "qp_threshold": 5000000,
    "share_classes": ["S", "D", "I", "F"],
    "target_channels": [
        "iCapital",
        "CAIS",
        "Moonfare",
        "Pershing",
        "Fidelity",
        "Schwab",
        "LPL",
    ],
}

# =============================================================================
# Platform Accessibility Matrix
# =============================================================================

PLATFORMS_ACCESSIBILITY = {
    "iCapital": {
        "tier": 1,
        "status": "TARGET",
        "custodian_indicators": ["iCapital One", "iCapital"],
        "adv_keywords": ["iCapital", "digital platform", "private markets"],
        "keywords": ["iCapital One", "iCapital", "iCapital", "digital platform", "private markets"],
        "score_weight": 1.5,
    },
    "CAIS": {
        "tier": 1,
        "status": "TARGET",
        "custodian_indicators": ["CAIS", "CAIS platform"],
        "adv_keywords": ["CAIS", "alternative investments", "CAIS platform"],
        "keywords": ["CAIS", "CAIS platform", "CAIS", "alternative investments", "CAIS platform"],
        "score_weight": 1.5,
    },
    "Moonfare": {
        "tier": 2,
        "status": "TARGET",
        "custodian_indicators": ["Moonfare"],
        "adv_keywords": ["Moonfare", "private funds", "Moonfare platform"],
        "keywords": ["Moonfare", "Moonfare", "private funds", "Moonfare platform"],
        "score_weight": 1.2,
    },
    "Altvia": {
        "tier": 2,
        "status": "POSSIBLE",
        "custodian_indicators": ["Altvia"],
        "adv_keywords": ["Altvia", "alternative solutions"],
        "keywords": ["Altvia", "Altvia", "alternative solutions"],
        "score_weight": 1.0,
    },
    "Pershing": {
        "tier": 1,
        "status": "ACCESSIBLE",
        "custodian_indicators": ["Pershing"],
        "adv_keywords": ["Pershing", "BNY Pershing"],
        "keywords": ["Pershing", "Pershing", "BNY Pershing"],
        "score_weight": 1.3,
    },
    "Fidelity": {
        "tier": 1,
        "status": "ACCESSIBLE",
        "custodian_indicators": [
            "Fidelity",
            "Fidelity Investments",
            "National Financial Services",
        ],
        "adv_keywords": ["Fidelity", "NFS", "National Financial Services"],
        "keywords": ["Fidelity", "Fidelity Investments", "National Financial Services", "Fidelity", "NFS", "National Financial Services"],
        "score_weight": 1.3,
    },
    "Schwab": {
        "tier": 1,
        "status": "ACCESSIBLE",
        "custodian_indicators": ["Schwab", "Charles Schwab", "Schwab Institutional"],
        "adv_keywords": ["Schwab", "Charles Schwab"],
        "keywords": ["Schwab", "Charles Schwab", "Schwab Institutional", "Schwab", "Charles Schwab"],
        "score_weight": 1.3,
    },
    "Merrill Lynch": {
        "tier": 2,
        "status": "POSSIBLE",
        "custodian_indicators": ["Merrill Lynch", "Bank of America Merrill Lynch"],
        "adv_keywords": ["Merrill Lynch", "BofA ML"],
        "keywords": ["Merrill Lynch", "Bank of America Merrill Lynch", "Merrill Lynch", "BofA ML"],
        "score_weight": 1.1,
    },
    "Morgan Stanley": {
        "tier": 2,
        "status": "POSSIBLE",
        "custodian_indicators": ["Morgan Stanley"],
        "adv_keywords": ["Morgan Stanley", "MSDW"],
        "keywords": ["Morgan Stanley", "Morgan Stanley", "MSDW"],
        "score_weight": 1.1,
    },
    "UBS": {
        "tier": 2,
        "status": "POSSIBLE",
        "custodian_indicators": ["UBS"],
        "adv_keywords": ["UBS", "UBS Financial Services"],
        "keywords": ["UBS", "UBS", "UBS Financial Services"],
        "score_weight": 1.1,
    },
    "LPL": {
        "tier": 1,
        "status": "ACCESSIBLE",
        "custodian_indicators": ["LPL Financial", "LPL"],
        "adv_keywords": ["LPL", "LPL Financial"],
        "keywords": ["LPL Financial", "LPL", "LPL", "LPL Financial"],
        "score_weight": 1.3,
    },
    "Raymond James": {
        "tier": 2,
        "status": "POSSIBLE",
        "custodian_indicators": ["Raymond James"],
        "adv_keywords": ["Raymond James"],
        "keywords": ["Raymond James", "Raymond James"],
        "score_weight": 1.1,
    },
}

# =============================================================================
# Inaccessible Custodians
# =============================================================================

INACCESSIBLE_CUSTODIANS = [
    "Edward Jones",
    "Vanguard Personal Advisor",
    "Betterment",
    "Wealthfront",
]

# =============================================================================
# Signal Definitions with Weights and Talking Points
# =============================================================================

SIGNALS = {
    "custodian_isq_platform": {
        "name": "custodian_isq_platform",
        "weight": 10,
        "category": "platform_accessibility",
        "description": "Firm is on ISQ-accessible platform",
        "talking_point_template": "Your firm is already connected to {platform}, which provides seamless access to ISQ OpenInfra",
        "talking_points": [
            "Your firm is already connected to {platform}, which provides seamless access to ISQ OpenInfra",
            "We're seeing strong adoption at {custodian} clients on {platform}",
        ],
    },
    "custodian_alternatives_capable": {
        "name": "custodian_alternatives_capable",
        "weight": 7,
        "category": "platform_accessibility",
        "description": "Custodian shows alternative investment capability",
        "talking_point_template": "{custodian} has demonstrated strong alternative investment capabilities",
        "talking_points": [
            "{custodian} has demonstrated strong alternative investment capabilities",
            "Your custodian platform is equipped for private markets and infrastructure investments",
        ],
    },
    "new_platform_added": {
        "name": "new_platform_added",
        "weight": 9,
        "category": "platform_accessibility",
        "description": "Firm recently added a new digital platform or custodian integration",
        "talking_point_template": "We noticed you recently integrated {platform} - perfect timing for ISQ distribution",
        "talking_points": [
            "We noticed you recently integrated {platform} - perfect timing for ISQ distribution",
            "The addition of {platform} enables you to offer ISQ OpenInfra to your clients",
        ],
    },
    "new_private_funds": {
        "name": "new_private_funds",
        "weight": 10,
        "category": "investment_expansion",
        "description": "Firm recently added private funds to offerings",
        "talking_point_template": "Your recent expansion into private funds aligns perfectly with ISQ OpenInfra",
        "talking_points": [
            "Your recent expansion into private funds aligns perfectly with ISQ OpenInfra",
            "We're seeing strong demand for infrastructure funds among your client base",
        ],
    },
    "existing_private_funds_no_infra": {
        "name": "existing_private_funds_no_infra",
        "weight": 6,
        "category": "investment_expansion",
        "description": "Firm offers private funds but no infrastructure funds yet",
        "talking_point_template": "Your private fund offering is missing infrastructure - a key allocation area",
        "talking_points": [
            "Your private fund offering is missing infrastructure - a key allocation area",
            "Infrastructure funds are a natural complement to your private equity offerings",
        ],
    },
    "new_real_estate_added": {
        "name": "new_real_estate_added",
        "weight": 5,
        "category": "investment_expansion",
        "description": "Firm recently added real estate funds or strategies",
        "talking_point_template": "Real estate allocation suggests openness to alternative real assets like infrastructure",
        "talking_points": [
            "Real estate allocation suggests openness to alternative real assets like infrastructure",
            "Your clients interested in real estate would also benefit from ISQ OpenInfra",
        ],
    },
    "aum_growth_25_pct": {
        "name": "aum_growth_25_pct",
        "weight": 7,
        "category": "aum_milestone",
        "description": "Firm achieved 25% AUM growth year-over-year",
        "talking_point_template": "Your impressive 25% growth demonstrates strong client confidence",
        "talking_points": [
            "Your impressive 25% growth demonstrates strong client confidence",
            "Growing AUM is an ideal time to expand alternative investment offerings",
        ],
    },
    "aum_growth_25pct": {
        "name": "aum_growth_25pct",
        "weight": 7,
        "category": "aum_milestone",
        "description": "Firm achieved 25% AUM growth year-over-year",
        "talking_point_template": "Your impressive 25% growth demonstrates strong client confidence",
        "talking_points": [
            "Your impressive 25% growth demonstrates strong client confidence",
            "Growing AUM is an ideal time to expand alternative investment offerings",
        ],
    },
    "aum_growth_50_pct": {
        "name": "aum_growth_50_pct",
        "weight": 9,
        "category": "aum_milestone",
        "description": "Firm achieved 50% AUM growth year-over-year",
        "talking_point_template": "Congratulations on your 50% AUM growth - this is exceptional",
        "talking_points": [
            "Congratulations on your 50% AUM growth - this is exceptional",
            "Rapid growth creates opportunities for new investment offerings like ISQ",
        ],
    },
    "aum_growth_50pct": {
        "name": "aum_growth_50pct",
        "weight": 9,
        "category": "aum_milestone",
        "description": "Firm achieved 50% AUM growth year-over-year",
        "talking_point_template": "Congratulations on your 50% AUM growth - this is exceptional",
        "talking_points": [
            "Congratulations on your 50% AUM growth - this is exceptional",
            "Rapid growth creates opportunities for new investment offerings like ISQ",
        ],
    },
    "aum_1b_crossed": {
        "name": "aum_1b_crossed",
        "weight": 8,
        "category": "aum_milestone",
        "description": "Firm crossed $1B in AUM",
        "talking_point_template": "Reaching $1B AUM is a significant milestone that typically correlates with expanded offerings",
        "talking_points": [
            "Reaching $1B AUM is a significant milestone that typically correlates with expanded offerings",
            "At $1B+ AUM, ISQ OpenInfra becomes a strategic fit for your alternatives mix",
        ],
    },
    "aum_crossed_1b": {
        "name": "aum_crossed_1b",
        "weight": 8,
        "category": "aum_milestone",
        "description": "Firm crossed $1B in AUM",
        "talking_point_template": "Reaching $1B AUM is a significant milestone that typically correlates with expanded offerings",
        "talking_points": [
            "Reaching $1B AUM is a significant milestone that typically correlates with expanded offerings",
            "At $1B+ AUM, ISQ OpenInfra becomes a strategic fit for your alternatives mix",
        ],
    },
    "aum_500m_crossed": {
        "name": "aum_500m_crossed",
        "weight": 6,
        "category": "aum_milestone",
        "description": "Firm crossed $500M in AUM",
        "talking_point_template": "Your $500M+ AUM suggests capacity for alternative investment offerings",
        "talking_points": [
            "Your $500M+ AUM suggests capacity for alternative investment offerings",
            "At your scale, infrastructure funds are becoming increasingly important",
        ],
    },
    "aum_crossed_500m": {
        "name": "aum_crossed_500m",
        "weight": 6,
        "category": "aum_milestone",
        "description": "Firm crossed $500M in AUM",
        "talking_point_template": "Your $500M+ AUM suggests capacity for alternative investment offerings",
        "talking_points": [
            "Your $500M+ AUM suggests capacity for alternative investment offerings",
            "At your scale, infrastructure funds are becoming increasingly important",
        ],
    },
    "hnw_clients_increased": {
        "name": "hnw_clients_increased",
        "weight": 8,
        "category": "client_composition",
        "description": "Firm's high-net-worth client base is growing",
        "talking_point_template": "Your growing HNW base aligns perfectly with ISQ's target client profile",
        "talking_points": [
            "Your growing HNW base aligns perfectly with ISQ's target client profile",
            "HNW clients expect access to institutional-grade infrastructure investments",
        ],
    },
    "hnw_client_growth_20pct": {
        "name": "hnw_client_growth_20pct",
        "weight": 8,
        "category": "client_composition",
        "description": "Firm's high-net-worth client base grew 20% or more",
        "talking_point_template": "Your 20%+ HNW client growth aligns perfectly with ISQ's target client profile",
        "talking_points": [
            "Your 20%+ HNW client growth aligns perfectly with ISQ's target client profile",
            "HNW clients expect access to institutional-grade infrastructure investments",
        ],
    },
    "institutional_clients_added": {
        "name": "institutional_clients_added",
        "weight": 6,
        "category": "client_composition",
        "description": "Firm recently added institutional clients or entered institutional space",
        "talking_point_template": "Your expansion into institutional clients opens doors to larger ISQ allocations",
        "talking_points": [
            "Your expansion into institutional clients opens doors to larger ISQ allocations",
            "Institutional investors are increasingly seeking infrastructure exposure",
        ],
    },
    "family_office_type": {
        "name": "family_office_type",
        "weight": 9,
        "category": "client_composition",
        "description": "Firm is family office or family office administrator",
        "talking_point_template": "Family offices are ideal investors in ISQ OpenInfra",
        "talking_points": [
            "Family offices are ideal investors in ISQ OpenInfra",
            "Your family office clients need sophisticated alternative investments like infrastructure",
        ],
    },
    "family_office_detected": {
        "name": "family_office_detected",
        "weight": 9,
        "category": "client_composition",
        "description": "Firm is family office or family office administrator",
        "talking_point_template": "Family offices are ideal investors in ISQ OpenInfra",
        "talking_points": [
            "Family offices are ideal investors in ISQ OpenInfra",
            "Your family office clients need sophisticated alternative investments like infrastructure",
        ],
    },
    "new_cio_or_cio_equivalent": {
        "name": "new_cio_or_cio_equivalent",
        "weight": 8,
        "category": "personnel",
        "description": "Firm recently hired CIO or Chief Investment Officer equivalent",
        "talking_point_template": "A new CIO often brings fresh perspectives on alternative investments",
        "talking_points": [
            "A new CIO often brings fresh perspectives on alternative investments",
            "We'd like to introduce ISQ to your new investment leadership",
        ],
    },
    "new_cio_hired": {
        "name": "new_cio_hired",
        "weight": 8,
        "category": "personnel",
        "description": "Firm recently hired CIO or Chief Investment Officer",
        "talking_point_template": "A new CIO often brings fresh perspectives on alternative investments",
        "talking_points": [
            "A new CIO often brings fresh perspectives on alternative investments",
            "We'd like to introduce ISQ to your new investment leadership",
        ],
    },
    "new_alternatives_hire": {
        "name": "new_alternatives_hire",
        "weight": 9,
        "category": "personnel",
        "description": "Firm recently hired alternatives specialist or private markets expert",
        "talking_point_template": "Your new alternatives hire signals commitment to private markets",
        "talking_points": [
            "Your new alternatives hire signals commitment to private markets",
            "ISQ OpenInfra would be a natural fit for your expanded alternatives team",
        ],
    },
    "alternatives_hire": {
        "name": "alternatives_hire",
        "weight": 9,
        "category": "personnel",
        "description": "Firm recently hired alternatives specialist or private markets expert",
        "talking_point_template": "Your new alternatives hire signals commitment to private markets",
        "talking_points": [
            "Your new alternatives hire signals commitment to private markets",
            "ISQ OpenInfra would be a natural fit for your expanded alternatives team",
        ],
    },
    "min_account_decreased": {
        "name": "min_account_decreased",
        "weight": 5,
        "category": "accessibility",
        "description": "Firm recently decreased minimum account size",
        "talking_point_template": "Lowering minimums shows commitment to accessibility and scale",
        "talking_points": [
            "Lowering minimums shows commitment to accessibility and scale",
            "This positions you well for ISQ's $25K minimum investment",
        ],
    },
    "fee_based_compensation": {
        "name": "fee_based_compensation",
        "weight": 4,
        "category": "compensation",
        "description": "Firm uses fee-based or AUM-based compensation model",
        "talking_point_template": "Fee-based compensation aligns with long-term alternative fund commitments",
        "talking_points": [
            "Fee-based compensation aligns with long-term alternative fund commitments",
            "Your fee model creates natural incentives for ISQ allocation",
        ],
    },
    "wirehouse_breakaway": {
        "name": "wirehouse_breakaway",
        "weight": 10,
        "category": "business_model",
        "description": "Advisor or firm recently broke away from wirehouse",
        "talking_point_template": "Wirehouses often restrict alternative investments - your independence opens new opportunities",
        "talking_points": [
            "Wirehouses often restrict alternative investments - your independence opens new opportunities",
            "ISQ OpenInfra is frequently added by advisors immediately after independence",
        ],
    },
    "recently_registered": {
        "name": "recently_registered",
        "weight": 7,
        "category": "regulatory",
        "description": "Firm recently registered as RIA or became regulated",
        "talking_point_template": "New RIA registration often brings expanded alternative investment capabilities",
        "talking_points": [
            "New RIA registration often brings expanded alternative investment capabilities",
            "Now that you're independently registered, ISQ OpenInfra is available to you",
        ],
    },
    "schwab_adoption": {
        "name": "schwab_adoption",
        "weight": 7,
        "category": "platform_accessibility",
        "description": "Firm has adopted Schwab platform",
        "talking_point_template": "Your Schwab platform adoption makes ISQ OpenInfra easily accessible",
        "talking_points": [
            "Your Schwab platform adoption makes ISQ OpenInfra easily accessible",
            "Schwab clients have seamless access to ISQ distribution",
        ],
    },
    "fidelity_adoption": {
        "name": "fidelity_adoption",
        "weight": 7,
        "category": "platform_accessibility",
        "description": "Firm has adopted Fidelity platform",
        "talking_point_template": "Your Fidelity platform adoption makes ISQ OpenInfra easily accessible",
        "talking_points": [
            "Your Fidelity platform adoption makes ISQ OpenInfra easily accessible",
            "Fidelity clients have seamless access to ISQ distribution",
        ],
    },
    "pershing_adoption": {
        "name": "pershing_adoption",
        "weight": 7,
        "category": "platform_accessibility",
        "description": "Firm has adopted Pershing platform",
        "talking_point_template": "Your Pershing platform adoption makes ISQ OpenInfra easily accessible",
        "talking_points": [
            "Your Pershing platform adoption makes ISQ OpenInfra easily accessible",
            "Pershing clients have seamless access to ISQ distribution",
        ],
    },
    "broadridge_adoption": {
        "name": "broadridge_adoption",
        "weight": 6,
        "category": "platform_accessibility",
        "description": "Firm has adopted Broadridge platform",
        "talking_point_template": "Your Broadridge platform adoption enables ISQ OpenInfra access",
        "talking_points": [
            "Your Broadridge platform adoption enables ISQ OpenInfra access",
            "Broadridge clients can access ISQ distribution channels",
        ],
    },
    "dss_adoption": {
        "name": "dss_adoption",
        "weight": 5,
        "category": "platform_accessibility",
        "description": "Firm has adopted DSS platform",
        "talking_point_template": "Your DSS platform adoption enables ISQ OpenInfra access",
        "talking_points": [
            "Your DSS platform adoption enables ISQ OpenInfra access",
            "DSS integration supports alternative investment distribution",
        ],
    },
}

# =============================================================================
# QP Scoring Weights
# =============================================================================

QP_SCORING = {
    "aum_per_client_thresholds": {
        "5_million_plus": {"min": 5_000_000, "score": 10},
        "2_to_5_million": {"min": 2_000_000, "max": 5_000_000, "score": 7},
        "1_to_2_million": {"min": 1_000_000, "max": 2_000_000, "score": 4},
        "below_1_million": {"max": 1_000_000, "score": 1},
    },
    "family_office_override_score": 10,
    "hnw_percentage_thresholds": {
        "80_plus": {"min": 0.80, "score": 8},
        "50_to_80": {"min": 0.50, "max": 0.80, "score": 6},
        "below_50": {"max": 0.50, "score": 3},
    },
    "default_score": 3,
}

# =============================================================================
# Tier System
# =============================================================================

TIERS = {
    "tier_1": {
        "range": [25, 100],
        "label": "CALL TODAY",
        "color": "#00AA44",
        "description": "Highest priority - immediate outreach",
    },
    "tier_2": {
        "range": [15, 24.99],
        "label": "CALL THIS WEEK",
        "color": "#FF9900",
        "description": "High priority - this week",
    },
    "tier_3": {
        "range": [8, 14.99],
        "label": "NURTURE",
        "color": "#6699CC",
        "description": "Medium priority - ongoing nurture",
    },
    "tier_4": {
        "range": [0, 7.99],
        "label": "MONITOR",
        "color": "#AAAAAA",
        "description": "Low priority - monitoring",
    },
}

# =============================================================================
# SEC API Configuration
# =============================================================================

SEC_RATE_LIMIT_DELAY = 0.1
SEC_BULK_CSV_BASE_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
SEC_BULK_DOWNLOAD_URL = "https://www.sec.gov/Archives/edgar/index"
SEC_IAPD_COMPILATION_URL = "https://adviserinfo.sec.gov/compilation"
SEC_FORM_ADV_DATA_URL = "https://www.sec.gov/foia-services/frequently-requested-documents/form-adv-data"

# =============================================================================
# HTTP Configuration
# =============================================================================

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# =============================================================================
# Database Configuration
# =============================================================================

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_PATH = os.path.join(_BASE_DIR, "adv_signals.db")
DATABASE_TIMEOUT = 30

# =============================================================================
# Caching Configuration
# =============================================================================

CACHE_DIR = os.path.join(_BASE_DIR, "cache")
CACHE_EXPIRY_DAYS = 7

# =============================================================================
# Logging Configuration
# =============================================================================

LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# =============================================================================
# Pipeline & DB Config (used by daily_runner)
# =============================================================================

PIPELINE_CONFIG = {
    "sample_size": None,
    "min_aum": 100_000_000,
    "max_firms": 50_000,
}

DB_CONFIG = {
    "path": DATABASE_PATH,
    "timeout": DATABASE_TIMEOUT,
}
