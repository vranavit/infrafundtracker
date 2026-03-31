#!/usr/bin/env python3
"""
Static JSON Exporter for ADV Buying Signal Engine.

Runs the full pipeline (download SEC IAPD data → parse → score → tier)
and writes a single JSON file that the Netlify-hosted frontend can fetch.

Usage:
    python export_static.py                      # full run
    python export_static.py --sample             # sample data (no SEC download)
    python export_static.py --output /path/to/out.json
"""

import argparse
import json
import logging
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# Ensure adv_engine is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import PLATFORMS_ACCESSIBILITY, SIGNALS as SIGNAL_DEFS
from daily_runner import DailyRunner
from scrapers.adv_parser import FirmRecord

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("export_static")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Where to write the output JSON (relative to repo root)
DEFAULT_OUTPUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "ISQ_Tracker_Deploy",
    "adv_leads.json",
)

# Max number of individual lead cards to include (keeps JSON <1 MB)
MAX_LEADS = 200

# Platform display names mapping
PLATFORM_DISPLAY = {
    "Schwab": "Schwab",
    "Fidelity": "Fidelity",
    "Pershing": "Pershing (BNY)",
    "LPL": "LPL Financial",
    "iCapital": "iCapital",
    "CAIS": "CAIS",
    "Morgan Stanley": "Morgan Stanley",
    "Merrill Lynch": "Merrill Lynch",
    "UBS": "UBS",
    "Raymond James": "Raymond James",
    "Moonfare": "Moonfare",
    "Altvia": "Altvia",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_lead_card(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a pipeline result dict into the JSON shape the frontend expects.

    Frontend shape per lead:
    {
      id, firm, city, state, aum, clients, avgPerClient,
      tier, score, qpScore, qpExpl, platformAccess, platformTier,
      isqAccessible, signals: [{name, weight, evidence, isNew, category,
                                talkingPoint}],
      action
    }
    """
    firm: FirmRecord = result["firm"]
    signals_raw = result.get("signals", [])

    # Build signal list
    signals_out = []
    for sig in signals_raw:
        signals_out.append({
            "name": sig.name,
            "weight": sig.weight,
            "evidence": sig.evidence,
            "isNew": sig.is_new,
            "category": sig.category,
            "talkingPoint": sig.talking_point,
        })

    # Platform info
    platforms_detected = result.get("platforms", [])
    best_platform = platforms_detected[0] if platforms_detected else "None detected"
    platform_tier = result.get("platform_tier", 0)
    isq_accessible = platform_tier in [1, 2]

    # City is not in SEC bulk data; leave blank or parse from state
    city = ""

    # Avg AUM per client
    avg_per_client = (
        round(firm.aum_total / firm.num_clients)
        if firm.num_clients > 0
        else 0
    )

    # Build suggested action line
    action_parts = []
    if signals_out:
        top_sig = signals_out[0]
        action_parts.append(f"Lead with {top_sig['name'].replace('_', ' ')} signal.")
    if not isq_accessible:
        action_parts.append("No ISQ-accessible platform — needs onboarding first.")
    action = " ".join(action_parts) if action_parts else "Review signals before outreach."

    # Convert tier string ("tier_1") to integer (1) for frontend
    raw_tier = result.get("tier", "tier_4")
    if isinstance(raw_tier, str) and raw_tier.startswith("tier_"):
        tier_int = int(raw_tier.split("_")[1])
    elif isinstance(raw_tier, int):
        tier_int = raw_tier
    else:
        tier_int = 4

    return {
        "id": firm.sec_file_number,
        "firm": firm.firm_name,
        "city": city,
        "state": firm.state[:2].upper() if firm.state else "",
        "aum": round(firm.aum_total),
        "clients": firm.num_clients,
        "avgPerClient": avg_per_client,
        "tier": tier_int,
        "score": round(result.get("score", 0), 1),
        "qpScore": round(result.get("qp_score", 0), 1),
        "qpExpl": result.get("qp_explanation", ""),
        "platformAccess": best_platform,
        "platformTier": platform_tier,
        "isqAccessible": isq_accessible,
        "signals": signals_out,
        "action": action,
    }


def _build_state_counts(results: List[Dict[str, Any]]) -> Dict[str, int]:
    """Count leads per US state."""
    counts: Counter = Counter()
    for r in results:
        firm: FirmRecord = r["firm"]
        st = firm.state[:2].upper() if firm.state else ""
        if st and len(st) == 2:
            counts[st] += 1
    return dict(counts.most_common())


def _build_signal_freq(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Count total and new occurrences of each signal type across all results.
    """
    total: Counter = Counter()
    new: Counter = Counter()
    for r in results:
        for sig in r.get("signals", []):
            total[sig.name] += 1
            if sig.is_new:
                new[sig.name] += 1

    freq = []
    for name, count in total.most_common():
        freq.append({
            "name": name,
            "count": count,
            "newCount": new.get(name, 0),
        })
    return freq


def _build_platform_counts(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Count how many firms are on each platform.
    """
    counts: Counter = Counter()
    no_platform = 0

    for r in results:
        platforms = r.get("platforms", [])
        if not platforms:
            no_platform += 1
        else:
            for p in platforms:
                display = PLATFORM_DISPLAY.get(p, p)
                counts[display] += 1

    out = []
    for name, count in counts.most_common():
        # Look up tier from config
        raw_name = next(
            (k for k, v in PLATFORM_DISPLAY.items() if v == name), name
        )
        tier = PLATFORMS_ACCESSIBILITY.get(raw_name, {}).get("tier", 0)
        out.append({"name": name, "count": count, "tier": tier})

    if no_platform:
        out.append({"name": "No accessible platform", "count": no_platform, "tier": 0})

    return out


# ---------------------------------------------------------------------------
# Main export
# ---------------------------------------------------------------------------

def run_export(
    output_path: str = DEFAULT_OUTPUT,
    sample_mode: bool = False,
    sample_size: int | None = None,
) -> str:
    """
    Run the full ADV pipeline and write the static JSON file.

    Returns the output file path.
    """
    logger.info("Starting static JSON export...")
    runner = DailyRunner(dry_run=True)

    # Run the pipeline
    pipeline_results = runner.run_daily_pipeline(
        sample_size=sample_size, dry_run=True
    )

    all_results = pipeline_results.get("all_results", [])
    logger.info(f"Pipeline produced {len(all_results)} results")

    # Sort by score descending, take top N
    all_results.sort(key=lambda r: r.get("score", 0), reverse=True)
    top_results = all_results[:MAX_LEADS]

    # Build each section
    leads = [_build_lead_card(r) for r in top_results]
    state_counts = _build_state_counts(all_results)
    signal_freq = _build_signal_freq(all_results)
    platform_counts = _build_platform_counts(all_results)

    # KPI summary — tiers come as "tier_1"/"tier_2" strings from signal_scorer
    tier_counts = Counter()
    for r in all_results:
        raw = r.get("tier", "tier_4")
        if isinstance(raw, str) and raw.startswith("tier_"):
            tier_counts[int(raw.split("_")[1])] += 1
        elif isinstance(raw, int):
            tier_counts[raw] += 1

    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "total_firms_scanned": pipeline_results.get("total_firms_processed", 0),
        "total_signals_fired": pipeline_results.get("total_signals_fired", 0),
        "kpi": {
            "tier1_count": tier_counts.get(1, 0),
            "tier2_count": tier_counts.get(2, 0),
            "tier3_count": tier_counts.get(3, 0),
            "new_signals_today": sum(
                1
                for r in all_results
                for s in r.get("signals", [])
                if s.is_new
            ),
        },
        "leads": leads,
        "state_counts": state_counts,
        "signal_freq": signal_freq,
        "platform_counts": platform_counts,
    }

    # Write JSON
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    size_kb = out_path.stat().st_size / 1024
    logger.info(f"Wrote {out_path} ({size_kb:.1f} KB, {len(leads)} leads)")
    return str(out_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Export ADV engine data as static JSON for Netlify frontend"
    )
    parser.add_argument(
        "--output", "-o",
        default=DEFAULT_OUTPUT,
        help="Output JSON file path",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use sample data instead of downloading from SEC",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Limit to N firms (for testing)",
    )
    args = parser.parse_args()

    output = run_export(
        output_path=args.output,
        sample_mode=args.sample,
        sample_size=args.sample_size,
    )
    print(f"Export complete: {output}")


if __name__ == "__main__":
    main()
