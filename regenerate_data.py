#!/usr/bin/env python3
"""Regenerate all data JSON files from funds_config.json"""
import json
from datetime import datetime, timezone

# Load config
with open('/sessions/trusting-eager-galileo/infra_fresh/scripts/funds_config.json') as f:
    config = json.load(f)

now = datetime.now(timezone.utc).isoformat()

# 1. Build openinfra_historical.json
historical = {"funds": {}}
for fund in config["funds"]:
    fid = fund["id"]
    entries = []
    for h in fund.get("historical_nav", []):
        entry = {
            "date": h["date"],
            "nav": h.get("nav"),
            "aum_m": h.get("aum_m"),
            "subs_m": h.get("subs_m"),
            "redemptions_m": h.get("redemptions_m"),
            "dist_per_share": h.get("dist_per_share"),
            "source_label": h.get("source_label", ""),
            "source_url": h.get("source_url", "")
        }
        entries.append(entry)
    historical["funds"][fid] = {"historical": entries}

# 2. Build openinfra_nav.json (latest snapshot per fund)
nav_data = {"last_updated": now, "funds": {}}
for fund in config["funds"]:
    fid = fund["id"]
    hist = fund.get("historical_nav", [])
    
    # Get latest entry with NAV
    latest = None
    for h in reversed(hist):
        if h.get("nav") is not None:
            latest = h
            break
    
    # Calculate returns
    nav_entries_with_nav = [h for h in hist if h.get("nav") is not None]
    returns = {}
    
    if len(nav_entries_with_nav) >= 2:
        current = nav_entries_with_nav[-1]
        prev = nav_entries_with_nav[-2]
        returns["return_1m"] = round((current["nav"] / prev["nav"] - 1) * 100, 2)
    
    if len(nav_entries_with_nav) >= 4:
        current = nav_entries_with_nav[-1]
        three_back = nav_entries_with_nav[-4] if len(nav_entries_with_nav) >= 4 else nav_entries_with_nav[0]
        returns["return_3m"] = round((current["nav"] / three_back["nav"] - 1) * 100, 2)
    
    # ITD return
    if len(nav_entries_with_nav) >= 2:
        first_nav = nav_entries_with_nav[0]
        last_nav = nav_entries_with_nav[-1]
        returns["return_itd"] = round((last_nav["nav"] / first_nav["nav"] - 1) * 100, 2)
    
    # Get the latest data point (even if nav is None, for AUM)
    latest_any = hist[-1] if hist else None
    
    # Use known_aum_m as fallback if no aum_m in latest entry
    aum = None
    if latest and latest.get("aum_m"):
        aum = latest["aum_m"]
    elif latest_any and latest_any.get("aum_m"):
        aum = latest_any["aum_m"]
    elif fund.get("known_aum_m"):
        aum = fund["known_aum_m"]
    
    source_type = "SEC_8K" if fund.get("has_sec_filings") else "WEBSITE"
    
    nav_entry = {
        "fund_id": fid,
        "fund_name": fund["name"],
        "manager": fund["manager"],
        "is_primary": fund.get("is_primary", False),
        "benchmark_class": fund.get("benchmark_class", ""),
        "nav_per_share": fund.get("known_nav") if latest is None else latest["nav"],
        "nav_date": fund.get("known_nav_date") if latest is None else latest["date"],
        "total_aum_millions": aum,
        "gross_subscriptions_millions": latest.get("subs_m") if latest else None,
        "gross_redemptions_millions": latest.get("redemptions_m") if latest else None,
        "net_flows_millions": None,
        "distribution_per_share": latest.get("dist_per_share") if latest else None,
        "total_investors": None,
        "placement_agents": [],
        "source_type": source_type,
        "source_url": latest.get("source_url", "") if latest else fund.get("source_url", ""),
        "source_label": latest.get("source_label", "") if latest else fund.get("source_label", ""),
        "confidence": "HIGH" if fund.get("has_sec_filings") else "MEDIUM",
        "last_updated": now[:10],
        "fund_website": fund.get("website", ""),
        "sec_filings_url": fund.get("sec_filings_url", ""),
        "data_points_count": len(nav_entries_with_nav),
        "oldest_nav_date": nav_entries_with_nav[0]["date"] if nav_entries_with_nav else None,
        "latest_nav_date": nav_entries_with_nav[-1]["date"] if nav_entries_with_nav else None
    }
    nav_entry.update(returns)
    nav_data["funds"][fid] = nav_entry

# 3. Build openinfra_returns.json
returns_data = {"last_updated": now, "funds": {}}
for fid, fund_nav in nav_data["funds"].items():
    r = {"data_points_count": fund_nav.get("data_points_count", 0)}
    for key in ["return_1m", "return_3m", "return_6m", "return_1y", "return_ytd", "return_itd", 
                "annualised_itd", "oldest_nav_date", "latest_nav_date"]:
        if key in fund_nav:
            r[key] = fund_nav[key]
    returns_data["funds"][fid] = r

# 4. Build openinfra_metadata.json
metadata = {
    "last_run": now,
    "run_status": "success",
    "funds_updated": len(config["funds"]),
    "data_sources": ["SEC EDGAR 8-K filings", "Fund websites", "Earnings calls"],
    "next_scheduled_run": None,
    "version": "1.1.0"
}

# 5. Build openinfra_changes.json
changes = {
    "last_updated": now,
    "changes": [
        {
            "date": now[:10],
            "type": "data_update",
            "description": "Updated Ares ACI with Dec 2025, Jan 2026, Feb 2026 data (AUM now $2,782.2M). Updated KKR Infra with Feb 2026 data (AUM now $7,028.8M). Added CIK for Brookfield and Blue Owl ODIT."
        }
    ]
}

# Write all files
for path_prefix in ['/sessions/trusting-eager-galileo/infra_fresh/data', '/sessions/trusting-eager-galileo/infra_fresh/ISQ_Tracker_Deploy/data']:
    with open(f'{path_prefix}/openinfra_historical.json', 'w') as f:
        json.dump(historical, f, indent=2)
    with open(f'{path_prefix}/openinfra_nav.json', 'w') as f:
        json.dump(nav_data, f, indent=2)
    with open(f'{path_prefix}/openinfra_returns.json', 'w') as f:
        json.dump(returns_data, f, indent=2)
    with open(f'{path_prefix}/openinfra_metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    with open(f'{path_prefix}/openinfra_changes.json', 'w') as f:
        json.dump(changes, f, indent=2)

print("All data files regenerated successfully!")

# Print summary
print("\nFund summary:")
for fid, fnav in nav_data["funds"].items():
    nav = fnav.get("nav_per_share")
    aum = fnav.get("total_aum_millions")
    date = fnav.get("nav_date")
    itd = fnav.get("return_itd")
    print(f"  {fnav['fund_name']}: NAV=${nav}, AUM=${aum}M, Date={date}, ITD={itd}%")
