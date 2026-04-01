#!/usr/bin/env python3
"""
Merge SEC-extracted data into funds_config.json and regenerate all data files.
Adds missing months, fills in AUM/shares where available.
"""
import json
from datetime import datetime, timezone
from copy import deepcopy

BASE = '/sessions/trusting-eager-galileo/infra_fresh'

# Load current config
with open(f'{BASE}/scripts/funds_config.json') as f:
    config = json.load(f)

# Load SEC-extracted data
with open(f'{BASE}/scripts/sec_extracted_data.json') as f:
    sec_data = json.load(f)

# ============================================================
# STEP 1: Merge Ares ACI data
# ============================================================
ares_fund = next(f for f in config["funds"] if f["id"] == "ares_aci")
existing_dates = {h["date"] for h in ares_fund["historical_nav"]}

# SEC extracted: [date, nav, aum_m, portfolio_fv_m, cumulative_shares, gross_subs_m, dist_per_share]
for row in sec_data["ares_aci"]["data"]:
    dt, nav, aum_m, fv, cs, subs, dist = row
    if dt not in existing_dates:
        entry = {
            "date": dt,
            "nav": nav,
            "aum_m": aum_m if aum_m else None,
            "subs_m": subs if subs else None,
            "redemptions_m": None,
            "dist_per_share": dist if dist else None,
            "source_label": f"SEC 8-K {dt[:7]}",
            "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=2031750"
        }
        if cs:
            entry["cumulative_shares"] = cs
        if fv:
            entry["portfolio_fv_m"] = fv
        ares_fund["historical_nav"].append(entry)
    else:
        # Update existing entry with SEC data if it has more detail
        for h in ares_fund["historical_nav"]:
            if h["date"] == dt:
                if nav and not h.get("nav"):
                    h["nav"] = nav
                if aum_m and not h.get("aum_m"):
                    h["aum_m"] = aum_m
                if cs and not h.get("cumulative_shares"):
                    h["cumulative_shares"] = cs
                if fv and not h.get("portfolio_fv_m"):
                    h["portfolio_fv_m"] = fv
                if subs and not h.get("subs_m"):
                    h["subs_m"] = subs
                if dist and not h.get("dist_per_share"):
                    h["dist_per_share"] = dist
                break

# Sort by date
ares_fund["historical_nav"].sort(key=lambda x: x["date"])

# ============================================================
# STEP 2: Merge KKR data - replace sparse history with full 31 months
# ============================================================
kkr_fund = next(f for f in config["funds"] if f["id"] == "kkr_infra_conglomerate")
kkr_existing = {h["date"]: h for h in kkr_fund["historical_nav"]}

new_kkr_history = []
for row in sec_data["kkr_infra_conglomerate"]["data"]:
    dt, class_i_nav = row
    if dt in kkr_existing:
        # Keep existing entry (has more detail like AUM, total_shares)
        new_kkr_history.append(kkr_existing[dt])
    else:
        entry = {
            "date": dt,
            "nav": class_i_nav,
            "nav_class_i": class_i_nav,
            "aum_m": None,
            "subs_m": None,
            "source_label": f"SEC 8-K {dt[:7]}",
            "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=1948056"
        }
        new_kkr_history.append(entry)

# Sort by date
new_kkr_history.sort(key=lambda x: x["date"])
kkr_fund["historical_nav"] = new_kkr_history

# Update inception date to match earliest data
kkr_fund["inception_date"] = "2023-06-01"
kkr_fund["backfill_start_date"] = "2023-08-01"

# ============================================================
# STEP 3: Write updated config
# ============================================================
with open(f'{BASE}/scripts/funds_config.json', 'w') as f:
    json.dump(config, f, indent=2)
print(f"Updated funds_config.json")

# ============================================================
# STEP 4: Regenerate all data files
# ============================================================
now = datetime.now(timezone.utc).isoformat()

# 4a. Build openinfra_historical.json
historical = {"last_updated": now, "funds": {}}
for fund in config["funds"]:
    fid = fund["id"]
    entries = []
    for h in fund.get("historical_nav", []):
        if h.get("nav") is None and h.get("aum_m") is None and h.get("cumulative_raised_m") is None:
            continue  # Skip entries without NAV or AUM
        entry = {
            "date": h["date"],
            "nav": h["nav"],
            "aum_m": h.get("aum_m") or h.get("cumulative_raised_m"),
            "subs_m": h.get("subs_m"),
            "redemptions_m": h.get("redemptions_m"),
            "dist_per_share": h.get("dist_per_share"),
            "source_label": h.get("source_label", ""),
            "source_url": h.get("source_url", "")
        }
        entries.append(entry)
    historical["funds"][fid] = {"historical": entries}

# 4b. Build openinfra_nav.json (latest snapshot per fund)
nav_data = {"last_updated": now, "funds": {}}
for fund in config["funds"]:
    fid = fund["id"]
    hist = fund.get("historical_nav", [])

    # Get entries with valid NAV
    nav_entries = [h for h in hist if h.get("nav") is not None]

    # Get latest entry with NAV
    latest = nav_entries[-1] if nav_entries else None

    # Calculate returns properly
    returns = {}
    if len(nav_entries) >= 2:
        current = nav_entries[-1]
        prev = nav_entries[-2]
        returns["return_1m"] = round((current["nav"] / prev["nav"] - 1) * 100, 2)

    if len(nav_entries) >= 4:
        current = nav_entries[-1]
        three_back = nav_entries[-4]
        returns["return_3m"] = round((current["nav"] / three_back["nav"] - 1) * 100, 2)
    elif len(nav_entries) >= 2:
        # If we have fewer than 4 entries, use what we have for 3m
        current = nav_entries[-1]
        first = nav_entries[0]
        returns["return_3m"] = round((current["nav"] / first["nav"] - 1) * 100, 2)

    # 6-month return
    if len(nav_entries) >= 7:
        current = nav_entries[-1]
        six_back = nav_entries[-7]
        returns["return_6m"] = round((current["nav"] / six_back["nav"] - 1) * 100, 2)

    # 1-year return
    if len(nav_entries) >= 13:
        current = nav_entries[-1]
        twelve_back = nav_entries[-13]
        returns["return_1y"] = round((current["nav"] / twelve_back["nav"] - 1) * 100, 2)

    # YTD return (find last Dec entry or first entry of current year)
    current_year_entries = [h for h in nav_entries if h["date"].startswith("2026")]
    prev_year_dec = [h for h in nav_entries if h["date"].startswith("2025-12")]
    if current_year_entries and prev_year_dec:
        current = current_year_entries[-1]
        dec = prev_year_dec[-1]
        returns["return_ytd"] = round((current["nav"] / dec["nav"] - 1) * 100, 2)

    # ITD return
    if len(nav_entries) >= 2:
        first = nav_entries[0]
        last = nav_entries[-1]
        returns["return_itd"] = round((last["nav"] / first["nav"] - 1) * 100, 2)

    # AUM
    aum = None
    if latest and latest.get("aum_m"):
        aum = latest["aum_m"]
    elif fund.get("known_aum_m"):
        aum = fund["known_aum_m"]

    source_type = "SEC_8K" if fund.get("has_sec_filings") else "WEBSITE"

    nav_entry = {
        "fund_id": fid,
        "fund_name": fund["name"],
        "manager": fund["manager"],
        "is_primary": fund.get("is_primary", False),
        "benchmark_class": fund.get("benchmark_class", ""),
        "nav_per_share": latest["nav"] if latest else fund.get("known_nav"),
        "nav_date": latest["date"] if latest else fund.get("known_nav_date"),
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
        "data_points_count": len(nav_entries),
        "oldest_nav_date": nav_entries[0]["date"] if nav_entries else None,
        "latest_nav_date": nav_entries[-1]["date"] if nav_entries else None
    }
    nav_entry.update(returns)
    nav_data["funds"][fid] = nav_entry

# 4c. Build openinfra_returns.json
returns_data = {"last_updated": now, "funds": {}}
for fid, fund_nav in nav_data["funds"].items():
    r = {
        "data_points_count": fund_nav.get("data_points_count", 0),
        "oldest_nav_date": fund_nav.get("oldest_nav_date"),
        "latest_nav_date": fund_nav.get("latest_nav_date")
    }
    for key in ["return_1m", "return_3m", "return_6m", "return_1y", "return_ytd", "return_itd"]:
        if key in fund_nav:
            r[key] = fund_nav[key]
    returns_data["funds"][fid] = r

# 4d. Build openinfra_metadata.json
metadata = {
    "last_run": now,
    "last_updated": now,
    "run_status": "success",
    "funds_updated": len(config["funds"]),
    "data_sources": ["SEC EDGAR 8-K filings", "Fund websites", "Earnings calls"],
    "next_scheduled_run": None,
    "version": "2.0.0"
}

# 4e. Build openinfra_changes.json
changes = {
    "last_updated": now,
    "changes": [
        {
            "date": now[:10],
            "type": "major_update",
            "description": "Full SEC historical backfill: Ares ACI now has 15 monthly data points (Nov 2024 - Feb 2026), KKR Infra Conglomerate has 31 monthly data points (Aug 2023 - Feb 2026). Added 6m, 1y, YTD returns calculations."
        }
    ]
}

# Write all files to both locations
for path_prefix in [f'{BASE}/data', f'{BASE}/ISQ_Tracker_Deploy/data']:
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

print("\n=== All data files regenerated ===\n")

# Print summary
for fid, fnav in nav_data["funds"].items():
    nav = fnav.get("nav_per_share")
    aum = fnav.get("total_aum_millions")
    date = fnav.get("nav_date")
    pts = fnav.get("data_points_count", 0)
    oldest = fnav.get("oldest_nav_date")
    r1m = fnav.get("return_1m", "N/A")
    r3m = fnav.get("return_3m", "N/A")
    r6m = fnav.get("return_6m", "N/A")
    r1y = fnav.get("return_1y", "N/A")
    rytd = fnav.get("return_ytd", "N/A")
    ritd = fnav.get("return_itd", "N/A")
    print(f"{fnav['fund_name']}:")
    print(f"  NAV: ${nav}  |  AUM: ${aum}M  |  Date: {date}")
    print(f"  Data points: {pts}  |  History: {oldest} → {date}")
    print(f"  Returns: 1M={r1m}% | 3M={r3m}% | 6M={r6m}% | 1Y={r1y}% | YTD={rytd}% | ITD={ritd}%")
    print()
