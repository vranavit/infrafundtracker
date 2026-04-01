#!/usr/bin/env python3
"""
Inject AUM data collected from SEC EX-99.1 exhibits into funds_config.json.
- KKR Infra Conglomerate: 14 AUM data points from EX-99.1 exhibits
- Brookfield Infrastructure Income: 4 historical NAV/AUM data points from N-CSR/N-CSRS filings
"""
import json

BASE = '/sessions/trusting-eager-galileo/infra_fresh'

with open(f'{BASE}/scripts/funds_config.json') as f:
    config = json.load(f)

# ============================================================
# KKR Infrastructure Conglomerate - 14 AUM data points
# From EX-99.1 exhibits: Transactional NAV (in thousands) = total AUM
# ============================================================
kkr_aum_data = {
    # date -> (aum_m, total_shares)
    "2024-06-30": (1644.0, 60472340),
    "2024-07-31": (2418.0, 87078576),
    "2024-08-31": (2582.0, 91697259),
    "2024-09-30": (2260.0, 82206860),
    "2024-12-31": (2735.0, 96824157),
    "2025-01-31": (3519.0, 123271259),
    "2025-03-31": (3238.0, 114085418),
    "2025-04-30": (4246.0, 146071324),
    "2025-06-30": (3986.0, 138000777),
    "2025-09-30": (4682.0, 156970227),
    "2025-10-31": (5924.0, 199028037),
    "2025-11-30": (6243.0, 208179905),
    "2026-01-31": (6810.0, 226105449),
    "2026-02-28": (7029.0, 232031512),
}

kkr_fund = next(f for f in config["funds"] if f["id"] == "kkr_infra_conglomerate")
updated_kkr = 0
for h in kkr_fund["historical_nav"]:
    dt = h["date"]
    if dt in kkr_aum_data:
        aum_m, total_shares = kkr_aum_data[dt]
        h["aum_m"] = aum_m
        h["total_shares"] = total_shares
        if not h.get("source_label", "").startswith("SEC 8-K EX"):
            h["source_label"] = f"SEC 8-K EX-99.1 {dt[:7]}"
        updated_kkr += 1

print(f"KKR: Updated {updated_kkr} entries with AUM data")

# ============================================================
# Brookfield Infrastructure Income Fund - Historical NAV/AUM
# From N-CSR and N-CSRS filings (semi-annual/annual reports)
# ============================================================
brookfield_fund = next(f for f in config["funds"] if f["id"] == "brookfield_infra_income")

# Replace single entry with full history from N-CSR filings
brookfield_fund["historical_nav"] = [
    {
        "date": "2023-12-31",
        "nav": 10.07,
        "aum_m": 1590.0,
        "source_label": "N-CSR Annual Report Dec 2023",
        "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=1955857&type=N-CSR"
    },
    {
        "date": "2024-06-30",
        "nav": 10.25,
        "aum_m": 1900.0,
        "source_label": "N-CSRS Semi-Annual Jun 2024",
        "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=1955857&type=N-CSRS"
    },
    {
        "date": "2024-12-31",
        "nav": 10.44,
        "aum_m": 2395.0,
        "source_label": "N-CSR Annual Report Dec 2024",
        "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=1955857&type=N-CSR"
    },
    {
        "date": "2025-06-30",
        "nav": 10.63,
        "aum_m": 2978.0,
        "source_label": "N-CSRS Semi-Annual Jun 2025",
        "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=1955857&type=N-CSRS"
    },
    {
        "date": "2025-12-31",
        "nav": 10.89,
        "aum_m": 4786.0,
        "source_label": "N-CSR Annual Report Dec 2025",
        "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=1955857&type=N-CSR"
    }
]
brookfield_fund["known_nav"] = 10.89
brookfield_fund["known_nav_date"] = "2025-12-31"
brookfield_fund["known_aum_m"] = 4786.0

print(f"Brookfield: Set {len(brookfield_fund['historical_nav'])} historical entries")

# ============================================================
# Write updated config
# ============================================================
with open(f'{BASE}/scripts/funds_config.json', 'w') as f:
    json.dump(config, f, indent=2)

print("Updated funds_config.json successfully")

# Quick validation
for fund in config["funds"]:
    fid = fund["id"]
    hist = fund["historical_nav"]
    aum_count = sum(1 for h in hist if h.get("aum_m"))
    nav_count = sum(1 for h in hist if h.get("nav"))
    print(f"  {fund['name']}: {len(hist)} entries, {nav_count} with NAV, {aum_count} with AUM")
