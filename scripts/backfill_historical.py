#!/usr/bin/env python3
"""
backfill_historical.py

Run once after deployment to populate openinfra_historical.json
with all historical monthly 8-K NAV data since each fund's inception.

Usage: SEC_API_KEY=your_key python scripts/backfill_historical.py
"""

import json, os, sys, time, datetime, re
from pathlib import Path
from sec_api import QueryApi, ExtractorApi

# Resolve repo root (parent of scripts/)
REPO_ROOT = Path(__file__).resolve().parent.parent

API_KEY = os.environ.get("SEC_API_KEY", "")
queryApi     = QueryApi(api_key=API_KEY) if API_KEY else None
extractorApi = ExtractorApi(api_key=API_KEY) if API_KEY else None

DELAY_BETWEEN_CALLS = 0.4  # seconds — respects API rate limits

def get_all_8k_filings(cik: str, start_date: str) -> list:
    """
    Retrieves ALL 8-K filings for a given CIK since start_date.
    Paginates automatically to get the complete history.
    """
    all_filings = []
    from_index  = 0
    today       = datetime.date.today().isoformat()

    while True:
        query = {
            "query": f'cik:{cik} AND formType:"8-K"',
            "dateRange": {"startDate": start_date, "endDate": today},
            "from": str(from_index),
            "size": "50",
            "sort": [{"filingDate": {"order": "asc"}}]
        }
        try:
            response = queryApi.get_filings(query)
            batch    = response.get("filings", [])
            total    = response.get("total", {}).get("value", 0)
        except Exception as e:
            print(f"  Query error: {e}")
            break

        if not batch:
            break

        all_filings.extend(batch)
        from_index += len(batch)
        print(f"  Retrieved {from_index}/{total} filings...")

        if from_index >= total:
            break

        time.sleep(DELAY_BETWEEN_CALLS)

    return all_filings


def get_filing_url(filing: dict) -> str | None:
    """Extracts the direct .htm document URL from a filing object."""
    for doc in filing.get("documentFormatFiles", []):
        if (doc.get("type") in ("8-K", "8-K/A") and
                doc.get("documentUrl", "").endswith(".htm")):
            return doc["documentUrl"]
    return filing.get("linkToFilingDetails")


def extract_nav_from_section(text: str) -> float | None:
    """Extracts NAV per share from 8-K section text."""
    patterns = [
        # Standard pattern: "NAV per share was $X.XXXX"
        r'(?:Transactional\s+)?(?:Net Asset Value|NAV)\s+per\s+(?:Common\s+)?'
        r'(?:Share|Unit)\s*(?:was|is|of|:)?\s*\$?\s*([\d,]+\.[\d]{2,6})',
        # Table pattern: "$ 25.4101"
        r'\|\s*\$\s*\|\s*([\d]{2,3}\.[\d]{2,6})',
        # Inline pattern: "$25.4101 per share"
        r'\$\s*([\d]{2,3}\.[\d]{2,6})\s+per\s+(?:common\s+)?(?:share|unit)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1).replace(",", ""))
            except ValueError:
                continue
    return None


def extract_aum_from_section(text: str) -> float | None:
    """Extracts aggregate NAV (total AUM) in millions from 8-K section text."""
    patterns = [
        r'aggregate\s+(?:net asset value|NAV)\s+(?:was|is)\s+approximately'
        r'\s+\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion)',
        r'aggregate\s+NAV\s+was\s+approximately\s+\$\s*([\d,]+(?:\.\d+)?)'
        r'\s*(million|billion)',
        r'total\s+(?:NAV|net assets?)\s+(?:was|of|is)\s+approximately'
        r'\s+\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                value = float(match.group(1).replace(",", ""))
                unit  = match.group(2).lower()
                return round(value * 1000 if unit == "billion" else value, 2)
            except ValueError:
                continue
    return None


def extract_subscriptions_from_section(text: str) -> float | None:
    """Extracts monthly gross subscriptions in millions."""
    patterns = [
        r'aggregate\s+(?:purchase\s+)?(?:price|consideration)\s+of'
        r'\s+(?:approximately\s+)?\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion)',
        r'sold\s+interests\s+for\s+aggregate\s+consideration\s+of\s+approximately'
        r'\s+\$\s*([\d,]+(?:\.\d+)?)',
        r'gross\s+(?:proceeds|subscriptions)\s+of\s+\$\s*([\d,]+(?:\.\d+)?)'
        r'\s*(million|billion)',
        r'aggregate\s+purchase\s+price\s+of\s+\$\s*([\d,]+(?:\.\d+)?)\s*million',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                value = float(match.group(1).replace(",", ""))
                # If no unit captured, check if value is in raw dollars vs millions
                if len(match.groups()) > 1 and match.group(2):
                    unit = match.group(2).lower()
                    return round(value * 1000 if unit == "billion" else value, 2)
                # Value already in millions (pattern without unit group)
                return round(value, 2)
            except (ValueError, IndexError):
                continue
    return None


def extract_distribution_from_section(text: str) -> float | None:
    """Extracts distribution per share from 8-K section text."""
    patterns = [
        r'(?:gross\s+)?distribution\s+(?:of\s+)?\$\s*([\d]+\.[\d]{4,6})'
        r'\s+per\s+(?:common\s+)?(?:share|unit)',
        r'distribution\s+per\s+(?:common\s+)?(?:share|unit)\s+(?:of\s+)?\$\s*([\d]+\.[\d]{4,6})',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                continue
    return None


def extract_nav_date(text: str, period: str) -> str:
    """Extracts the NAV 'as of' date from section text."""
    patterns = [
        r'(?:NAV|net asset value).*?(?:as\s+of|for\s+the\s+(?:month|period)\s+ended?)'
        r'\s+([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})',
        r'as\s+of\s+([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                date_str = match.group(1).replace(",", "").strip()
                for fmt in ["%B %d %Y", "%B %Y"]:
                    try:
                        from datetime import datetime as dt
                        return dt.strptime(date_str, fmt).date().isoformat()
                    except ValueError:
                        continue
            except Exception:
                continue
    return period


def process_filing(filing: dict, fund_id: str) -> dict | None:
    """
    Processes a single 8-K filing and extracts all available data.
    Returns a historical data point dict or None if extraction fails.
    """
    filing_url  = get_filing_url(filing)
    filing_date = filing.get("filedAt", "")[:10]
    period      = filing.get("periodOfReport", "")

    if not filing_url:
        return None

    # Try Item 8.01 first (where open-ended funds publish NAV)
    section_text = ""
    for item_code in ["8-1", "7-1", "3-2"]:
        text = ""
        for attempt in range(3):
            try:
                text = extractorApi.get_section(filing_url, item_code, "text")
                if text and text.strip().lower() != "processing" and len(text) > 30:
                    break
                time.sleep(0.5)
            except Exception:
                time.sleep(0.5)

        if text and len(text) > 30:
            section_text = text
            break

    if not section_text:
        print(f"    No extractable text from {filing_url[:60]}...")
        return None

    nav          = extract_nav_from_section(section_text)
    aum          = extract_aum_from_section(section_text)
    subs         = extract_subscriptions_from_section(section_text)
    distribution = extract_distribution_from_section(section_text)
    nav_date     = extract_nav_date(section_text, period)

    if not nav and not aum:
        print(f"    No NAV or AUM found in filing for period {period}")
        return None

    return {
        "date":             nav_date or period or filing_date,
        "nav":              nav,
        "aum_m":            aum,
        "subs_m":           subs,
        "redemptions_m":    None,
        "dist_per_share":   distribution,
        "source_label":     f"SEC 8-K filed {filing_date}",
        "source_url":       filing_url,
        "filing_date":      filing_date,
        "accession_no":     filing.get("accessionNo", ""),
        "backfilled":       True,
    }


def merge_with_seed(backfilled_points: list, seed_points: list) -> list:
    """
    Merges backfilled data with seed data from funds_config.json.
    Backfilled data takes precedence over seed data for the same date.
    Seed data fills in months the backfill could not extract.
    """
    merged = {}

    # Load seed data first (lower priority)
    for point in seed_points:
        date = point.get("date")
        if date:
            merged[date] = {
                "date":           date,
                "nav":            point.get("nav"),
                "aum_m":          point.get("aum_m"),
                "subs_m":         point.get("subs_m"),
                "redemptions_m":  point.get("redemptions_m"),
                "dist_per_share": point.get("dist_per_share"),
                "source_label":   point.get("source_label", "Seed data"),
                "source_url":     point.get("source_url", ""),
            }

    # Overwrite/enrich with backfilled data (higher priority)
    for point in backfilled_points:
        date = point.get("date")
        if not date:
            continue
        existing = merged.get(date, {})
        merged[date] = {
            "date":           date,
            "nav":            point.get("nav") or existing.get("nav"),
            "aum_m":          point.get("aum_m") or existing.get("aum_m"),
            "subs_m":         point.get("subs_m") or existing.get("subs_m"),
            "redemptions_m":  point.get("redemptions_m") or existing.get("redemptions_m"),
            "dist_per_share": point.get("dist_per_share") or existing.get("dist_per_share"),
            "source_label":   point.get("source_label", "SEC 8-K"),
            "source_url":     point.get("source_url", existing.get("source_url", "")),
            "backfilled":     True,
        }

    # Return sorted by date ascending
    return sorted(merged.values(), key=lambda x: x["date"])


def main():
    config_path = REPO_ROOT / "scripts/funds_config.json"
    hist_path   = REPO_ROOT / "data/openinfra_historical.json"

    config = json.loads(config_path.read_text())

    # Load or initialise historical data
    if hist_path.exists():
        historical = json.loads(hist_path.read_text())
    else:
        historical = {"funds": {}}

    # Process each SEC-registered fund
    sec_funds = [f for f in config["funds"] if f.get("has_sec_filings") and f.get("cik")]

    for fund in sec_funds:
        fund_id    = fund["id"]
        fund_name  = fund["name"]
        cik        = fund["cik"]
        start_date = fund.get("backfill_start_date", "2024-01-01")
        seed_data  = fund.get("historical_nav", [])

        print(f"\nBackfilling: {fund_name}")
        print(f"  CIK: {cik} | Start date: {start_date}")

        # Retrieve all historical 8-K filings
        filings = get_all_8k_filings(cik, start_date)
        print(f"  Found {len(filings)} 8-K filings since {start_date}")

        # Process each filing
        backfilled = []
        for i, filing in enumerate(filings):
            period = filing.get("periodOfReport", "unknown")
            print(f"  Processing filing {i+1}/{len(filings)}: period {period}")

            point = process_filing(filing, fund_id)
            if point:
                backfilled.append(point)
                print(f"    Extracted: NAV={point.get('nav')}, "
                      f"AUM={point.get('aum_m')}M, Subs={point.get('subs_m')}M")
            else:
                print(f"    Skipped — no extractable data")

            # Rate limit: 0.4s between Extractor API calls
            time.sleep(DELAY_BETWEEN_CALLS)

        print(f"  Successfully extracted {len(backfilled)}/{len(filings)} data points")

        # Merge backfilled data with seed data
        merged = merge_with_seed(backfilled, seed_data)
        print(f"  Final series: {len(merged)} data points from "
              f"{merged[0]['date'] if merged else 'N/A'} to "
              f"{merged[-1]['date'] if merged else 'N/A'}")

        # Store in historical
        if fund_id not in historical["funds"]:
            historical["funds"][fund_id] = {}
        historical["funds"][fund_id]["historical"] = merged
        historical["funds"][fund_id]["fund_name"]  = fund_name
        historical["funds"][fund_id]["last_backfill"] = datetime.date.today().isoformat()

    # For non-SEC funds, load seed data only
    non_sec_funds = [f for f in config["funds"] if not f.get("has_sec_filings")]
    for fund in non_sec_funds:
        fund_id   = fund["id"]
        fund_name = fund["name"]
        seed_data = fund.get("historical_nav", [])

        print(f"\nLoading seed data for: {fund_name} ({len(seed_data)} points)")

        if fund_id not in historical["funds"]:
            historical["funds"][fund_id] = {}
        historical["funds"][fund_id]["historical"] = seed_data
        historical["funds"][fund_id]["fund_name"]  = fund_name
        historical["funds"][fund_id]["source"] = "website_or_manual"

    # Write updated historical file
    hist_path.parent.mkdir(parents=True, exist_ok=True)
    hist_path.write_text(json.dumps(historical, indent=2))

    # Print summary
    print("\n" + "="*60)
    print("BACKFILL COMPLETE")
    print("="*60)
    for fund_id, data in historical["funds"].items():
        series = data.get("historical", [])
        dates  = [p["date"] for p in series if p.get("date")]
        if dates:
            print(f"  {fund_id}: {len(series)} points | "
                  f"{min(dates)} to {max(dates)}")
        else:
            print(f"  {fund_id}: no data points")

    print(f"\nHistorical data written to: {hist_path}")
    print("Run the daily fetcher to add new months going forward:")
    print("  python scripts/fetch_openinfra_data.py")


if __name__ == "__main__":
    main()
