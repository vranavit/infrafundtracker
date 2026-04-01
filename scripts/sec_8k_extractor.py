import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from sec_api import QueryApi, ExtractorApi
import re, datetime, time

API_KEY  = os.environ.get("SEC_API_KEY", "")
queryApi = QueryApi(api_key=API_KEY) if API_KEY else None
extractorApi = ExtractorApi(api_key=API_KEY) if API_KEY else None

def fetch_sec_8k_data(fund: dict) -> dict | None:
    if not API_KEY:
        return None
    cik = fund.get("cik")
    if not cik:
        return None
    thirty_five_days_ago = (
        datetime.date.today() - datetime.timedelta(days=35)
    ).isoformat()
    today = datetime.date.today().isoformat()

    query = {
        "query": f'cik:{cik} AND formType:"8-K"',
        "dateRange": {"startDate": thirty_five_days_ago, "endDate": today},
        "from": "0",
        "size": "5",
        "sort": [{"filingDate": {"order": "desc"}}]
    }

    try:
        response = queryApi.get_filings(query)
        filings  = response.get("filings", [])
    except Exception as e:
        print(f"  Query API error for CIK {cik}: {e}")
        return None

    if not filings:
        return None

    filing      = filings[0]
    filing_date = filing.get("filedAt", "")[:10]
    period      = filing.get("periodOfReport", "")

    filing_url = None
    for doc in filing.get("documentFormatFiles", []):
        if (doc.get("type") in ("8-K","8-K/A") and
                doc.get("documentUrl","").endswith(".htm")):
            filing_url = doc["documentUrl"]
            break
    if not filing_url:
        filing_url = filing.get("linkToFilingDetails", "")

    if not filing_url:
        return None

    section_text = ""
    for item_code in ["8-1", "7-1"]:
        for attempt in range(3):
            try:
                text = extractorApi.get_section(filing_url, item_code, "text")
                if text and text.strip().lower() != "processing" and len(text) > 30:
                    section_text = text
                    break
                time.sleep(0.75)
            except Exception:
                time.sleep(0.75)
        if section_text:
            break

    if not section_text:
        return None

    from backfill_historical import (
        extract_nav_from_section, extract_aum_from_section,
        extract_subscriptions_from_section, extract_distribution_from_section,
        extract_nav_date
    )

    nav          = extract_nav_from_section(section_text)
    aum          = extract_aum_from_section(section_text)
    subs         = extract_subscriptions_from_section(section_text)
    distribution = extract_distribution_from_section(section_text)
    nav_date     = extract_nav_date(section_text, period)

    net_flows = subs

    return {
        "nav_per_share":               nav,
        "nav_date":                    nav_date or period,
        "total_aum_millions":          aum,
        "gross_subscriptions_millions": subs,
        "gross_redemptions_millions":  None,
        "net_flows_millions":          net_flows,
        "distribution_per_share":      distribution,
        "source_type":   "SEC_8K",
        "source_url":    filing_url,
        "source_label":  f"SEC 8-K filed {filing_date}",
        "sec_filings_index": filing.get("linkToFilingDetails", ""),
        "accession_no":  filing.get("accessionNo", ""),
        "filing_date":   filing_date,
        "period_of_report": period,
    }
