import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from sec_api import QueryApi, ExtractorApi, XbrlApi
import re, datetime

API_KEY      = os.environ.get("SEC_API_KEY", "")
queryApi     = QueryApi(api_key=API_KEY) if API_KEY else None
extractorApi = ExtractorApi(api_key=API_KEY) if API_KEY else None
xbrlApi      = XbrlApi(api_key=API_KEY) if API_KEY else None

def fetch_10q_data(fund: dict) -> dict | None:
    if not API_KEY:
        return None
    cik = fund.get("cik")
    if not cik:
        return None
    one_year_ago = (datetime.date.today() - datetime.timedelta(days=365)).isoformat()
    today        = datetime.date.today().isoformat()

    query = {
        "query": f'cik:{cik} AND formType:"10-Q"',
        "dateRange": {"startDate": one_year_ago, "endDate": today},
        "from": "0", "size": "1",
        "sort": [{"filingDate": {"order": "desc"}}]
    }
    try:
        response = queryApi.get_filings(query)
        filings  = response.get("filings", [])
    except Exception as e:
        print(f"  10-Q query error for {fund['name']}: {e}")
        return None
    if not filings:
        return None

    filing      = filings[0]
    filing_date = filing.get("filedAt", "")[:10]
    period      = filing.get("periodOfReport", "")

    filing_url = None
    for doc in filing.get("documentFormatFiles", []):
        if (doc.get("type") in ("10-Q","10-Q/A") and
                doc.get("documentUrl","").endswith(".htm")):
            filing_url = doc["documentUrl"]
            break
    if not filing_url:
        return None

    financials = {}
    try:
        xbrl_data = xbrlApi.xbrl_to_json(htm_url=filing_url)
        balance   = xbrl_data.get("BalanceSheets", {})

        for key in ["Assets", "StockholdersEquity", "NetAssets"]:
            entries = balance.get(key, [])
            instant = [e for e in entries
                       if "instant" in e.get("period", {})
                       and "segment" not in e]
            if instant:
                latest   = sorted(instant,
                                   key=lambda x: x["period"]["instant"],
                                   reverse=True)[0]
                raw      = float(latest.get("value", 0))
                decimals = int(latest.get("decimals", 0))
                divisor  = (10 ** abs(decimals)) if decimals < 0 else 1
                label    = "net_assets_millions" if key != "Assets" \
                            else "total_assets_millions"
                financials[label] = round(raw / divisor / 1_000_000, 2)
    except Exception as e:
        print(f"  XBRL failed for {fund['name']}: {e}")

    holdings = []
    try:
        text  = extractorApi.get_section(filing_url, "part1item1", "text")
        lines = text.split("\n") if text else []
        for line in lines:
            line = line.strip()
            if not line or len(line) < 15:
                continue
            val = re.search(r'\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion)?', line)
            if val and len(line) > 20:
                holdings.append({"raw_line": line[:200],
                                  "fair_value_raw": val.group(0)})
    except Exception:
        pass

    return {
        "holdings_date":     period,
        "filing_date":       filing_date,
        "source_url":        filing_url,
        "source_label":      f"SEC 10-Q {period}",
        "sec_filings_index": filing.get("linkToFilingDetails", ""),
        "accession_no":      filing.get("accessionNo", ""),
        "holdings":          holdings[:50],
        "financials":        financials,
    }
