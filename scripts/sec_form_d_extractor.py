import os, requests

API_KEY = os.environ.get("SEC_API_KEY", "")

def fetch_form_d_data(fund: dict) -> dict | None:
    if not API_KEY:
        return None
    fund_name = fund["name"]
    cik       = fund.get("cik")

    query_str = (f"primaryIssuer.cik:{cik}" if cik
                 else f'primaryIssuer.entityName:"{fund_name.replace(chr(34), "")}"')

    url     = f"https://api.sec-api.io/form-d?token={API_KEY}"
    payload = {"query": query_str, "from": "0", "size": "5",
                "sort": [{"filedAt": {"order": "desc"}}]}

    try:
        r = requests.post(url, json=payload,
                           headers={"Content-Type": "application/json"}, timeout=15)
        r.raise_for_status()
        offerings = r.json().get("offerings", [])
    except Exception as e:
        print(f"  Form D error for {fund_name}: {e}")
        return None

    if not offerings:
        return None

    offering      = offerings[0]
    offering_data = offering.get("offeringData", {})
    sales_amounts = offering_data.get("offeringSalesAmounts", {})
    investors     = offering_data.get("investors", {})
    filing_info   = offering_data.get("typeOfFiling", {})
    issuer        = offering.get("primaryIssuer", {})
    comp_list     = offering_data.get("salesCompensationList", {})

    total_sold_raw = sales_amounts.get("totalAmountSold")
    total_sold_m   = (round(total_sold_raw / 1_000_000, 2)
                      if total_sold_raw and total_sold_raw > 0 else None)

    agents = [r.get("recipientName","") for r in comp_list.get("recipient",[])
              if r.get("recipientName")]

    return {
        "total_amount_sold_m":    total_sold_m,
        "total_investors":        investors.get("totalNumberAlreadyInvested"),
        "date_of_first_sale":     filing_info.get("dateOfFirstSale",{}).get("value"),
        "min_investment_usd":     offering_data.get("minimumInvestmentAccepted"),
        "placement_agents":       agents,
        "form_d_accession":       offering.get("accessionNo"),
        "form_d_filed_at":        offering.get("filedAt","")[:10],
        "form_d_url": (
            f"https://www.sec.gov/cgi-bin/browse-edgar?"
            f"action=getcompany&CIK={issuer.get('cik','')}&type=D"
            if issuer.get("cik") else ""
        ),
        "form_d_label": f"SEC Form D filed {offering.get('filedAt','')[:10]}",
    }
