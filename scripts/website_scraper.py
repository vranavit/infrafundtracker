import requests, re
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "InfraFundTracker/1.0 research@infraintelligence.com",
    "Accept":     "text/html,application/xhtml+xml"
}


def fetch_website_data(fund: dict) -> dict | None:
    scrapers = {
        "kkr_kif_lux": scrape_kkr_luxembourg,
    }
    scraper = scrapers.get(fund["id"])
    if scraper:
        try:
            result = scraper(fund)
            if result:
                return result
        except Exception as e:
            print(f"  Live scrape failed for {fund['name']}: {e}")

    return fallback_to_seed(fund)


def scrape_kkr_luxembourg(fund: dict) -> dict | None:
    url = "https://kseries.kkr.com/kif/"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text()
        match = re.search(r'LU2575970327.*?([\d]{2,3}\.[\d]{2,4})', text, re.DOTALL)
        if match:
            return {
                "nav_per_share": float(match.group(1)),
                "source_type":   "WEBSITE",
                "source_url":    url,
                "source_label":  "KSeries Website",
                "confidence":    "MEDIUM",
            }
    except Exception:
        pass
    return None


def fallback_to_seed(fund: dict) -> dict:
    return {
        "nav_per_share":      fund.get("known_nav"),
        "nav_date":           fund.get("known_nav_date"),
        "total_aum_millions": fund.get("known_aum_m"),
        "source_type":        "WEBSITE",
        "source_url":         fund.get("source_url", fund.get("website", "")),
        "source_label":       fund.get("source_label", "Manual"),
        "confidence":         "MEDIUM",
    }
