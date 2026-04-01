import sys, os, json, datetime, traceback
from pathlib import Path

# Ensure sibling modules are importable regardless of cwd
sys.path.insert(0, os.path.dirname(__file__))

from sec_8k_extractor     import fetch_sec_8k_data
from sec_10q_extractor    import fetch_10q_data
from sec_form_d_extractor import fetch_form_d_data
from website_scraper      import fetch_website_data
from returns_calculator   import calculate_returns
from changelog_generator  import generate_changelog
from data_validator       import validate_all

# Resolve repo root (parent of scripts/)
REPO_ROOT = Path(__file__).resolve().parent.parent


def write_json(path: str, data: dict):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(data, indent=2, default=str))
    print(f"  Written: {path}")


def merge_seed_data(fund: dict, result: dict) -> dict:
    """Falls back to seed data from config if live fetch returned nothing."""
    if not result.get("nav_per_share") and fund.get("known_nav"):
        result["nav_per_share"] = fund["known_nav"]
        result["nav_date"]      = fund.get("known_nav_date")
    if not result.get("total_aum_millions") and fund.get("known_aum_m"):
        result["total_aum_millions"] = fund["known_aum_m"]
    result["fund_website"]    = fund.get("website", "")
    result["sec_filings_url"] = fund.get("sec_filings_url", "")
    return result


def main():
    config  = json.loads((REPO_ROOT / "scripts/funds_config.json").read_text())
    today   = datetime.date.today().isoformat()
    now     = datetime.datetime.utcnow().isoformat() + "Z"
    results = {}
    errors  = []

    # Load existing historical data
    hist_path = REPO_ROOT / "data/openinfra_historical.json"
    if hist_path.exists():
        historical = json.loads(hist_path.read_text())
    else:
        historical = {"funds": {}}

    for fund in config["funds"]:
        print(f"\nProcessing: {fund['name']}")

        fund_result = {
            "fund_id":          fund["id"],
            "fund_name":        fund["name"],
            "manager":          fund["manager"],
            "is_primary":       fund["is_primary"],
            "benchmark_class":  fund["benchmark_class"],
            "nav_per_share":    None,
            "nav_date":         None,
            "total_aum_millions":          None,
            "gross_subscriptions_millions": None,
            "gross_redemptions_millions":  None,
            "net_flows_millions":          None,
            "distribution_per_share":      None,
            "total_investors":             None,
            "placement_agents":            [],
            "source_type":   None,
            "source_url":    None,
            "source_label":  None,
            "confidence":    "LOW",
            "last_updated":  today,
        }

        fund_result = merge_seed_data(fund, fund_result)

        if fund.get("has_sec_filings") and fund.get("cik"):
            try:
                sec_data = fetch_sec_8k_data(fund)
                if sec_data:
                    fund_result.update(sec_data)
                    fund_result["confidence"] = "HIGH"
                    print(f"  SEC 8-K: NAV={sec_data.get('nav_per_share')}, "
                          f"AUM={sec_data.get('total_aum_millions')}M")
            except Exception as e:
                errors.append(f"SEC 8-K failed for {fund['name']}: {e}")
                traceback.print_exc()

            try:
                form_d = fetch_form_d_data(fund)
                if form_d:
                    fund_result["total_investors"]    = form_d.get("total_investors")
                    fund_result["placement_agents"]   = form_d.get("placement_agents", [])
                    fund_result["form_d_url"]         = form_d.get("form_d_url")
                    fund_result["form_d_label"]       = form_d.get("form_d_label")
                    fund_result["date_of_first_sale"] = form_d.get("date_of_first_sale")
                    print(f"  Form D: investors={form_d.get('total_investors')}")
            except Exception as e:
                print(f"  Form D failed: {e}")
        else:
            try:
                web_data = fetch_website_data(fund)
                if web_data:
                    fund_result.update(web_data)
                    fund_result["confidence"] = "MEDIUM"
            except Exception as e:
                errors.append(f"Website fetch failed for {fund['name']}: {e}")

        results[fund["id"]] = fund_result

    # Load previous snapshot for changelog comparison
    nav_path = REPO_ROOT / "data/openinfra_nav.json"
    previous_data = {}
    if nav_path.exists():
        prev = json.loads(nav_path.read_text())
        for fid, fdata in prev.get("funds", {}).items():
            previous_data[fid] = {
                "nav_per_share":      fdata.get("nav_per_share"),
                "total_aum_millions": fdata.get("total_aum_millions"),
            }

    # Update historical with today's new data point
    for fund in config["funds"]:
        fid     = fund["id"]
        current = results.get(fid, {})

        if fid not in historical["funds"]:
            historical["funds"][fid] = {"historical": [], "fund_name": fund["name"]}

        existing_series = historical["funds"][fid].get("historical", [])

        # Add seed data if not already present
        for seed_point in fund.get("historical_nav", []):
            seed_date = seed_point.get("date")
            if seed_date and not any(h["date"] == seed_date for h in existing_series):
                existing_series.append({
                    "date":           seed_date,
                    "nav":            seed_point.get("nav"),
                    "aum_m":          seed_point.get("aum_m"),
                    "subs_m":         seed_point.get("subs_m"),
                    "redemptions_m":  seed_point.get("redemptions_m"),
                    "dist_per_share": seed_point.get("dist_per_share"),
                    "source_label":   seed_point.get("source_label", "Seed"),
                    "source_url":     seed_point.get("source_url", ""),
                })

        # Add today's freshly fetched data point
        current_nav  = current.get("nav_per_share")
        current_date = current.get("nav_date")
        if current_nav and current_date:
            if not any(h["date"] == current_date for h in existing_series):
                existing_series.append({
                    "date":           current_date,
                    "nav":            current_nav,
                    "aum_m":          current.get("total_aum_millions"),
                    "subs_m":         current.get("gross_subscriptions_millions"),
                    "redemptions_m":  current.get("gross_redemptions_millions"),
                    "dist_per_share": current.get("distribution_per_share"),
                    "source_label":   current.get("source_label", "Daily fetch"),
                    "source_url":     current.get("source_url", ""),
                })

        # Sort by date
        historical["funds"][fid]["historical"] = sorted(
            existing_series, key=lambda x: x["date"]
        )

        # Calculate returns from complete series
        series  = historical["funds"][fid]["historical"]
        returns = calculate_returns(fid, series)
        results[fid].update(returns)

    # Generate changelog
    changes = generate_changelog(results, previous_data)

    # Validate
    warnings = validate_all(results)
    if warnings:
        print(f"\nValidation warnings: {warnings}")

    # Write all output files
    write_json(str(REPO_ROOT / "data/openinfra_nav.json"),
               {"last_updated": now, "funds": results})
    write_json(str(REPO_ROOT / "data/openinfra_historical.json"), historical)
    write_json(str(REPO_ROOT / "data/openinfra_returns.json"),
               {"last_updated": now, "funds": {
                   fid: {k: v for k, v in results[fid].items()
                          if k.startswith("return_") or k in
                          ["data_points_count","oldest_nav_date","latest_nav_date","annualised_itd"]}
                   for fid in results
               }})
    write_json(str(REPO_ROOT / "data/openinfra_changes.json"),
               {"generated_at": now, "period_days": 7, "changes": changes})
    write_json(str(REPO_ROOT / "data/openinfra_metadata.json"), {
        "last_updated":             now,
        "next_update":              "tomorrow 06:30 UTC",
        "funds_monitored":          len(config["funds"]),
        "funds_with_sec_filings":   sum(1 for f in config["funds"]
                                        if f.get("has_sec_filings")),
        "run_status":               "COMPLETE" if not errors else "PARTIAL",
        "errors":                   errors,
        "validation_warnings":      warnings,
    })

    print(f"\nCompleted. {len(results)} funds. "
          f"{len(changes)} changes. {len(errors)} errors.")


if __name__ == "__main__":
    main()
