def validate_all(results: dict) -> list:
    warnings = []
    for fund_id, fund in results.items():
        nav = fund.get("nav_per_share")
        aum = fund.get("total_aum_millions")
        if nav is not None:
            if nav <= 0:
                warnings.append(f"{fund_id}: NAV zero or negative ({nav})")
            if nav > 200:
                warnings.append(f"{fund_id}: NAV suspiciously high ({nav})")
        if aum is not None and aum > 50000:
            warnings.append(f"{fund_id}: AUM suspiciously high ({aum}M)")
        if not fund.get("source_url"):
            warnings.append(f"{fund_id}: No source URL")
    return warnings
