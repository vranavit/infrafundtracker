import datetime


def calculate_returns(fund_id: str, historical: list) -> dict:
    """
    Calculates month-by-month returns from the complete historical NAV series.
    Returns dict with 1M, 3M, 6M, 1Y, YTD, ITD, and annualised ITD returns.
    """
    if not historical or len(historical) < 2:
        return {"data_points_count": len(historical) if historical else 0}

    series = sorted(
        [h for h in historical if h.get("nav")],
        key=lambda x: x["date"]
    )
    if len(series) < 2:
        return {"data_points_count": len(series)}

    current      = series[-1]
    current_nav  = current["nav"]
    current_date = current["date"]

    def nav_n_months_ago(n: int) -> float | None:
        target = (
            datetime.date.fromisoformat(current_date)
            - datetime.timedelta(days=n * 30)
        ).isoformat()
        candidates = [s for s in series if s["date"] <= target and s.get("nav")]
        return candidates[-1]["nav"] if candidates else None

    def calc_return(nav_then: float | None) -> float | None:
        if nav_then and nav_then > 0:
            return round((current_nav / nav_then) - 1, 6)
        return None

    inception_nav          = series[0]["nav"]
    months_since_inception = max(1, len(series))
    itd_return             = calc_return(inception_nav)
    annualised_itd         = None

    if itd_return is not None:
        annualised_itd = round(
            (1 + itd_return) ** (12 / months_since_inception) - 1, 6
        )

    current_year   = current_date[:4]
    ytd_candidates = [s for s in series
                      if s["date"].startswith(current_year) and s.get("nav")]
    ytd_nav        = ytd_candidates[0]["nav"] if ytd_candidates else None

    return {
        "return_1m":         calc_return(nav_n_months_ago(1)),
        "return_3m":         calc_return(nav_n_months_ago(3)),
        "return_6m":         calc_return(nav_n_months_ago(6)),
        "return_1y":         calc_return(nav_n_months_ago(12)),
        "return_ytd":        calc_return(ytd_nav),
        "return_itd":        itd_return,
        "annualised_itd":    annualised_itd,
        "data_points_count": len(series),
        "oldest_nav_date":   series[0]["date"],
        "latest_nav_date":   current_date,
    }
