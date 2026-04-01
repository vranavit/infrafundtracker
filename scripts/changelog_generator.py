import datetime


def generate_changelog(current_data: dict, previous_data: dict) -> list:
    changes = []
    today   = datetime.date.today().isoformat()

    for fund_id, current in current_data.items():
        previous  = previous_data.get(fund_id, {})
        fund_name = current.get("fund_name", fund_id)

        nav_curr = current.get("nav_per_share")
        nav_prev = previous.get("nav_per_share") or previous.get("nav")
        if nav_curr and nav_prev and nav_prev > 0:
            change = (nav_curr - nav_prev) / nav_prev
            if abs(change) >= 0.0025:
                changes.append({
                    "fund_id":      fund_id,
                    "fund_name":    fund_name,
                    "change_type":  "NAV_UPDATE",
                    "old_value":    nav_prev,
                    "new_value":    nav_curr,
                    "change_pct":   round(change * 100, 3),
                    "date":         current.get("nav_date", today),
                    "source_url":   current.get("source_url", ""),
                    "source_label": current.get("source_label", ""),
                    "significance": "HIGH" if abs(change) >= 0.01 else "MEDIUM",
                    "description": (
                        f"{fund_name} NAV "
                        f"{'increased' if change > 0 else 'decreased'} "
                        f"{abs(change)*100:.2f}% to ${nav_curr:.4f} "
                        f"as of {current.get('nav_date','')}"
                    ),
                    "detected_at": today,
                })

        aum_curr = current.get("total_aum_millions")
        aum_prev = previous.get("total_aum_millions") or previous.get("aum_m")
        if aum_curr and aum_prev and aum_prev > 0:
            change = (aum_curr - aum_prev) / aum_prev
            if abs(change) >= 0.02:
                changes.append({
                    "fund_id":      fund_id,
                    "fund_name":    fund_name,
                    "change_type":  "AUM_UPDATE",
                    "old_value":    aum_prev,
                    "new_value":    aum_curr,
                    "change_pct":   round(change * 100, 2),
                    "date":         today,
                    "source_url":   current.get("source_url", ""),
                    "source_label": current.get("source_label", ""),
                    "significance": "HIGH" if abs(change) >= 0.10 else "MEDIUM",
                    "description": (
                        f"{fund_name} AUM "
                        f"{'grew' if change > 0 else 'declined'} "
                        f"{abs(change)*100:.1f}% to ${aum_curr:,.0f}M"
                    ),
                    "detected_at": today,
                })

    significance_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    changes.sort(key=lambda x: (
        significance_order.get(x["significance"], 2), x["fund_name"]
    ))
    return changes
