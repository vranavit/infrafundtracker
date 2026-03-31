"""
Alert Generator - Daily Brief Generation

Generates executive summary, lead cards, and markdown briefs for
daily signal engine results.
"""

import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Dict, Any, Optional

from signals.signal_detector import Signal
from scrapers.adv_parser import FirmRecord

logger = logging.getLogger(__name__)


@dataclass
class LeadCard:
    """Single prospect lead card with all signals and metrics."""
    firm_name: str
    location: str
    aum: float
    avg_per_client: float
    tier: str
    score: float
    qp_score: float
    platform_access: str  # "Tier 1", "Tier 2", etc.
    signals_fired: List[Dict[str, str]]  # List of {name, evidence, talking_point}
    recommended_action: str
    registration_date: Optional[str] = None
    contact_info: Optional[str] = None


@dataclass
class DailyBrief:
    """Complete daily brief with all results."""
    run_date: datetime
    executive_summary: str
    tier_1_leads: List[LeadCard]
    tier_2_leads: List[LeadCard]
    tier_3_leads: List[LeadCard]
    new_signals_this_week: Dict[str, int]  # Signal name -> count
    platform_summary: Dict[str, int]  # Platform -> firm count
    geography_summary: Dict[str, int]  # State -> firm count
    total_signals_fired: int
    total_new_signals: int
    total_firms_analyzed: int


class AlertGenerator:
    """
    Generates daily briefs and reports from signal detection results.

    Creates both structured (dict) and formatted (markdown) outputs
    for dashboards, emails, and Slack notifications.
    """

    def __init__(self):
        """Initialize alert generator."""
        logger.info("Initialized AlertGenerator")

    def generate_daily_brief(
        self,
        run_date: datetime,
        all_results: List[Dict[str, Any]]
    ) -> DailyBrief:
        """
        Generate complete daily brief from scoring results.

        Args:
            run_date: Date of the run
            all_results: List of result dicts with: firm, score, signals, qp_score, platform_score

        Returns:
            DailyBrief with organized results
        """
        # Separate results by tier
        tier_1_results = [r for r in all_results if r["tier"] == "tier_1"]
        tier_2_results = [r for r in all_results if r["tier"] == "tier_2"]
        tier_3_results = [r for r in all_results if r["tier"] == "tier_3"]

        # Build lead cards
        tier_1_leads = [self._build_lead_card(r) for r in tier_1_results]
        tier_2_leads = [self._build_lead_card(r) for r in tier_2_results]
        tier_3_leads = [self._build_lead_card(r) for r in tier_3_results]

        # Compute summaries
        new_signals_this_week = self._compute_signal_summary(all_results)
        platform_summary = self._compute_platform_summary(all_results)
        geography_summary = self._compute_geography_summary(all_results)

        # Count signals
        total_signals = sum(len(r.get("signals", [])) for r in all_results)
        total_new_signals = sum(
            len([s for s in r.get("signals", []) if s.is_new])
            for r in all_results
        )

        # Generate executive summary
        exec_summary = self._generate_executive_summary(
            len(tier_1_leads),
            len(tier_2_leads),
            len(tier_3_leads),
            total_signals,
            total_new_signals
        )

        brief = DailyBrief(
            run_date=run_date,
            executive_summary=exec_summary,
            tier_1_leads=tier_1_leads,
            tier_2_leads=tier_2_leads,
            tier_3_leads=tier_3_leads,
            new_signals_this_week=new_signals_this_week,
            platform_summary=platform_summary,
            geography_summary=geography_summary,
            total_signals_fired=total_signals,
            total_new_signals=total_new_signals,
            total_firms_analyzed=len(all_results)
        )

        logger.info(
            f"Generated daily brief for {run_date.date()} - "
            f"Tier 1: {len(tier_1_leads)}, Tier 2: {len(tier_2_leads)}, "
            f"Tier 3: {len(tier_3_leads)}"
        )

        return brief

    def _build_lead_card(self, result: Dict[str, Any]) -> LeadCard:
        """Build a single lead card from result dict."""
        firm = result["firm"]
        signals = result["signals"]

        # Build signals list
        signals_fired = [
            {
                "name": s.name,
                "evidence": s.evidence,
                "talking_point": s.talking_point
            }
            for s in signals
        ]

        # Determine recommended action
        recommended_action = self._get_recommended_action(
            result["tier"],
            len(signals),
            any(s.is_new for s in signals)
        )

        return LeadCard(
            firm_name=firm.firm_name,
            location=f"{firm.state}, {firm.country}" if firm.state else firm.country,
            aum=firm.aum_total,
            avg_per_client=firm.avg_aum_per_client,
            tier=result["tier"],
            score=result["score"],
            qp_score=result["qp_score"],
            platform_access=self._format_platform_tier(result.get("platform_tier")),
            signals_fired=signals_fired,
            recommended_action=recommended_action,
            registration_date=(
                firm.registration_date.isoformat()
                if firm.registration_date
                else None
            ),
            contact_info=firm.website
        )

    def _get_recommended_action(
        self,
        tier: str,
        signal_count: int,
        has_new_signals: bool
    ) -> str:
        """Generate recommended action based on tier and signals."""
        if tier == "tier_1":
            action = "IMMEDIATE OUTREACH"
            if has_new_signals and signal_count >= 3:
                return f"{action} - Hot prospect with {signal_count} signals"
            return f"{action} - {signal_count} signals detected"
        elif tier == "tier_2":
            action = "PRIORITY OUTREACH"
            return f"{action} - {signal_count} signals, strong growth trajectory"
        elif tier == "tier_3":
            action = "SCHEDULE OUTREACH"
            return f"{action} - Monitor and engage when resources permit"
        else:
            return "Add to watch list"

    def _format_platform_tier(self, tier: Optional[int]) -> str:
        """Format platform tier for display."""
        if tier is None:
            return "No Tier 1/2 Platform"
        return f"Tier {tier}"

    def _compute_signal_summary(
        self, all_results: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """Count signals by name."""
        summary: Dict[str, int] = {}
        for result in all_results:
            for signal in result.get("signals", []):
                if signal.is_new:
                    summary[signal.name] = summary.get(signal.name, 0) + 1
        return dict(sorted(summary.items(), key=lambda x: x[1], reverse=True))

    def _compute_platform_summary(
        self, all_results: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """Count firms by platform tier."""
        summary: Dict[str, int] = {}
        for result in all_results:
            tier = result.get("platform_tier")
            tier_key = f"Tier {tier}" if tier else "No Tier 1/2"
            summary[tier_key] = summary.get(tier_key, 0) + 1
        return summary

    def _compute_geography_summary(
        self, all_results: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """Count firms by state."""
        summary: Dict[str, int] = {}
        for result in all_results:
            firm = result["firm"]
            if firm.state:
                summary[firm.state] = summary.get(firm.state, 0) + 1
        return dict(sorted(summary.items(), key=lambda x: x[1], reverse=True)[:10])

    def _generate_executive_summary(
        self,
        tier_1_count: int,
        tier_2_count: int,
        tier_3_count: int,
        total_signals: int,
        new_signals: int
    ) -> str:
        """Generate executive summary text."""
        return (
            f"ADV Buying Signal Engine Daily Report\n"
            f"{'='*50}\n"
            f"Priority Prospects (Tier 1): {tier_1_count}\n"
            f"Strong Prospects (Tier 2): {tier_2_count}\n"
            f"Monitor (Tier 3): {tier_3_count}\n"
            f"Total Signals Fired: {total_signals}\n"
            f"New Signals Today: {new_signals}\n"
        )

    def generate_markdown_brief(self, brief: DailyBrief) -> str:
        """
        Generate markdown-formatted brief for email/Slack.

        Args:
            brief: DailyBrief object

        Returns:
            Markdown-formatted string
        """
        lines = []

        # Header
        lines.append("# ADV Buying Signal Engine - Daily Brief")
        lines.append(f"**Date:** {brief.run_date.strftime('%B %d, %Y')}")
        lines.append("")

        # Executive Summary
        lines.append("## Executive Summary")
        lines.append(brief.executive_summary)
        lines.append("")

        # Tier 1 Leads
        if brief.tier_1_leads:
            lines.append("## Tier 1: Priority Prospects")
            lines.append(f"*{len(brief.tier_1_leads)} high-quality prospects*\n")
            for card in brief.tier_1_leads:
                lines.extend(self._format_lead_card_markdown(card))
        else:
            lines.append("## Tier 1: Priority Prospects")
            lines.append("*No Tier 1 prospects today*\n")

        # Tier 2 Leads
        if brief.tier_2_leads:
            lines.append("## Tier 2: Strong Prospects")
            lines.append(f"*{len(brief.tier_2_leads)} solid opportunities*\n")
            for card in brief.tier_2_leads[:5]:  # Show top 5
                lines.extend(self._format_lead_card_markdown(card))
            if len(brief.tier_2_leads) > 5:
                lines.append(
                    f"*... and {len(brief.tier_2_leads) - 5} more Tier 2 prospects*\n"
                )
        else:
            lines.append("## Tier 2: Strong Prospects")
            lines.append("*No Tier 2 prospects today*\n")

        # Insights
        lines.append("## Key Insights")

        if brief.new_signals_this_week:
            lines.append("### Top New Signals")
            for signal_name, count in list(brief.new_signals_this_week.items())[:5]:
                lines.append(f"- {signal_name}: {count} firms")
            lines.append("")

        if brief.platform_summary:
            lines.append("### Platform Distribution")
            for platform, count in brief.platform_summary.items():
                lines.append(f"- {platform}: {count} firms")
            lines.append("")

        if brief.geography_summary:
            lines.append("### Top States")
            for state, count in list(brief.geography_summary.items())[:5]:
                lines.append(f"- {state}: {count} firms")
            lines.append("")

        # Footer
        lines.append("---")
        lines.append(
            f"*Report generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S ET')}*"
        )
        lines.append(
            "*For more details, visit the ADV Engine dashboard or contact the data team.*"
        )

        return "\n".join(lines)

    def _format_lead_card_markdown(self, card: LeadCard) -> List[str]:
        """Format a single lead card as markdown."""
        lines = []

        # Header
        lines.append(f"### {card.firm_name}")
        lines.append(
            f"**{card.location}** | AUM: ${card.aum:,.0f} | "
            f"Score: {card.score:.0f}/100 ({card.tier.upper()})"
        )
        lines.append("")

        # Key metrics
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        lines.append(f"| Avg AUM/Client | ${card.avg_per_client:,.0f} |")
        lines.append(f"| QP Score | {card.qp_score:.1f}/10 |")
        lines.append(f"| Platform Access | {card.platform_access} |")
        lines.append("")

        # Signals
        if card.signals_fired:
            lines.append("**Signals Fired:**")
            for sig in card.signals_fired:
                lines.append(f"- **{sig['name']}** - {sig['evidence']}")
                lines.append(f"  > {sig['talking_point']}")
            lines.append("")

        # Recommended action
        lines.append(f"**Action:** {card.recommended_action}")
        lines.append("")

        return lines
