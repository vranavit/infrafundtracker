"""
Signal Scorer - Overall Buying Signal Score Computation

Combines signal weights, QP score, and platform score into final buying signal score.
Applies multipliers, gates, and tier assignments.
"""

import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple

from config import TIERS
from scrapers.adv_parser import FirmRecord
from signals.signal_detector import Signal

logger = logging.getLogger(__name__)


@dataclass
class OverallScore:
    """Overall buying signal score with tier and label."""
    score: float  # 0-100
    tier: str  # "tier_1", "tier_2", "tier_3", "tier_4"
    label: str  # "Priority Prospect", "Strong Prospect", etc.
    description: str
    normalized_score: float  # 0-100
    base_score: float  # Before normalization
    new_signal_bonus: float
    qp_multiplier: float
    platform_gate: Optional[str]  # Gate applied, if any


class SignalScorer:
    """
    Computes overall buying signal score combining all components.

    Scoring formula:
    1. Base score: sum of signal weights
    2. New signal bonus: +3 per new signal
    3. QP multiplier: 0.5 + (qp_score / 10.0)
    4. Platform gate: cap score based on best platform tier
    5. Normalize to 0-100
    6. Assign tier from TIERS config

    Platform gating rules:
    - No Tier 1 or 2 platform → cap at 14 (Tier 3 max)
    - Has Tier 2 only (no Tier 1) → cap at 24
    - Has Tier 1 → no cap
    """

    # Platform gate caps
    # None means "no custodian data available" - don't penalize, just skip gate
    PLATFORM_GATE_CAPS = {
        1: None,  # Tier 1: no cap
        2: 24,    # Tier 2: cap at 24 (Tier 3 max without upgrade)
        3: 14,    # Tier 3: cap at 14
        None: None,  # No platform data: skip gate (don't penalize missing data)
    }

    # Score normalization factor (maps internal score to 0-100 range)
    NORMALIZATION_FACTOR = 0.5

    def __init__(self):
        """Initialize signal scorer."""
        self.tiers_config = TIERS
        logger.info("Initialized SignalScorer")

    def compute_overall_score(
        self,
        signals: List[Signal],
        qp_score: float,
        platform_score: float,
        platform_best_tier: Optional[int],
        firm: FirmRecord
    ) -> OverallScore:
        """
        Compute overall buying signal score.

        Args:
            signals: List of detected Signal objects
            qp_score: QP probability score (0-10)
            platform_score: Platform accessibility score (0-10)
            platform_best_tier: Best platform tier found (1, 2, 3, or None)
            firm: FirmRecord for additional context

        Returns:
            OverallScore with final score, tier, and label
        """
        # Step 1: Base score (sum of signal weights)
        base_score = sum(s.weight for s in signals)

        # Step 2: New signal bonus (+3 per new signal)
        new_signals = [s for s in signals if s.is_new]
        new_signal_bonus = len(new_signals) * 3

        # Step 3: QP multiplier (0.5 + qp_score/10.0)
        qp_multiplier = 0.5 + (qp_score / 10.0)

        # Subtotal before platform gate
        subtotal = (base_score + new_signal_bonus) * qp_multiplier

        # Step 4: Platform gate
        gate_cap = self.PLATFORM_GATE_CAPS.get(platform_best_tier)
        platform_gate_applied = None

        if gate_cap is not None and subtotal > gate_cap:
            subtotal = gate_cap
            platform_gate_applied = f"Capped at {gate_cap} (Tier {platform_best_tier})"

        # Step 5: Normalize to 0-100
        normalized_score = min(100, subtotal * self.NORMALIZATION_FACTOR)

        # Step 6: Determine tier
        tier, label, description = self._get_tier_from_score(normalized_score)

        if signals:
            logger.info(
                f"{firm.firm_name}: Final score {normalized_score:.1f}/100 ({tier}) - "
                f"Signals: {len(signals)}, QP: {qp_score:.1f}, Platform: Tier {platform_best_tier}"
            )

        return OverallScore(
            score=normalized_score,
            tier=tier,
            label=label,
            description=description,
            normalized_score=normalized_score,
            base_score=base_score,
            new_signal_bonus=new_signal_bonus,
            qp_multiplier=qp_multiplier,
            platform_gate=platform_gate_applied
        )

    def _get_tier_from_score(self, score: float) -> Tuple[str, str, str]:
        """
        Determine tier from score.

        Args:
            score: Normalized score (0-100)

        Returns:
            Tuple of (tier_key, label, description)
        """
        for tier_key, tier_info in sorted(
            self.tiers_config.items(),
            key=lambda x: x[1]["range"][0],
            reverse=True
        ):
            score_min, score_max = tier_info["range"]
            if score_min <= score <= score_max:
                return (
                    tier_key,
                    tier_info["label"],
                    tier_info["description"]
                )

        # Default to tier_4 if no match
        tier_info = self.tiers_config["tier_4"]
        return "tier_4", tier_info["label"], tier_info["description"]
