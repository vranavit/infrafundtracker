"""
Platform Scorer - Platform Accessibility Scoring

Matches firm's custodians against platform accessibility configuration
and assigns scores based on platform tier (Tier 1/2 preferred).
"""

import logging
from dataclasses import dataclass
from typing import List, Tuple, Optional

from config import PLATFORMS_ACCESSIBILITY
from scrapers.adv_parser import FirmRecord

logger = logging.getLogger(__name__)


@dataclass
class PlatformScore:
    """Platform accessibility score with details."""
    score: float  # 0-10
    platforms_detected: List[str]
    best_tier: Optional[int]
    tier_breakdown: dict  # {"tier_1": [...], "tier_2": [...], ...}


class PlatformScorer:
    """
    Scores firm's platform accessibility based on custodian matches.

    Platform tiers:
    - Tier 1: Schwab, Fidelity (modern, comprehensive)
    - Tier 2: Pershing, Broadridge, DST/SS&C, HSBC, UBS
    - Tier 3: Others, specialized/niche platforms

    Scoring:
    - Tier 1 platform: score 10
    - Tier 2 platform: score 8
    - Tier 3 platform: score 5
    - No identified platforms: score 3
    """

    # Score mapping by platform tier
    TIER_SCORES = {
        1: 10,  # Tier 1 platforms (best)
        2: 8,   # Tier 2 platforms (good)
        3: 5,   # Tier 3 platforms (acceptable)
        None: 3,  # No platforms identified
    }

    def __init__(self):
        """Initialize platform scorer."""
        self.platforms_config = PLATFORMS_ACCESSIBILITY
        logger.info("Initialized PlatformScorer")

    def score_platform_accessibility(
        self, firm: FirmRecord
    ) -> PlatformScore:
        """
        Score firm's platform accessibility.

        Args:
            firm: FirmRecord to score

        Returns:
            PlatformScore with score (0-10), detected platforms, and tier info
        """
        if not firm.custodian_names:
            return PlatformScore(
                score=self.TIER_SCORES[None],
                platforms_detected=[],
                best_tier=None,
                tier_breakdown={"tier_1": [], "tier_2": [], "tier_3": []}
            )

        # Match custodians against known platforms
        detected_platforms = []
        tier_breakdown = {"tier_1": [], "tier_2": [], "tier_3": []}
        best_tier = None

        custodian_lower = [c.lower() for c in firm.custodian_names]

        for platform_key, platform_info in self.platforms_config.items():
            keywords = platform_info.get("keywords", [])
            tier = platform_info.get("tier")

            for custodian in custodian_lower:
                for keyword in keywords:
                    if keyword.lower() in custodian:
                        if platform_key not in detected_platforms:
                            detected_platforms.append(platform_key)

                        # Track by tier
                        tier_key = f"tier_{tier}"
                        if tier_key in tier_breakdown:
                            if platform_key not in tier_breakdown[tier_key]:
                                tier_breakdown[tier_key].append(platform_key)

                        # Update best tier (lower number = better)
                        if best_tier is None or tier < best_tier:
                            best_tier = tier

                        break  # Move to next custodian

        # Calculate score based on best tier
        score = self.TIER_SCORES.get(best_tier, self.TIER_SCORES[None])

        logger.debug(
            f"{firm.firm_name}: Platform score {score}/10 - "
            f"Tier {best_tier} - Platforms: {detected_platforms}"
        )

        return PlatformScore(
            score=score,
            platforms_detected=detected_platforms,
            best_tier=best_tier,
            tier_breakdown=tier_breakdown
        )
