"""
Platform accessibility checker module.

Checks platform accessibility for ISQ distribution and computes
accessibility scores based on custodian configurations.
"""

import logging
from typing import List, Dict, Optional
from dataclasses import dataclass

from config import PLATFORMS_ACCESSIBILITY
from .custodian_mapper import CustodianMapper

logger = logging.getLogger(__name__)


@dataclass
class PlatformAccessibility:
    """Platform accessibility result."""

    platform_name: str
    is_accessible: bool
    tier: int
    status: str
    confidence_score: float
    detected_via: List[str]
    score_weight: float
    keywords_matched: List[str]

    def __repr__(self) -> str:
        return (
            f"PlatformAccessibility({self.platform_name}, {self.status}, "
            f"score={self.confidence_score:.2f})"
        )


class PlatformChecker:
    """Checks and scores platform accessibility for ISQ."""

    def __init__(self):
        """Initialize platform checker."""
        self.mapper = CustodianMapper()
        self.platforms = PLATFORMS_ACCESSIBILITY

    def check_platform_accessibility(
        self, custodians: List[str]
    ) -> List[PlatformAccessibility]:
        """Check accessibility of all platforms for given custodians."""
        results = []
        detected_platforms = self.mapper.get_all_platforms_from_custodians(custodians)

        for platform_name in detected_platforms:
            config = self.platforms.get(platform_name, {})

            matched_via = []
            for custodian in custodians:
                if platform_name in self.mapper.detect_platforms(custodian):
                    matched_via.append(custodian)

            matched_keywords = self._get_matched_keywords(
                custodians, platform_name
            )

            confidence = self._compute_confidence_score(
                custodians, platform_name, matched_via
            )

            is_accessible = (
                config.get("status") in ["TARGET", "ACCESSIBLE"]
            )

            result = PlatformAccessibility(
                platform_name=platform_name,
                is_accessible=is_accessible,
                tier=config.get("tier", 3),
                status=config.get("status", "UNKNOWN"),
                confidence_score=confidence,
                detected_via=matched_via,
                score_weight=config.get("score_weight", 1.0),
                keywords_matched=matched_keywords,
            )

            results.append(result)

        return sorted(results, key=lambda x: x.confidence_score, reverse=True)

    def _get_matched_keywords(self, custodians: List[str], platform_name: str) -> List[str]:
        """Get keywords that matched for a platform."""
        config = self.platforms.get(platform_name, {})
        keywords = config.get("adv_keywords", [])

        matched = []
        custodian_text = " ".join(custodians).lower()

        for keyword in keywords:
            if keyword.lower() in custodian_text:
                matched.append(keyword)

        return matched

    def _compute_confidence_score(
        self, custodians: List[str], platform_name: str, matched_via: List[str]
    ) -> float:
        """Compute confidence score for platform detection."""
        if not matched_via:
            return 0.0

        base_score = min(len(matched_via) * 0.2, 0.8)

        keywords = self._get_matched_keywords(custodians, platform_name)
        keyword_boost = min(len(keywords) * 0.05, 0.2)

        score = base_score + keyword_boost
        return min(score, 1.0)

    def get_best_platform(self, custodians: List[str]) -> Optional[PlatformAccessibility]:
        """Get best/highest priority platform for custodian list."""
        accessible = self.check_platform_accessibility(custodians)

        accessible_platforms = [p for p in accessible if p.is_accessible]

        if not accessible_platforms:
            return None

        def sort_key(p):
            is_target = p.status == "TARGET"
            return (-is_target, p.tier, -p.confidence_score)

        return min(accessible_platforms, key=sort_key)

    def compute_platform_score(
        self, custodians: List[str], use_best_only: bool = False
    ) -> float:
        """Compute aggregated platform accessibility score."""
        accessible = self.check_platform_accessibility(custodians)

        if not accessible:
            return 0.0

        if use_best_only:
            best = self.get_best_platform(custodians)
            if best:
                return (
                    best.confidence_score
                    * best.score_weight
                    * (1.5 if best.is_accessible else 0.5)
                ) * 100
            return 0.0

        total_score = 0.0
        weighted_count = 0

        for platform in accessible:
            if platform.is_accessible:
                score_contribution = (
                    platform.confidence_score * platform.score_weight * 1.5
                )
            else:
                score_contribution = (
                    platform.confidence_score * platform.score_weight * 0.5
                )

            total_score += score_contribution
            weighted_count += platform.score_weight

        if weighted_count == 0:
            return 0.0

        normalized = (total_score / weighted_count) * 100
        return min(normalized, 100.0)

    def get_platform_summary(self, custodians: List[str]) -> Dict:
        """Get detailed platform accessibility summary."""
        accessible = self.check_platform_accessibility(custodians)

        target_platforms = [p for p in accessible if p.status == "TARGET"]
        accessible_platforms = [p for p in accessible if p.status == "ACCESSIBLE"]
        possible_platforms = [p for p in accessible if p.status == "POSSIBLE"]

        tier_1 = [p for p in accessible if p.tier == 1]
        tier_2 = [p for p in accessible if p.tier == 2]
        tier_3 = [p for p in accessible if p.tier >= 3]

        return {
            "total_platforms": len(accessible),
            "by_status": {
                "TARGET": len(target_platforms),
                "ACCESSIBLE": len(accessible_platforms),
                "POSSIBLE": len(possible_platforms),
            },
            "by_tier": {
                "Tier 1": len(tier_1),
                "Tier 2": len(tier_2),
                "Tier 3+": len(tier_3),
            },
            "platforms": [
                {
                    "name": p.platform_name,
                    "status": p.status,
                    "tier": p.tier,
                    "confidence": f"{p.confidence_score:.2f}",
                    "detected_via": p.detected_via,
                }
                for p in accessible
            ],
            "best_platform": (
                self.get_best_platform(custodians).platform_name
                if self.get_best_platform(custodians)
                else None
            ),
            "overall_score": f"{self.compute_platform_score(custodians):.1f}",
            "is_isq_accessible": len(target_platforms) > 0 or len(accessible_platforms) > 0,
        }

    def rank_platforms(self, custodians: List[str]) -> List[Dict]:
        """Rank platforms by accessibility priority."""
        accessible = self.check_platform_accessibility(custodians)

        ranked = []

        target = sorted(
            [p for p in accessible if p.status == "TARGET"],
            key=lambda x: (x.tier, -x.confidence_score),
        )
        for rank, platform in enumerate(target, 1):
            ranked.append({
                "rank": rank,
                "priority": "CRITICAL",
                "platform": platform.platform_name,
                "status": platform.status,
                "tier": platform.tier,
                "confidence": f"{platform.confidence_score:.2f}",
                "reason": "TARGET platform - highest priority",
            })

        accessible_plat = sorted(
            [p for p in accessible if p.status == "ACCESSIBLE"],
            key=lambda x: (x.tier, -x.confidence_score),
        )
        for rank, platform in enumerate(accessible_plat, len(ranked) + 1):
            ranked.append({
                "rank": rank,
                "priority": "HIGH",
                "platform": platform.platform_name,
                "status": platform.status,
                "tier": platform.tier,
                "confidence": f"{platform.confidence_score:.2f}",
                "reason": "Accessible platform",
            })

        possible = sorted(
            [p for p in accessible if p.status == "POSSIBLE"],
            key=lambda x: (-x.confidence_score, x.tier),
        )
        for rank, platform in enumerate(possible, len(ranked) + 1):
            ranked.append({
                "rank": rank,
                "priority": "MEDIUM",
                "platform": platform.platform_name,
                "status": platform.status,
                "tier": platform.tier,
                "confidence": f"{platform.confidence_score:.2f}",
                "reason": "Possible platform - follow-up needed",
            })

        return ranked


_default_checker = None


def get_checker() -> PlatformChecker:
    """Get default platform checker instance."""
    global _default_checker
    if _default_checker is None:
        _default_checker = PlatformChecker()
    return _default_checker


def check_platform_accessibility(custodians: List[str]) -> List[PlatformAccessibility]:
    """Convenience function to check platform accessibility."""
    checker = get_checker()
    return checker.check_platform_accessibility(custodians)


def get_best_platform(custodians: List[str]) -> Optional[PlatformAccessibility]:
    """Convenience function to get best platform."""
    checker = get_checker()
    return checker.get_best_platform(custodians)


def compute_platform_score(
    custodians: List[str], use_best_only: bool = False
) -> float:
    """Convenience function to compute platform score."""
    checker = get_checker()
    return checker.compute_platform_score(custodians, use_best_only)
