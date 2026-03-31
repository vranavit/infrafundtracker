"""
Custodian to platform mapper module.

Maps custodian names to accessible platforms and detects platform
accessibility for ISQ distribution.
"""

import logging
from typing import List, Dict, Optional, Any

from config import PLATFORMS_ACCESSIBILITY, INACCESSIBLE_CUSTODIANS

logger = logging.getLogger(__name__)


class CustodianMapper:
    """Maps custodians to platforms and detects ISQ accessibility."""

    def __init__(self):
        """Initialize mapper with platform configuration."""
        self.platforms = PLATFORMS_ACCESSIBILITY
        self.inaccessible = set(INACCESSIBLE_CUSTODIANS)
        self._build_custodian_index()

    def _build_custodian_index(self) -> None:
        """Build reverse index from custodian names to platforms."""
        self.custodian_to_platforms: Dict[str, List[str]] = {}

        for platform_name, platform_config in self.platforms.items():
            indicators = platform_config.get("custodian_indicators", [])
            for indicator in indicators:
                indicator_lower = indicator.lower()
                if indicator_lower not in self.custodian_to_platforms:
                    self.custodian_to_platforms[indicator_lower] = []
                self.custodian_to_platforms[indicator_lower].append(platform_name)

    def _normalize_custodian_name(self, custodian: str) -> str:
        """Normalize custodian name for matching."""
        return custodian.strip().lower()

    def is_inaccessible_custodian(self, custodian: str) -> bool:
        """Check if custodian is in inaccessible list."""
        normalized = self._normalize_custodian_name(custodian)

        for inaccessible in self.inaccessible:
            if normalized == inaccessible.lower():
                return True

            inaccessible_lower = inaccessible.lower()
            if (
                inaccessible_lower in normalized
                or normalized in inaccessible_lower
            ):
                return True

        return False

    def detect_platforms(self, custodian: str) -> List[str]:
        """Detect which platforms are accessible via a custodian."""
        if self.is_inaccessible_custodian(custodian):
            return []

        normalized = self._normalize_custodian_name(custodian)
        detected = set()

        for platform_name, platform_config in self.platforms.items():
            indicators = platform_config.get("custodian_indicators", [])

            for indicator in indicators:
                indicator_lower = indicator.lower()

                if normalized == indicator_lower:
                    detected.add(platform_name)
                elif indicator_lower in normalized:
                    detected.add(platform_name)
                elif normalized in indicator_lower:
                    detected.add(platform_name)

        for platform_name, platform_config in self.platforms.items():
            keywords = platform_config.get("adv_keywords", [])

            for keyword in keywords:
                keyword_lower = keyword.lower()
                if keyword_lower in normalized:
                    detected.add(platform_name)

        return sorted(detected)

    def map_custodians_to_platforms(
        self, custodians: List[str]
    ) -> Dict[str, List[str]]:
        """Map a list of custodians to their accessible platforms."""
        result = {}

        for custodian in custodians:
            if custodian.strip():
                platforms = self.detect_platforms(custodian)
                result[custodian] = platforms

        return result

    def get_all_platforms_from_custodians(
        self, custodians: List[str]
    ) -> List[str]:
        """Get aggregated list of all accessible platforms from custodians."""
        all_platforms = set()

        for custodian in custodians:
            platforms = self.detect_platforms(custodian)
            all_platforms.update(platforms)

        return sorted(all_platforms)

    def get_target_platforms(self, custodians: List[str]) -> List[str]:
        """Get only TARGET status platforms from custodians."""
        all_platforms = self.get_all_platforms_from_custodians(custodians)
        return [
            p
            for p in all_platforms
            if self.platforms.get(p, {}).get("status") == "TARGET"
        ]

    def get_accessible_platforms(self, custodians: List[str]) -> List[str]:
        """Get TARGET and ACCESSIBLE status platforms."""
        all_platforms = self.get_all_platforms_from_custodians(custodians)
        return [
            p
            for p in all_platforms
            if self.platforms.get(p, {}).get("status")
            in ["TARGET", "ACCESSIBLE"]
        ]

    def get_possible_platforms(self, custodians: List[str]) -> List[str]:
        """Get all accessible platforms including POSSIBLE status."""
        return self.get_all_platforms_from_custodians(custodians)

    def get_platform_info(self, platform_name: str) -> Optional[Dict]:
        """Get full platform configuration."""
        return self.platforms.get(platform_name)

    def get_platform_status(self, platform_name: str) -> Optional[str]:
        """Get platform accessibility status."""
        config = self.get_platform_info(platform_name)
        return config.get("status") if config else None

    def get_platform_tier(self, platform_name: str) -> Optional[int]:
        """Get platform tier."""
        config = self.get_platform_info(platform_name)
        return config.get("tier") if config else None

    def calculate_platform_coverage(self, custodians: List[str]) -> Dict[str, Any]:
        """Calculate platform coverage metrics for custodian list."""
        all_platforms = self.get_all_platforms_from_custodians(custodians)
        target_platforms = self.get_target_platforms(custodians)
        accessible_platforms = self.get_accessible_platforms(custodians)

        return {
            "total_platforms": len(all_platforms),
            "target_platforms": len(target_platforms),
            "accessible_platforms": len(accessible_platforms),
            "platforms": all_platforms,
            "target_list": target_platforms,
            "accessible_list": accessible_platforms,
            "is_accessible": len(accessible_platforms) > 0,
            "has_target": len(target_platforms) > 0,
        }


_default_mapper = None


def get_mapper() -> CustodianMapper:
    """Get default mapper instance."""
    global _default_mapper
    if _default_mapper is None:
        _default_mapper = CustodianMapper()
    return _default_mapper


def map_custodians_to_platforms(custodians: List[str]) -> Dict[str, List[str]]:
    """Convenience function to map custodians."""
    mapper = get_mapper()
    return mapper.map_custodians_to_platforms(custodians)


def detect_platforms(custodian: str) -> List[str]:
    """Convenience function to detect platforms for custodian."""
    mapper = get_mapper()
    return mapper.detect_platforms(custodian)


def is_inaccessible_custodian(custodian: str) -> bool:
    """Convenience function to check if custodian is inaccessible."""
    mapper = get_mapper()
    return mapper.is_inaccessible_custodian(custodian)
