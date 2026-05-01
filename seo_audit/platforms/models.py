from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class PlatformDetection:
    platform: str
    confidence: int
    signals: dict[str, str | int | bool] = field(default_factory=dict)
    template_hint: str = ""


def choose_stronger(
    left: PlatformDetection | None,
    right: PlatformDetection | None,
) -> PlatformDetection | None:
    if left is None:
        return right
    if right is None:
        return left
    if right.confidence > left.confidence:
        return right
    return left
