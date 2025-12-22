from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TF:
    name: str
    seconds: int


TF_15M = TF("15m", 15 * 60)
TF_1H = TF("1h", 60 * 60)
TF_4H = TF("4h", 4 * 60 * 60)
TF_1D = TF("1d", 24 * 60 * 60)
