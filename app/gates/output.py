from __future__ import annotations
from typing import Iterable
from dataclasses import dataclass, asdict

from typing import Any, Dict, Optional, Tuple

from app.data.models import MarketSnapshot
from app.gates.gate1_htf import Gate1Result
from app.gates.gate2_derivatives import Gate2Result
from app.gates.gate3_structure import Gate3Result


@dataclass(frozen=True)
class GateMeta:
    """
    Metadata for idempotency + journaling.
    Keep it minimal and stable.
    """
    symbol: str
    ts: int  # unix seconds (or candle close ts if you prefer)
    # Candle anchors used to dedup. If your candle model differs, you can fill None.
    candle_4h_close_ts: Optional[int] = None
    candle_1h_close_ts: Optional[int] = None
    candle_15m_close_ts: Optional[int] = None
    # Snapshot price refs (best-effort)
    mark: Optional[float] = None
    spread_pct: Optional[float] = None


@dataclass(frozen=True)
class GatePack:
    """
    Single payload object carrying all gate outputs + derived eligibility.

    Downstream components should consume GatePack only.
    """
    meta: GateMeta
    g1: Gate1Result
    g2: Gate2Result
    g3: Gate3Result

    # --- Derived flags (stable semantics) ---
    @property
    def passed_all(self) -> bool:
        return bool(self.g1.passed and self.g2.passed and self.g3.passed)

    @property
    def trade_eligible(self) -> bool:
        """
        Gate2 can be "alert_only" even when regime is obvious; Gate3 already fail-closes that,
        but keep this extra guard for safety.
        """
        if not self.passed_all:
            return False
        if bool(getattr(self.g2, "alert_only", False)):
            return False
        return True

    @property
    def intent(self) -> Optional[str]:
        # Prefer Gate3 intent; fallback to HTF intent if you add later.
        return getattr(self.g3, "intent", None)

    @property
    def reason_chain(self) -> Tuple[str, str, str]:
        return (str(self.g1.reason), str(self.g2.reason), str(self.g3.reason))

    @property
    def stable_key(self) -> str:
        """
        Deterministic key for dedup/state-machine identity.
        You can extend with setup_id later, but this is the base gate identity.
        """
        sym = (self.meta.symbol or "").upper()
        i = (self.intent or "NONE").upper()
        t1 = self.meta.candle_1h_close_ts or 0
        t4 = self.meta.candle_4h_close_ts or 0
        return f"{sym}|{i}|gates|4h:{t4}|1h:{t1}"

    def summary(self) -> Dict[str, Any]:
        """
        Compact, log-friendly summary.
        """
        return {
            "symbol": self.meta.symbol,
            "trade_eligible": self.trade_eligible,
            "passed_all": self.passed_all,
            "intent": self.intent,
            "g1_reason": self.g1.reason,
            "g2_regime": getattr(self.g2, "regime", None),
            "g2_reason": self.g2.reason,
            "g2_alert_only": getattr(self.g2, "alert_only", None),
            "g3_reason": self.g3.reason,
            "zone_kind": getattr(getattr(self.g3, "zone", None), "kind", None),
            "zone_fill": getattr(getattr(self.g3, "zone", None), "fill_pct", None),
            "tp2_candidate": getattr(self.g3, "tp2_candidate", None),
            "key": self.stable_key,
        }


def _safe_candle_close_ts(candles: Any) -> Optional[int]:
    """
    Extract the anchor timestamp of the last candle.
    In this codebase, Candle has `ts` (see app/data/models.py).
    We treat it as the stable candle timestamp used for dedup/state anchoring.
    """
    if not candles:
        return None
    # candles_* are List[Candle] where Candle.ts is int
    try:
        last = candles[-1]
        v = getattr(last, "ts", None)
        if isinstance(v, (int, float)):
            return int(v)
    except Exception:
        return None
    return None


def build_gate_meta(snapshot: MarketSnapshot, now_ts: int) -> GateMeta:
    symbol = str(getattr(snapshot, "symbol", "") or "")
    mark = getattr(snapshot, "mark", getattr(snapshot, "mark_price", None))
    spread_pct = getattr(snapshot, "spread_pct", None)
    return GateMeta(
        symbol=symbol,
        ts=int(now_ts),
        candle_4h_close_ts=_safe_candle_close_ts(getattr(snapshot, "candles_4h", None)),
        candle_1h_close_ts=_safe_candle_close_ts(getattr(snapshot, "candles_1h", None)),
        candle_15m_close_ts=_safe_candle_close_ts(getattr(snapshot, "candles_15m", None)),
        mark=float(mark) if isinstance(mark, (int, float)) else None,
        spread_pct=float(spread_pct) if isinstance(spread_pct, (int, float)) else None,
    )


def build_gate_pack(
    snapshot: MarketSnapshot,
    now_ts: int,
    g1: Gate1Result,
    g2: Gate2Result,
    g3: Gate3Result,
) -> GatePack:
    meta = build_gate_meta(snapshot, now_ts=now_ts)
    return GatePack(meta=meta, g1=g1, g2=g2, g3=g3)


def dataclass_to_json_safe(obj: Any) -> Any:
    """
    Convert dataclasses (including nested) into JSON-safe dicts.
    - Converts floats/ints/str/bool/None unchanged
    - Converts dict/list/tuple recursively
    - For unknown objects: tries __dict__ then str()
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (list, tuple)):
        return [dataclass_to_json_safe(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): dataclass_to_json_safe(v) for k, v in obj.items()}
    # dataclass?
    if hasattr(obj, "__dataclass_fields__"):
        return {k: dataclass_to_json_safe(v) for k, v in asdict(obj).items()}
    # plain object
    d = getattr(obj, "__dict__", None)
    if isinstance(d, dict):
        return {str(k): dataclass_to_json_safe(v) for k, v in d.items()}
    return str(obj)
