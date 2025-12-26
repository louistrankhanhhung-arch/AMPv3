from __future__ import annotations

import time
import logging
import math
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Tuple

from app.data.cache import TTLCache
from app.data.models import Derivatives1H
from app.exchange.base import ExchangeClient

logger = logging.getLogger("amp_smc")

@dataclass(frozen=True)
class Gate2DerivativesCtx:
    """
    Derivatives context for Gate 2 (Regime A/B).
    Keeps rolling series + computed OI delta and spike score (z-score on OI delta).
    """
    symbol: str
    exchange: str
    last: Derivatives1H
    ts: int
    bucket_ts: int
    oi_delta: Optional[float]
    oi_delta_pct: Optional[float]
    oi_spike_z: Optional[float]
    funding_z: Optional[float]
    funding_mean: Optional[float]
    funding_std: Optional[float]
    ratio_dev: Optional[float]
    oi_slope_4h_pct: Optional[float]
    confirm4h: bool
    confirm4h_reason: str
    ready: bool
    history_len: int

class DerivativesFetcher:
    def __init__(self, client: ExchangeClient, cache: TTLCache) -> None:
        self.client = client
        self.cache = cache

    def get_derivatives_1h(self, symbol: str, ttl_sec: int = 30) -> Derivatives1H:
        key = ("deriv_1h", self.client.name, symbol)
        cached = self.cache.get(key)
        if cached is not None:
            return cached

        d = self.client.fetch_derivatives_1h(symbol=symbol)
        self.cache.set(key, d, ttl_sec=ttl_sec)
        return d

    def get_gate2_ctx(
        self,
        symbol: str,
        ttl_sec: int = 30,
        hist_maxlen: int = 72,
        z_window: int = 24,
    ) -> Gate2DerivativesCtx:
        """
        Returns a Gate2DerivativesCtx while keeping compatibility with existing pipeline.
        - Uses get_derivatives_1h() for TTL caching of the latest point.
        - Stores rolling series in cache persistent storage (in-memory, no TTL).
        """
        d = self.get_derivatives_1h(symbol, ttl_sec=ttl_sec)
        now = int(time.time())
        # Normalize to 1H buckets to avoid multiple appends per hour (scheduler tick noise).
        bucket_ts = (now // 3600) * 3600

        # Rolling series per exchange+symbol (no TTL)
        # NOTE: Use a fully-qualified string key to avoid any TTLCache implementations
        # that may normalize/flatten tuple keys and accidentally share deques across symbols.
        series_key = f"deriv_series_1h:{self.client.name}:{symbol}"
        series = self.cache.get_or_create_deque(series_key, maxlen=hist_maxlen)

        # Append at most once per 1H bucket (dedup).
        # If bucket already exists, keep the latest observation for that bucket.
        last_point = series[-1] if len(series) > 0 else None
        should_append = True
        if isinstance(last_point, dict):
            lp_bucket = last_point.get("bucket_ts")
            lp_sym = last_point.get("symbol")
            lp_ex = last_point.get("exchange")
            if lp_bucket == bucket_ts and lp_sym == symbol and lp_ex == self.client.name:
                should_append = False

        point = {
            "ts": now,
            "bucket_ts": bucket_ts,
            "exchange": self.client.name,
            "symbol": symbol,
            "oi": d.open_interest,
            "funding": d.funding_rate,
            "ratio_long_pct": getattr(d, "ratio_long_pct", None),
        }
        if should_append:
            series.append(point)
        else:
            # Replace the last point in-place to keep freshest values for the same hour.
            try:
                series[-1] = point
            except Exception:
                # Fallback: append if deque does not support item assignment (shouldn't happen)
                series.append(point)

        # Defensive: filter points to the exact (exchange, symbol) in case a shared deque ever happens.
        # This guarantees per-symbol z-score outputs even under cache key collisions.
        pts_all = list(series)
        pts_sym = [
            p for p in pts_all
            if p.get("symbol") == symbol and p.get("exchange") == self.client.name
        ]
        # Also keep only one point per bucket (dedup safeguard).
        # (If any duplicates exist due to prior versions, keep the latest per bucket.)
        by_bucket: Dict[int, dict] = {}
        for p in pts_sym:
            b = p.get("bucket_ts")
            if isinstance(b, int):
                by_bucket[b] = p
        pts_sym = [by_bucket[k] for k in sorted(by_bucket.keys())]

        # Compute OI delta from previous point (best-effort)
        oi_delta: Optional[float] = None
        oi_delta_pct: Optional[float] = None
        if len(pts_sym) >= 2:
            prev = pts_sym[-2].get("oi")
            cur = pts_sym[-1].get("oi")
            if isinstance(prev, (int, float)) and isinstance(cur, (int, float)):
                oi_delta = float(cur) - float(prev)
                if prev and prev != 0:
                    oi_delta_pct = (oi_delta / float(prev)) * 100.0

        # Spike score (z-score) on recent OI deltas (use pct deltas for scale stability)
        oi_spike_z: Optional[float] = None
        # --- DEBUG (temporary) ---
        oi_mean: Optional[float] = None
        oi_std: Optional[float] = None
        if oi_delta_pct is not None:
            # build recent pct deltas
            deltas: List[float] = []
            # Use last z_window+1 points to compute deltas
            pts = pts_sym[-(z_window + 1) :]
            for i in range(1, len(pts)):
                p = pts[i - 1].get("oi")
                c = pts[i].get("oi")
                if isinstance(p, (int, float)) and isinstance(c, (int, float)) and p != 0:
                    deltas.append(((float(c) - float(p)) / float(p)) * 100.0)

            # Need enough samples to estimate variance
            if len(deltas) >= max(8, min(12, z_window // 2)):
                oi_mean = sum(deltas) / len(deltas)
                var = sum((x - oi_mean) ** 2 for x in deltas) / max(1, (len(deltas) - 1))
                oi_std = math.sqrt(var)
                if oi_std > 1e-12:
                    oi_spike_z = (float(oi_delta_pct) - oi_mean) / oi_std
                else:
                    oi_spike_z = 0.0

                # --- DEBUG: per-symbol OI delta distribution (temporary) ---
                try:
                    last3d = deltas[-3:]
                    logger.debug(
                        "G2_OI_STATS | %s | mean=%+.3e std=%.3e cur_d=%+.3e z=%+.3f last3=%s n=%d",
                        symbol,
                        float(oi_mean),
                        float(oi_std),
                        float(oi_delta_pct),
                        float(oi_spike_z) if oi_spike_z is not None else float("nan"),
                        [float(f"{x:+.3e}") for x in last3d],
                        len(deltas),
                    )
                except Exception:
                    pass

        # Funding z-score (best-effort) on recent funding samples
        funding_z: Optional[float] = None
        funding_mean: Optional[float] = None
        funding_std: Optional[float] = None
        cur_funding = d.funding_rate
        if cur_funding is not None:
            fvals: List[float] = []
            pts_f = pts_sym[-z_window:]
            for p in pts_f:
                fv = p.get("funding")
                if isinstance(fv, (int, float)):
                    fvals.append(float(fv))
            if len(fvals) >= max(8, min(12, z_window // 2)):
                funding_mean = sum(fvals) / len(fvals)
                fvar = sum((x - funding_mean) ** 2 for x in fvals) / max(1, (len(fvals) - 1))
                funding_std = math.sqrt(fvar)
                if funding_std > 1e-12:
                    funding_z = (float(cur_funding) - funding_mean) / funding_std
                else:
                    funding_z = 0.0

                # --- DEBUG: per-symbol funding distribution (temporary) ---
                try:
                    last3 = fvals[-3:]
                    logger.debug(
                        "G2_FUNDING_STATS | %s | mean=%+.3e std=%.3e cur=%+.3e fz=%+.3f last3=%s n=%d",
                        symbol,
                        float(funding_mean),
                        float(funding_std),
                        float(cur_funding),
                        float(funding_z) if funding_z is not None else float("nan"),
                        [float(f"{x:+.3e}") for x in last3],
                        len(fvals),
                    )
                except Exception:
                    pass

        # Ratio deviation from 50 (%). Useful for crowding checks.
        ratio_dev: Optional[float] = None
        rlp = getattr(d, "ratio_long_pct", None)
        if isinstance(rlp, (int, float)):
            ratio_dev = abs(float(rlp) - 50.0)

        # --- 4H confirm v1 (simple, derived from 1H buckets) ---
        oi_slope_4h_pct: Optional[float] = None
        confirm4h = False
        confirm4h_reason = "na"
        # Require at least 5 bucket points to compute 4H slope (now vs 4h-ago).
        if len(pts_sym) >= 5:
            oi_now = pts_sym[-1].get("oi")
            oi_4h = pts_sym[-5].get("oi")
            if isinstance(oi_now, (int, float)) and isinstance(oi_4h, (int, float)) and float(oi_4h) != 0.0:
                oi_slope_4h_pct = ((float(oi_now) - float(oi_4h)) / float(oi_4h)) * 100.0

        # Confirm rule:
        # - For squeeze regimes, require persistence in the last 4 buckets (2-of-4).
        # - Persistence checks are computed here and used downstream in Gate2 classification.
        if len(pts_sym) >= 4:
            last4 = pts_sym[-4:]
            # ratio skew persistence (>= 67.5 or <= 32.5)
            ratio_hits = 0
            for p in last4:
                rv = p.get("ratio_long_pct")
                if isinstance(rv, (int, float)) and (float(rv) >= 67.5 or float(rv) <= 32.5):
                    ratio_hits += 1
            # funding extreme persistence: use abs(funding) guard (works even without z)
            fund_hits = 0
            for p in last4:
                fv = p.get("funding")
                if isinstance(fv, (int, float)) and abs(float(fv)) >= 0.00015:
                    fund_hits += 1
            if ratio_hits >= 2 or fund_hits >= 2:
                confirm4h = True
                confirm4h_reason = f"persist_ratio={ratio_hits}_fund={fund_hits}"

        # A-mode: only trust Gate 2 when we have enough samples
        ready = len(pts_sym) >= max(12, min(18, z_window))    

        return Gate2DerivativesCtx(
            symbol=symbol,
            exchange=self.client.name,
            last=d,
            ts=now,
            bucket_ts=bucket_ts,
            oi_delta=oi_delta,
            oi_delta_pct=oi_delta_pct,
            oi_spike_z=oi_spike_z,
            funding_z=funding_z,
            funding_mean=funding_mean,
            funding_std=funding_std,
            ratio_dev=ratio_dev,
            oi_slope_4h_pct=oi_slope_4h_pct,
            confirm4h=confirm4h,
            confirm4h_reason=confirm4h_reason,
            ready=ready,
            history_len=len(pts_sym),
        )
