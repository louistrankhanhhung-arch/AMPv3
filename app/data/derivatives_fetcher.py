from __future__ import annotations

import time
import math
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Tuple

from app.data.cache import TTLCache
from app.data.models import Derivatives1H
from app.exchange.base import ExchangeClient

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
    oi_delta: Optional[float]
    oi_delta_pct: Optional[float]
    oi_spike_z: Optional[float]
    funding_z: Optional[float]
    ratio_dev: Optional[float]
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

        # Rolling series per exchange+symbol (no TTL)
        # NOTE: Use a fully-qualified string key to avoid any TTLCache implementations
        # that may normalize/flatten tuple keys and accidentally share deques across symbols.
        series_key = f"deriv_series_1h:{self.client.name}:{symbol}"
        series = self.cache.get_or_create_deque(series_key, maxlen=hist_maxlen)

        # Append latest point (ts, oi, funding, ratio_long_pct)
        series.append(
            {
                "ts": now,
                "exchange": self.client.name,
                "symbol": symbol,
                "oi": d.open_interest,
                "funding": d.funding_rate,
                "ratio_long_pct": getattr(d, "ratio_long_pct", None),
            }
        )

        # Defensive: filter points to the exact (exchange, symbol) in case a shared deque ever happens.
        # This guarantees per-symbol z-score outputs even under cache key collisions.
        pts_all = list(series)
        pts_sym = [
            p for p in pts_all
            if p.get("symbol") == symbol and p.get("exchange") == self.client.name
        ]

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

        # Spike score (z-score) on recent OI deltas
        oi_spike_z: Optional[float] = None
        if oi_delta is not None:
            # build recent deltas
            deltas: List[float] = []
            # Use last z_window+1 points to compute deltas
            pts = pts_sym[-(z_window + 1) :]
            for i in range(1, len(pts)):
                p = pts[i - 1].get("oi")
                c = pts[i].get("oi")
                if isinstance(p, (int, float)) and isinstance(c, (int, float)) and p != 0:
                    deltas.append(float(c) - float(p))

            # Need enough samples to estimate variance
            if len(deltas) >= max(8, min(12, z_window // 2)):
                mean = sum(deltas) / len(deltas)
                var = sum((x - mean) ** 2 for x in deltas) / max(1, (len(deltas) - 1))
                std = math.sqrt(var)
                if std > 1e-12:
                    oi_spike_z = (oi_delta - mean) / std
                else:
                    oi_spike_z = 0.0

        # Funding z-score (best-effort) on recent funding samples
        funding_z: Optional[float] = None
        cur_funding = d.funding_rate
        if cur_funding is not None:
            fvals: List[float] = []
            pts_f = pts_sym[-z_window:]
            for p in pts_f:
                fv = p.get("funding")
                if isinstance(fv, (int, float)):
                    fvals.append(float(fv))
            if len(fvals) >= max(8, min(12, z_window // 2)):
                fmean = sum(fvals) / len(fvals)
                fvar = sum((x - fmean) ** 2 for x in fvals) / max(1, (len(fvals) - 1))
                fstd = math.sqrt(fvar)
                if fstd > 1e-12:
                    funding_z = (float(cur_funding) - fmean) / fstd
                else:
                    funding_z = 0.0

        # Ratio deviation from 50 (%). Useful for crowding checks.
        ratio_dev: Optional[float] = None
        rlp = getattr(d, "ratio_long_pct", None)
        if isinstance(rlp, (int, float)):
            ratio_dev = abs(float(rlp) - 50.0)

        # A-mode: only trust Gate 2 when we have enough samples
        ready = len(pts_sym) >= max(12, min(18, z_window))    

        return Gate2DerivativesCtx(
            symbol=symbol,
            exchange=self.client.name,
            last=d,
            ts=now,
            oi_delta=oi_delta,
            oi_delta_pct=oi_delta_pct,
            oi_spike_z=oi_spike_z,
            funding_z=funding_z,
            ratio_dev=ratio_dev,
            ready=ready,
            history_len=len(pts_sym),
        )
