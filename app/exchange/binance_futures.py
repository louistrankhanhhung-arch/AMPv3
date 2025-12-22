from __future__ import annotations

import time
import requests
from typing import List, Optional, Dict, Any, Tuple

from app.config import AppConfig
from app.data.models import Candle, Derivatives1H
from app.exchange.base import ExchangeClient

def _normalize_long_pct(raw_ratio: Optional[float], ratio_kind: str, meta: Dict[str, str]) -> Optional[float]:
    """
    Normalize to long percent in [0, 100].
    - For Binance globalLongShortAccountRatio, 'longAccount' may be 0..1 or 0..100 depending on endpoint behavior.
    - For 'longShortRatio' (L/S), cannot reliably convert to % without both sides -> return None.
    """
    if raw_ratio is None:
        return None

    if ratio_kind == "longAccount":
        x = float(raw_ratio)
        # If looks like percent already
        if x > 1.5:
            meta["ratio_scale"] = "0-100"
            return round(max(0.0, min(100.0, x)), 2)
        meta["ratio_scale"] = "0-1"
        return round(max(0.0, min(100.0, x * 100.0)), 2)
    meta["ratio_scale"] = "ls_ratio"
    return None


class BinanceFuturesClient(ExchangeClient):
    name = "binance"

    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self.base = "https://fapi.binance.com"

    def ping(self) -> bool:
        try:
            r = requests.get(f"{self.base}/fapi/v1/ping", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def fetch_ohlcv(self, symbol: str, interval: str, limit: int = 200) -> List[Candle]:
        # interval: "15m", "1h", "4h"
        url = f"{self.base}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()

        out: List[Candle] = []
        for row in data:
            # row: [open_time, o, h, l, c, v, close_time, ...]
            ts = int(row[0] // 1000)
            out.append(Candle(ts=ts, o=float(row[1]), h=float(row[2]), l=float(row[3]), c=float(row[4]), v=float(row[5])))
        return out

    def fetch_mark_price(self, symbol: str) -> Optional[float]:
        try:
            r = requests.get(f"{self.base}/fapi/v1/premiumIndex", params={"symbol": symbol}, timeout=8)
            r.raise_for_status()
            return float(r.json()["markPrice"])
        except Exception:
            return None

    def fetch_top_of_book(self, symbol: str) -> Optional[Tuple[float, float]]:
        """
        Return (bid, ask) from Binance futures bookTicker.
        """
        try:
            r = requests.get(f"{self.base}/fapi/v1/ticker/bookTicker", params={"symbol": symbol}, timeout=8)
            r.raise_for_status()
            j = r.json()
            bid = float(j["bidPrice"])
            ask = float(j["askPrice"])
            return (bid, ask)
        except Exception:
            return None

    def fetch_spread_bps(self, symbol: str) -> Optional[float]:
        # Best-effort using bookTicker (top-of-book)
        try:
            tob = self.fetch_top_of_book(symbol)
            if not tob:
                return None
            bid, ask = tob
            mid = (bid + ask) / 2.0
            if mid <= 0:
                return None
            return (ask - bid) / mid * 10_000.0
        except Exception:
            return None

    def fetch_derivatives_1h(self, symbol: str) -> Derivatives1H:
        meta: Dict[str, str] = {"source": "binance"}

        # Funding rate (latest)
        funding = None
        try:
            r = requests.get(f"{self.base}/fapi/v1/fundingRate", params={"symbol": symbol, "limit": 1}, timeout=10)
            r.raise_for_status()
            arr = r.json()
            if arr:
                funding = float(arr[0]["fundingRate"])
        except Exception as e:
            meta["funding_err"] = str(e)

        # Open Interest (current)
        # Binance USD-M endpoint returns openInterest in "contracts" (base-asset units).
        # For Gate 2, we prefer USD notional: OI_notional ≈ OI_contracts * markPrice.
        oi = None  # will store USD notional (USDT)
        oi_contracts: Optional[float] = None
        try:
            r = requests.get(f"{self.base}/fapi/v1/openInterest", params={"symbol": symbol}, timeout=10)
            r.raise_for_status()
            oi_contracts = float(r.json()["openInterest"])
        except Exception as e:
            meta["oi_err"] = str(e)

        # Convert OI to USD notional using mark price (best-effort).
        mark = self.fetch_mark_price(symbol)
        if oi_contracts is not None:
            meta["oi_kind"] = "contracts"
            meta["oi_contracts"] = str(oi_contracts)
            if mark is not None and mark > 0:
                oi = float(oi_contracts) * float(mark)
                meta["oi_notional_ccy"] = "USDT"
                meta["oi_mark_used"] = str(mark)
            else:
                # Fallback to contracts if mark is unavailable; keep oi as None for strict mode if desired
                meta["oi_notional_err"] = "mark_unavailable"
                oi = None

        # Long/Short Ratio: Binance provides "Global Long/Short Account Ratio" endpoints.
        # We'll attempt 1h interval, last 1 datapoint. If fails, return None.
        ratio = None
        ratio_kind = ""
        try:
            r = requests.get(
                f"{self.base}/futures/data/globalLongShortAccountRatio",
                params={"symbol": symbol, "period": "1h", "limit": 1},
                timeout=10,
            )
            r.raise_for_status()
            arr = r.json()
            if arr:
                # Fields include longAccount, shortAccount, longShortRatio depending on endpoint.
                # We'll store longAccount as a proxy ratio if available.
                long_acc = arr[0].get("longAccount")
                if long_acc is not None:
                    ratio = float(long_acc)  # percent of longs (0-1 or 0-100 depending); keep raw in meta too
                    ratio_kind = "longAccount"
                    meta["ratio_kind"] = ratio_kind
                else:
                    lsr = arr[0].get("longShortRatio")
                    if lsr is not None:
                        ratio = float(lsr)
                        ratio_kind = "longShortRatio"
                        meta["ratio_kind"] = ratio_kind
        except Exception as e:
            meta["ratio_err"] = str(e)

        ratio_long_pct = _normalize_long_pct(ratio, ratio_kind, meta)

        return Derivatives1H(
            funding_rate=funding,
            open_interest=oi,
            long_short_ratio=ratio,
            ratio_long_pct=ratio_long_pct,
            meta=meta,
        )
