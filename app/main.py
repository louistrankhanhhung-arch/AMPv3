from __future__ import annotations

import time
from typing import Optional

from app.config import AppConfig
from app.utils.logger import setup_logger
from app.exchange.router import ExchangeRouter
from app.data.cache import TTLCache
from app.data.market_fetcher import MarketFetcher
from app.data.derivatives_fetcher import DerivativesFetcher
from app.data.models import MarketSnapshot

from app.gates.gate1_htf import gate1_htf_clarity
from app.gates.gate2_derivatives import gate2_derivatives_regime


def build_snapshot(symbol: str, market: MarketFetcher, deriv: DerivativesFetcher, client) -> MarketSnapshot:
    candles_15m = market.get_candles(symbol, "15m", limit=240, ttl_sec=20)
    candles_1h = market.get_candles(symbol, "1h", limit=240, ttl_sec=40)
    candles_4h = market.get_candles(symbol, "4h", limit=240, ttl_sec=90)

    d1h = deriv.get_derivatives_1h(symbol, ttl_sec=30)

    mark = client.fetch_mark_price(symbol)
    spread = client.fetch_spread_bps(symbol)

    bid = None
    ask = None
    spread_pct = None
    try:
        tob = client.fetch_top_of_book(symbol)
        if tob:
            bid, ask = tob
            mid = (bid + ask) / 2.0
            if mid > 0:
                spread_pct = (ask - bid) / mid * 100.0
    except Exception:
        # best-effort only
        pass

    now = int(time.time())
    return MarketSnapshot(
        symbol=symbol,
        candles_15m=candles_15m,
        candles_1h=candles_1h,
        candles_4h=candles_4h,
        deriv_1h=d1h,
        spread_bps=spread,
        spread_pct=spread_pct,
        bid=bid,
        ask=ask,
        mark_price=mark,
        last_updated_ts=now,
    )


def main() -> None:
    log = setup_logger()
    cfg = AppConfig.load()

    router = ExchangeRouter(cfg)
    cache = TTLCache()

    while True:
        try:
            client = router.get_client()
            market = MarketFetcher(client, cache)
            deriv = DerivativesFetcher(client, cache)

            for sym in cfg.symbols:
                snap = build_snapshot(sym, market, deriv, client)

                # --- IMPORTANT: Always update/persist derivatives rolling series ---
                # Do this BEFORE Gate 1 so history accumulates even when Gate 1 fails.
                # This fixes "restart -> hist=1" syndrome and makes Gate 2 ready faster.
                ctx2 = deriv.get_gate2_ctx(sym, ttl_sec=30)

                # Tầng 1: chỉ log để kiểm tra pipeline.
                # Tầng 2 trở đi sẽ gọi gates/smc và notify.
                log.info(
                    "SNAPSHOT %s | ex=%s | mark=%s | bid=%s | ask=%s | spread_bps=%s | spread_pct=%s | funding=%s | oi=%s | ratio=%s | ratio_long_pct=%s",
                    snap.symbol,
                    client.name,
                    snap.mark_price,
                    snap.bid,
                    snap.ask,
                    snap.spread_bps,
                    snap.spread_pct,
                    snap.deriv_1h.funding_rate,
                    snap.deriv_1h.open_interest,
                    snap.deriv_1h.long_short_ratio,
                    getattr(snap.deriv_1h, "ratio_long_pct", None),
                )

                # Tầng 2 - Gate 1: HTF Clarity (4H)
                g1 = gate1_htf_clarity(snap)
                log.info(
                    "G1 %s %s | reason=%s | bias=%s loc=%s pos=%.2f | liq_above=%s liq_below=%s | spread_pct=%s | ratio_long_pct=%s",
                    snap.symbol,
                    "PASS" if g1.passed else "FAIL",
                    getattr(g1, "reason", None),
                    (g1.htf.bias if g1.htf else None),
                    (g1.htf.location if g1.htf else None),
                    (g1.htf.pos_pct if g1.htf else -1),
                    (g1.liq.above if g1.liq else None),
                    (g1.liq.below if g1.liq else None),
                    snap.spread_pct,
                    getattr(snap.deriv_1h, "ratio_long_pct", None),
                )

                # Tầng 3 - Gate 2: Derivatives Regime (ONLY if Gate 1 passed - A-mode strict)
                if g1.passed:
                    g2 = gate2_derivatives_regime(snap, ctx2)
                    log.info(
                        "G2 %s %s | reason=%s regime=%s | ratio_long_pct=%s | funding=%s fz=%s | oi_d_pct=%s oi_spike_z=%s | hist=%s",
                        snap.symbol,
                        "PASS" if g2.passed else "FAIL",
                        getattr(g2, "reason", None),
                        getattr(g2, "regime", None),
                        getattr(g2, "ratio_long_pct", None),
                        getattr(g2, "funding", None),
                        getattr(g2, "funding_z", None),
                        getattr(g2, "oi_delta_pct", None),
                        getattr(g2, "oi_spike_z", None),
                        getattr(ctx2, "history_len", None),
                    )

            time.sleep(cfg.scan_interval_sec)

        except Exception as e:
            log.exception("Main loop error: %s", e)
            time.sleep(10)


if __name__ == "__main__":
    main()
