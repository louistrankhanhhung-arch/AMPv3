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

            time.sleep(cfg.scan_interval_sec)

        except Exception as e:
            log.exception("Main loop error: %s", e)
            time.sleep(10)


if __name__ == "__main__":
    main()
