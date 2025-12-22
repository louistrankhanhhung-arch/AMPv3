from __future__ import annotations

import time
from typing import Optional

from config import AppConfig
from utils.logger import setup_logger
from exchange.router import ExchangeRouter
from data.cache import TTLCache
from data.market_fetcher import MarketFetcher
from data.derivatives_fetcher import DerivativesFetcher
from data.models import MarketSnapshot


def build_snapshot(symbol: str, market: MarketFetcher, deriv: DerivativesFetcher, client) -> MarketSnapshot:
    candles_15m = market.get_candles(symbol, "15m", limit=240, ttl_sec=20)
    candles_1h = market.get_candles(symbol, "1h", limit=240, ttl_sec=40)
    candles_4h = market.get_candles(symbol, "4h", limit=240, ttl_sec=90)

    d1h = deriv.get_derivatives_1h(symbol, ttl_sec=30)

    mark = client.fetch_mark_price(symbol)
    spread = client.fetch_spread_bps(symbol)

    now = int(time.time())
    return MarketSnapshot(
        symbol=symbol,
        candles_15m=candles_15m,
        candles_1h=candles_1h,
        candles_4h=candles_4h,
        deriv_1h=d1h,
        spread_bps=spread,
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
                    "SNAPSHOT %s | ex=%s | mark=%s | spread_bps=%s | funding=%s | oi=%s | ratio=%s",
                    snap.symbol,
                    client.name,
                    snap.mark_price,
                    snap.spread_bps,
                    snap.deriv_1h.funding_rate,
                    snap.deriv_1h.open_interest,
                    snap.deriv_1h.long_short_ratio,
                )

            time.sleep(cfg.scan_interval_sec)

        except Exception as e:
            log.exception("Main loop error: %s", e)
            time.sleep(10)


if __name__ == "__main__":
    main()
