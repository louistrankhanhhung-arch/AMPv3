from __future__ import annotations

import time
from typing import Optional

from app.config import AppConfig
from app.utils.logger import setup_logger
from app.exchange.router import ExchangeRouter
from app.data.cache import TTLCache
from app.data.market_fetcher import MarketFetcher
from app.data.derivatives_fetcher import DerivativesFetcher
from app.data.models import MarketSnapshot, Derivatives1H

from app.gates.gate1_htf import gate1_htf_clarity
from app.gates.gate2_derivatives import gate2_derivatives_regime
from app.gates.gate3_structure import gate3_structure_confirmation_v0
from app.signals.planner import build_plan_v0
from app.gates.scoring import score_signal_v1


def build_snapshot(
    symbol: str,
    market: MarketFetcher,
    deriv: DerivativesFetcher,
    client,
    d1h: Optional[Derivatives1H] = None,
) -> MarketSnapshot:
    candles_15m = market.get_candles(symbol, "15m", limit=240, ttl_sec=20)
    candles_1h = market.get_candles(symbol, "1h", limit=240, ttl_sec=40)
    candles_4h = market.get_candles(symbol, "4h", limit=240, ttl_sec=90)

    # Reuse derivatives snapshot if already fetched (avoid double-fetch / rate-limit risk)
    if d1h is None:
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
                # Always warm-up + persist derivatives rolling series BEFORE Gate 1
                # so Gate2 becomes restart-proof and does not depend on G1 pass.
                ctx2 = deriv.get_gate2_ctx(sym, ttl_sec=30)

                # Build snapshot reusing derivatives from ctx2 (single fetch source-of-truth)
                snap = build_snapshot(sym, market, deriv, client, d1h=ctx2.last)
                
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

                # --- One-line journal (fail-closed, correct reason semantics) ---
                # Gate fail -> gate reason
                # Planner fail -> planner reason
                # Score skip -> score reasons

                if not g1.passed:
                    log.info(
                        "JOURNAL %s | ex=%s | stage=G1_FAIL | reason=%s | g1=%s/%s pos=%.2f | spread_pct=%s | ratio_long_pct=%s",
                        snap.symbol,
                        client.name,
                        getattr(g1, "reason", None),
                        (g1.htf.bias if g1.htf else None),
                        (g1.htf.location if g1.htf else None),
                        (g1.htf.pos_pct if g1.htf else -1),
                        snap.spread_pct,
                        getattr(snap.deriv_1h, "ratio_long_pct", None),
                    )
                    continue

                # Tầng 3 - Gate 2: Derivatives Regime (A-mode strict)
                g2 = gate2_derivatives_regime(snap, ctx2)
                log.info(
                    "G2 %s %s | reason=%s regime=%s | alert_only=%s | ratio_long_pct=%s | funding=%s fz=%s | oi_d_pct=%s oi_spike_z=%s | oi_slope_4h_pct=%s | hist=%s",
                    snap.symbol,
                    "PASS" if g2.passed else "FAIL",
                    getattr(g2, "reason", None),
                    getattr(g2, "regime", None),
                    getattr(g2, "alert_only", False),
                    getattr(g2, "ratio_long_pct", None),
                    getattr(g2, "funding", None),
                    getattr(g2, "funding_z", None),
                    getattr(g2, "oi_delta_pct", None),
                    getattr(g2, "oi_spike_z", None),
                    getattr(g2, "oi_slope_4h_pct", None),
                    getattr(ctx2, "history_len", None),
                )

                if (not g2.passed) or bool(getattr(g2, "alert_only", False)):
                    log.info(
                        "JOURNAL %s | ex=%s | stage=G2_FAIL | reason=%s | g1=%s/%s pos=%.2f | g2=%s conf=%s hint=%s alert=%s | ratio_long_pct=%s | funding_z=%s | oi_slope_4h_pct=%s",
                        snap.symbol,
                        client.name,
                        getattr(g2, "reason", None),
                        (g1.htf.bias if g1.htf else None),
                        (g1.htf.location if g1.htf else None),
                        (g1.htf.pos_pct if g1.htf else -1),
                        getattr(g2, "regime", None),
                        getattr(g2, "confidence", None),
                        getattr(g2, "directional_bias_hint", None),
                        getattr(g2, "alert_only", False),
                        getattr(g2, "ratio_long_pct", None),
                        getattr(g2, "funding_z", None),
                        getattr(g2, "oi_slope_4h_pct", None),
                    )
                    continue

                # Tầng 4 - Gate 3: Structure Confirmation (SMC)
                g3 = gate3_structure_confirmation_v0(snap, g1, g2)
                g3_mode = None
                g3_trigger = None
                try:
                    g3_mode = (g3.notes or {}).get("mode")
                    g3_trigger = (g3.notes or {}).get("trigger")
                except Exception:
                    # best-effort; keep None
                    pass
                log.info(
                    "G3 %s %s | reason=%s | mode=%s trigger=%s | tp2_candidate=%s | zone=%s | struct=%s trend=%s | break_level=%s",
                    snap.symbol,
                    "PASS" if g3.passed else "FAIL",
                    getattr(g3, "reason", None),
                    g3_mode,
                    g3_trigger,
                    getattr(g3, "tp2_candidate", None),
                    (getattr(g3.zone, "kind", None) if getattr(g3, "zone", None) else None),
                    (getattr(g3.structure, "reason", None) if getattr(g3, "structure", None) else None),
                    (getattr(g3.structure, "trend", None) if getattr(g3, "structure", None) else None),
                    (getattr(g3.structure, "break_level", None) if getattr(g3, "structure", None) else None),
                )

                if not g3.passed:
                    log.info(
                        "JOURNAL %s | ex=%s | stage=G3_FAIL | reason=%s | mode=%s trigger=%s | g1=%s/%s pos=%.2f | g2=%s conf=%s hint=%s | struct=%s",
                        snap.symbol,
                        client.name,
                        getattr(g3, "reason", None),
                        g3_mode,
                        g3_trigger,
                        (g1.htf.bias if g1.htf else None),
                        (g1.htf.location if g1.htf else None),
                        (g1.htf.pos_pct if g1.htf else -1),
                        getattr(g2, "regime", None),
                        getattr(g2, "confidence", None),
                        getattr(g2, "directional_bias_hint", None),
                        (getattr(g3.structure, "reason", None) if getattr(g3, "structure", None) else None),
                    )
                    continue

                # Planner (REAL RR)
                plan = build_plan_v0(snap, g1, g2, g3, min_rr_tp2=2.5)
                if plan is None:
                    # best-effort planner reason (since planner currently returns None only)
                    log.info(
                        "JOURNAL %s | ex=%s | stage=PLANNER_FAIL | reason=%s | intent=%s | tp2_candidate=%s | zone=%s",
                        snap.symbol,
                        client.name,
                        "planner_guard_fail",
                        getattr(g3, "intent", None),
                        getattr(g3, "tp2_candidate", None),
                        (getattr(g3.zone, "kind", None) if getattr(g3, "zone", None) else None),
                    )
                    continue

                # Score (strict, requires plan)
                s = score_signal_v1(snap, g1, g2, g3, plan=plan, only_trade_tiers=("A", "B"))
                if not s.passed:
                    log.info(
                        "JOURNAL %s | ex=%s | stage=SCORE_SKIP | reason=%s | tier=%s score=%s rr2=%.2f | intent=%s | E1=%.4f SL=%.4f",
                        plan.symbol,
                        client.name,
                        ",".join(s.reasons[:6]),
                        s.tier,
                        s.score_0_100,
                        s.rr_tp2,
                        plan.intent,
                        plan.entry1,
                        plan.sl,
                    )
                    continue

                # Final journal (PLAN + SCORE in one line, stable keys)
                tp_map = {tp.name: tp.price for tp in plan.tps}
                log.info(
                    "JOURNAL %s | ex=%s | stage=OK | tier=%s score=%s rr2=%.2f rmult=%.2f | intent=%s | "
                    "E1=%.4f E2=%s SL=%.4f | TP1=%.4f TP2=%.4f TP3=%.4f TP4=%.4f TP5=%.4f | "
                    "g1=%s/%s pos=%.2f | g2=%s conf=%s hint=%s | g3=%s | zone=%s fill=%s | leeway=%.4f(%s)",
                    plan.symbol,
                    client.name,
                    s.tier,
                    s.score_0_100,
                    s.rr_tp2,
                    s.risk_mult,
                    plan.intent,
                    plan.entry1,
                    (f"{plan.entry2:.4f}" if plan.entry2 is not None else "NA"),
                    plan.sl,
                    float(tp_map.get("TP1", 0.0)),
                    float(tp_map.get("TP2", 0.0)),
                    float(tp_map.get("TP3", 0.0)),
                    float(tp_map.get("TP4", 0.0)),
                    float(tp_map.get("TP5", 0.0)),
                    (g1.htf.bias if g1.htf else None),
                    (g1.htf.location if g1.htf else None),
                    (g1.htf.pos_pct if g1.htf else -1),
                    getattr(g2, "regime", None),
                    getattr(g2, "confidence", None),
                    getattr(g2, "directional_bias_hint", None),
                    f"{getattr(g3, 'reason', None)}|{g3_mode}|{g3_trigger}",
                    (getattr(getattr(g3, "zone", None), "kind", None)),
                    (getattr(getattr(g3, "zone", None), "fill_pct", None)),
                    plan.leeway_price,
                    plan.leeway_reason,
                )

            time.sleep(cfg.scan_interval_sec)

        except Exception as e:
            log.exception("Main loop error: %s", e)
            time.sleep(10)


if __name__ == "__main__":
    main()
