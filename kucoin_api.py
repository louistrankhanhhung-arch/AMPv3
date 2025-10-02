# kucoin_api.py (refactored: SPOT-only + partial-bar only for 1H)
# -----------------------------------------------------------------
# - Unified ccxt client (SPOT only)
# - Symbol normalization/validation
# - Clean DataFrame conversion
# - Retry/backoff
# - Deep historical pagination
# - Partial-bar drop is applied ONLY for 1H timeframe (per requirement)

from __future__ import annotations

import time
import random
from typing import Dict, Iterable, List, Optional

import pandas as pd
import ccxt  # type: ignore

# ---------------------------------
# Timeframe mapping (friendly -> ccxt)
# ---------------------------------
TIMEFRAME_MAP: Dict[str, str] = {
    "1H": "1h",
    "4H": "4h",
    "1D": "1d",
    "1W": "1w",
}

# ---------------------------------
# Client factory (single source of truth) — SPOT only
# ---------------------------------
_DEF_TIMEOUT_MS = 15000


def _exchange(
    kucoin_key: Optional[str] = None,
    kucoin_secret: Optional[str] = None,
    kucoin_passphrase: Optional[str] = None,
    *,
    timeout_ms: int = _DEF_TIMEOUT_MS,
    enable_rate_limit: bool = True,
    proxy: Optional[str] = None,
) -> ccxt.kucoin:
    """Create a configured KuCoin ccxt client for SPOT only. Public data does not require keys."""
    cfg: Dict[str, object] = {
        "enableRateLimit": enable_rate_limit,
        "timeout": timeout_ms,
        "options": {"defaultType": "spot"},  # enforce SPOT
    }
    if kucoin_key and kucoin_secret and kucoin_passphrase:
        cfg.update({
            "apiKey": kucoin_key,
            "secret": kucoin_secret,
            "password": kucoin_passphrase,
        })
    if proxy:
        cfg["proxies"] = {"http": proxy, "https": proxy}

    ex = ccxt.kucoin(cfg)
    ex.load_markets()  # loads SPOT markets
    return ex

# ---------------------------------
# Symbol normalization & validation
# ---------------------------------

def _normalize_symbol(symbol: str) -> str:
    """Convert common forms (e.g., 'BTCUSDT', 'btc-usdt', 'btc/usdt') to 'BTC/USDT'."""
    s = (symbol or "").strip().upper().replace("-", "/")
    if "/" in s:
        base, quote = s.split("/", 1)
        return f"{base}/{quote}"
    if s.endswith("USDT"):
        return f"{s[:-4]}/USDT"
    if s.endswith("USD"):
        return f"{s[:-3]}/USD"
    return s


def _validate_symbol(ex: ccxt.Exchange, symbol: str) -> str:
    """Normalize symbol and ensure it exists in the currently loaded SPOT markets.

    Tries common quote variants if the initial form is not found.
    Raises ValueError if not resolvable.
    """
    sym = _normalize_symbol(symbol)
    if sym in ex.markets:
        return sym

    # try common quote alternatives
    base = sym.split("/")[0] if "/" in sym else sym
    for quote in ("USDT", "USD", "USDC", "TUSD"):
        candidate = f"{base}/{quote}"
        if candidate in ex.markets:
            return candidate

    raise ValueError(
        f"Symbol '{symbol}' not found in SPOT markets")

def _is_rate_limit(e: Exception) -> bool:
    """Detect KuCoin user-level rate limit (429000) and generic 429 messages."""
    s = str(e)
    return isinstance(e, ccxt.RateLimitExceeded) or ("429000" in s) or ("Too many requests" in s)


# ---------------------------------
# DataFrame conversion & cleaning
# ---------------------------------

def _to_dataframe(ohlcv: List[List[float]]) -> pd.DataFrame:
    """Convert ccxt OHLCV to a clean pandas DataFrame indexed by UTC time.

    ccxt OHLCV format: [ts_ms, open, high, low, close, volume]
    """
    cols = ["ts", "open", "high", "low", "close", "volume"]
    if not ohlcv:
        return pd.DataFrame(columns=cols[1:])

    df = pd.DataFrame(ohlcv, columns=cols)
    df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df = (
        df.set_index("time")[cols[1:]]
        .apply(pd.to_numeric, errors="coerce")
        .replace([float("inf"), float("-inf")], pd.NA)
        .sort_index()
    )
    # drop duplicated timestamps, keep the last (most up-to-date) record
    df = df[~df.index.duplicated(keep="last")]

    # drop rows with NaN in O/H/L/C (volume can be 0.0)
    df = df.dropna(subset=["open", "high", "low", "close"])
    # volume: fill NaN with 0 for safety
    if "volume" in df.columns:
        df["volume"] = df["volume"].fillna(0.0)

    return df

# ---------------------------------
# Helpers
# ---------------------------------

def _ccxt_timeframe_str(tf: str) -> str:
    return TIMEFRAME_MAP.get(tf.upper(), tf)


def _bar_ms(ex: ccxt.Exchange, tf_str: str) -> int:
    """Milliseconds per bar for the ccxt timeframe string (e.g., '4h')."""
    return int(ex.parse_timeframe(tf_str) * 1000)


def _drop_partial_bar(df: pd.DataFrame, bar_ms: int) -> pd.DataFrame:
    """Drop the last bar if it appears to be still forming (partial)."""
    if df is None or df.empty or len(df) < 2:
        return df
    # pandas Timestamp -> ns; convert to ms
    last_ms = int(df.index[-1].value / 1e6)
    if (last_ms % bar_ms) != 0:
        return df.iloc[:-1]
    return df


def _retry_sleep(attempt: int, base: float = 0.5, cap: float = 5.0) -> None:
    """Exponential backoff sleep with jitter."""
    delay = min(cap, base * (2 ** attempt))
    time.sleep(delay)

# ---------------------------------
# Public API
# ---------------------------------

def fetch_ohlcv(
    symbol: str,
    timeframe: str = "4H",
    *,
    limit: int = 300,
    since_ms: Optional[int] = None,
    kucoin_key=None, kucoin_secret=None, kucoin_passphrase=None,
    timeout_ms=_DEF_TIMEOUT_MS, enable_rate_limit=True, proxy=None,
    max_retries: int = 6,
    drop_partial: bool = True,
    ex: Optional[ccxt.kucoin] = None
) -> pd.DataFrame:
    """
    Fetch OHLCV for a single timeframe.
    - If `ex` is provided, reuse it (no new load_markets()).
    - Drop partial bar only for 1H when `drop_partial=True`.
    - Robust backoff with jitter on 429000 / Too many requests.
    """
    tf_str = TIMEFRAME_MAP.get(timeframe.upper(), timeframe)
    _ex = ex or _exchange(kucoin_key, kucoin_secret, kucoin_passphrase,
                          timeout_ms=timeout_ms, enable_rate_limit=enable_rate_limit, proxy=proxy)
    sym = _validate_symbol(_ex, symbol)

    # retry loop for robustness
    attempt = 0
    while True:
        try:
            raw = _ex.fetch_ohlcv(sym, timeframe=tf_str, since=since_ms, limit=limit)
            df = _to_dataframe(raw)
            # chỉ cắt nến chưa đóng nếu là timeframe 1H (và cờ drop_partial bật)
            if drop_partial and timeframe.upper() == "1H" and not df.empty:
                df = _drop_partial_bar(df, _bar_ms(_ex, tf_str))
            return df
        except (ccxt.NetworkError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as e:
            if attempt >= max_retries:
                raise
            sleep_s = min(20.0, 1.0 * (2 ** attempt)) + random.uniform(0.05, 0.35)
            time.sleep(sleep_s)
            attempt += 1
            continue
        except ccxt.ExchangeError as e:
            # KuCoin 429 user-level rate limit
            if _is_rate_limit(e) and attempt < max_retries:
                sleep_s = min(20.0, 1.0 * (2 ** attempt)) + random.uniform(0.05, 0.35)
                time.sleep(sleep_s)
                attempt += 1
                continue
            raise


def fetch_ohlcv_history(
    symbol: str,
    timeframe: str = "4H",
    *,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
    limit: int = 300,
    kucoin_key: Optional[str] = None,
    kucoin_secret: Optional[str] = None,
    kucoin_passphrase: Optional[str] = None,
    timeout_ms: int = _DEF_TIMEOUT_MS,
    enable_rate_limit: bool = True,
    proxy: Optional[str] = None,
    drop_partial: bool = False,
    max_retries: int = 3,
    sleep_sec: float = 0.2,
    max_pages: Optional[int] = None,
) -> pd.DataFrame:
    """Deep historical pagination of OHLCV (SPOT only).

    Fetches multiple pages to cover [start_ms, end_ms] (UTC ms). If start_ms is
    None, it will page backwards from 'now' until max_pages (if set) or until no
    more data is returned.

    Partial-bar dropping at the end is applied ONLY for 1H timeframe if requested.
    """
    tf_str = _ccxt_timeframe_str(timeframe)
    ex = _exchange(
        kucoin_key, kucoin_secret, kucoin_passphrase,
        timeout_ms=timeout_ms,
        enable_rate_limit=enable_rate_limit,
        proxy=proxy,
    )
    sym = _validate_symbol(ex, symbol)

    bar_ms = _bar_ms(ex, tf_str)
    if end_ms is None:
        end_ms = int(time.time() * 1000)

    frames: List[pd.DataFrame] = []
    pages = 0

    cursor_since = start_ms
    if cursor_since is None:
        cursor_since = end_ms - limit * bar_ms

    while True:
        if max_pages is not None and pages >= max_pages:
            break

        df = fetch_ohlcv(
            sym,
            timeframe=tf_str,
            limit=limit,
            since_ms=cursor_since,
            kucoin_key=kucoin_key,
            kucoin_secret=kucoin_secret,
            kucoin_passphrase=kucoin_passphrase,
            timeout_ms=timeout_ms,
            enable_rate_limit=enable_rate_limit,
            proxy=proxy,
            drop_partial=False,  # handle partial at the end only
            max_retries=max_retries,
        )

        if df.empty:
            break

        frames.append(df)
        pages += 1

        first_ts = int(df.index[0].value / 1e6)  # ms
        if start_ms is not None and first_ts <= start_ms:
            break

        cursor_since = first_ts - limit * bar_ms
        time.sleep(sleep_sec)

    if not frames:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])  # empty

    out = pd.concat(frames, axis=0).sort_index()
    out = out[~out.index.duplicated(keep="last")]

    if start_ms is not None:
        out = out[out.index >= pd.to_datetime(start_ms, unit="ms", utc=True)]
    if end_ms is not None:
        out = out[out.index <= pd.to_datetime(end_ms, unit="ms", utc=True)]

    # Only drop partial bar for 1H timeframe
    if drop_partial and tf_str == "1h" and not out.empty:
        out = _drop_partial_bar(out, bar_ms)

    return out


def fetch_batch(
    symbol: str,
    timeframes: Iterable[str] = ("1H", "4H", "1D"),
    *,
    limit: int = 300,
    since_ms: Optional[int] = None,
    kucoin_key=None, kucoin_secret=None, kucoin_passphrase=None,
    timeout_ms=_DEF_TIMEOUT_MS, enable_rate_limit=True, proxy=None,
    sleep_between_tf: float = 0.3,
    drop_partial: bool = True,
    ex: Optional[ccxt.kucoin] = None
) -> Dict[str, pd.DataFrame]:
    _ex = ex or _exchange(kucoin_key, kucoin_secret, kucoin_passphrase,
                          timeout_ms=timeout_ms, enable_rate_limit=enable_rate_limit, proxy=proxy)
    sym = _validate_symbol(_ex, symbol)

    out: Dict[str, pd.DataFrame] = {}
    for tf in timeframes:
        # Apply partial-bar drop only for 1H
        partial_flag = drop_partial and (tf.upper() == "1H")
        out[tf] = fetch_ohlcv(
            sym, timeframe=tf, limit=limit, since_ms=since_ms,
            kucoin_key=kucoin_key, kucoin_secret=kucoin_secret, kucoin_passphrase=kucoin_passphrase,
            timeout_ms=timeout_ms, enable_rate_limit=enable_rate_limit, proxy=proxy,
            drop_partial=drop_partial, ex=_ex
        )
        # giảm burst giữa các khung thời gian để tránh 429
        if sleep_between_tf and sleep_between_tf > 0:
            time.sleep(float(sleep_between_tf))
    return out

# --------------------------
# Example (commented):
# --------------------------
# if __name__ == "__main__":
#     # Fetch last ~300 1H candles for BTC/USDT (spot) and drop partial bar
#     df = fetch_ohlcv("BTCUSDT", timeframe="1H", drop_partial=True)
#     print(df.tail())
#
#     # Deep history: last 365 days of 4H bars (realtime, no partial dropping)
#     one_year_ms = int((time.time() - 365 * 86400) * 1000)
#     hist = fetch_ohlcv_history(
#         "BTC/USDT",
#         timeframe="4H",
#         start_ms=one_year_ms,
#         drop_partial=False,
#     )
#     print(hist.shape)
