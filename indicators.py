# indicators.py
import pandas as pd
import numpy as np

# =========================
# Moving Averages & Oscillators
# =========================
def ema(series: pd.Series, span: int) -> pd.Series:
    # TradingView-compatible
    series = pd.to_numeric(series, errors="coerce")
    return series.ewm(span=span, adjust=False).mean()

def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    delta = s.diff()
    up, down = delta.clip(lower=0), -delta.clip(upper=0)
    roll_up   = up.ewm(alpha=1/period, adjust=False).mean()   # Wilder
    roll_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    # Wilder’s RMA (sát TV hơn)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def bollinger(series: pd.Series, window: int = 20, num_std: float = 2.0):
    s = pd.to_numeric(series, errors="coerce")
    mid = s.rolling(window).mean()
    std = s.rolling(window).std(ddof=0)
    upper, lower = mid + num_std*std, mid - num_std*std
    return upper, mid, lower

def rolling_zscore(series: pd.Series, window=20):
    mu = series.rolling(window).mean()
    sigma = series.rolling(window).std()
    return (series - mu) / sigma.replace(0, np.nan)

# =========================
# Enrich: base indicators
# =========================
# --- indicators.py ---

def enrich_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df = df.sort_index()
    c = pd.to_numeric(df["close"], errors="coerce")

    # EMA/RSI/BB/ATR như bạn đã có...
    df["ema20"] = ema(c, 20)
    df["ema50"] = ema(c, 50)
    df["rsi14"] = rsi(c, 14)

    bb_u, bb_m, bb_l = bollinger(c, 20, 2.0)
    df["bb_upper"], df["bb_mid"], df["bb_lower"] = bb_u, bb_m, bb_l

    # >>> Thêm: bb_width_pct (an toàn số học)
    width = (df["bb_upper"] - df["bb_lower"])
    base = df["bb_mid"].where(df["bb_mid"].abs() > 1e-12, other=c)  # mid=0 thì fallback close
    df["bb_width_pct"] = (width / base.abs()) * 100.0
    df["bb_width_pct"] = df["bb_width_pct"].replace([np.inf, -np.inf], np.nan)

    df["atr14"] = atr(df, 14)
    df["vol_sma20"] = pd.to_numeric(df["volume"], errors="coerce").rolling(20).mean()
    df["vol_ratio"] = df["volume"] / df["vol_sma20"]

    return df


# =========================
# Enrich: volume, candle anatomy, SMAs (cho SR mềm)
# =========================
def enrich_more(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Volume features
    out['vol_z20'] = rolling_zscore(out['volume'], 20)
    out['vol_up']  = (out['close'] > out['open']).astype(int)   # nến xanh
    out['vol_dn']  = (out['close'] < out['open']).astype(int)   # nến đỏ

    # Candle anatomy
    body = (out['close'] - out['open']).abs()
    rng  = (out['high'] - out['low']).replace(0, np.nan)
    out['body_pct']       = (body / rng * 100).clip(0, 100)
    out['upper_wick_pct'] = ((out['high'] - out[['open','close']].max(axis=1)) / rng * 100).clip(lower=0)
    out['lower_wick_pct'] = (((out[['open','close']].min(axis=1) - out['low']) / rng) * 100).clip(lower=0)

    # Soft SR components
    out['sma20'] = out['close'].rolling(20).mean()
    out['sma50'] = out['close'].rolling(50).mean()
    return out

# =========================
# Liquidity Zones (Volume Profile)
# =========================
def calc_vp(df: pd.DataFrame, window_bars: int = 120, bins: int = 24, top_k: int = 5):
    """
    Volume profile đơn giản trên 'window_bars' nến gần nhất.
    Dùng HLC3 làm đại diện giá mỗi nến để bucket theo bins.
    Trả về list dict sắp xếp theo volume giảm dần.
    """
    if len(df) == 0:
        return []
    sub = df.tail(window_bars)
    hlc3 = (sub['high'] + sub['low'] + sub['close']) / 3.0

    lo = float(sub['low'].min())
    hi = float(sub['high'].max())
    if not np.isfinite(lo) or not np.isfinite(hi) or lo >= hi:
        return []

    edges = np.linspace(lo, hi, bins + 1)
    idx = np.digitize(hlc3.values, edges) - 1
    idx = np.clip(idx, 0, bins - 1)

    vol_bins = np.zeros(bins)
    for i, v in zip(idx, sub['volume'].values):
        if np.isfinite(v):
            vol_bins[i] += v

    zones = []
    for i in range(bins):
        p_lo, p_hi = float(edges[i]), float(edges[i+1])
        zones.append({
            "price_range": (p_lo, p_hi),
            "price_mid": round((p_lo + p_hi) / 2.0, 2),
            "volume_sum": float(vol_bins[i]),
        })
    zones = sorted(zones, key=lambda x: -x["volume_sum"])
    return zones[:top_k]

# =========================
# Funding & Open Interest (KuCoin Futures via ccxt)
# =========================
def _normalize_futures_symbol_spotlike(symbol: str) -> str:
    """
    Chuyển 'BTCUSDT'/'BTC/USDT' thành 'BTC/USDT:USDT' (KuCoin Futures perpetual).
    Bạn có thể truyền trực tiếp symbol futures hợp lệ để bỏ qua bước này.
    """
    s = symbol.upper().replace(" ", "")
    if "/" in s:
        base, quote = s.split("/", 1)
        return f"{base}/{quote}:USDT"
    if s.endswith("USDT"):
        base = s[:-4]
        return f"{base}/USDT:USDT"
    if s.endswith("USD"):
        base = s[:-3]
        return f"{base}/USD:USDT"
    return f"{s}/USDT:USDT"

def fetch_futures_sentiment(symbol: str, futures_symbol: str = None):
    """
    Lấy funding rate & open interest từ KuCoin Futures qua ccxt.
    - symbol: dùng để suy luận futures_symbol nếu không truyền.
    - futures_symbol: ví dụ 'BTC/USDT:USDT'. Nếu bạn biết chính xác, nên truyền vào.
    Trả về dict hoặc {'error': '...'} nếu không lấy được.
    """
    try:
        import ccxt  # lazy import
    except Exception as e:
        return {"error": f"ccxt not installed: {e}"}

    try:
        fut = ccxt.kucoinfutures({"enableRateLimit": True})
        fut.load_markets()
    except Exception as e:
        return {"error": f"init kucoinfutures failed: {e}"}

    fsym = futures_symbol or _normalize_futures_symbol_spotlike(symbol)

    # Funding rate
    fr = None
    try:
        fr = fut.fetch_funding_rate(fsym)
    except Exception as e:
        return {"error": f"fetch_funding_rate failed for {fsym}: {e}"}

    # Open interest (may be unavailable on some versions)
    oi = None
    try:
        oi = fut.fetch_open_interest(fsym)
    except Exception:
        oi = None  # optional

    out = {
        "symbol": fsym,
        "funding_rate": fr.get("fundingRate") if isinstance(fr, dict) else None,
        "funding_interval": fr.get("fundingInterval") if isinstance(fr, dict) else None,
        "funding_timestamp": fr.get("timestamp") if isinstance(fr, dict) else None,
        "funding_info_raw": fr,
        "open_interest": (oi or {}).get("openInterestAmount") if isinstance(oi, dict) else None,
        "open_interest_raw": oi,
    }
    return out

# Convenience alias (giữ tên ngắn như bạn quen dùng)
def fetch_funding_oi(symbol: str, futures_symbol: str = None) -> dict:
    return fetch_futures_sentiment(symbol, futures_symbol)
