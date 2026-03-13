from js import fetch, Headers, Object
from pyodide.ffi import to_js
from workers import Response, WorkerEntrypoint
import json
import math
from datetime import datetime, time as dtime, timedelta

# ═══════════════════════════════════════════════════════════════════════
# PRECONFIGURED SCREENERS
# ═══════════════════════════════════════════════════════════════════════

SCREENERS = [
    {
        "id": "goat1",
        "name": "GOAT1 — Monthly Long Holdings",
        "url": "https://www.screener.in/screens/3525076/goat1/",
        "query": """Is not SME AND
Market Capitalization > 200 AND
Return on equity > 12 AND
Return on capital employed > 12 AND
Debt to equity < 1 AND
Pledged percentage < 15 AND
Cash from operations last year > 0 AND
Sales growth 3Years > 8 AND
Profit growth 3Years > 10 AND
YOY Quarterly sales growth > 8 AND
YOY Quarterly profit growth > 10 AND
OPM > 10 AND
OPM latest quarter > OPM preceding year quarter AND
Piotroski score > 5 AND
Price to Earning < Industry PE AND
Promoter holding > 35 AND
(Change in FII holding > 0 OR Change in DII holding > 0) AND
Current price > DMA 50 AND
Current price > DMA 200 AND
RSI > 45 AND
RSI < 78 AND
MACD > MACD Signal AND
Volume > Volume 1month average""",
        "enabled": True,
        "interval_minutes": 5,
        "start_time": "09:15",
        "end_time": "15:30",
        "start_date": "",
        "end_date": "",
        "last_run_epoch": 0,
    },
    {
        "id": "opus-tele",
        "name": "Opus-Tele — Momentum Screen",
        "url": "https://www.screener.in/screens/3535927/opus-tele/",
        "query": """Is not SME AND
Return over 1month > 0 AND
Return over 1week > 0 AND
Return over 1day > 0 AND
Return over 3months > 0 AND
Current price > DMA 50 AND
Current price > DMA 200 AND
DMA 50 > DMA 200 AND
DMA 50 > DMA 50 previous day AND
DMA 200 > DMA 200 previous day AND
RSI > 52 AND
RSI < 68 AND
MACD > MACD Signal AND
MACD > 0 AND
MACD > MACD Previous Day AND
MACD Signal > MACD Signal Previous Day AND
Volume > Volume 1month average AND
Current price > High price * 0.85 AND
Profit after tax > 0 AND
Return on capital employed > 15 AND
Return on equity > 12 AND
Debt to equity < 1 AND
Piotroski score > 5 AND
Sales growth 3Years > 8 AND
Profit growth 3Years > 5 AND
Cash from operations last year > 0 AND
Free cash flow last year > 0 AND
Pledged percentage < 10 AND
Promoter holding > 30 AND
Quick ratio > 0.8 AND
Price to Earning < 80 AND
YOY Quarterly sales growth > 10 AND
YOY Quarterly profit growth > 10""",
        "enabled": True,
        "interval_minutes": 5,
        "start_time": "09:15",
        "end_time": "15:30",
        "start_date": "",
        "end_date": "",
        "last_run_epoch": 0,
    },
]

DEFAULT_SETTINGS = {
    "enabled": True,
    "schedule_mode": "individual",
    "interval_minutes": 5,
    "start_time": "09:15",
    "end_time": "15:30",
    "start_date": "",
    "end_date": "",
    "last_run": "",
    "last_run_epoch": 0,
    "total_runs": 0,
    "min_score": 50,
}

# ═══════════════════════════════════════════════════════════════════════
# TIMEFRAME DEFINITIONS (Yahoo Finance)
# ═══════════════════════════════════════════════════════════════════════

TIMEFRAMES = {
    "intraday":  {"label": "Intraday (Today)",     "interval": "1m",  "range": "1d",   "candles_min": 20},
    "d2_3":      {"label": "2–3 Days",             "interval": "15m", "range": "5d",   "candles_min": 20},
    "week":      {"label": "1 Week",               "interval": "30m", "range": "1mo",  "candles_min": 20},
    "w2":        {"label": "1–2 Weeks",            "interval": "1h",  "range": "1mo",  "candles_min": 15},
    "month":     {"label": "1 Month",              "interval": "1d",  "range": "3mo",  "candles_min": 15},
    "m2":        {"label": "2 Months",             "interval": "1d",  "range": "6mo",  "candles_min": 15},
    "m3":        {"label": "3 Months",             "interval": "1d",  "range": "1y",   "candles_min": 15},
    "m6":        {"label": "6 Months",             "interval": "1wk", "range": "2y",   "candles_min": 15},
}

# Which candles to use per timeframe (last N candles)
TF_CANDLE_LIMIT = {
    "intraday": None,   # all intraday
    "d2_3":     96,     # ~2-3 trading days at 15m
    "week":     52,     # ~1 week at 30m
    "w2":       80,     # ~2 weeks at 1h
    "month":    22,     # ~1 month daily
    "m2":       44,     # ~2 months daily
    "m3":       66,     # ~3 months daily
    "m6":       26,     # ~6 months weekly
}

# ═══════════════════════════════════════════════════════════════════════
# HELPERS & TECHNICAL ANALYSIS ENGINE
# ═══════════════════════════════════════════════════════════════════════

async def js_fetch(url, method="GET", headers=None, body=None):
    h = Headers.new(to_js(headers or {}))
    if body:
        resp = await fetch(url, method=method, headers=h, body=body)
    else:
        resp = await fetch(url, method=method, headers=h)
    text = await resp.text()
    return int(str(resp.status)), str(text)

def json_response(data, status=200):
    return Response.new(
        json.dumps(data),
        to_js({"status": status, "headers": {"Content-Type": "application/json"}},
              dict_converter=Object.fromEntries))

def html_response(html, status=200):
    return Response.new(
        html,
        to_js({"status": status, "headers": {"Content-Type": "text/html"}},
              dict_converter=Object.fromEntries))

def ist_now():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def is_market_hours():
    now = ist_now()
    if now.weekday() >= 5: return False
    return dtime(9, 15) <= now.time() <= dtime(15, 30)

def is_entry_window():
    t = ist_now().time()
    return dtime(9, 30) <= t <= dtime(11, 30) or dtime(14, 0) <= t <= dtime(14, 45)

def fmt_price(p):
    if p is None: return "N/A"
    return f"₹{round(float(p), 2)}"

def fmt_pct(a, b):
    try:
        v = round((float(a) - float(b)) / float(b) * 100, 2)
        return f"{'+' if v >= 0 else ''}{v}%"
    except:
        return "N/A"

# ═══════════════════════════════════════════════════════════════════════
# SCREENER.IN SCRAPER
# ═══════════════════════════════════════════════════════════════════════

def parse_screener_url(company_url):
    """
    Extract symbol + exchange from Screener.in company URL.

    URL formats seen in the wild:
      NSE+BSE stock : /company/TATAMOTORS/
      NSE+BSE stock : /company/TATAMOTORS/consolidated/
      BSE-only stock: /company/543916/
      BSE-only stock: /company/543916/consolidated/

    Returns (symbol, exchange) where exchange is "NS" or "BO".
    """
    if not company_url:
        return "", "NS"

    # Strip trailing slash, split, drop empty segments
    parts = [p for p in company_url.strip("/").split("/") if p]
    # parts example: ["company", "TATAMOTORS", "consolidated"]
    # We want the segment right after "company"
    try:
        idx    = parts.index("company")
        raw    = parts[idx + 1].upper()
    except (ValueError, IndexError):
        # Fallback: take second-to-last non-empty segment
        raw = parts[-2].upper() if len(parts) >= 2 else parts[-1].upper()

    # If the segment is purely numeric → BSE scrip code → use .BO
    if raw.isdigit():
        return raw, "BO"

    # Alphabetic / alphanumeric → NSE symbol → use .NS
    return raw, "NS"

async def fetch_screener_results(screener_url, cookie=""):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }
    if cookie:
        headers["Cookie"] = cookie
    url = screener_url + ("&format=json" if "?" in screener_url else "?format=json")
    status, text = await js_fetch(url, headers=headers)
    if status != 200: return []
    try:
        data = json.loads(text)
        stocks = []
        for r in data.get("results", []):
            symbol, exchange = parse_screener_url(r.get("url", ""))
            if not symbol:
                continue
            stocks.append({
                "name":     r.get("name", ""),
                "symbol":   symbol,
                "exchange": exchange,          # "NS" or "BO"
                "price":    r.get("price", 0),
                "change":   r.get("change", 0),
            })
        return stocks
    except:
        return []

# ═══════════════════════════════════════════════════════════════════════
# YAHOO FINANCE — MULTI-TIMEFRAME CANDLE FETCH
# ═══════════════════════════════════════════════════════════════════════

def _parse_candles(text, symbol_label):
    """Parse Yahoo Finance JSON into candle list."""
    try:
        data   = json.loads(text)
        result = data["chart"]["result"][0]
        ts     = result["timestamp"]
        q      = result["indicators"]["quote"][0]
        opens, highs, lows, closes, volumes = (
            q.get(k, []) for k in ["open", "high", "low", "close", "volume"])
        candles = []
        for i, t in enumerate(ts):
            if None in [opens[i], highs[i], lows[i], closes[i], volumes[i]]:
                continue
            ist_t = datetime.utcfromtimestamp(t) + timedelta(hours=5, minutes=30)
            candles.append({
                "time": ist_t, "open": opens[i], "high": highs[i],
                "low": lows[i], "close": closes[i], "volume": volumes[i],
            })
        return candles
    except:
        return []

async def fetch_candles(symbol, exchange, interval, yf_range):
    """
    Fetch OHLCV candles from Yahoo Finance.
    exchange = "NS" (NSE) or "BO" (BSE).
    If exchange is empty (e.g. index symbols like ^NSEI), use symbol as-is.
    Falls back to the other exchange if primary returns no data.
    """
    # For index symbols (empty exchange), use symbol directly without suffix
    if exchange:
        primary  = f"{symbol}.{exchange}"
        fallback = f"{symbol}.{'BO' if exchange == 'NS' else 'NS'}"
    else:
        primary  = symbol
        fallback = None

    base_url  = "https://query1.finance.yahoo.com/v8/finance/chart"
    params    = f"?interval={interval}&range={yf_range}&includePrePost=false"

    # Try primary exchange first
    status, text = await js_fetch(
        f"{base_url}/{primary}{params}",
        headers={"User-Agent": "Mozilla/5.0"}
    )
    if status == 200:
        candles = _parse_candles(text, primary)
        if candles:
            return candles, primary

    # Fallback to other exchange (skip for index symbols)
    if fallback:
        status, text = await js_fetch(
            f"{base_url}/{fallback}{params}",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        if status == 200:
            candles = _parse_candles(text, fallback)
            if candles:
                return candles, fallback

    return [], primary  # both failed

async def fetch_all_timeframes(symbol, exchange):
    """Fetch all 8 timeframes, returns {tf_key: candles} and resolved_symbol."""
    results         = {}
    resolved_symbol = f"{symbol}.{exchange}"

    for tf_key, tf_cfg in TIMEFRAMES.items():
        candles, used_sym = await fetch_candles(
            symbol, exchange, tf_cfg["interval"], tf_cfg["range"])

        # Track which Yahoo symbol actually returned data
        if candles:
            resolved_symbol = used_sym

        limit = TF_CANDLE_LIMIT.get(tf_key)
        if limit and len(candles) > limit:
            candles = candles[-limit:]
        results[tf_key] = candles

    return results, resolved_symbol

# ═══════════════════════════════════════════════════════════════════════
# TECHNICAL INDICATOR LIBRARY  (pure Python, zero external deps)
# ═══════════════════════════════════════════════════════════════════════

def calc_ema(closes, period):
    if len(closes) < period: return []
    k    = 2 / (period + 1)
    emas = [sum(closes[:period]) / period]
    for c in closes[period:]:
        emas.append(c * k + emas[-1] * (1 - k))
    return emas

def last_ema(closes, period):
    e = calc_ema(closes, period)
    return round(e[-1], 2) if e else None

def calc_atr(candles, period=14):
    if len(candles) < 2: return None
    trs = [max(candles[i]["high"]-candles[i]["low"],
               abs(candles[i]["high"]-candles[i-1]["close"]),
               abs(candles[i]["low"]-candles[i-1]["close"]))
           for i in range(1, len(candles))]
    if not trs: return None
    if len(trs) < period: return sum(trs)/len(trs)
    atr = sum(trs[:period])/period
    for tr in trs[period:]:
        atr = (atr*(period-1)+tr)/period
    return round(atr, 4)

def calc_rsi(closes, period=14):
    if len(closes) < period+1: return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i]-closes[i-1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    ag = sum(gains[:period])/period; al = sum(losses[:period])/period
    for i in range(period, len(gains)):
        ag = (ag*(period-1)+gains[i])/period
        al = (al*(period-1)+losses[i])/period
    return round(100-(100/(1+ag/al)), 2) if al != 0 else 100.0

def calc_macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow+signal: return None, None, None
    ema_fast = calc_ema(closes, fast)
    ema_slow = calc_ema(closes, slow)
    diff = len(ema_fast)-len(ema_slow)
    macd_line = [ema_fast[i+diff]-ema_slow[i] for i in range(len(ema_slow))]
    sig_line  = calc_ema(macd_line, signal)
    if not sig_line: return None, None, None
    hist = macd_line[-1]-sig_line[-1]
    return round(macd_line[-1],4), round(sig_line[-1],4), round(hist,4)

def calc_bb(closes, period=20, mult=2.0):
    if len(closes) < period: return None, None, None
    w   = closes[-period:]
    mid = sum(w)/period
    std = math.sqrt(sum((x-mid)**2 for x in w)/period)
    return round(mid-mult*std,2), round(mid,2), round(mid+mult*std,2)

def calc_rsi_signal(rsi):
    if rsi is None: return "neutral"
    if rsi < 30: return "oversold"
    if rsi > 70: return "overbought"
    if 45 <= rsi <= 70: return "bullish"
    return "neutral"

def calc_stoch(candles, k_period=14, d_period=3):
    if len(candles) < k_period: return None, None
    k_vals = []
    for i in range(k_period-1, len(candles)):
        w  = candles[i-k_period+1:i+1]
        lo = min(c["low"] for c in w); hi = max(c["high"] for c in w)
        cl = candles[i]["close"]
        k_vals.append(round(((cl-lo)/(hi-lo)*100) if (hi-lo)>0 else 50, 2))
    d = sum(k_vals[-d_period:])/min(d_period, len(k_vals))
    return k_vals[-1], round(d, 2)

def calc_adx(candles, period=14):
    if len(candles) < period*2: return None
    def smooth(vals, p):
        s=sum(vals[:p]); res=[s]
        for v in vals[p:]: s=s-s/p+v; res.append(s)
        return res
    trs,pdms,ndms=[],[],[]
    for i in range(1, len(candles)):
        h,l,pc=candles[i]["high"],candles[i]["low"],candles[i-1]["close"]
        trs.append(max(h-l,abs(h-pc),abs(l-pc)))
        ph=h-candles[i-1]["high"]; pl=candles[i-1]["low"]-l
        pdms.append(ph if ph>pl and ph>0 else 0)
        ndms.append(pl if pl>ph and pl>0 else 0)
    st,sp,sn=smooth(trs,period),smooth(pdms,period),smooth(ndms,period)
    dx=[]
    for i in range(len(st)):
        if st[i]==0: continue
        pdi=100*sp[i]/st[i]; ndi=100*sn[i]/st[i]
        d=pdi+ndi
        if d: dx.append(100*abs(pdi-ndi)/d)
    return round(sum(dx[-period:])/min(period,len(dx)),2) if dx else None

def calc_supertrend(candles, period=7, mult=3.0):
    if len(candles) < period+2: return None, None
    n   = min(len(candles), period*5)
    src = candles[-n:]
    closes=[c["close"] for c in src]
    st=(src[0]["high"]+src[0]["low"])/2; direction=-1
    for i in range(1, len(src)):
        atr_i=calc_atr(src[max(0,i-period):i+1], min(period,i)) or 0.01
        mid=(src[i]["high"]+src[i]["low"])/2
        ub=mid+mult*atr_i; lb=mid-mult*atr_i
        if closes[i]>st: st=lb; direction=1
        else: st=ub; direction=-1
    return direction, round(st, 2)

def calc_vwap(candles):
    tv=sum(((c["high"]+c["low"]+c["close"])/3)*c["volume"] for c in candles)
    v =sum(c["volume"] for c in candles)
    return round(tv/v,2) if v>0 else None

def calc_orb(candles):
    orb=[c for c in candles if dtime(9,15)<=c["time"].time()<=dtime(9,30)]
    if not orb: return None, None
    return max(c["high"] for c in orb), min(c["low"] for c in orb)

def calc_williams_r(candles, period=14):
    if len(candles)<period: return None
    w=candles[-period:]; hi=max(c["high"] for c in w); lo=min(c["low"] for c in w)
    cl=candles[-1]["close"]
    return round(-100*(hi-cl)/(hi-lo),2) if (hi-lo)>0 else -50

def calc_support_resistance(candles, lookback=20):
    """Find key S/R levels from recent highs and lows."""
    if len(candles) < lookback: lookback = len(candles)
    recent = candles[-lookback:]
    highs  = sorted([c["high"] for c in recent], reverse=True)
    lows   = sorted([c["low"]  for c in recent])
    # Cluster nearby levels
    def cluster(levels, tol=0.005):
        if not levels: return []
        result=[levels[0]]
        for l in levels[1:]:
            if abs(l-result[-1])/result[-1] > tol:
                result.append(l)
        return result[:3]
    return cluster(highs[:8]), cluster(lows[:8])

def ema_ribbon_bullish(closes):
    vals = [last_ema(closes, p) for p in [5,13,21,34]]
    if None in vals: return False
    return vals[0]>vals[1]>vals[2]>vals[3]

def volume_surge(candles, lookback=20):
    if len(candles)<lookback+1: return False
    avg=sum(c["volume"] for c in candles[-lookback-1:-1])/lookback
    return candles[-1]["volume"]>avg*1.5 if avg>0 else False

def higher_highs_higher_lows(candles, lookback=10):
    if len(candles)<lookback: return False
    r=candles[-lookback:]
    highs=[c["high"] for c in r]; lows=[c["low"] for c in r]
    return (highs[-1]>highs[0] and highs[-1]>highs[len(highs)//2] and
            lows[-1]>lows[0]   and lows[-1]>lows[len(lows)//2])

def pivot_points(candles):
    """Classic pivot points from last completed candle."""
    if len(candles) < 2: return {}
    c = candles[-2]  # last completed
    H,L,C = c["high"], c["low"], c["close"]
    P = (H+L+C)/3
    return {
        "P":  round(P, 2),
        "R1": round(2*P-L, 2),
        "R2": round(P+(H-L), 2),
        "R3": round(H+2*(P-L), 2),
        "S1": round(2*P-H, 2),
        "S2": round(P-(H-L), 2),
        "S3": round(L-2*(H-P), 2),
    }

def calc_ichimoku(candles):
    if len(candles) < 52: return None, None, None, None
    highs = [c["high"] for c in candles]
    lows  = [c["low"]  for c in candles]
    t_h, t_l = max(highs[-9:]), min(lows[-9:])
    tenkan = (t_h + t_l) / 2
    k_h, k_l = max(highs[-26:]), min(lows[-26:])
    kijun = (k_h + k_l) / 2
    span_a = (tenkan + kijun) / 2
    s_h, s_l = max(highs[-52:]), min(lows[-52:])
    span_b = (s_h + s_l) / 2
    return tenkan, kijun, span_a, span_b

def calc_awesome_oscillator(candles):
    if len(candles) < 34: return None
    mid = [(c["high"]+c["low"])/2 for c in candles]
    sma5 = sum(mid[-5:])/5
    sma34 = sum(mid[-34:])/34
    return round(sma5 - sma34, 4)

def calc_bb_squeeze(closes, bb_lo, bb_hi):
    if bb_hi == bb_lo or closes[-1] == 0: return False
    width_pct = (bb_hi - bb_lo) / closes[-1]
    return width_pct < 0.05

def calc_fib_levels(candles, period=100):
    if len(candles) < period: period = len(candles)
    highs = [c["high"] for c in candles[-period:]]
    lows  = [c["low"]  for c in candles[-period:]]
    sh, sl = max(highs), min(lows)
    diff = sh - sl
    if diff == 0: return [], []
    fibs = [sl+0.236*diff, sl+0.382*diff, sl+0.5*diff, sl+0.618*diff]
    exts = [sh+0.272*diff, sh+0.618*diff]
    return [round(f, 2) for f in fibs], [round(e, 2) for e in exts]

def calc_time_scenarios(price, daily_atr, m15_atr):
    sc = {}
    if m15_atr:
        sc["Today"] = {"target": round(price + m15_atr*2, 2), "exit": "3:15 PM Today"}
    else:
        sc["Today"] = {"target": round(price + daily_atr*0.2, 2) if daily_atr else 0, "exit": "3:15 PM Today"}
    if daily_atr:
        sc["2 Days"] = {"target": round(price + daily_atr*1.5, 2), "exit": "In 2 Days"}
        sc["1 Week"] = {"target": round(price + daily_atr*3.5, 2), "exit": "Next Week"}
        sc["1 Month"]= {"target": round(price + daily_atr*10, 2),  "exit": "In 1 Month"}
    return sc

def fetch_nifty_trend(nifty_candles):
    if not nifty_candles or len(nifty_candles) < 50: return "Unknown"
    closes = [c["close"] for c in nifty_candles]
    ema50 = last_ema(closes, 50)
    if not ema50: return "Unknown"
    return "🟢 BULLISH (Above EMA 50)" if closes[-1] > ema50 else "🔴 BEARISH (Below EMA 50)"

def calc_obv(candles):
    """On-Balance Volume — tracks cumulative volume flow direction."""
    if len(candles) < 2: return None, None
    obv = 0
    for i in range(1, len(candles)):
        if candles[i]["close"] > candles[i-1]["close"]:
            obv += candles[i]["volume"]
        elif candles[i]["close"] < candles[i-1]["close"]:
            obv -= candles[i]["volume"]
    # Compare OBV trend: current vs 10 periods ago
    obv_prev = 0
    lookback = min(10, len(candles) - 1)
    for i in range(1, len(candles) - lookback):
        if candles[i]["close"] > candles[i-1]["close"]:
            obv_prev += candles[i]["volume"]
        elif candles[i]["close"] < candles[i-1]["close"]:
            obv_prev -= candles[i]["volume"]
    rising = obv > obv_prev
    return obv, rising

def calc_cmf(candles, period=20):
    """Chaikin Money Flow — institutional accumulation/distribution."""
    if len(candles) < period: return None
    recent = candles[-period:]
    mfv_sum = 0
    vol_sum = 0
    for c in recent:
        hl = c["high"] - c["low"]
        if hl == 0:
            mf_mult = 0
        else:
            mf_mult = ((c["close"] - c["low"]) - (c["high"] - c["close"])) / hl
        mfv_sum += mf_mult * c["volume"]
        vol_sum += c["volume"]
    return round(mfv_sum / vol_sum, 4) if vol_sum > 0 else 0

def calc_parabolic_sar(candles, af_start=0.02, af_max=0.20):
    """Parabolic SAR — trend reversal and trailing stop."""
    if len(candles) < 3: return None, None
    # Initialize
    is_long = candles[1]["close"] > candles[0]["close"]
    sar = candles[0]["low"] if is_long else candles[0]["high"]
    ep = candles[0]["high"] if is_long else candles[0]["low"]
    af = af_start

    for i in range(1, len(candles)):
        prev_sar = sar
        sar = prev_sar + af * (ep - prev_sar)

        if is_long:
            sar = min(sar, candles[i-1]["low"])
            if i >= 2: sar = min(sar, candles[i-2]["low"])
            if candles[i]["low"] < sar:
                is_long = False
                sar = ep
                ep = candles[i]["low"]
                af = af_start
            else:
                if candles[i]["high"] > ep:
                    ep = candles[i]["high"]
                    af = min(af + af_start, af_max)
        else:
            sar = max(sar, candles[i-1]["high"])
            if i >= 2: sar = max(sar, candles[i-2]["high"])
            if candles[i]["high"] > sar:
                is_long = True
                sar = ep
                ep = candles[i]["high"]
                af = af_start
            else:
                if candles[i]["low"] < ep:
                    ep = candles[i]["low"]
                    af = min(af + af_start, af_max)

    # 1 = bullish (SAR below price), -1 = bearish (SAR above price)
    direction = 1 if is_long else -1
    return direction, round(sar, 2)

def check_golden_cross(closes):
    """Check if EMA50 recently crossed above EMA200 (Golden Cross)."""
    if len(closes) < 210: return None
    ema50_now  = last_ema(closes, 50)
    ema200_now = last_ema(closes, 200)
    ema50_prev = last_ema(closes[:-5], 50)
    ema200_prev = last_ema(closes[:-5], 200)
    if None in [ema50_now, ema200_now, ema50_prev, ema200_prev]:
        return None
    if ema50_now > ema200_now and ema50_prev <= ema200_prev:
        return "GOLDEN"  # Just crossed — extremely bullish
    if ema50_now < ema200_now and ema50_prev >= ema200_prev:
        return "DEATH"   # Just crossed — extremely bearish
    if ema50_now > ema200_now:
        return "ABOVE"   # Sustained bullish
    return "BELOW"       # Sustained bearish

def macd_histogram_accelerating(candles_closes):
    """Check if MACD histogram is getting bigger (momentum building)."""
    if len(candles_closes) < 40: return None
    _, _, hist_now = calc_macd(candles_closes)
    _, _, hist_prev = calc_macd(candles_closes[:-1])
    if hist_now is None or hist_prev is None: return None
    return hist_now > hist_prev and hist_now > 0

def calc_relative_strength_vs_nifty(stock_candles, nifty_candles, period=22):
    """Compare stock's N-period return vs Nifty's N-period return."""
    if not stock_candles or len(stock_candles) < period: return None
    if not nifty_candles or len(nifty_candles) < period: return None
    stock_ret = (stock_candles[-1]["close"] - stock_candles[-period]["close"]) / stock_candles[-period]["close"] * 100
    nifty_ret = (nifty_candles[-1]["close"] - nifty_candles[-period]["close"]) / nifty_candles[-period]["close"] * 100
    rs = round(stock_ret - nifty_ret, 2)
    if rs > 5: return rs, "Leading"
    if rs < -5: return rs, "Lagging"
    return rs, "Neutral"

# ═══════════════════════════════════════════════════════════════════════
# HIGH-CONVICTION INDICATORS
# ═══════════════════════════════════════════════════════════════════════

def calc_mfi(candles, period=14):
    """Money Flow Index — volume-weighted RSI. Combines price + volume."""
    if len(candles) < period + 1:
        return None
    typical_prices = [(c["high"] + c["low"] + c["close"]) / 3 for c in candles]
    pos_flow = 0
    neg_flow = 0
    for i in range(-period, 0):
        tp = typical_prices[i]
        tp_prev = typical_prices[i - 1]
        money_flow = tp * candles[i]["volume"]
        if tp > tp_prev:
            pos_flow += money_flow
        elif tp < tp_prev:
            neg_flow += money_flow
    if neg_flow == 0:
        return 100.0
    ratio = pos_flow / neg_flow
    return round(100 - (100 / (1 + ratio)), 2)

def calc_cci(candles, period=20):
    """Commodity Channel Index — deviation of price from its mean."""
    if len(candles) < period:
        return None
    tp = [(c["high"] + c["low"] + c["close"]) / 3 for c in candles[-period:]]
    mean_tp = sum(tp) / period
    mean_dev = sum(abs(t - mean_tp) for t in tp) / period
    if mean_dev == 0:
        return 0
    return round((tp[-1] - mean_tp) / (0.015 * mean_dev), 2)

def calc_keltner(candles, ema_period=20, atr_mult=1.5):
    """Keltner Channel — ATR-based bands (more reliable than BB for trends)."""
    if len(candles) < ema_period + 14:
        return None, None, None
    closes = [c["close"] for c in candles]
    mid = last_ema(closes, ema_period)
    atr = calc_atr(candles, 14)
    if mid is None or atr is None:
        return None, None, None
    upper = round(mid + atr_mult * atr, 2)
    lower = round(mid - atr_mult * atr, 2)
    return lower, round(mid, 2), upper

def calc_candle_patterns(candles):
    """Detect high-probability bullish candlestick patterns in last 3 candles."""
    if len(candles) < 3:
        return None
    def body(c):
        return abs(c["close"] - c["open"])
    def upper_wick(c):
        return c["high"] - max(c["open"], c["close"])
    def lower_wick(c):
        return min(c["open"], c["close"]) - c["low"]
    def is_bullish(c):
        return c["close"] > c["open"]
    def is_bearish(c):
        return c["close"] < c["open"]

    for i in range(-2, 0):
        prev = candles[i - 1]
        curr = candles[i]
        # Bullish Engulfing
        if (is_bearish(prev) and is_bullish(curr) and
            curr["open"] <= prev["close"] and curr["close"] >= prev["open"] and
            body(curr) > body(prev)):
            return "Bullish Engulfing"
        # Hammer
        br = curr["high"] - curr["low"]
        if br > 0:
            lw = lower_wick(curr)
            b = body(curr)
            if b > 0 and lw >= 2 * b and upper_wick(curr) <= b * 0.5:
                return "Hammer"

    # Morning Star
    c1, c2, c3 = candles[-3], candles[-2], candles[-1]
    if (is_bearish(c1) and body(c1) > 0 and
        body(c2) < body(c1) * 0.4 and
        is_bullish(c3) and body(c3) > body(c1) * 0.5 and
        c3["close"] > (c1["open"] + c1["close"]) / 2):
        return "Morning Star"
    return None

def calc_rvol(candles, lookback=20):
    """Relative Volume — precise ratio of current volume to average."""
    if len(candles) < lookback + 1:
        return None
    avg_vol = sum(c["volume"] for c in candles[-(lookback + 1):-1]) / lookback
    if avg_vol == 0:
        return None
    return round(candles[-1]["volume"] / avg_vol, 2)

def calc_ad_line(candles):
    """Accumulation/Distribution Line — volume-weighted close position."""
    if len(candles) < 2:
        return None, None
    ad = 0
    for c in candles:
        hl = c["high"] - c["low"]
        if hl == 0:
            clv = 0
        else:
            clv = ((c["close"] - c["low"]) - (c["high"] - c["close"])) / hl
        ad += clv * c["volume"]
    # Compare current A/D vs 10 bars ago
    ad_prev = 0
    lookback = min(10, len(candles) - 1)
    for c in candles[:len(candles) - lookback]:
        hl = c["high"] - c["low"]
        if hl == 0:
            clv = 0
        else:
            clv = ((c["close"] - c["low"]) - (c["high"] - c["close"])) / hl
        ad_prev += clv * c["volume"]
    rising = ad > ad_prev
    return ad, rising

def calc_rsi_divergence(candles, period=14, lookback=5):
    """
    Detect bullish RSI divergence: price makes lower low but RSI makes higher low.
    Returns: "bullish" | "bearish" | None
    """
    closes = [c["close"] for c in candles]
    if len(closes) < period + lookback * 2 + 1:
        return None

    recent_slice = closes[-(lookback):]
    prev_slice   = closes[-(lookback * 2):-lookback]

    recent_low_price = min(recent_slice)
    prev_low_price   = min(prev_slice)

    rsi_now  = calc_rsi(closes, period)
    rsi_prev = calc_rsi(closes[:-(lookback)], period)

    if rsi_now is None or rsi_prev is None:
        return None

    # Bullish divergence: price lower low, RSI higher low
    if recent_low_price < prev_low_price and rsi_now > rsi_prev:
        return "bullish"
    # Bearish divergence: price higher high, RSI lower high
    recent_high_price = max(recent_slice)
    prev_high_price   = max(prev_slice)
    if recent_high_price > prev_high_price and rsi_now < rsi_prev:
        return "bearish"
    return None

def calc_range_high_proximity(candles):
    """
    How close is current price to the highest high in the available window.
    Returns (pct_from_high, status).
    """
    if len(candles) < 10:
        return None, None
    highs = [c["high"] for c in candles]
    window_high = max(highs)
    current  = candles[-1]["close"]
    if window_high == 0:
        return None, None
    pct = round((current - window_high) / window_high * 100, 2)
    if pct >= -5:
        return pct, "Near High"
    elif pct >= -15:
        return pct, "Healthy"
    elif pct >= -30:
        return pct, "Pullback"
    else:
        return pct, "Deep Correction"


# PER-TIMEFRAME ANALYSIS ENGINE
# ═══════════════════════════════════════════════════════════════════════

def analyze_timeframe(candles, tf_key, price):
    """
    Full technical analysis for one timeframe.
    Returns a rich dict with score, signals, levels, recommendation.
    """
    if len(candles) < TIMEFRAMES[tf_key]["candles_min"]:
        return {"error": "Insufficient data", "score": 0, "max_score": 0}

    closes  = [c["close"] for c in candles]
    atr     = calc_atr(candles, 14)
    rsi     = calc_rsi(closes, 14)
    macd_l, macd_s, macd_h = calc_macd(closes)
    bb_lo, bb_mid, bb_hi   = calc_bb(closes, 20)
    stk_k, stk_d           = calc_stoch(candles, 14, 3)
    adx_val                = calc_adx(candles, 14)
    wr                     = calc_williams_r(candles, 14)
    st_dir, st_val         = calc_supertrend(candles, 7, 3.0)
    ema9                   = last_ema(closes, 9)
    ema21                  = last_ema(closes, 21)
    ema50                  = last_ema(closes, 50)
    ema200                 = last_ema(closes, 200)
    ribbon                 = ema_ribbon_bullish(closes)
    hh_hl                  = higher_highs_higher_lows(candles, 10)
    vol_surge              = volume_surge(candles, 20)
    pivots                 = pivot_points(candles)
    res_levels, sup_levels = calc_support_resistance(candles, 20)
    tenkan, kijun, spa, spb = calc_ichimoku(candles)
    ao                     = calc_awesome_oscillator(candles)
    bb_sqz                 = calc_bb_squeeze(closes, bb_lo, bb_hi) if bb_lo else False
    fib_lvls, fib_exts     = calc_fib_levels(candles, 100)
    obv_val, obv_rising    = calc_obv(candles)
    cmf_val                = calc_cmf(candles, 20)
    psar_dir, psar_val     = calc_parabolic_sar(candles)
    # Golden Cross is meaningful only on higher-timeframe datasets
    if tf_key in {"month", "m2", "m3", "m6"}:
        gc_status = check_golden_cross(closes)
    else:
        gc_status = None
    macd_accel             = macd_histogram_accelerating(closes)
    rsi_div                = calc_rsi_divergence(candles, 14, 5)
    # Keep range-high proximity on swing/positional timeframes only
    if tf_key in {"month", "m2", "m3", "m6"}:
        hi52_pct, hi52_status = calc_range_high_proximity(candles)
    else:
        hi52_pct, hi52_status = None, None
    mfi_val                = calc_mfi(candles, 14)
    cci_val                = calc_cci(candles, 20)
    kc_lo, kc_mid, kc_hi   = calc_keltner(candles, 20, 1.5)
    candle_pat             = calc_candle_patterns(candles)
    rvol_val               = calc_rvol(candles, 20)
    ad_val, ad_rising      = calc_ad_line(candles)

    # ORB + VWAP only for intraday
    orb_hi = orb_lo = vwap = None
    if tf_key == "intraday":
        orb_hi, orb_lo = calc_orb(candles)
        vwap           = calc_vwap(candles)

    checks = {}

    # 1. Trend Direction (EMA 9 > 21)
    if ema9 and ema21:
        ok = ema9 > ema21
        checks["Trend (EMA9>21)"] = (ok, f"{'✅' if ok else '❌'} EMA9({ema9}) {'>' if ok else '<'} EMA21({ema21})")
    else:
        checks["Trend (EMA9>21)"] = (None, "⏳ Data insufficient")

    # 2. EMA Ribbon
    checks["EMA Ribbon"] = (ribbon,
        "✅ 5>13>21>34 bullish stack" if ribbon else "❌ EMAs not aligned")

    # 2b. Ichimoku Cloud
    if spa and spb:
        ok = price > spa and price > spb
        checks["Ichimoku Cloud"] = (ok, f"{'✅' if ok else '❌'} Price {'above' if ok else 'in/below'} Cloud")
    else:
        checks["Ichimoku Cloud"] = (None, "⏳ Cloud unavailable")

    # 3. Supertrend
    if st_dir is not None:
        checks["Supertrend (7,3)"] = (st_dir==1,
            f"{'✅ Bullish' if st_dir==1 else '❌ Bearish'} @ {fmt_price(st_val)}")
    else:
        checks["Supertrend (7,3)"] = (None, "⏳ Insufficient data")

    # 4. MACD
    if macd_l is not None:
        ok = macd_l > macd_s and macd_h > 0
        checks["MACD"] = (ok,
            f"{'✅' if ok else '❌'} Line({macd_l}) vs Signal({macd_s}), Hist({macd_h})")
    else:
        checks["MACD"] = (None, "⏳ MACD unavailable")

    # 5. RSI Zone
    if rsi is not None:
        ok = 40 <= rsi <= 75
        checks["RSI Zone"] = (ok, f"{'✅' if ok else '❌'} RSI {rsi} ({'bullish zone' if ok else ('OB' if rsi>75 else 'OS' if rsi<30 else 'weak')})")
    else:
        checks["RSI Zone"] = (None, "⏳ RSI unavailable")

    # 6. Stochastic
    if stk_k is not None and stk_d is not None:
        ok = stk_k > stk_d
        checks["Stochastic"] = (ok, f"{'✅' if ok else '❌'} %K({stk_k}) vs %D({stk_d})")
    else:
        checks["Stochastic"] = (None, "⏳ Stoch unavailable")

    # 6b. Awesome Oscillator
    if ao is not None:
        ok = ao > 0
        checks["Awesome Osc"] = (ok, f"{'✅' if ok else '❌'} AO {ao:0.2f}")
    else:
        checks["Awesome Osc"] = (None, "⏳ AO unavailable")

    # 7. ADX
    if adx_val is not None:
        ok = adx_val > 20
        checks["ADX Strength"] = (ok, f"{'✅' if ok else '❌'} ADX {adx_val} ({'trending' if ok else 'ranging'})")
    else:
        checks["ADX Strength"] = (None, "⏳ ADX unavailable")

    # 8. Williams %R
    if wr is not None:
        ok = -80 <= wr <= -20
        checks["Williams %R"] = (ok, f"{'✅' if ok else '❌'} %R {wr}")
    else:
        checks["Williams %R"] = (None, "⏳ %R unavailable")

    # 9. Bollinger Bands (Squeeze is metadata only — NOT scored)
    if bb_lo and bb_mid and bb_hi:
        ok = price > bb_mid and price < bb_hi * 0.99
        checks["Bollinger Bands"] = (ok,
            f"{'✅' if ok else '❌'} {bb_lo}/{bb_mid}/{bb_hi}")
    else:
        checks["Bollinger Bands"] = (None, "⏳ BB unavailable")

    # 10. Price Structure
    checks["HH+HL Structure"] = (hh_hl,
        "✅ Higher Highs + Higher Lows" if hh_hl else "❌ No uptrend structure")

    # 11. Volume
    checks["Volume Surge"] = (vol_surge,
        "✅ Volume above average" if vol_surge else "❌ Weak/no volume surge")

    # 12. ORB / EMA50 (timeframe-specific)
    if tf_key == "intraday":
        if orb_hi:
            ok = price > orb_hi
            checks["ORB Breakout"] = (ok, f"{'✅' if ok else '❌'} {'Above' if ok else 'Below'} ORB {fmt_price(orb_hi)}")
        else:
            checks["ORB Breakout"] = (None, "⏳ ORB not formed")
    else:
        if ema50:
            ok = price > ema50
            checks["Price > EMA50"] = (ok, f"{'✅' if ok else '❌'} Price vs EMA50({ema50})")
        else:
            checks["Price > EMA50"] = (None, "⏳ EMA50 unavailable")

    # 13. VWAP / EMA200 (timeframe-specific)
    if tf_key == "intraday":
        if vwap:
            ok = price > vwap
            checks["VWAP"] = (ok, f"{'✅' if ok else '❌'} {'Above' if ok else 'Below'} VWAP {fmt_price(vwap)}")
        else:
            checks["VWAP"] = (None, "⏳ VWAP unavailable")
    else:
        if ema200:
            ok = price > ema200
            checks["Price > EMA200"] = (ok, f"{'✅' if ok else '❌'} Price vs EMA200({ema200})")
        else:
            checks["Price > EMA200"] = (None, "⏳ EMA200 unavailable")

    # 14. OBV (On-Balance Volume)
    if obv_rising is not None:
        checks["OBV Trend"] = (obv_rising,
            f"{'✅' if obv_rising else '❌'} Volume {'flowing in' if obv_rising else 'flowing out'}")
    else:
        checks["OBV Trend"] = (None, "⏳ OBV unavailable")

    # 15. Chaikin Money Flow
    if cmf_val is not None:
        ok = cmf_val > 0
        checks["CMF (Money Flow)"] = (ok,
            f"{'✅' if ok else '❌'} CMF {cmf_val} ({'Net buying' if ok else 'Net selling'})")
    else:
        checks["CMF (Money Flow)"] = (None, "⏳ CMF unavailable")

    # 16. Parabolic SAR
    if psar_dir is not None:
        ok = psar_dir == 1
        checks["Parabolic SAR"] = (ok,
            f"{'✅' if ok else '❌'} SAR {'below price (bullish)' if ok else 'above price (bearish)'} @ {fmt_price(psar_val)}")
    else:
        checks["Parabolic SAR"] = (None, "⏳ PSAR unavailable")

    # 17. MACD Histogram Acceleration
    if macd_accel is not None:
        checks["MACD Momentum"] = (macd_accel,
            f"{'✅' if macd_accel else '❌'} Histogram {'accelerating ↑' if macd_accel else 'decelerating ↓'}")
    else:
        checks["MACD Momentum"] = (None, "⏳ MACD accel unavailable")

    # 18. Golden/Death Cross
    if gc_status is not None:
        ok = gc_status in ["GOLDEN", "ABOVE"]
        label = {"GOLDEN": "🏅 Just crossed! (Golden Cross)", "ABOVE": "✅ EMA50 > EMA200",
                 "DEATH": "💀 Death Cross!", "BELOW": "❌ EMA50 < EMA200"}
        checks["Golden Cross"] = (ok, label.get(gc_status, "❌"))
    else:
        checks["Golden Cross"] = (None, "⏳ Need 200+ periods")

    # 19. Fibonacci Position
    if fib_lvls and len(fib_lvls) >= 3:
        ok = price > fib_lvls[2]  # Above 50% retracement = bullish
        checks["Fib Position"] = (ok,
            f"{'✅' if ok else '❌'} Price {'above' if ok else 'below'} Fib 50% ({fib_lvls[2]})")
    else:
        checks["Fib Position"] = (None, "⏳ Fib unavailable")

    # 20. RSI Divergence
    if rsi_div is not None:
        ok = rsi_div == "bullish"
        checks["RSI Divergence"] = (ok,
            f"{'✅ Bullish divergence detected!' if ok else '❌ Bearish divergence — caution'}")
    else:
        checks["RSI Divergence"] = (None, "⏳ No divergence")

    # 21. Range High Proximity (within available timeframe window)
    if hi52_pct is not None:
        ok = hi52_status in ["Near High", "Healthy"]
        checks["Range High"] = (ok,
            f"{'✅' if ok else '❌'} {hi52_pct:+.1f}% from high ({hi52_status})")
    else:
        checks["Range High"] = (None, "⏳ Insufficient history")

    # 22. Money Flow Index (MFI)
    if mfi_val is not None:
        ok = 20 <= mfi_val <= 80
        checks["MFI"] = (ok,
            f"{'✅' if ok else '❌'} MFI {mfi_val} ({'healthy flow' if ok else ('OB' if mfi_val>80 else 'OS')})")
    else:
        checks["MFI"] = (None, "⏳ MFI unavailable")

    # 23. CCI
    if cci_val is not None:
        ok = 0 < cci_val < 200
        checks["CCI"] = (ok,
            f"{'✅' if ok else '❌'} CCI {cci_val} ({'bullish momentum' if ok else ('extreme' if cci_val>=200 else 'bearish')})")
    else:
        checks["CCI"] = (None, "⏳ CCI unavailable")

    # 24. Keltner Channel
    if kc_lo and kc_mid and kc_hi:
        ok = price > kc_mid and price < kc_hi
        checks["Keltner Channel"] = (ok,
            f"{'✅' if ok else '❌'} Price {'in trend zone' if ok else ('above upper KC' if price >= kc_hi else 'below mid KC')}")
    else:
        checks["Keltner Channel"] = (None, "⏳ KC unavailable")

    # 25. Candlestick Pattern
    if candle_pat:
        checks["Candle Pattern"] = (True, f"✅ {candle_pat} detected!")
    else:
        checks["Candle Pattern"] = (None, "⏳ No pattern")

    # 26. Relative Volume (RVOL)
    if rvol_val is not None:
        ok = rvol_val >= 1.2
        checks["RVOL"] = (ok,
            f"{'✅' if ok else '❌'} RVOL {rvol_val}x ({'strong participation' if ok else 'low interest'})")
    else:
        checks["RVOL"] = (None, "⏳ RVOL unavailable")

    # 27. Accumulation/Distribution
    if ad_rising is not None:
        checks["A/D Line"] = (ad_rising,
            f"{'✅' if ad_rising else '❌'} {'Accumulation (smart money in)' if ad_rising else 'Distribution (selling)'}")
    else:
        checks["A/D Line"] = (None, "⏳ A/D unavailable")

    # SCORE
    scored = {k: v for k, v in checks.items() if v[0] is not None}
    score  = sum(1 for v in scored.values() if v[0])
    max_sc = len(scored)
    raw_pct = round(score/max_sc*100) if max_sc > 0 else 0

    # Category-weighted scoring to reduce correlated-indicator overcounting
    # Each category contributes by weight, regardless of raw indicator count inside it.
    category_weights = {
        "trend": 0.34,
        "momentum": 0.26,
        "volume_flow": 0.24,
        "regime": 0.16,
    }
    category_stats = {
        "trend": {"passed": 0, "available": 0},
        "momentum": {"passed": 0, "available": 0},
        "volume_flow": {"passed": 0, "available": 0},
        "regime": {"passed": 0, "available": 0},
    }

    def _check_category(name):
        if name in {
            "Trend (EMA9>21)", "EMA Ribbon", "Ichimoku Cloud", "Supertrend (7,3)",
            "HH+HL Structure", "Price > EMA50", "Price > EMA200", "VWAP",
            "Golden Cross", "Parabolic SAR", "Fib Position", "Range High"
        }:
            return "trend"
        if name in {
            "MACD", "RSI Zone", "Stochastic", "Awesome Osc", "Williams %R",
            "MACD Momentum", "RSI Divergence", "CCI", "Candle Pattern"
        }:
            return "momentum"
        if name in {
            "Volume Surge", "OBV Trend", "CMF (Money Flow)", "MFI", "RVOL", "A/D Line"
        }:
            return "volume_flow"
        if name in {"ADX Strength", "Bollinger Bands", "Keltner Channel", "ORB Breakout"}:
            return "regime"
        return "trend"

    for check_name, (passed, _) in checks.items():
        if passed is None:
            continue
        cat = _check_category(check_name)
        category_stats[cat]["available"] += 1
        if passed:
            category_stats[cat]["passed"] += 1

    weighted_num = 0.0
    weighted_den = 0.0
    category_breakdown = {}
    for cat, w in category_weights.items():
        av = category_stats[cat]["available"]
        ps = category_stats[cat]["passed"]
        if av == 0:
            category_breakdown[cat] = {"passed": ps, "available": av, "pct": None}
            continue
        cat_pct = round(ps / av * 100)
        category_breakdown[cat] = {"passed": ps, "available": av, "pct": cat_pct}
        weighted_num += w * (ps / av)
        weighted_den += w
    pct = round(weighted_num / weighted_den * 100) if weighted_den > 0 else 0

    # Grade
    if   pct >= 85: grade = "A+"; signal = "STRONG BUY"
    elif pct >= 70: grade = "A";  signal = "BUY"
    elif pct >= 57: grade = "B";  signal = "WEAK BUY"
    elif pct >= 43: grade = "C";  signal = "NEUTRAL"
    else:           grade = "D";  signal = "AVOID"

    # Bias
    bias = "BULLISH" if pct >= 57 else ("NEUTRAL" if pct >= 43 else "BEARISH")

    # Entry / Stop / Targets using ATR
    entry = sl = t1 = t2 = t3 = None
    if atr and price:
        entry = round(price, 2)
        sl    = round(price - 1.5*atr, 2)
        t1    = round(price + 1.5*atr, 2)   # 1:1 R:R
        t2    = round(price + 3.0*atr, 2)   # 1:2 R:R
        t3    = round(price + 5.0*atr, 2)   # 1:3.3 R:R

        # Use ORB low as stop for intraday if tighter
        if orb_lo and orb_lo < price and orb_lo > sl:
            sl = round(orb_lo * 0.998, 2)

        # Use pivot S1 as stop if available and tighter
        if pivots.get("S1") and pivots["S1"] < price and pivots["S1"] > sl:
            sl = round(pivots["S1"] * 0.998, 2)

    # Buy zone and sell zone
    buy_zone  = f"{fmt_price(sl)} – {fmt_price(round(price*1.005,2))}" if sl else "N/A"
    sell_zone = f"{fmt_price(t1)} – {fmt_price(t2)}"                   if t1 else "N/A"

    # Timing recommendation
    timing = _get_timing_recommendation(tf_key, signal, pct)

    return {
        "tf_key":      tf_key,
        "tf_label":    TIMEFRAMES[tf_key]["label"],
        "score":       score,
        "max_score":   max_sc,
        "pct":         pct,
        "raw_pct":     raw_pct,
        "grade":       grade,
        "signal":      signal,
        "bias":        bias,
        "category_breakdown": category_breakdown,
        "checks":      {k: {"pass": v[0], "detail": v[1]} for k, v in checks.items()},
        "entry":       entry,
        "sl":          sl,
        "t1":          t1,
        "t2":          t2,
        "t3":          t3,
        "buy_zone":    buy_zone,
        "sell_zone":   sell_zone,
        "timing":      timing,
        "atr":         round(atr, 2) if atr else None,
        "vwap":        vwap,
        "fib_lvls":    fib_lvls,
        "fib_exts":    fib_exts,
        "pivots":      pivots,
        "bb_hi":       bb_hi,
        "bb_lo":       bb_lo,
        "bb_sqz":      bb_sqz,
        "ema20":       last_ema(closes, 20),
        "rsi":         rsi,
        "adx":         adx_val,
        "macd":        macd_l,
        "macd_signal": macd_s,
        "macd_hist":   macd_h,
        "orb_high":    orb_hi,
        "orb_low":     orb_lo,
        "ema9":        ema9,
        "ema21":       ema21,
        "ema50":       ema50,
        "resistance":  res_levels[:2] if res_levels else [],
        "support":     sup_levels[:2] if sup_levels else [],
        "st_dir":      st_dir,
        "st_val":      st_val
    }

def _get_timing_recommendation(tf_key, signal, score_pct):
    """Generate specific buy/sell timing guidance per timeframe."""
    now = ist_now()
    t   = now.time()
    day = now.weekday()  # 0=Mon, 4=Fri

    if score_pct < 43:
        return {"action": "AVOID", "buy_when": "Do not enter — too many negatives", "sell_when": "N/A", "hold_duration": "N/A"}

    timing_map = {
        "intraday": {
            "buy_when":       "Enter 9:30–10:15 AM on ORB breakout candle close, OR 2:00–2:30 PM on volume surge",
            "sell_when":      "Exit by 3:00–3:10 PM same day, or on target hit",
            "hold_duration":  "Same day (close all positions by 3:10 PM)",
            "avoid":          "Avoid entry after 2:45 PM or during 12:00–1:30 PM dead zone",
        },
        "d2_3": {
            "buy_when":       "Enter on 15-min chart breakout above recent high with volume, best in morning session",
            "sell_when":      "Exit on next 1–2 day rally or if daily close below entry",
            "hold_duration":  "2–3 trading days",
            "avoid":          "Avoid entering on Friday (weekend gap risk)",
        },
        "week": {
            "buy_when":       "Enter on daily candle close above resistance with volume",
            "sell_when":      "Exit at weekly R1/R2 pivot or on RSI > 70",
            "hold_duration":  "5–7 trading days (1 week)",
            "avoid":          "Avoid chasing if already up >5% in a day",
        },
        "w2": {
            "buy_when":       "Enter on pullback to EMA21 or 50 with RSI 50–60 reversal",
            "sell_when":      "Exit at ATR 2x–3x target or on MACD cross-down",
            "hold_duration":  "1–2 weeks",
            "avoid":          "Avoid entering if result season starts within 1 week",
        },
        "month": {
            "buy_when":       "Enter on first 3 days of the month on daily breakout above 20-day high",
            "sell_when":      "Exit on monthly R1 pivot hit or RSI > 70 on daily",
            "hold_duration":  "3–4 weeks",
            "avoid":          "Avoid in last week of month (window dressing / mutual fund selling)",
        },
        "m2": {
            "buy_when":       "Enter on weekly close above key resistance with EMA ribbon aligned",
            "sell_when":      "Scale out 50% at T1, remaining at T2, trail stop from T1",
            "hold_duration":  "6–8 weeks",
            "avoid":          "Avoid if broad market (Nifty) in downtrend",
        },
        "m3": {
            "buy_when":       "Enter on quarterly trend breakout, EMA50 as base support",
            "sell_when":      "Exit at T2/T3 targets or on quarterly earnings disappointment",
            "hold_duration":  "2–3 months",
            "avoid":          "Avoid in May–June (pre-budget uncertainty) or Sep–Oct (FII outflows)",
        },
        "m6": {
            "buy_when":       "Enter on major breakout from base/accumulation zone on weekly chart",
            "sell_when":      "Trail stop below weekly EMA21; exit in stages at T1, T2, T3",
            "hold_duration":  "4–6 months",
            "avoid":          "Avoid buying if weekly RSI > 70 already (wait for pullback to EMA)",
        },
    }

    rec = timing_map.get(tf_key, {})
    rec["action"] = signal

    # Real-time overlay
    if tf_key == "intraday":
        if dtime(9, 15) <= t <= dtime(9, 30):
            rec["now"] = "⏳ ORB forming — watch, don't trade yet"
        elif dtime(9, 30) <= t <= dtime(11, 30):
            rec["now"] = "🟢 PRIME entry window — confirm breakout then enter"
        elif dtime(11, 30) <= t <= dtime(13, 30):
            rec["now"] = "🟡 Dead zone — avoid new entries, manage existing"
        elif dtime(14, 0) <= t <= dtime(14, 45):
            rec["now"] = "🟢 Second entry window active"
        elif dtime(14, 45) <= t <= dtime(15, 10):
            rec["now"] = "🟡 Exit zone — close positions, don't enter"
        else:
            rec["now"] = "🔴 Market closed or pre-open"
    else:
        if day == 0:
            rec["now"] = "📅 Monday — good day to initiate fresh positions"
        elif day == 4:
            rec["now"] = "📅 Friday — consider booking partial profits, weekend risk"
        else:
            rec["now"] = "📅 Mid-week — valid entry day"

    return rec

# ═══════════════════════════════════════════════════════════════════════
# MASTER MULTI-TIMEFRAME AGGREGATOR
# ═══════════════════════════════════════════════════════════════════════

def aggregate_mtf(tf_results, price):
    """
    Combines all timeframe signals into one master recommendation.
    Higher timeframes get more weight.
    """
    weights = {
        "intraday": 1, "d2_3": 1, "week": 2, "w2": 2,
        "month": 3, "m2": 3, "m3": 4, "m6": 4,
    }
    total_weight = 0; bullish_weight = 0
    signals = {}
    for tf_key, result in tf_results.items():
        if result.get("error") or result.get("max_score", 0) == 0:
            continue
        w   = weights.get(tf_key, 1)
        pct = result.get("pct", 0)
        total_weight   += w
        bullish_weight += w * (pct/100)
        signals[tf_key] = result.get("signal", "NEUTRAL")

    overall_pct = round(bullish_weight/total_weight*100) if total_weight else 0

    # Best timeframe to act on
    best_tf = max(
        [(k, v) for k, v in tf_results.items() if v.get("pct", 0) > 0 and not v.get("error")],
        key=lambda x: x[1].get("pct", 0), default=(None, {})
    )

    # Confluences
    buy_confluences = []
    if all(tf_results.get(k, {}).get("signal") in ["BUY","STRONG BUY"]
           for k in ["week","month","m3"] if k in tf_results):
        buy_confluences.append("Weekly + Monthly + Quarterly all bullish")
    if tf_results.get("intraday", {}).get("orb_high") and \
       price and price > tf_results["intraday"]["orb_high"]:
        buy_confluences.append("Price above intraday ORB")
    if tf_results.get("intraday", {}).get("vwap") and \
       price and price > tf_results["intraday"]["vwap"]:
        buy_confluences.append("Price above VWAP")

    # MTF RSI alignment bonus: if RSI bullish on 6+ timeframes, boost conviction
    rsi_bullish_count = sum(
        1 for r in tf_results.values()
        if r.get("rsi") and 40 <= r["rsi"] <= 75 and not r.get("error")
    )
    if rsi_bullish_count >= 6:
        buy_confluences.append(f"RSI bullish on {rsi_bullish_count}/8 timeframes")
        overall_pct = min(100, overall_pct + 5)  # 5% bonus

    # Compute master signal from final overall_pct (post-bonus)
    if   overall_pct >= 90: master = "💎 DIAMOND ALERT — High Accuracy Setup"
    elif overall_pct >= 80: master = "🔥 STRONG BUY  — All timeframes aligned"
    elif overall_pct >= 65: master = "✅ BUY         — Most timeframes bullish"
    elif overall_pct >= 50: master = "🟡 WEAK BUY   — Mixed signals, caution"
    elif overall_pct >= 35: master = "🟠 NEUTRAL    — Wait for clarity"
    else:                   master = "🚫 AVOID      — Bearish across timeframes"

    return {
        "overall_pct":   overall_pct,
        "master_signal": master,
        "signals":       signals,
        "best_timeframe": best_tf[0],
        "confluences":   buy_confluences,
    }

# ═══════════════════════════════════════════════════════════════════════
# ENRICH STOCK — FULL MTF ANALYSIS
# ═══════════════════════════════════════════════════════════════════════

async def enrich_stock(stock, nifty_candles=None):
    symbol   = stock.get("symbol", "")
    exchange = stock.get("exchange", "NS")   # "NS" or "BO" from parse_screener_url
    price    = float(stock.get("price", 0))
    if not symbol:
        return stock

    # Fetch all 8 timeframes — passes correct exchange suffix, auto-fallback built in
    all_candles, resolved_sym = await fetch_all_timeframes(symbol, exchange)

    # Use Yahoo Finance latest price if screener.in price is 0
    if price == 0 or price == 0.0:
        for candles in all_candles.values():
            if candles:
                price = candles[-1]["close"]
                stock["price"] = price
                break

    # Analyze each timeframe
    tf_results = {}
    for tf_key, candles in all_candles.items():
        tf_results[tf_key] = analyze_timeframe(candles, tf_key, price)

    # Master aggregation
    mtf = aggregate_mtf(tf_results, price)
    
    # Bug Fix: Use d2_3 (15-min candles) ATR for "Today" scenario, not 1-min
    d_atr = tf_results.get("month", {}).get("atr")
    m15_atr = tf_results.get("d2_3", {}).get("atr")  # 15m ATR, not 1m
    stock["scenarios"] = calc_time_scenarios(price, d_atr, m15_atr)

    # Relative Strength vs Nifty (uses monthly candles for ~1 month comparison)
    monthly_candles = all_candles.get("month", [])
    rs_result = calc_relative_strength_vs_nifty(monthly_candles, nifty_candles, 22)
    if rs_result:
        stock["rs_vs_nifty"] = {"value": rs_result[0], "status": rs_result[1]}
    else:
        stock["rs_vs_nifty"] = {"value": 0, "status": "Unknown"}

    stock["tf_analysis"]    = tf_results
    stock["mtf_summary"]    = mtf
    stock["entry_window"]   = is_entry_window()
    stock["yahoo_symbol"]   = resolved_sym
    stock["exchange_label"] = "BSE" if exchange == "BO" else "NSE"
    stock["bb_sqz_meta"]    = tf_results.get("month", {}).get("bb_sqz", False)
    return stock

# ═══════════════════════════════════════════════════════════════════════
# TELEGRAM MESSAGE FORMATTER
# Each stock = 2 messages (overview + details) to stay under Telegram 4096 limit
# ═══════════════════════════════════════════════════════════════════════

def format_master_alert(stock, screener_name, nifty_trend="Unknown"):
    name      = stock.get("name", "")
    symbol    = stock.get("symbol", "")
    price     = stock.get("price", 0)
    mtf       = stock.get("mtf_summary", {})
    tfa       = stock.get("tf_analysis", {})
    scenarios = stock.get("scenarios", {})
    rs_data   = stock.get("rs_vs_nifty", {})
    bb_sqz    = stock.get("bb_sqz_meta", False)
    
    overall = mtf.get("overall_pct", 0)
    master_sig = mtf.get("master_signal", "")
    is_diamond = "DIAMOND" in master_sig
    below_min = bool(stock.get("below_min_score", False))
    min_score = int(stock.get("min_score", 0) or 0)
    
    stars = "⭐️" * (overall // 20) if overall >= 20 else ""
    if not stars: stars = "❌"
    
    best_tf = mtf.get("best_timeframe", "d2_3")
    br = tfa.get(best_tf, {})
    
    # Volume Check
    vol_surge = br.get("checks", {}).get("Volume Surge", {}).get("pass", False)
    vol_text = "🔥 High" if vol_surge else "Normal"
    
    rsi = br.get("rsi", "N/A")
    atr = br.get("atr") or 1
    
    # ATR-based targets with proper fallbacks
    r1 = br.get("t1") or round(price + atr * 1.5, 2)
    r2 = br.get("t2") or round(price + atr * 3.0, 2)
    r3 = br.get("t3") or round(price + atr * 5.0, 2)
    
    s1 = br.get("entry") or round(price, 2)
    s2 = br.get("sl") or round(price - atr * 1.5, 2)
    s3 = round(s2 - atr, 2)
    
    # Relative Strength
    rs_icon = "🟢" if rs_data.get("status") == "Leading" else ("🔴" if rs_data.get("status") == "Lagging" else "🟡")
    rs_text = f"{rs_icon} {rs_data.get('status', 'N/A')} ({rs_data.get('value', 0):+.1f}% vs Nifty)"
    
    # BB Squeeze alert
    sqz_text = "🔥 BB SQUEEZE DETECTED — Breakout imminent!" if bb_sqz else ""
    
    lines = [
        f"{'💎' if is_diamond else '🔥'} <b>{master_sig}: {name} ({symbol})</b>",
        f"💰 <b>₹{round(price, 2)}</b> | Vol: {vol_text} | {br.get('max_score', 21)} Checks",
        "",
        "─── <b>1. THE MARKET PULSE</b> ─────────",
        f"📊 <b>Nifty:</b> {nifty_trend}",
        f"📈 <b>Conviction:</b> {stars} ({overall}%)",
        f"🏹 <b>Status:</b> {'Breakout' if vol_surge else 'Consol.'} (RSI: {rsi})",
        f"💪 <b>Strength:</b> {rs_text}",
    ]
    if below_min and min_score > 0:
        lines.append(f"⚠️ <b>Below threshold:</b> {overall}% &lt; {min_score}% (watchlist risk)")
    
    if sqz_text:
        lines.append(f"⚡ {sqz_text}")
    
    lines += [
        "",
        "─── <b>2. HOLDING SCENARIOS</b> ─────────",
        f"🚀 <b>Today</b>  → {fmt_price(scenarios.get('Today',{}).get('target'))} | Exit {scenarios.get('Today',{}).get('exit','3:15 PM')}",
        f"📅 <b>2 Days</b> → {fmt_price(scenarios.get('2 Days',{}).get('target'))} | Exit {scenarios.get('2 Days',{}).get('exit','-')}",
        f"🗓️ <b>1 Week</b> → {fmt_price(scenarios.get('1 Week',{}).get('target'))} | Exit {scenarios.get('1 Week',{}).get('exit','-')}",
        f"🌒 <b>1 Month</b>→ {fmt_price(scenarios.get('1 Month',{}).get('target'))} | Exit {scenarios.get('1 Month',{}).get('exit','-')}",
        "",
        "─── <b>3. TECHNICAL WALLS</b> ───────────",
        f"🚀 <b>R3:</b> {fmt_price(r3)} | 🟢 <b>S1:</b> {fmt_price(s1)} (BUY ZONE)",
        f"🎻 <b>R2:</b> {fmt_price(r2)} | 🟡 <b>S2:</b> {fmt_price(s2)} (STOP LOSS)",
        f"🎻 <b>R1:</b> {fmt_price(r1)} | 🟠 <b>S3:</b> {fmt_price(s3)} (Danger)",
        "",
        "─── <b>4. YOUR ACTION ROADMAP</b> ─────────",
        f"✅ <b>BUY:</b> Between {fmt_price(s1)} – {fmt_price(price)}",
        f"⏳ <b>HOLD:</b> {br.get('timing',{}).get('hold_duration', '2-5 Days')}",
        f"🛡️ <b>DEFEND:</b> At {fmt_price(r1)}, move Stop to entry",
        f"💰 <b>EXIT:</b> 50% at {fmt_price(r2)}, rest at {fmt_price(r3)}",
        "",
        f"<i>{screener_name}</i> | 🕐 {ist_now().strftime('%d-%b %H:%M IST')}"
    ]
    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════════
# TELEGRAM SENDER
# ═══════════════════════════════════════════════════════════════════════

async def send_telegram(token, chat_id, message):
    import asyncio
    url     = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True})
    hdrs = Headers.new(to_js({"Content-Type": "application/json"}))
    for attempt in range(3):
        resp = await fetch(url, method="POST", body=payload, headers=hdrs)
        status = int(str(resp.status))
        if status == 200:
            # Pace messages: 1s delay after each send to stay under Telegram rate limit
            await asyncio.sleep(1)
            return True
        if status == 429:
            print(f"TELEGRAM: Rate limited (429), retry {attempt+1}/3 in 3s")
            await asyncio.sleep(3)
            continue
        body = str(await resp.text())
        print(f"TELEGRAM: Send failed status={status} chat={chat_id} body={body[:200]}")
        return False
    print(f"TELEGRAM: Failed after 3 retries chat={chat_id}")
    return False

async def send_stock_alerts(token, chat_id, stock, screener_name, nifty_trend="Unknown"):
    msg1 = format_master_alert(stock, screener_name, nifty_trend)
    await send_telegram(token, chat_id, msg1)

# ═══════════════════════════════════════════════════════════════════════
# SCREENER.IN HTML PARSER
# ═══════════════════════════════════════════════════════════════════════

def extract_csrf(html):
    for line in html.split("\n"):
        if "csrfmiddlewaretoken" in line:
            try: return line.split('value="')[1].split('"')[0]
            except: pass
    return ""

def url_encode(text):
    result = ""
    safe = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~ "
    for ch in text:
        if ch in safe:
            result += "+" if ch == " " else ch
        else:
            result += "".join(f"%{b:02X}" for b in ch.encode())
    return result

def parse_table(html):
    headers, rows = [], []
    for part in html.split("<th")[1:]:
        cell = extract_between(part, ">", "</th>")
        while "<" in cell and ">" in cell:
            cell = cell[:cell.find("<")] + cell[cell.find(">")+1:]
        clean = " ".join(cell.split())
        if clean:
            headers.append(clean)
    tbody = extract_between(html, "<tbody>", "</tbody>")
    for tr in tbody.split("<tr")[1:]:
        cells = []
        symbol = ""
        for td in tr.split("<td")[1:]:
            inner = extract_between(td, ">", "</td>")
            if "<a" in inner:
                # Extract symbol from /company/SYMBOL/ slug
                href = extract_between(inner, 'href="', '"')
                if "/company/" in href:
                    symbol = href.split("/company/")[1].strip("/").split("/")[0]
                inner = extract_between(inner, ">", "</a>")
            while "<" in inner and ">" in inner:
                inner = inner[:inner.find("<")] + inner[inner.find(">")+1:]
            cells.append(" ".join(inner.split()))
        if cells:
            if symbol: cells.append(symbol)
            rows.append(cells)
    if rows and len(rows[0]) > len(headers):
        headers.append("_symbol")
    return headers, rows

def extract_between(s, start, end):
    i = s.find(start)
    if i == -1: return ""
    i += len(start)
    j = s.find(end, i)
    if j == -1: return ""
    return s[i:j]



# ═══════════════════════════════════════════════════════════════════════════
# MAIN WORKER CLASS
# ═══════════════════════════════════════════════════════════════════════════

class Default(WorkerEntrypoint):

    async def fetch(self, request):
        url = str(request.url)
        method = str(request.method)

        if "/api/settings" in url and method == "GET":
            return self._json(await self._get_settings())
        if "/api/settings" in url and method == "POST":
            body = json.loads(str(await request.text()))
            cur = await self._get_settings()
            cur.update(body)
            await self.env.KV.put("settings", json.dumps(cur))
            return self._json({"ok": True, "settings": cur})

        if "/api/screeners" in url and method == "GET":
            return self._json(await self._get_screeners())
        if "/api/screeners/delete" in url and method == "POST":
            body = json.loads(str(await request.text()))
            del_id = body.get("id", "")
            screeners = await self._get_screeners()
            screeners = [s for s in screeners if s["id"] != del_id]
            await self.env.KV.put("screeners", json.dumps(screeners))
            return self._json({"ok": True, "screeners": screeners})
        if "/api/screeners/toggle" in url and method == "POST":
            body = json.loads(str(await request.text()))
            toggle_id = body.get("id", "")
            screeners = await self._get_screeners()
            for s in screeners:
                if s["id"] == toggle_id:
                    s["enabled"] = not s.get("enabled", True)
                    break
            await self.env.KV.put("screeners", json.dumps(screeners))
            return self._json({"ok": True, "screeners": screeners})
        if "/api/screeners" in url and method == "POST":
            body = json.loads(str(await request.text()))
            screeners = await self._get_screeners()
            found = False
            for i, s in enumerate(screeners):
                if s["id"] == body.get("id"):
                    screeners[i].update(body)
                    found = True
                    break
            if not found:
                screeners.append(body)
            await self.env.KV.put("screeners", json.dumps(screeners))
            return self._json({"ok": True, "screeners": screeners})

        if "/api/telegram/delete" in url and method == "POST":
            body = json.loads(str(await request.text()))
            del_idx = int(body.get("index", -1))
            accts = await self._get_telegram_accounts()
            if 0 <= del_idx < len(accts):
                accts.pop(del_idx)
            await self.env.KV.put("telegram_accounts", json.dumps(accts))
            return self._json({"ok": True, "accounts": accts})
        if "/api/telegram" in url and method == "GET":
            return self._json(await self._get_telegram_accounts())
        if "/api/telegram" in url and method == "POST":
            body = json.loads(str(await request.text()))
            accts = await self._get_telegram_accounts()
            accts.append({"name": body.get("name",""), "token": body.get("token",""), "chat_id": body.get("chat_id","")})
            await self.env.KV.put("telegram_accounts", json.dumps(accts))
            return self._json({"ok": True, "accounts": accts})

        # ── API: Test Telegram ──
        if "/api/test" in url and method == "POST":
            try:
                accts = await self._get_telegram_accounts()
                for a in accts:
                    await send_telegram(a["token"], a["chat_id"], "✅ <b>Test from screener-alerts-mtf</b>\nTelegram connection works!")
                return self._json({"ok": True, "sent_to": len(accts)})
            except Exception as e:
                return self._json({"ok": False, "error": str(e)})

        # ── API: Debug Yahoo Finance directly ──
        if "/api/debug_yf" in url and method == "GET":
            # Extract q params manually
            try:
                q = url.split("?")[1]
                params = dict(kv.split("=") for kv in q.split("&"))
                sym = params.get("symbol", "AETHER")
                exc = params.get("exchange", "NS")
                debug_yf = {"symbol": sym, "exchange": exc}
                
                # Direct test of fetch_candles
                candles, used_sym = await fetch_candles(sym, exc, "1d", "3mo")
                debug_yf["candles_count"] = len(candles)
                debug_yf["used_sym"] = used_sym
                if candles:
                    debug_yf["latest_close"] = candles[-1]["close"]
                else:
                    # Do a raw fetch to see response code
                    base_url  = "https://query1.finance.yahoo.com/v8/finance/chart"
                    target = f"{sym}.{exc}"
                    status, text = await js_fetch(f"{base_url}/{target}?interval=1d&range=3mo", headers={"User-Agent": "Mozilla/5.0"})
                    debug_yf["raw_status"] = status
                    debug_yf["raw_text_preview"] = text[:200]
                
                return self._json(debug_yf)
            except Exception as e:
                return self._json({"error": str(e)})

        # ── API: Clear prev_names (reset) ──
        if "/api/clear" in url and method == "POST":
            screeners = await self._get_screeners()
            for s in screeners:
                try:
                    await self.env.KV.delete(f"prev_names_{s['id']}")
                except: pass
            return self._json({"ok": True, "cleared": len(screeners)})

        # ── API: Debug (run scraper with full diagnostics) ──
        if "/api/debug" in url and method == "POST":
            debug = {"steps": []}
            screeners = await self._get_screeners()
            s = screeners[1] if len(screeners) > 1 else None
            if not s:
                return self._json({"error": "No screeners"})
            scr_url = s["url"]
            scr_query = s["query"]
            scr_name = s.get("name", s["id"])
            debug["screener"] = scr_name
            try:
                # Step 1: GET screener page
                get_headers = Headers.new(to_js({
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml",
                }))
                get_resp = await fetch(scr_url, method="GET", headers=get_headers)
                html = str(await get_resp.text())
                debug["steps"].append({"step": "GET screener", "status": int(str(get_resp.status)), "html_len": len(html)})

                # Step 2: Extract CSRF
                csrf_token = extract_csrf(html)
                debug["steps"].append({"step": "CSRF extract", "found": bool(csrf_token), "token_preview": csrf_token[:20] if csrf_token else "NONE"})

                # Step 3: Get cookie
                set_cookie = str(get_resp.headers.get("set-cookie") or "")
                csrf_cookie = ""
                for part in set_cookie.split(";"):
                    part = part.strip()
                    if part.startswith("csrftoken="):
                        csrf_cookie = part
                        break
                debug["steps"].append({"step": "Cookie", "found": bool(csrf_cookie)})

                # Step 4: POST screener query
                body_str = f"csrfmiddlewaretoken={csrf_token}&query={url_encode(scr_query)}&order="
                post_headers = Headers.new(to_js({
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": scr_url,
                    "Origin": "https://www.screener.in",
                    "X-Requested-With": "XMLHttpRequest",
                    "Cookie": csrf_cookie,
                }))
                post_resp = await fetch(scr_url, method="POST", body=body_str, headers=post_headers)
                result_html = str(await post_resp.text())
                debug["steps"].append({"step": "POST query", "status": int(str(post_resp.status)), "result_len": len(result_html), "preview": result_html[:200]})

                # Step 5: Parse table
                headers_row, rows = parse_table(result_html)
                debug["steps"].append({"step": "Parse table", "headers": headers_row[:5], "row_count": len(rows)})

                if rows:
                    # Step 6: Try enriching first stock
                    d = dict(zip(headers_row, rows[0]))
                    name = d.get("Name", "")
                    symbol = d.get("_symbol", name.replace(" ","").replace(".","").upper())
                    exchange = "BO" if symbol.isdigit() else "NS"
                    price_str = d.get("CMP / LTP", d.get("Current Price", "0"))
                    try:
                        price = float(str(price_str).replace(",",""))
                    except:
                        price = 0
                    debug["steps"].append({"step": "First stock", "name": name, "symbol": symbol, "exchange": exchange, "price": price})

                    # Step 7: Try Yahoo Finance for just 1 timeframe
                    try:
                        candles, used_sym = await fetch_candles(symbol, exchange, "1d", "3mo")
                        debug["steps"].append({"step": "Yahoo Finance (1d/3mo)", "candles": len(candles), "symbol_used": used_sym})
                    except Exception as ye:
                        debug["steps"].append({"step": "Yahoo Finance", "error": str(ye)})

            except Exception as e:
                debug["steps"].append({"step": "CRASHED", "error": str(e), "type": type(e).__name__})

            return self._json(debug)

        if "/api/trigger" in url and method == "POST":
            from datetime import datetime, timezone, timedelta
            IST = timezone(timedelta(hours=5, minutes=30))
            now = datetime.now(IST)
            now_epoch = int(now.timestamp())
            screeners = await self._get_screeners()
            # Accept optional {id} to trigger a single screener
            body = {}
            try:
                body = json.loads(str(await request.text()))
            except:
                pass
            target_id = body.get("id", None)
            count = 0
            for s in screeners:
                if not s.get("enabled", True):
                    continue
                if target_id and s.get("id") != target_id:
                    continue
                await self._run_single(s, is_manual=True)
                s["last_run_epoch"] = now_epoch
                count += 1
            if count:
                await self.env.KV.put("screeners", json.dumps(screeners))
                fresh = await self._get_settings()
                fresh["last_run"] = now.isoformat()
                fresh["total_runs"] = fresh.get("total_runs", 0) + 1
                await self.env.KV.put("settings", json.dumps(fresh))
            return self._json({"ok": True, "triggered": count, "id": target_id or "all"})

        if "/api/cron" in url and method == "POST":
            await self.scheduled(None, self.env, None)
            return self._json({"ok": True, "cron_processed": True})

        if method == "OPTIONS":
            return Response("", headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
            })

        return Response(DASHBOARD_HTML, headers={"Content-Type": "text/html"})

    async def scheduled(self, event, env, ctx):
        """Per-screener scheduling — runs ONE screener per cron cycle (most overdue first)."""
        settings = await self._get_settings()
        if not settings.get("enabled", True):
            print("CRON: Global power OFF — skipping all")
            return
        from datetime import datetime, timezone, timedelta
        IST = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(IST)
        now_epoch = int(now.timestamp())
        today = now.strftime("%Y-%m-%d")
        now_m = now.hour * 60 + now.minute
        screeners = await self._get_screeners()
        print(f"CRON: {now.strftime('%H:%M:%S')} IST | {len(screeners)} screeners | now_m={now_m}")

        # Find the most overdue screener that's eligible to run
        best = None
        best_overdue = -1
        for s in screeners:
            sid = s.get("id", "?")
            if not s.get("enabled", True):
                print(f"CRON [{sid}]: SKIP — disabled")
                continue
            if not self._in_time_window(s, now_m, today):
                print(f"CRON [{sid}]: SKIP — outside window {s.get('start_time','?')}-{s.get('end_time','?')}")
                continue
            s_interval = int(s.get("interval_minutes", 5))
            s_last = int(s.get("last_run_epoch", 0))
            elapsed = (now_epoch - s_last) / 60 if s_last else 9999
            if elapsed < s_interval:
                print(f"CRON [{sid}]: SKIP — interval {s_interval}m, elapsed {elapsed:.1f}m")
                continue
            # This screener is eligible — pick the most overdue one
            overdue = elapsed - s_interval
            if overdue > best_overdue:
                best = s
                best_overdue = overdue

        if best:
            sid = best.get("id", "?")
            print(f"CRON [{sid}]: RUNNING (overdue by {best_overdue:.1f}m)")
            await self._run_single(best)
            best["last_run_epoch"] = now_epoch
            await self.env.KV.put("screeners", json.dumps(screeners))
            fresh = await self._get_settings()
            fresh["last_run"] = now.isoformat()
            fresh["total_runs"] = fresh.get("total_runs", 0) + 1
            await self.env.KV.put("settings", json.dumps(fresh))
            print(f"CRON: Done — total_runs={fresh['total_runs']}")
        else:
            print("CRON: No screeners due this cycle")

    def _in_time_window(self, cfg, now_m, today):
        try:
            sh, sm = map(int, cfg.get("start_time", "09:15").split(":"))
            eh, em = map(int, cfg.get("end_time", "15:30").split(":"))
            if now_m < sh * 60 + sm or now_m > eh * 60 + em:
                return False
        except:
            pass
        sd = cfg.get("start_date", "")
        ed = cfg.get("end_date", "")
        if sd and today < sd: return False
        if ed and today > ed: return False
        return True

    def _json(self, data):
        return Response(json.dumps(data), headers={
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        })

    async def _get_settings(self):
        raw = await self.env.KV.get("settings")
        if raw:
            saved = json.loads(str(raw))
            m = dict(DEFAULT_SETTINGS)
            m.update(saved)
            return m
        return dict(DEFAULT_SETTINGS)

    async def _get_screeners(self):
        raw = await self.env.KV.get("screeners")
        if raw:
            return json.loads(str(raw))
        await self.env.KV.put("screeners", json.dumps(SCREENERS))
        return list(SCREENERS)

    async def _get_telegram_accounts(self):
        raw = await self.env.KV.get("telegram_accounts")
        if raw:
            return json.loads(str(raw))
        seed = [
            {"name": "Account 1", "token": "YOUR_BOT_TOKEN_1", "chat_id": "YOUR_CHAT_ID_1"},
            {"name": "Account 2", "token": "YOUR_BOT_TOKEN_2", "chat_id": "YOUR_CHAT_ID_2"},
        ]
        await self.env.KV.put("telegram_accounts", json.dumps(seed))
        return seed

    async def _send_all(self, text):
        accts = await self._get_telegram_accounts()
        for a in accts:
            await send_telegram(a["token"], a["chat_id"], text)

    async def _send_all_stock(self, stock, screener_name, nifty_trend="Unknown"):
        accts = await self._get_telegram_accounts()
        msg1 = format_master_alert(stock, screener_name, nifty_trend)
        for a in accts:
            await send_telegram(a["token"], a["chat_id"], msg1)

    async def _run_single(self, screener, is_manual=False):
        sid = screener["id"]
        scr_url = screener["url"]
        scr_query = screener["query"]
        scr_name = screener.get("name", sid)
        settings = await self._get_settings()
        try:
            min_score = int(settings.get("min_score", 50))
        except:
            min_score = 50
        min_score = max(0, min(100, min_score))
        now_str = ist_now().strftime("%d-%b-%Y %H:%M IST")

        try:
            get_headers = Headers.new(to_js({
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
            }))
            get_resp = await fetch(scr_url, method="GET", headers=get_headers)
            html = await get_resp.text()
            csrf_token = extract_csrf(str(html))

            set_cookie = str(get_resp.headers.get("set-cookie") or "")
            csrf_cookie = ""
            for part in set_cookie.split(";"):
                part = part.strip()
                if part.startswith("csrftoken="):
                    csrf_cookie = part
                    break

            body_str = f"csrfmiddlewaretoken={csrf_token}&query={url_encode(scr_query)}&order="
            post_headers = Headers.new(to_js({
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": scr_url,
                "Origin": "https://www.screener.in",
                "X-Requested-With": "XMLHttpRequest",
                "Cookie": csrf_cookie,
            }))
            post_resp = await fetch(scr_url, method="POST", body=body_str, headers=post_headers)
            result_html = str(await post_resp.text())

            headers_row, rows = parse_table(result_html)

            prev_key = f"prev_names_{sid}"
            prev_json = await self.env.KV.get(prev_key)
            prev_names = json.loads(str(prev_json)) if prev_json else []
            curr_names = [dict(zip(headers_row, r)).get("Name", "") for r in rows]

            entered = [n for n in curr_names if n not in prev_names]
            exited  = [n for n in prev_names if n not in curr_names]

            sent_count = 0
            below_threshold_count = 0
            nifty_trend = "Unknown"
            nifty_candles = []
            if curr_names:
                nifty_candles, _ = await fetch_candles("^NSEI", "", "1d", "3mo")
                nifty_trend = fetch_nifty_trend(nifty_candles)
                print(f"NIFTY: Fetched {len(nifty_candles)} candles, trend={nifty_trend}")

                for row in rows:
                    d = dict(zip(headers_row, row))
                    name = d.get("Name", "")
                    symbol = d.get("_symbol", name.replace(" ","").replace(".","").upper())
                    exchange = "BO" if symbol.isdigit() else "NS"
                    price_str = d.get("CMP / LTP", d.get("Current Price", "0"))
                    try:
                        price = float(str(price_str).replace(",",""))
                    except:
                        price = 0

                    stock = {"name": name, "symbol": symbol, "exchange": exchange,
                             "price": price, "change": d.get("% Chg", d.get("Chg %", "0"))}
                    enriched = await enrich_stock(stock, nifty_candles=nifty_candles)
                    overall_pct = int(enriched.get("mtf_summary", {}).get("overall_pct", 0) or 0)
                    if overall_pct < min_score:
                        below_threshold_count += 1
                        print(f"LOW-CONVICTION [{sid}] {symbol}: overall_pct={overall_pct} < min_score={min_score}")
                        enriched["below_min_score"] = True
                    else:
                        enriched["below_min_score"] = False
                    enriched["min_score"] = min_score
                    await self._send_all_stock(enriched, scr_name, nifty_trend)
                    sent_count += 1

            if exited:
                await self._send_all(
                    f"📈 <b>{scr_name}</b>\n"
                    f"🚬 <b>EXITED:</b> {', '.join(exited)}\n"
                    f"🕐 {now_str}"
                )

            # ALWAYS send a summary message
            await self._send_all(
                f"📈 <b>{scr_name}</b>\n"
                f"✅ Cron run complete\n"
                f"📊 Stocks found: <b>{len(rows)}</b> | Alerts sent: <b>{sent_count}</b> | "
                f"Below threshold (&lt;{min_score}%): <b>{below_threshold_count}</b>\n"
                f"🕐 {now_str}"
            )

            await self.env.KV.put(prev_key, json.dumps(curr_names))

        except Exception as e:
            try:
                await self._send_all(
                    f"📈 <b>{scr_name}</b>\n"
                    f"❌ <b>Error:</b> {str(e)[:200]}\n"
                    f"🕐 {now_str}"
                )
            except:
                pass


# ═══════════════════════════════════════════════════════════════════════
# DASHBOARD HTML
# ═══════════════════════════════════════════════════════════════════════

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Screener Alerts — MTF</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#0a0a0f;--s1:#12121a;--s2:#1a1a26;--s3:#22222f;
  --bdr:#2a2a3a;--t1:#e4e4ed;--t2:#9494a8;--t3:#6a6a80;
  --acc:#6c5ce7;--acc2:#a29bfe;--accg:rgba(108,92,231,.15);
  --grn:#00d68f;--grng:rgba(0,214,143,.12);
  --red:#ff6b6b;--redg:rgba(255,107,107,.12);
  --r:12px;--rl:16px;
}
body{font-family:'Inter',sans-serif;background:var(--bg);color:var(--t1);min-height:100vh}
.c{max-width:760px;margin:0 auto;padding:24px 16px}
.hdr{text-align:center;margin-bottom:28px;padding:28px 0 20px}
.hdr h1{font-size:22px;font-weight:700;letter-spacing:-.5px}
.hdr h1 span{color:var(--acc2)}
.hdr p{color:var(--t3);font-size:13px;margin-top:4px}

.sbar{display:flex;align-items:center;gap:10px;padding:14px 18px;background:var(--s1);border:1px solid var(--bdr);border-radius:var(--r);margin-bottom:16px}
.sdot{width:10px;height:10px;border-radius:50%;flex-shrink:0;animation:pulse 2s ease-in-out infinite}
.sdot.on{background:var(--grn);box-shadow:0 0 8px var(--grn)}.sdot.off{background:var(--red);box-shadow:0 0 8px var(--red);animation:none}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.stxt{font-size:13px;color:var(--t2);flex:1}.stxt b{color:var(--t1);font-weight:600}
.sruns{font-size:12px;color:var(--t3)}

.cd{background:var(--s1);border:1px solid var(--bdr);border-radius:var(--rl);padding:20px;margin-bottom:14px;transition:border-color .2s}
.cd:hover{border-color:var(--s3)}
.ct{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:1.2px;color:var(--t3);margin-bottom:14px}

.trow{display:flex;align-items:center;justify-content:space-between;gap:16px}
.tlbl{font-size:15px;font-weight:500}.tsub{font-size:12px;color:var(--t3);margin-top:2px}
.tgl{position:relative;width:52px;height:28px;flex-shrink:0}
.tgl input{opacity:0;width:0;height:0}
.tgl .sl{position:absolute;inset:0;background:var(--s3);border-radius:14px;cursor:pointer;transition:.3s}
.tgl .sl:before{content:'';position:absolute;width:22px;height:22px;left:3px;bottom:3px;background:#fff;border-radius:50%;transition:.3s}
.tgl input:checked+.sl{background:var(--grn)}.tgl input:checked+.sl:before{transform:translateX(24px)}

.fg{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}
.f{display:flex;flex-direction:column;gap:4px}
.f label{font-size:11px;font-weight:500;color:var(--t3);text-transform:uppercase;letter-spacing:.8px}
.f select,.f input{background:var(--s2);border:1px solid var(--bdr);border-radius:8px;padding:10px 12px;color:var(--t1);font-family:inherit;font-size:14px;outline:none;transition:.2s;width:100%}
.f select:focus,.f input:focus{border-color:var(--acc);box-shadow:0 0 0 3px var(--accg)}
.f select{cursor:pointer;appearance:none;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' fill='%236a6a80'%3E%3Cpath d='M6 8L1 3h10z'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 12px center}

.brow{display:flex;gap:10px;margin-top:16px}
.btn{flex:1;padding:12px;border:none;border-radius:var(--r);font-family:inherit;font-size:14px;font-weight:600;cursor:pointer;transition:.2s;display:flex;align-items:center;justify-content:center;gap:6px}
.bp{background:var(--acc);color:#fff}.bp:hover{background:#5b4bd5;box-shadow:0 4px 16px var(--accg)}.bp:active{transform:scale(.97)}
.bs{background:var(--s2);color:var(--t1);border:1px solid var(--bdr)}.bs:hover{background:var(--s3)}
.btn:disabled{opacity:.5;cursor:not-allowed}
.btn-sm{padding:8px 14px;font-size:12px;flex:0}

/* Per-screener schedule always visible */

/* Disabled card */
.cd.disabled{opacity:.4;pointer-events:none}

/* Screener list */
.scr-item{background:var(--s2);border:1px solid var(--bdr);border-radius:var(--r);padding:14px 16px;margin-bottom:10px;transition:border-color .2s}
.scr-item:hover{border-color:var(--t3)}
.scr-top{display:flex;align-items:center;gap:12px}
.scr-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.scr-dot.on{background:var(--grn)}.scr-dot.off{background:var(--red)}
.scr-info{flex:1;min-width:0}
.scr-name{font-size:14px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.scr-url{font-size:11px;color:var(--t3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.scr-actions{display:flex;gap:6px;flex-shrink:0}
.scr-btn{background:var(--s3);border:1px solid var(--bdr);border-radius:6px;padding:6px 10px;font-size:11px;color:var(--t2);cursor:pointer;transition:.2s;font-family:inherit}
.scr-btn:hover{border-color:var(--t3);color:var(--t1)}
.scr-btn.del{color:var(--red);border-color:transparent}.scr-btn.del:hover{border-color:var(--red);background:var(--redg)}

/* Per-screener schedule (shown in individual mode) */
.scr-sched{margin-top:10px;padding-top:10px;border-top:1px solid var(--bdr)}
.scr-sched .fg{margin-top:6px}

/* Modal */
.modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.6);backdrop-filter:blur(4px);display:none;z-index:50;justify-content:center;align-items:center}
.modal-bg.show{display:flex}
.modal{background:var(--s1);border:1px solid var(--bdr);border-radius:var(--rl);padding:24px;width:90%;max-width:520px;max-height:80vh;overflow-y:auto}
.modal h2{font-size:16px;font-weight:700;margin-bottom:16px}
.modal .f{margin-bottom:12px}
.modal .f textarea{background:var(--s2);border:1px solid var(--bdr);border-radius:8px;padding:10px 12px;color:var(--t1);font-family:'Courier New',monospace;font-size:12px;outline:none;width:100%;min-height:120px;resize:vertical;transition:.2s}
.modal .f textarea:focus{border-color:var(--acc);box-shadow:0 0 0 3px var(--accg)}

.toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);padding:12px 24px;border-radius:var(--r);font-size:13px;font-weight:500;background:var(--s2);border:1px solid var(--bdr);color:var(--t1);box-shadow:0 8px 32px rgba(0,0,0,.4);transition:.4s cubic-bezier(.4,0,.2,1);z-index:100;white-space:nowrap}
.toast.show{transform:translateX(-50%) translateY(0)}
.toast.ok{border-color:var(--grn);background:linear-gradient(135deg,var(--s2),rgba(0,214,143,.08))}
.toast.err{border-color:var(--red);background:linear-gradient(135deg,var(--s2),rgba(255,107,107,.08))}

.spinner{width:16px;height:16px;border:2px solid transparent;border-top:2px solid currentColor;border-radius:50%;animation:spin .6s linear infinite;display:none}
@keyframes spin{to{transform:rotate(360deg)}}
.footer{text-align:center;padding:20px 0;font-size:11px;color:var(--t3)}.footer a{color:var(--acc2);text-decoration:none}
</style>
</head>
<body>
<div class="c">
  <div class="hdr">
    <h1>📊 <span>Screener Alerts</span> MTF</h1>
    <p>Multi-Timeframe Technical Analysis ÔÇó 8 TFs ├ù 21 Checks → Telegram</p>
  </div>

  <div class="sbar">
    <div class="sdot" id="dot"></div>
    <div class="stxt" id="stxt">Loading...</div>
    <div class="sruns" id="sruns"></div>
  </div>

  <!-- Power -->
  <div class="cd">
    <div class="ct">Global Power</div>
    <div class="trow">
      <div><div class="tlbl" id="pLbl">Cron Enabled</div><div class="tsub" id="pSub">All screeners</div></div>
      <label class="tgl"><input type="checkbox" id="enTgl" onchange="saveSets()"><span class="sl"></span></label>
    </div>
  </div>

  <!-- Last Run Info -->
  <div class="cd">
    <div class="ct">Schedule Info</div>
    <div class="fg">
      <div class="f"><label>Last Run</label><input id="lrDisp" readonly style="color:var(--t3);cursor:default"></div>
      <div class="f"><label>Total Runs</label><input id="trDisp" readonly style="color:var(--t3);cursor:default"></div>
    </div>
    <div class="tsub" style="text-align:center;margin-top:8px">Each screener has its own interval & time window</div>
  </div>

  <!-- Screeners -->
  <div class="cd">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
      <div class="ct" style="margin:0">Screeners</div>
      <button class="btn bp btn-sm" onclick="openModal()">+ Add</button>
    </div>
    <div id="scrList"></div>
  </div>

  <!-- Telegram Accounts -->
  <div class="cd">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
      <div class="ct" style="margin:0">Telegram Accounts</div>
      <button class="btn bp btn-sm" onclick="openTgModal()">+ Add</button>
    </div>
    <div id="tgList"></div>
  </div>

  <!-- Actions -->
  <div class="brow">
    <button class="btn bp" id="trigBtn" onclick="trigNow()">⚡ Run All Now <div class="spinner" id="trigSp"></div></button>
    <button class="btn bs" onclick="load()">🔄 Refresh</button>
  </div>

  <div class="footer"><a href="https://www.screener.in" target="_blank">screener.in ↑</a></div>
</div>

<!-- Add/Edit Modal -->
<div class="modal-bg" id="modalBg" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <h2 id="modalTitle">Add Screener</h2>
    <div class="f"><label>ID (slug, no spaces)</label><input id="mId" placeholder="my-screen"></div>
    <div class="f"><label>Name</label><input id="mName" placeholder="My Screen — Description"></div>
    <div class="f"><label>Screener.in URL</label><input id="mUrl" placeholder="https://www.screener.in/screens/..."></div>
    <div class="f"><label>Query</label><textarea id="mQuery" placeholder="Is not SME AND&#10;Market Capitalization > 200 AND&#10;..."></textarea></div>
    <div id="modalSchedSection">
      <div class="ct" style="margin-top:16px">Individual Schedule</div>
      <div class="fg">
        <div class="f"><label>Interval</label>
          <select id="mInterval">
            <option value="1">Every 1 min</option><option value="2">Every 2 min</option>
            <option value="3">Every 3 min</option><option value="5" selected>Every 5 min</option>
            <option value="10">Every 10 min</option><option value="15">Every 15 min</option>
            <option value="30">Every 30 min</option><option value="60">Every 1 hour</option>
          </select>
        </div>
        <div class="f"><label>&nbsp;</label></div>
      </div>
      <div class="fg">
        <div class="f"><label>Start Time</label><input type="time" id="mSTime" value="09:15"></div>
        <div class="f"><label>End Time</label><input type="time" id="mETime" value="15:30"></div>
      </div>
      <div class="fg">
        <div class="f"><label>Start Date</label><input type="date" id="mSDate"></div>
        <div class="f"><label>End Date</label><input type="date" id="mEDate"></div>
      </div>
    </div>
    <div class="brow">
      <button class="btn bp" onclick="saveScr()">💾 Save</button>
      <button class="btn bs" onclick="closeModal()">Cancel</button>
    </div>
  </div>
</div>

<!-- Telegram Modal -->
<div class="modal-bg" id="tgModalBg" onclick="if(event.target===this)closeTgModal()">
  <div class="modal">
    <h2>Add Telegram Account</h2>
    <div class="f"><label>Name (label)</label><input id="tgName" placeholder="My Account"></div>
    <div class="f"><label>Bot Token</label><input id="tgToken" placeholder="1234567890:AABBC..."></div>
    <div class="f"><label>Chat ID</label><input id="tgChatId" placeholder="1234567890"></div>
    <div class="brow">
      <button class="btn bp" onclick="saveTg()">💾 Save</button>
      <button class="btn bs" onclick="closeTgModal()">Cancel</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
const A=location.origin;
let _sets={},_scrs=[],_tg=[];

const INTERVAL_OPTS = {'1':'1 min','2':'2 min','3':'3 min','5':'5 min','10':'10 min','15':'15 min','30':'30 min','60':'1 hr'};

async function load(){
  try{
    const[sr,sc,tg]=await Promise.all([fetch(A+'/api/settings').then(r=>r.json()),fetch(A+'/api/screeners').then(r=>r.json()),fetch(A+'/api/telegram').then(r=>r.json())]);
    _sets=sr;_scrs=sc;_tg=tg;
    document.getElementById('enTgl').checked=sr.enabled;
    updStatus(sr);renderScrs(sc);renderTg(tg);
  }catch(e){toast('Failed to load','err')}
}

function updStatus(s){
  const d=document.getElementById('dot'),t=document.getElementById('stxt'),r=document.getElementById('sruns');
  const en=_scrs.filter(x=>x.enabled!==false).length;
  if(s.enabled){
    d.className='sdot on';
    t.innerHTML=`<b>Active</b> · Per-Screener · ${en} screener(s)`;
  }else{
    d.className='sdot off';t.innerHTML='<b>Paused</b> · All cron triggers skipped';
  }
  r.innerHTML=s.total_runs?'🔢 '+s.total_runs+' runs':'';
  const lr=document.getElementById('lrDisp');
  if(s.last_run){try{lr.value=new Date(s.last_run).toLocaleString('en-IN',{timeZone:'Asia/Kolkata',hour:'2-digit',minute:'2-digit',second:'2-digit',day:'2-digit',month:'short',year:'numeric',hour12:true})}catch(e){lr.value=s.last_run}}
  else lr.value='Never';
  const tr=document.getElementById('trDisp');if(tr)tr.value=s.total_runs||0;
}

function renderScrs(list){
  const el=document.getElementById('scrList');
  if(!list.length){el.innerHTML='<div style="text-align:center;padding:24px;color:var(--t3)">No screeners. Click + Add.</div>';return}
  el.innerHTML=list.map(s=>{
    let schedHtml='';
    {
      const iv=s.interval_minutes||5;
      const st=s.start_time||'09:15';
      const et=s.end_time||'15:30';
      schedHtml=`<div class="scr-sched">
        <div class="fg">
          <div class="f"><label>Interval</label>
            <select onchange="updScrSched('${s.id}','interval_minutes',parseInt(this.value))">
              ${Object.entries(INTERVAL_OPTS).map(([v,l])=>`<option value="${v}"${parseInt(v)===iv?' selected':''}>${l}</option>`).join('')}
            </select>
          </div>
          <div class="f"><label>Window</label>
            <input type="text" value="${st} – ${et}" readonly style="color:var(--t3);cursor:default;font-size:12px">
          </div>
        </div>
        <div class="fg">
          <div class="f"><label>Start Time</label><input type="time" value="${st}" onchange="updScrSched('${s.id}','start_time',this.value)"></div>
          <div class="f"><label>End Time</label><input type="time" value="${et}" onchange="updScrSched('${s.id}','end_time',this.value)"></div>
        </div>
        <div class="fg">
          <div class="f"><label>Start Date</label><input type="date" value="${s.start_date||''}" onchange="updScrSched('${s.id}','start_date',this.value)"></div>
          <div class="f"><label>End Date</label><input type="date" value="${s.end_date||''}" onchange="updScrSched('${s.id}','end_date',this.value)"></div>
        </div>
      </div>`;
    }
    return `<div class="scr-item">
      <div class="scr-top">
        <div class="scr-dot ${s.enabled!==false?'on':'off'}"></div>
        <div class="scr-info">
          <div class="scr-name">${esc(s.name||s.id)}</div>
          <div class="scr-url">${esc(s.url||'')} · every ${s.interval_minutes||5}m</div>
        </div>
        <div class="scr-actions">
          <button class="scr-btn" onclick="toggleScr('${s.id}')">${s.enabled!==false?'⏸ Pause':'▶ Resume'}</button>
          <button class="scr-btn" onclick="editScr('${s.id}')">✏️</button>
          <button class="scr-btn del" onclick="delScr('${s.id}')">🗑</button>
        </div>
      </div>
      ${schedHtml}
    </div>`;
  }).join('');
}

let _timer;
async function saveSets(){
  clearTimeout(_timer);_timer=setTimeout(async()=>{
    try{
      const p={enabled:document.getElementById('enTgl').checked,schedule_mode:'individual'};
      const r=await fetch(A+'/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)});
      const d=await r.json();if(d.ok){_sets=d.settings;updStatus(d.settings);toast('Settings saved Ô£ô','ok')}
    }catch(e){toast('Save failed','err')}
  },300);
}

async function updScrSched(id,field,val){
  const s=_scrs.find(x=>x.id===id);
  if(!s)return;
  s[field]=val;
  try{
    const r=await fetch(A+'/api/screeners',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(s)});
    const d=await r.json();
    if(d.ok){_scrs=d.screeners;toast('Schedule saved Ô£ô','ok')}
  }catch(e){toast('Failed','err')}
}

async function toggleScr(id){
  try{const r=await fetch(A+'/api/screeners/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})});
  const d=await r.json();if(d.ok){_scrs=d.screeners;renderScrs(_scrs);updStatus(_sets);toast('Toggled Ô£ô','ok')}}catch(e){toast('Failed','err')}
}
async function delScr(id){
  if(!confirm('Delete this screener?'))return;
  try{const r=await fetch(A+'/api/screeners/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})});
  const d=await r.json();if(d.ok){_scrs=d.screeners;renderScrs(_scrs);updStatus(_sets);toast('Deleted Ô£ô','ok')}}catch(e){toast('Failed','err')}
}

function openModal(s){
  document.getElementById('modalTitle').textContent=s?'Edit Screener':'Add Screener';
  document.getElementById('mId').value=s?s.id:'';document.getElementById('mId').readOnly=!!s;
  document.getElementById('mName').value=s?s.name:'';
  document.getElementById('mUrl').value=s?s.url:'';
  document.getElementById('mQuery').value=s?s.query:'';
  document.getElementById('mInterval').value=String(s?s.interval_minutes||5:5);
  document.getElementById('mSTime').value=s?s.start_time||'09:15':'09:15';
  document.getElementById('mETime').value=s?s.end_time||'15:30':'15:30';
  document.getElementById('mSDate').value=s?s.start_date||'':'';
  document.getElementById('mEDate').value=s?s.end_date||'':'';
  document.getElementById('modalBg').classList.add('show');
}
function closeModal(){document.getElementById('modalBg').classList.remove('show')}
function editScr(id){const s=_scrs.find(x=>x.id===id);if(s)openModal(s)}

async function saveScr(){
  const o={id:document.getElementById('mId').value.trim(),name:document.getElementById('mName').value.trim(),
    url:document.getElementById('mUrl').value.trim(),query:document.getElementById('mQuery').value.trim(),enabled:true,
    interval_minutes:parseInt(document.getElementById('mInterval').value),
    start_time:document.getElementById('mSTime').value,end_time:document.getElementById('mETime').value,
    start_date:document.getElementById('mSDate').value,end_date:document.getElementById('mEDate').value,
    last_run_epoch:0};
  if(!o.id||!o.url||!o.query){toast('Fill all fields','err');return}
  // Preserve last_run_epoch if editing
  const existing=_scrs.find(x=>x.id===o.id);
  if(existing)o.last_run_epoch=existing.last_run_epoch||0;
  try{const r=await fetch(A+'/api/screeners',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(o)});
  const d=await r.json();if(d.ok){_scrs=d.screeners;renderScrs(_scrs);closeModal();toast('Saved Ô£ô','ok')}}catch(e){toast('Failed','err')}
}

async function trigNow(){
  const b=document.getElementById('trigBtn'),sp=document.getElementById('trigSp');b.disabled=true;sp.style.display='inline-block';
  const enabled=_scrs.filter(s=>s.enabled!==false);
  if(!enabled.length){toast('No enabled screeners','err');b.disabled=false;sp.style.display='none';return;}
  let done=0;
  for(const s of enabled){
    toast(`⚡ Running ${s.name||s.id} (${++done}/${enabled.length})...`,'ok');
    try{await fetch(A+'/api/trigger',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:s.id})})}
    catch(e){toast(`❌ ${s.id} failed`,'err')}
  }
  toast(`✅ Done! Triggered ${done} screener(s)`,'ok');
  setTimeout(load,1000);
  b.disabled=false;sp.style.display='none';
}

// ── Telegram accounts ──
function renderTg(list){
  const el=document.getElementById('tgList');
  if(!list.length){el.innerHTML='<div style="text-align:center;padding:24px;color:var(--t3)">No accounts. Click + Add.</div>';return}
  el.innerHTML=list.map((a,i)=>`
    <div class="scr-item">
      <div class="scr-top">
        <div class="scr-dot on"></div>
        <div class="scr-info">
          <div class="scr-name">📱 ${esc(a.name||'Account '+(i+1))}</div>
          <div class="scr-url">Chat: ${esc(a.chat_id)} · Bot: ${esc(a.token.substring(0,12))}...</div>
        </div>
        <div class="scr-actions">
          <button class="scr-btn del" onclick="delTg(${i})">🗑</button>
        </div>
      </div>
    </div>`).join('');
}
function openTgModal(){document.getElementById('tgName').value='';document.getElementById('tgToken').value='';document.getElementById('tgChatId').value='';document.getElementById('tgModalBg').classList.add('show')}
function closeTgModal(){document.getElementById('tgModalBg').classList.remove('show')}
async function saveTg(){
  const o={name:document.getElementById('tgName').value.trim(),token:document.getElementById('tgToken').value.trim(),chat_id:document.getElementById('tgChatId').value.trim()};
  if(!o.token||!o.chat_id){toast('Fill token and chat ID','err');return}
  try{const r=await fetch(A+'/api/telegram',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(o)});
  const d=await r.json();if(d.ok){_tg=d.accounts;renderTg(_tg);closeTgModal();toast('Account added Ô£ô','ok')}}catch(e){toast('Failed','err')}
}
async function delTg(idx){
  if(!confirm('Remove this Telegram account?'))return;
  try{const r=await fetch(A+'/api/telegram/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({index:idx})});
  const d=await r.json();if(d.ok){_tg=d.accounts;renderTg(_tg);toast('Removed Ô£ô','ok')}}catch(e){toast('Failed','err')}
}

function toast(m,t){const e=document.getElementById('toast');e.textContent=m;e.className='toast '+t+' show';setTimeout(()=>e.className='toast',2500)}
function esc(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}

load();
</script>
</body>
</html>"""
