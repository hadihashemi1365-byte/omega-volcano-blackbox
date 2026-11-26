#!/usr/bin/env python3
"""
OMEGA S7 MID-CAP ANTI-SPIKE STAIRCASE LAB v1.0 (2-YEAR, 80-COIN AUDIT)
======================================================================
Tests whether clean multi-hour 'Staircase' breakouts (like SSV +36%) can be
captured systematically across ~80 Nobitex-listed mid-cap & large-cap USDT pairs
while strictly filtering out vertical 1-candle traps (like ME / MOVE) and
illiquid books (PIT 24h Binance QuoteVol >= $15M).

4 Orthogonal Engines (all 100% Spot-Only, Strict Next-Open, Stop-First Gap-Protected):
  - ST1_1H_STAIRCASE_PURE      : 3 consecutive 1H Higher-Low & Higher-Close bars,
                                 anti-spike cap (max bar <= +6.5%), TBR >= 0.56, Vol >= 1.5x
  - ST2_1H_STAIRCASE_BTC_GATED : ST1 + BTC > EMA24(1H) & Market Breadth >= 45%
  - ST3_1H_PULLBACK_CONT       : Early staircase (+4%..+15% in 12h, no >8% spike)
                                 followed by first clean pullback to EMA12 + green TBR >= 0.56
  - ST4_4H_STAIRCASE_SWING     : 4H-resampled Staircase on same 80-coin universe (2 consecutive
                                 4H Higher-Low/Higher-Close bars, anti-spike <= +10%)

Evaluates BOTH 0.26% (USDT) and 0.50% (IRT) RT fees over 730 days (~17,520 1H bars),
plus Half-Year Regimes, Top/Bottom Coin Attribution, Leave-2-Out, and Last-48h Trades!
"""

import argparse
import concurrent.futures
import csv
import datetime as dt
import io
import math
import sys
import zipfile
from typing import Dict, List, Optional, Set, Tuple

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    requests = None
    HTTPAdapter = None

VERSION = "1.0"
UNIVERSE_80 = [
    # Large & Upper-Mid Caps
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT",
    "LINKUSDT", "SUIUSDT", "LTCUSDT", "NEARUSDT", "APTUSDT", "DOTUSDT", "TRXUSDT", "ATOMUSDT",
    "UNIUSDT", "AAVEUSDT", "ARBUSDT", "OPUSDT", "TAOUSDT", "FETUSDT", "ZECUSDT", "TONUSDT",
    "RENDERUSDT", "INJUSDT", "FILUSDT", "ETCUSDT", "XLMUSDT", "HBARUSDT", "ICPUSDT", "VETUSDT",
    "ALGOUSDT", "GRTUSDT", "SANDUSDT", "MANAUSDT", "AXSUSDT", "THETAUSDT", "RUNEUSDT", "FTMUSDT",
    # Mid-Caps & Today's Exact Screenshot Coins (including SSV, ZRO, LAYER, ME, MOVE, WIF, SEI)
    "SSVUSDT", "ZROUSDT", "LAYERUSDT", "VIRTUALUSDT", "SEIUSDT", "WIFUSDT", "MEUSDT", "MOVEUSDT",
    "TIAUSDT", "ENAUSDT", "STRKUSDT", "ONDOUSDT", "PENDLEUSDT", "JUPUSDT", "PYTHUSDT", "WLDUSDT",
    "EIGENUSDT", "ETHFIUSDT", "LDOUSDT", "CRVUSDT", "MKRUSDT", "COMPUSDT", "SNXUSDT", "SUSHIUSDT",
    "DYDXUSDT", "GMXUSDT", "ENSUSDT", "IMXUSDT", "GALAUSDT", "CHZUSDT", "FLOWUSDT", "ROSEUSDT",
    "ONEUSDT", "SFPUSDT", "BATUSDT", "TRBUSDT", "TWTUSDT", "METISUSDT", "CGPTUSDT", "CAKEUSDT",
]
BINANCE_VISION = "https://data.binance.vision/data"
BINANCE_REST_HOSTS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
]


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def fmt_ts(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M")


def _get_session():
    s = requests.Session()
    if HTTPAdapter is not None:
        ad = HTTPAdapter(pool_connections=40, pool_maxsize=40, max_retries=2)
        s.mount("https://", ad)
        s.mount("http://", ad)
    s.headers.update({"User-Agent": f"OMEGA-S7-Staircase/{VERSION}"})
    return s


def _month_list(start_dt: dt.datetime, end_dt: dt.datetime) -> List[Tuple[int, int]]:
    out = []
    y, m = start_dt.year, start_dt.month
    while (y, m) <= (end_dt.year, end_dt.month):
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def _fetch_zip_csv_rows(sess, url: str, timeout: int = 16) -> List[List[str]]:
    try:
        r = sess.get(url, timeout=timeout)
        if r.status_code != 200 or not r.content:
            return []
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                return list(csv.reader(io.StringIO(f.read().decode("utf-8", errors="ignore"))))
    except Exception:
        return []


# ==============================================================================
# 1. FAST PARALLEL 1H SPOT KLINE FETCHER (80 COINS x 2 YEARS IN ~2-3 MIN)
# ==============================================================================

def fetch_spot_1h_klines(sess, sym: str, start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, dict]:
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    bars: Dict[int, dict] = {}

    for y, m in _month_list(start_dt, end_dt):
        url = f"{BINANCE_VISION}/spot/monthly/klines/{sym}/1h/{sym}-1h-{y:04d}-{m:02d}.zip"
        for row in _fetch_zip_csv_rows(sess, url):
            if len(row) < 11 or not row[0]:
                continue
            try:
                ts = int(float(row[0]))
                if ts > 1e14:
                    ts //= 1000
                if ts < start_ms or ts > end_ms:
                    continue
                c = float(row[4])
                if c <= 0:
                    continue
                bars[ts] = {
                    "t": ts, "o": float(row[1]), "h": float(row[2]),
                    "l": float(row[3]), "c": c,
                    "qv": float(row[7]), "nt": max(1.0, float(row[8])),
                    "tbqv": float(row[10]),
                }
            except Exception:
                continue

    # Tail fill current incomplete month via REST (data-api.binance.vision = no US geo-block)
    last_ts = max(bars.keys()) + 3600000 if bars else start_ms
    if last_ts < end_ms - 3600000:
        for host in BINANCE_REST_HOSTS:
            try:
                cur = last_ts
                while cur < end_ms:
                    url = f"{host}/api/v3/klines?symbol={sym}&interval=1h&startTime={cur}&limit=1000"
                    r = sess.get(url, timeout=12)
                    if r.status_code != 200:
                        break
                    data = r.json()
                    if not isinstance(data, list) or not data:
                        break
                    for k in data:
                        ts = int(k[0])
                        if ts > end_ms:
                            continue
                        c = float(k[4])
                        if c <= 0:
                            continue
                        bars[ts] = {
                            "t": ts, "o": float(k[1]), "h": float(k[2]),
                            "l": float(k[3]), "c": c,
                            "qv": float(k[7]), "nt": max(1.0, float(k[8])),
                            "tbqv": float(k[10]),
                        }
                    nxt = int(data[-1][0]) + 3600000
                    if nxt <= cur or len(data) < 1000:
                        break
                    cur = nxt
                if bars:
                    break
            except Exception:
                continue
    return bars


# ==============================================================================
# 2. CAUSAL INDICATORS & 1H / 4H DATASET BUILDERS
# ==============================================================================

def calc_ema(arr: List[float], length: int) -> List[float]:
    out = [0.0] * len(arr)
    if not arr:
        return out
    k = 2.0 / (length + 1.0)
    e = arr[0]
    for i, v in enumerate(arr):
        e = v if i == 0 else v * k + e * (1.0 - k)
        out[i] = e
    return out


def calc_sma(arr: List[float], length: int) -> List[float]:
    out = [0.0] * len(arr)
    s = 0.0
    for i, v in enumerate(arr):
        s += v
        if i >= length:
            s -= arr[i - length]
        out[i] = s / min(i + 1, length)
    return out


def calc_sum(arr: List[float], length: int) -> List[float]:
    out = [0.0] * len(arr)
    s = 0.0
    for i, v in enumerate(arr):
        s += v
        if i >= length:
            s -= arr[i - length]
        out[i] = s
    return out


def calc_atr(h: List[float], l: List[float], c: List[float], length: int) -> List[float]:
    tr = [0.0] * len(c)
    for i in range(len(c)):
        if i == 0:
            tr[i] = h[i] - l[i]
        else:
            pc = c[i - 1]
            tr[i] = max(h[i] - l[i], abs(h[i] - pc), abs(l[i] - pc))
    return calc_sma(tr, length)


def build_1h_dataset(raw_k: Dict[str, Dict[int, dict]]) -> dict:
    btc_ts = sorted(raw_k["BTCUSDT"].keys())
    T = len(btc_ts)
    # Keep any symbol with at least 400 hours (~17 days) of PIT data (no survivorship bias!)
    syms = [s for s in raw_k.keys() if len(raw_k[s]) >= 400]
    series = {}

    for s in syms:
        km = raw_k[s]
        o, h, l, c, qv, tbr, avg_sz, valid = [], [], [], [], [], [], [], []
        last_c = None
        for ts in btc_ts:
            b = km.get(ts)
            if b is not None:
                o.append(b["o"]); h.append(b["h"]); l.append(b["l"]); c.append(b["c"])
                qv.append(b["qv"])
                rng = b["h"] - b["l"]
                clv = (b["c"] - b["l"]) / rng if rng > 0 else 0.5
                tbr.append(min(1.0, max(0.0, b["tbqv"] / b["qv"])) if b["qv"] > 0 else clv)
                avg_sz.append(b["qv"] / max(1.0, b["nt"]))
                last_c = b["c"]
                valid.append(True)
            else:
                px = last_c if last_c is not None else 0.0
                o.append(px); h.append(px); l.append(px); c.append(px)
                qv.append(0.0); tbr.append(0.5); avg_sz.append(0.0)
                valid.append(False)

        ema12 = calc_ema(c, 12)
        ema24 = calc_ema(c, 24)
        atr14 = calc_atr(h, l, c, 14)
        atr48 = calc_sma(atr14, 48)
        vol20 = calc_sma(qv, 20)
        qv24h = calc_sum(qv, 24)
        tbr3 = calc_sma(tbr, 3)
        avg_sz24 = calc_sma(avg_sz, 24)
        sz_ratio = [avg_sz[i] / max(1e-9, avg_sz24[i]) for i in range(T)]

        series[s] = {
            "o": o, "h": h, "l": l, "c": c, "qv": qv, "qv24h": qv24h,
            "tbr": tbr, "tbr3": tbr3, "sz_ratio": sz_ratio, "valid": valid,
            "ema12": ema12, "ema24": ema24, "atr14": atr14, "atr48": atr48, "vol20": vol20,
        }

    breadth = [0.5] * T
    for i in range(T):
        cnt, above = 0, 0
        for s in syms:
            if series[s]["valid"][i] and series[s]["qv24h"][i] >= 1e7:
                cnt += 1
                if series[s]["c"][i] > series[s]["ema24"][i]:
                    above += 1
        if cnt > 0:
            breadth[i] = above / cnt

    return {"ts": btc_ts, "T": T, "syms": syms, "series": series, "breadth": breadth}


def resample_to_4h(raw_k: Dict[str, Dict[int, dict]]) -> dict:
    """Resample 1H klines to strict 4H buckets (00, 04, 08, 12, 16, 20 UTC)."""
    raw_4h: Dict[str, Dict[int, dict]] = {}
    for sym, km in raw_k.items():
        buckets: Dict[int, List[dict]] = {}
        for ts, b in km.items():
            b_ts = (ts // 14400000) * 14400000
            buckets.setdefault(b_ts, []).append(b)
        out_m = {}
        for b_ts, arr in buckets.items():
            arr.sort(key=lambda x: x["t"])
            if len(arr) < 4:
                continue
            out_m[b_ts] = {
                "t": b_ts,
                "o": arr[0]["o"],
                "h": max(x["h"] for x in arr),
                "l": min(x["l"] for x in arr),
                "c": arr[-1]["c"],
                "qv": sum(x["qv"] for x in arr),
                "nt": sum(x["nt"] for x in arr),
                "tbqv": sum(x["tbqv"] for x in arr),
            }
        raw_4h[sym] = out_m

    btc_ts = sorted(raw_4h["BTCUSDT"].keys())
    T = len(btc_ts)
    syms = [s for s in raw_4h.keys() if len(raw_4h[s]) >= 100]
    series = {}

    for s in syms:
        km = raw_4h[s]
        o, h, l, c, qv, tbr, valid = [], [], [], [], [], [], []
        last_c = None
        for ts in btc_ts:
            b = km.get(ts)
            if b is not None:
                o.append(b["o"]); h.append(b["h"]); l.append(b["l"]); c.append(b["c"])
                qv.append(b["qv"])
                rng = b["h"] - b["l"]
                clv = (b["c"] - b["l"]) / rng if rng > 0 else 0.5
                tbr.append(min(1.0, max(0.0, b["tbqv"] / b["qv"])) if b["qv"] > 0 else clv)
                last_c = b["c"]
                valid.append(True)
            else:
                px = last_c if last_c is not None else 0.0
                o.append(px); h.append(px); l.append(px); c.append(px)
                qv.append(0.0); tbr.append(0.5)
                valid.append(False)

        ema6 = calc_ema(c, 6)
        ema12 = calc_ema(c, 12)
        atr14 = calc_atr(h, l, c, 14)
        atr42 = calc_sma(atr14, 42)
        vol20 = calc_sma(qv, 20)
        qv24h = calc_sum(qv, 6)  # 6 x 4H = 24h quote volume

        series[s] = {
            "o": o, "h": h, "l": l, "c": c, "qv": qv, "qv24h": qv24h, "tbr": tbr, "valid": valid,
            "ema6": ema6, "ema12": ema12, "atr14": atr14, "atr42": atr42, "vol20": vol20,
        }
    return {"ts": btc_ts, "T": T, "syms": syms, "series": series}


# ==============================================================================
# 3. SIMULATOR (NEXT-OPEN, STOP-FIRST GAP-PROTECTED, ATR FLOOR, Q_BTC)
# ==============================================================================

def simulate_staircase(
    ds: dict,
    fee_rt: float,
    max_slots: int,
    signal_fn,
    cfg: dict,
    exclude_syms: Optional[Set[str]] = None,
) -> dict:
    T = ds["T"]
    ts_arr = ds["ts"]
    ser = ds["series"]
    btc = ser["BTCUSDT"]
    excl = exclude_syms or set()
    active_syms = [s for s in ds["syms"] if s not in excl]

    eq = [1.0] * T
    cash = 1.0
    open_pos = []
    trades = []
    start_idx = cfg["start_idx"]
    btc_ema_key = cfg["btc_ema_key"]
    atr_long_key = cfg["atr_long_key"]

    for i in range(start_idx, T):
        # 1) Manage open positions
        for p in range(len(open_pos) - 1, -1, -1):
            pos = open_pos[p]
            d = ser[pos["sym"]]
            if not d["valid"][i]:
                continue

            exit_px, reason = None, None
            if d["o"][i] <= pos["stop"]:
                exit_px = d["o"][i]
                reason = "TRAIL_GAP" if pos["be"] else "STOP_GAP"
            elif d["l"][i] <= pos["stop"]:
                exit_px = pos["stop"]
                reason = "TRAIL" if pos["be"] else "STOP"
            elif i - pos["entry_i"] >= cfg["max_hold"]:
                exit_px = d["c"][i]
                reason = "TIME"
            elif i == T - 1:
                exit_px = d["c"][i]
                reason = "END_CLOSE"

            if exit_px is None and cfg.get("q_btc", True):
                if i - pos["entry_i"] >= 1 and btc["c"][i] < btc[btc_ema_key][i] and btc["tbr"][i] < 0.44:
                    exit_px, reason = d["c"][i], "Q_BTC"

            if exit_px is not None:
                gross = (exit_px - pos["entry_px"]) / pos["entry_px"]
                net = (1.0 + gross) * (1.0 - fee_rt) - 1.0
                cash += pos["weight"] * (1.0 + net)
                trades.append({
                    "sym": pos["sym"].replace("USDT", ""),
                    "entry_i": pos["entry_i"], "exit_i": i,
                    "entry_ts": ts_arr[pos["entry_i"]], "exit_ts": ts_arr[i],
                    "net": net, "reason": reason,
                })
                open_pos.pop(p)
            else:
                if d["h"][i] > pos["hh"]:
                    pos["hh"] = d["h"][i]
                gain = (pos["hh"] - pos["entry_px"]) / pos["entry_px"]
                if not pos["be"] and gain >= cfg["be_trig"]:
                    pos["be"] = True
                    pos["stop"] = max(pos["stop"], pos["entry_px"] * (1.0 + fee_rt + 0.002))
                if pos["be"]:
                    eff_atr = max(d["atr14"][i], 0.70 * d[atr_long_key][i])
                    trail_px = pos["hh"] - cfg["trail_atr"] * eff_atr
                    if trail_px > pos["stop"]:
                        pos["stop"] = trail_px

        # 2) MTM equity
        mtm = cash
        for pos in open_pos:
            d = ser[pos["sym"]]
            px = d["c"][i] if d["valid"][i] else pos["entry_px"]
            mtm += pos["weight"] * (px / pos["entry_px"])
        eq[i] = mtm

        # 3) Entries at bar i close -> fill at bar i+1 OPEN
        if i < T - 2 and len(open_pos) < max_slots:
            held = {x["sym"] for x in open_pos}
            cands = []
            for s in active_syms:
                if s in held:
                    continue
                d = ser[s]
                if not d["valid"][i] or not d["valid"][i + 1]:
                    continue
                # PIT liquidity gate: >= $15M Binance 24h quote volume (eliminates thin books like WIF/SEI on Nobitex)
                if d["qv24h"][i] < 1.5e7:
                    continue
                sc = signal_fn(s, d, i, ds)
                if sc is not None and math.isfinite(sc):
                    cands.append((sc, s))
            cands.sort(key=lambda x: x[0], reverse=True)
            for _, s in cands[: max_slots - len(open_pos)]:
                d = ser[s]
                entry_px = d["o"][i + 1]
                if entry_px <= 0:
                    continue
                eff_atr = max(d["atr14"][i], 0.70 * d[atr_long_key][i])
                if eff_atr <= 0:
                    eff_atr = entry_px * 0.02
                stop_px = entry_px - cfg["stop_atr"] * eff_atr
                if cfg.get("use_prev_low", False):
                    stop_px = min(stop_px, d["l"][i - 1] * 0.996)
                stop_px = max(stop_px, entry_px * 0.93)  # -7% hard cap
                alloc = min(cash, mtm / max_slots)
                if alloc <= 1e-6:
                    break
                cash -= alloc
                open_pos.append({
                    "sym": s, "entry_i": i + 1, "entry_px": entry_px,
                    "stop": stop_px, "be": False, "hh": entry_px, "weight": alloc,
                })

    return {"eq": eq, "trades": trades}


# ==============================================================================
# 4. FOUR ANTI-SPIKE STAIRCASE SIGNALS
# ==============================================================================

def _staircase_core_1h(s, d, i) -> Optional[float]:
    if not (d["valid"][i - 1] and d["valid"][i - 2] and d["valid"][i - 6]):
        return None
    # 3 consecutive Higher Lows & Higher Closes
    if not (d["l"][i] > d["l"][i - 1] > d["l"][i - 2]):
        return None
    if not (d["c"][i] > d["c"][i - 1] > d["c"][i - 2]):
        return None
    greens = sum(1 for k in (i, i - 1, i - 2) if d["c"][k] > d["o"][k])
    if greens < 2 or d["c"][i] <= d["o"][i]:
        return None
    # Anti-Vertical-Spike Filter (kills ME / MOVE 1-candle blowoffs!)
    r0 = (d["c"][i] - d["o"][i]) / d["o"][i] if d["o"][i] > 0 else 0.0
    r1 = (d["c"][i - 1] - d["o"][i - 1]) / d["o"][i - 1] if d["o"][i - 1] > 0 else 0.0
    r2 = (d["c"][i - 2] - d["o"][i - 2]) / d["o"][i - 2] if d["o"][i - 2] > 0 else 0.0
    if max(r0, r1, r2) > 0.065:
        return None
    # Early-to-mid staircase: 6h move between +2.5% and +12.0%
    r6 = (d["c"][i] - d["c"][i - 6]) / d["c"][i - 6] if d["c"][i - 6] > 0 else 0.0
    if r6 < 0.025 or r6 > 0.12:
        return None
    if d["c"][i] <= d["ema24"][i]:
        return None
    if d["tbr"][i] < 0.56 or d["tbr3"][i] < 0.54 or d["qv"][i] < 1.5 * d["vol20"][i] or d["sz_ratio"][i] < 1.10:
        return None
    return d["tbr"][i] + r6 * 2.0 + (d["sz_ratio"][i] - 1.0) * 0.3


def sig_st1_pure(s, d, i, ds):
    return _staircase_core_1h(s, d, i)


def sig_st2_btc_gated(s, d, i, ds):
    btc = ds["series"]["BTCUSDT"]
    if btc["c"][i] <= btc["ema24"][i] or ds["breadth"][i] < 0.45:
        return None
    return _staircase_core_1h(s, d, i)


def sig_st3_pullback_cont(s, d, i, ds):
    """First clean pullback to EMA12 after a non-spiky 12h staircase (+4%..+15%)."""
    if not d["valid"][i - 12]:
        return None
    r12 = (d["c"][i - 1] - d["c"][i - 12]) / d["c"][i - 12] if d["c"][i - 12] > 0 else 0.0
    if r12 < 0.04 or r12 > 0.15:
        return None
    # Reject if any single bar in last 12h was a vertical spike > +8%
    for k in range(i - 11, i):
        if d["o"][k] > 0 and (d["c"][k] - d["o"][k]) / d["o"][k] > 0.08:
            return None
    if d["c"][i] <= d["ema24"][i]:
        return None
    # Pullback touched EMA12 and closed green above EMA12 with strong taker buy
    if d["l"][i] > d["ema12"][i] * 1.005 or d["c"][i] <= d["ema12"][i] or d["c"][i] <= d["o"][i]:
        return None
    if d["tbr"][i] < 0.56 or d["qv"][i] < 1.2 * d["vol20"][i]:
        return None
    return d["tbr"][i] + r12


def sig_st4_4h_staircase(s, d, i, ds):
    """4H Staircase: 2 consecutive Higher-Low & Higher-Close green 4H bars, anti-spike <= +10%."""
    if not (d["valid"][i - 1] and d["valid"][i - 3]):
        return None
    if not (d["l"][i] > d["l"][i - 1] and d["c"][i] > d["c"][i - 1]):
        return None
    if d["c"][i] <= d["o"][i] or d["c"][i - 1] <= d["o"][i - 1]:
        return None
    r0 = (d["c"][i] - d["o"][i]) / d["o"][i] if d["o"][i] > 0 else 0.0
    r1 = (d["c"][i - 1] - d["o"][i - 1]) / d["o"][i - 1] if d["o"][i - 1] > 0 else 0.0
    if max(r0, r1) > 0.10:
        return None
    r3 = (d["c"][i] - d["c"][i - 3]) / d["c"][i - 3] if d["c"][i - 3] > 0 else 0.0
    if r3 < 0.04 or r3 > 0.18:
        return None
    if d["c"][i] <= d["ema12"][i] or d["tbr"][i] < 0.56 or d["qv"][i] < 1.5 * d["vol20"][i]:
        return None
    return d["tbr"][i] + r3 * 2.0


# ==============================================================================
# 5. METRICS & RUNNER
# ==============================================================================

def summarize(label: str, res: dict, ds: dict, start_idx: int, bars_per_year: float) -> dict:
    tr, eq = res["trades"], res["eq"]
    n = len(tr)
    split_idx = int(start_idx + (ds["T"] - start_idx) * 0.70)
    days = max(1.0, (ds["ts"][-1] - ds["ts"][start_idx]) / 86400000.0)
    per_mo = (n / days) * 30.0
    wins = [x["net"] for x in tr if x["net"] > 0]
    losses = [abs(x["net"]) for x in tr if x["net"] <= 0]
    gp, gl = sum(wins), sum(losses)
    wr = (len(wins) / n * 100.0) if n > 0 else 0.0
    pf = (gp / gl) if gl > 0 else (99.0 if gp > 0 else 0.0)
    wl = ((gp / len(wins)) / (gl / len(losses))) if (wins and losses) else 0.0

    srt = sorted([x["net"] for x in tr], reverse=True)[5:]
    egp, egl = sum(v for v in srt if v > 0), sum(abs(v) for v in srt if v <= 0)
    ex5 = (egp / egl) if egl > 0 else (99.0 if egp > 0 else 0.0)

    oos = [x["net"] for x in tr if x["entry_i"] >= split_idx]
    ogp, ogl = sum(v for v in oos if v > 0), sum(abs(v) for v in oos if v <= 0)
    oos_pf = (ogp / ogl) if ogl > 0 else (99.0 if ogp > 0 else 0.0)

    peak, max_dd, rets = eq[start_idx], 0.0, []
    for k in range(start_idx + 1, len(eq)):
        if eq[k] > peak:
            peak = eq[k]
        dd = (eq[k] - peak) / peak
        if dd < max_dd:
            max_dd = dd
        if eq[k - 1] > 0:
            rets.append((eq[k] - eq[k - 1]) / eq[k - 1])
    mr = sum(rets) / max(1, len(rets))
    vr = sum((r - mr) ** 2 for r in rets) / max(1, len(rets))
    sh = (mr / math.sqrt(vr)) * math.sqrt(bars_per_year) if vr > 0 else 0.0
    ret = (eq[-1] - 1.0) * 100.0

    verdict = "NO-GO"
    if n >= 50 and pf >= 1.30 and oos_pf >= 1.25 and ex5 >= 1.15 and max_dd > -0.18 and ret > 0:
        verdict = "GO"
    elif n >= 30 and pf >= 1.15 and oos_pf >= 1.10 and ret > 0:
        verdict = "WATCH"

    return {
        "label": label, "verdict": verdict, "n": n, "per_mo": per_mo, "wr": wr,
        "pf": pf, "ex5": ex5, "oos_pf": oos_pf, "oos_n": len(oos),
        "ret": ret, "dd": max_dd * 100.0, "sh": sh, "wl": wl,
    }


def fmt_row(s: dict) -> str:
    return (
        f"{s['label']:26s} [{s['verdict']:5s}] N={s['n']:4d} ({s['per_mo']:4.0f}/m) "
        f"WR={s['wr']:4.1f}% PF={s['pf']:.2f} Ex5={s['ex5']:.2f} "
        f"OOS={s['oos_pf']:.2f}({s['oos_n']:3d}) Ret={s['ret']:+6.1f}% "
        f"DD={s['dd']:6.1f}% Sh={s['sh']:+5.2f} W/L={s['wl']:.2f}x"
    )


def run_suite(ds1h: dict, ds4h: dict, fee_rt: float, exclude_syms: Optional[Set[str]] = None):
    cfg1h = {"start_idx": 60, "btc_ema_key": "ema12", "atr_long_key": "atr48", "stop_atr": 2.2, "be_trig": 0.025, "trail_atr": 2.4, "max_hold": 48, "q_btc": True, "use_prev_low": True}
    cfg4h = {"start_idx": 45, "btc_ema_key": "ema6",  "atr_long_key": "atr42", "stop_atr": 2.2, "be_trig": 0.030, "trail_atr": 2.4, "max_hold": 18, "q_btc": True, "use_prev_low": False}
    return {
        "ST1_1H_STAIRCASE_PURE":      (simulate_staircase(ds1h, fee_rt, 3, sig_st1_pure, cfg1h, exclude_syms), ds1h, 60, 8760.0),
        "ST2_1H_STAIRCASE_BTC_GATED": (simulate_staircase(ds1h, fee_rt, 3, sig_st2_btc_gated, cfg1h, exclude_syms), ds1h, 60, 8760.0),
        "ST3_1H_PULLBACK_CONT":       (simulate_staircase(ds1h, fee_rt, 3, sig_st3_pullback_cont, {**cfg1h, "stop_atr": 2.0, "use_prev_low": False}, exclude_syms), ds1h, 60, 8760.0),
        "ST4_4H_STAIRCASE_SWING":     (simulate_staircase(ds4h, fee_rt, 3, sig_st4_4h_staircase, cfg4h, exclude_syms), ds4h, 45, 2190.0),
    }


def run_self_test() -> bool:
    T = 460
    t0 = int(dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)
    raw_k = {}
    for s_idx, sym in enumerate(["BTCUSDT", "SSVUSDT", "MEUSDT"]):
        km = {}
        px = 100.0 * (s_idx + 1)
        for i in range(T):
            ts = t0 + i * 3600000
            # SSVUSDT has clean staircase bars (+1.5% each); MEUSDT has vertical +12% spike trap!
            if sym == "MEUSDT" and i == 150:
                chg = 0.12
            elif sym == "SSVUSDT" and 140 <= i <= 165:
                chg = 0.018
            else:
                chg = math.sin((i + s_idx) * 0.12) * 0.01
            o = px
            c = max(5.0, o * (1.0 + chg))
            h, l = max(o, c) * 1.004, min(o, c) * 0.996
            qv = 2e6 * (2.2 if 140 <= i <= 165 else 1.0)
            km[ts] = {"t": ts, "o": o, "h": h, "l": l, "c": c, "qv": qv, "nt": 600.0, "tbqv": qv * (0.63 if c > o else 0.37)}
            px = c
        raw_k[sym] = km

    ds1h = build_1h_dataset(raw_k)
    ds4h = resample_to_4h(raw_k)
    checks = []
    # 1) Datasets built cleanly
    checks.append(ds1h["T"] == T and len(ds1h["syms"]) == 3 and ds4h["T"] >= 100)
    # 2) Anti-spike filter MUST reject MEUSDT's +12% vertical candle at i=150
    checks.append(sig_st1_pure("MEUSDT", ds1h["series"]["MEUSDT"], 150, ds1h) is None)
    # 3) All 4 engines run cleanly with finite metrics
    suite = run_suite(ds1h, ds4h, 0.0026)
    checks.append(len(suite) == 4 and all(math.isfinite(summarize(k, v[0], v[1], v[2], v[3])["pf"]) for k, v in suite.items()))
    # 4) Gap-down + Next-Open check
    gap_k = {"BTCUSDT": {}}
    for i in range(420):
        ts = t0 + i * 3600000
        o, h, l, c = (82.0, 83.0, 81.0, 82.0) if i == 102 else (100.0, 101.0, 99.0, 100.5)
        gap_k["BTCUSDT"][ts] = {"t": ts, "o": o, "h": h, "l": l, "c": c, "qv": 2e6, "nt": 500.0, "tbqv": 1.2e6}
    gds = build_1h_dataset(gap_k)
    gres = simulate_staircase(gds, 0.0026, 1, lambda s, d, i, _ds: 1.0 if i == 100 else None,
                              {"start_idx": 60, "btc_ema_key": "ema12", "atr_long_key": "atr48", "stop_atr": 2.0, "be_trig": 0.03, "trail_atr": 2.0, "max_hold": 10, "q_btc": False})
    checks.append(len(gres["trades"]) == 1 and gres["trades"][0]["entry_i"] == 101 and gres["trades"][0]["reason"] == "STOP_GAP")
    # 5) Zero-trade safety
    zres = simulate_staircase(gds, 0.0026, 1, lambda s, d, i, _ds: None,
                              {"start_idx": 60, "btc_ema_key": "ema12", "atr_long_key": "atr48", "stop_atr": 2.0, "be_trig": 0.03, "trail_atr": 2.0, "max_hold": 10, "q_btc": False})
    zst = summarize("ZERO", zres, gds, 60, 8760.0)
    checks.append(zst["n"] == 0 and zst["pf"] == 0.0 and zst["verdict"] == "NO-GO")

    passed = sum(1 for c in checks if c)
    print(f"STAIRCASE_SELF_TEST: {passed}/{len(checks)} PASS")
    return passed == len(checks)


def main():
    ap = argparse.ArgumentParser(description="OMEGA S7 Mid-Cap Anti-Spike Staircase Lab v1.0")
    ap.add_argument("--days", type=int, default=730)
    ap.add_argument("--out", type=str, default="OMEGA_S7_MIDCAP_STAIRCASE_REPORT.txt")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        sys.exit(0 if run_self_test() else 1)

    if requests is None:
        print("ERROR: requests required")
        sys.exit(1)

    end_dt = utc_now().replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=1)
    start_dt = end_dt - dt.timedelta(days=args.days)
    print(f"[OMEGA STAIRCASE v{VERSION}] Fetching {args.days}d 1H spot data for {len(UNIVERSE_80)} coins...")

    sess = _get_session()
    raw_k: Dict[str, Dict[int, dict]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(fetch_spot_1h_klines, sess, sym, start_dt, end_dt): sym for sym in UNIVERSE_80}
        done = 0
        for fut in concurrent.futures.as_completed(futs):
            sym = futs[fut]
            done += 1
            try:
                bars = fut.result()
                if len(bars) >= 400:
                    raw_k[sym] = bars
            except Exception:
                pass
            if done % 16 == 0 or done == len(UNIVERSE_80):
                print(f"  Fetched {done}/{len(UNIVERSE_80)} coins (valid={len(raw_k)})")

    if "BTCUSDT" not in raw_k or len(raw_k["BTCUSDT"]) < 500:
        print("ERROR: Insufficient BTCUSDT 1H bars.")
        sys.exit(1)

    ds1h = build_1h_dataset(raw_k)
    ds4h = resample_to_4h(raw_k)
    btc_c = ds1h["series"]["BTCUSDT"]["c"]
    btc_ret = (btc_c[-1] / btc_c[60] - 1.0) * 100.0

    lines = [
        "=" * 92,
        f"OMEGA S7 MID-CAP ANTI-SPIKE STAIRCASE LAB v{VERSION} ({utc_now():%Y-%m-%d})",
        f"Universe: {len(ds1h['syms'])}/{len(UNIVERSE_80)} coins (PIT 24hVol >= $15M) | Bars: {ds1h['T']} (1H) / {ds4h['T']} (4H)",
        f"Span: {fmt_ts(ds1h['ts'][60])} .. {fmt_ts(ds1h['ts'][-1])} | BTC Buy&Hold: {btc_ret:+.1f}%",
        "Execution: Strict Next-Open, Stop-First (Gap-Protected), 70% ATR Floor, 70/30 IS/OOS",
        "=" * 92,
        "",
        "--- TABLE 1: STAIRCASE ENGINES AT 0.26% RT (NOBITEX USDT) ---",
    ]

    suite_026 = run_suite(ds1h, ds4h, 0.0026)
    stats_026 = [summarize(k, v[0], v[1], v[2], v[3]) for k, v in suite_026.items()]
    stats_026.sort(key=lambda x: x["pf"], reverse=True)
    for s in stats_026:
        lines.append(fmt_row(s))

    lines.extend(["", "--- TABLE 2: STAIRCASE ENGINES AT 0.50% RT (NOBITEX IRT) ---"])
    suite_050 = run_suite(ds1h, ds4h, 0.0050)
    stats_050 = [summarize(k, v[0], v[1], v[2], v[3]) for k, v in suite_050.items()]
    stats_050.sort(key=lambda x: x["pf"], reverse=True)
    for s in stats_050:
        lines.append(fmt_row(s))

    # Deep Dive on #1 Engine
    best_name = stats_026[0]["label"]
    best_res, best_ds, best_sidx, best_bpy = suite_026[best_name]
    lines.extend(["", f"--- DEEP DIVE ON #1 ENGINE: {best_name} (0.26% RT) ---"])
    by_half: Dict[str, List[float]] = {}
    for x in best_res["trades"]:
        d = dt.datetime.fromtimestamp(x["entry_ts"] / 1000.0, tz=dt.timezone.utc)
        key = f"{d.year}-H{1 if d.month <= 6 else 2}"
        by_half.setdefault(key, []).append(x["net"])
    for key in sorted(by_half.keys()):
        arr = by_half[key]
        gp, gl = sum(v for v in arr if v > 0), sum(abs(v) for v in arr if v <= 0)
        pf = gp / gl if gl > 0 else 99.0
        wr = sum(1 for v in arr if v > 0) / len(arr) * 100.0
        lines.append(f"  {key}: N={len(arr):3d} WR={wr:4.1f}% PF={pf:.2f} SumNet={sum(arr)*100:+6.1f}%")

    by_coin: Dict[str, List[float]] = {}
    for x in best_res["trades"]:
        by_coin.setdefault(x["sym"], []).append(x["net"])
    coin_ranked = sorted(by_coin.items(), key=lambda kv: sum(kv[1]), reverse=True)
    lines.append("")
    lines.append("  TOP 10 COINS:")
    for sym, arr in coin_ranked[:10]:
        gp, gl = sum(v for v in arr if v > 0), sum(abs(v) for v in arr if v <= 0)
        pf = gp / gl if gl > 0 else 99.0
        lines.append(f"    {sym:8s}: N={len(arr):3d} PF={pf:.2f} SumNet={sum(arr)*100:+6.1f}%")
    lines.append("  BOTTOM 5 COINS:")
    for sym, arr in coin_ranked[-5:]:
        gp, gl = sum(v for v in arr if v > 0), sum(abs(v) for v in arr if v <= 0)
        pf = gp / gl if gl > 0 else 99.0
        lines.append(f"    {sym:8s}: N={len(arr):3d} PF={pf:.2f} SumNet={sum(arr)*100:+6.1f}%")

    top2 = {f"{coin_ranked[i][0]}USDT" for i in range(min(2, len(coin_ranked)))}
    l2_suite = run_suite(ds1h, ds4h, 0.0026, exclude_syms=top2)
    l2_r, l2_ds, l2_sidx, l2_bpy = l2_suite[best_name]
    lines.append("")
    lines.append(f"  Dropped Top-2 Coins: {sorted(top2)}")
    lines.append("  " + fmt_row(summarize(f"{best_name}_L2O", l2_r, l2_ds, l2_sidx, l2_bpy)))

    # Last 48h Trades Across ALL 4 Engines (Did we catch SSV & dodge ME/MOVE?)
    cut48 = ds1h["ts"][-1] - 48 * 3600000
    recent = []
    for ename, (eres, _, _, _) in suite_026.items():
        for x in eres["trades"]:
            if x["entry_ts"] >= cut48:
                recent.append((ename, x))
    recent.sort(key=lambda kv: kv[1]["entry_ts"], reverse=True)
    lines.extend(["", f"--- LAST 48H TRADES ACROSS ALL 4 ENGINES ({len(recent)}) ---"])
    for ename, x in recent[:30]:
        lines.append(
            f"  {ename:26s} {x['sym']:8s} {fmt_ts(x['entry_ts'])} -> {fmt_ts(x['exit_ts'])} "
            f"{x['reason']:10s} Net={x['net']*100:+6.2f}%"
        )

    report = "\n".join(lines)
    print("\n" + report)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    print(f"\nSaved staircase report to {args.out}")


if __name__ == "__main__":
    main()
