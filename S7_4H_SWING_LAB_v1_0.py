#!/usr/bin/env python3
"""
OMEGA S7 4H SWING & VOLCANO-IGNITION LAB v1.0 (2-YEAR AUDIT)
============================================================
Builds on two empirical discoveries:
  1. 1H Torture Audit (17,521 bars): slowing lookback (36h->48h->60h) monotonically
     improved PF/OOS/MaxDD; Q_BTC and OI_EXHAUST added edge while Q_STALL/Q_FLOW hurt.
  2. Volcano Black-Box Forensic (4 days, 17,688 rows): pre-explosion signature is
     aggressive Taker Buy Ratio (0.657 vs 0.495), positive Net Buy Flow, and
     negative/low Funding Rate — with payoffs unfolding over 4H (+7.5% to +12.25%).

Tests 4 orthogonal 4H spot-only engines over 730 days (~4,380 4H bars) across
20 deep Nobitex USDT pairs at BOTH 0.26% (USDT) and 0.50% (IRT) RT fees, plus
Half-Year Regimes, Per-Coin Attribution, and Leave-2-Out for the top engine.

Execution Rigor:
  - 100% Spot-Only, Strict Next-Open, Stop-First (Gap-Protected), 70% ATR Floor
  - Causal Chandelier Trailing Stop, 70/30 Chronological IS/OOS Split
  - 100% Free Public Archives (data.binance.vision + Deribit DVOL), zero API keys.
"""

import argparse
import concurrent.futures
import csv
import datetime as dt
import io
import math
import sys
import time
import zipfile
from typing import Dict, List, Optional, Set, Tuple

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    requests = None
    HTTPAdapter = None

VERSION = "1.0"
UNIVERSE_20 = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT",
    "LTCUSDT", "NEARUSDT", "TAOUSDT", "ZECUSDT", "AAVEUSDT",
    "TIAUSDT", "ENAUSDT", "STRKUSDT", "TONUSDT", "RENDERUSDT",
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
        ad = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=2)
        s.mount("https://", ad)
        s.mount("http://", ad)
    s.headers.update({"User-Agent": f"OMEGA-S7-4H-Lab/{VERSION}"})
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


def _day_list(start_dt: dt.datetime, end_dt: dt.datetime) -> List[dt.date]:
    out = []
    cur, end = start_dt.date(), end_dt.date()
    while cur <= end:
        out.append(cur)
        cur += dt.timedelta(days=1)
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
# 1. FREE 4H HISTORICAL ARCHIVE FETCHERS
# ==============================================================================

def fetch_spot_4h_klines(sym: str, start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, dict]:
    sess = _get_session()
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    bars: Dict[int, dict] = {}

    for y, m in _month_list(start_dt, end_dt):
        url = f"{BINANCE_VISION}/spot/monthly/klines/{sym}/4h/{sym}-4h-{y:04d}-{m:02d}.zip"
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
    last_ts = max(bars.keys()) + 14400000 if bars else start_ms
    if last_ts < end_ms - 14400000:
        for host in BINANCE_REST_HOSTS:
            try:
                cur = last_ts
                while cur < end_ms:
                    url = f"{host}/api/v3/klines?symbol={sym}&interval=4h&startTime={cur}&limit=1000"
                    r = sess.get(url, timeout=15)
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
                    nxt = int(data[-1][0]) + 14400000
                    if nxt <= cur or len(data) < 1000:
                        break
                    cur = nxt
                if bars:
                    break
            except Exception:
                continue
    return bars


def fetch_futures_oi_4h(sym: str, start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, float]:
    """Fetch 5m futures metrics daily ZIPs and bucket to 4H close OI (USD)."""
    sess = _get_session()
    out_4h: Dict[int, float] = {}
    days = _day_list(start_dt, end_dt)

    def _one_day(d: dt.date):
        url = f"{BINANCE_VISION}/futures/um/daily/metrics/{sym}/{sym}-metrics-{d:%Y-%m-%d}.zip"
        out = {}
        for row in _fetch_zip_csv_rows(sess, url, timeout=12):
            if len(row) < 4 or not row[0] or row[0] == "create_time":
                continue
            try:
                t_dt = dt.datetime.strptime(row[0][:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
                b_hr = (t_dt.hour // 4) * 4
                b_ms = int(t_dt.replace(hour=b_hr, minute=0, second=0).timestamp() * 1000)
                if row[3]:
                    out[b_ms] = float(row[3])
            except Exception:
                continue
        return out

    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as ex:
        for res in ex.map(_one_day, days):
            out_4h.update(res)
    return out_4h


def fetch_futures_funding_4h(sym: str, start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, float]:
    sess = _get_session()
    funding: Dict[int, float] = {}
    for y, m in _month_list(start_dt, end_dt):
        url = f"{BINANCE_VISION}/futures/um/monthly/fundingRate/{sym}/{sym}-fundingRate-{y:04d}-{m:02d}.zip"
        for row in _fetch_zip_csv_rows(sess, url):
            if len(row) < 3 or not row[0] or not row[0][0].isdigit():
                continue
            try:
                ts = int(float(row[0]))
                b_ms = (ts // 14400000) * 14400000
                funding[b_ms] = float(row[2])
            except Exception:
                continue
    return funding


def fetch_deribit_dvol_4h(start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, float]:
    sess = _get_session()
    out: Dict[int, float] = {}
    cur_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    chunk_ms = 900 * 3600 * 1000
    while cur_ms < end_ms:
        nxt_ms = min(end_ms, cur_ms + chunk_ms)
        url = (
            "https://www.deribit.com/api/v2/public/get_volatility_index_data"
            f"?currency=BTC&resolution=3600&start_timestamp={cur_ms}&end_timestamp={nxt_ms}"
        )
        try:
            r = sess.get(url, timeout=15)
            if r.status_code == 200:
                for row in r.json().get("result", {}).get("data", []):
                    if len(row) >= 5:
                        b_ms = (int(row[0]) // 14400000) * 14400000
                        out[b_ms] = float(row[4])
        except Exception:
            pass
        cur_ms = nxt_ms
        time.sleep(0.10)
    return out


# ==============================================================================
# 2. CAUSAL INDICATORS & DATASET ALIGNMENT
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


def calc_atr(h: List[float], l: List[float], c: List[float], length: int) -> List[float]:
    tr = [0.0] * len(c)
    for i in range(len(c)):
        if i == 0:
            tr[i] = h[i] - l[i]
        else:
            pc = c[i - 1]
            tr[i] = max(h[i] - l[i], abs(h[i] - pc), abs(l[i] - pc))
    return calc_sma(tr, length)


def calc_hh(h: List[float], length: int) -> List[float]:
    T = len(h)
    out = [0.0] * T
    for i in range(T):
        out[i] = max(h[max(0, i - length):i]) if i > 0 else h[0]
    return out


def build_dataset_4h(
    raw_k: Dict[str, Dict[int, dict]],
    raw_oi: Dict[str, Dict[int, float]],
    raw_fund: Dict[str, Dict[int, float]],
    dvol_map: Dict[int, float],
) -> dict:
    btc_ts = sorted(raw_k["BTCUSDT"].keys())
    T = len(btc_ts)
    syms = [s for s in raw_k.keys() if len(raw_k[s]) >= int(T * 0.65)]
    series = {}

    for s in syms:
        km = raw_k[s]
        om = raw_oi.get(s, {})
        fm = raw_fund.get(s, {})
        o, h, l, c, qv, tbr, clv, avg_sz, net_flow, oi, fund, valid = [], [], [], [], [], [], [], [], [], [], [], []
        last_c, last_oi, last_f = None, 0.0, 0.0
        for ts in btc_ts:
            b = km.get(ts)
            if b is not None:
                o.append(b["o"]); h.append(b["h"]); l.append(b["l"]); c.append(b["c"])
                qv.append(b["qv"])
                rng = b["h"] - b["l"]
                c_lv = (b["c"] - b["l"]) / rng if rng > 0 else 0.5
                clv.append(c_lv)
                r_tbr = min(1.0, max(0.0, b["tbqv"] / b["qv"])) if b["qv"] > 0 else c_lv
                tbr.append(r_tbr)
                avg_sz.append(b["qv"] / max(1.0, b["nt"]))
                net_flow.append((2.0 * r_tbr - 1.0) * b["qv"])
                last_c = b["c"]
                valid.append(True)
            else:
                px = last_c if last_c is not None else 0.0
                o.append(px); h.append(px); l.append(px); c.append(px)
                qv.append(0.0); tbr.append(0.5); clv.append(0.5)
                avg_sz.append(0.0); net_flow.append(0.0)
                valid.append(False)
            if ts in om:
                last_oi = om[ts]
            if ts in fm:
                last_f = fm[ts]
            oi.append(last_oi); fund.append(last_f)

        ema6 = calc_ema(c, 6)
        ema12 = calc_ema(c, 12)
        ema24 = calc_ema(c, 24)
        atr14 = calc_atr(h, l, c, 14)
        atr42 = calc_sma(atr14, 42)
        vol20 = calc_sma(qv, 20)
        avg_sz18 = calc_sma(avg_sz, 18)
        tbr3 = calc_sma(tbr, 3)
        sz_ratio = [avg_sz[i] / max(1e-9, avg_sz18[i]) for i in range(T)]

        # Rolling 12-bar (48h) return volatility
        vol12 = [0.02] * T
        for i in range(12, T):
            rs = [(c[j] - c[j - 1]) / c[j - 1] for j in range(i - 11, i + 1) if c[j - 1] > 0]
            if rs:
                m = sum(rs) / len(rs)
                vol12[i] = math.sqrt(max(1e-10, sum((x - m) ** 2 for x in rs) / len(rs)))

        series[s] = {
            "o": o, "h": h, "l": l, "c": c, "qv": qv, "tbr": tbr, "tbr3": tbr3, "clv": clv,
            "net_flow": net_flow, "valid": valid, "ema6": ema6, "ema12": ema12, "ema24": ema24,
            "atr14": atr14, "atr42": atr42, "vol20": vol20, "vol12": vol12, "oi": oi, "fund": fund,
            "sz_ratio": sz_ratio, "hh15": calc_hh(h, 15),
        }

    dvol = [50.0] * T
    last_dv = 50.0
    for i, ts in enumerate(btc_ts):
        if ts in dvol_map:
            last_dv = dvol_map[ts]
        dvol[i] = last_dv

    breadth_ema = [0.5] * T
    for i in range(T):
        cnt, above = 0, 0
        for s in syms:
            if series[s]["valid"][i]:
                cnt += 1
                if series[s]["c"][i] > series[s]["ema12"][i]:
                    above += 1
        if cnt > 0:
            breadth_ema[i] = above / cnt

    return {"ts": btc_ts, "T": T, "syms": syms, "series": series, "dvol": dvol, "breadth_ema": breadth_ema}


# ==============================================================================
# 3. 4H PORTFOLIO SIMULATORS (STRICT NEXT-OPEN, STOP-FIRST GAP-PROTECTED)
# ==============================================================================

def run_4h_event_engine(
    ds: dict,
    fee_rt: float,
    max_slots: int,
    signal_fn,
    exit_cfg: dict,
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
    start_idx = 50

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
            elif i - pos["entry_i"] >= exit_cfg["max_hold"]:
                exit_px = d["c"][i]
                reason = "TIME"
            elif i == T - 1:
                exit_px = d["c"][i]
                reason = "END_CLOSE"

            # Proven Q_BTC exit only (dropped losing Q_STALL/Q_FLOW from 1H ablation)
            if exit_px is None and exit_cfg.get("q_btc", False):
                if i - pos["entry_i"] >= 1 and btc["c"][i] < btc["ema6"][i] and btc["tbr"][i] < 0.44:
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
                if not pos["be"] and gain >= exit_cfg["be_trig"]:
                    pos["be"] = True
                    pos["stop"] = max(pos["stop"], pos["entry_px"] * (1.0 + fee_rt + 0.002))
                if pos["be"]:
                    eff_atr = max(d["atr14"][i], 0.70 * d["atr42"][i])
                    trail_mult = exit_cfg["trail_atr"]
                    if exit_cfg.get("oi_exhaust", False) and gain >= 0.03 and d["oi"][i - 2] > 0:
                        if (d["oi"][i] - d["oi"][i - 2]) / d["oi"][i - 2] < -0.02:
                            trail_mult = 1.4
                    trail_px = pos["hh"] - trail_mult * eff_atr
                    if trail_px > pos["stop"]:
                        pos["stop"] = trail_px

        # 2) MTM equity
        mtm = cash
        for pos in open_pos:
            d = ser[pos["sym"]]
            px = d["c"][i] if d["valid"][i] else pos["entry_px"]
            mtm += pos["weight"] * (px / pos["entry_px"])
        eq[i] = mtm

        # 3) Entries at bar i close -> filled strictly at bar i+1 OPEN
        if i < T - 2 and len(open_pos) < max_slots:
            held = {x["sym"] for x in open_pos}
            cands = []
            for s in active_syms:
                if s in held:
                    continue
                d = ser[s]
                if not d["valid"][i] or not d["valid"][i + 1]:
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
                eff_atr = max(d["atr14"][i], 0.70 * d["atr42"][i])
                if eff_atr <= 0:
                    eff_atr = entry_px * 0.025
                stop_px = entry_px - exit_cfg["stop_atr"] * eff_atr
                if exit_cfg.get("use_swing_low2", False):
                    stop_px = min(stop_px, min(d["l"][i], d["l"][i - 1]) * 0.996)
                stop_px = max(stop_px, entry_px * 0.92)  # -8% hard cap
                alloc = min(cash, mtm / max_slots)
                if alloc <= 1e-6:
                    break
                cash -= alloc
                open_pos.append({
                    "sym": s, "entry_i": i + 1, "entry_px": entry_px,
                    "stop": stop_px, "be": False, "hh": entry_px, "weight": alloc,
                })

    return {"eq": eq, "trades": trades}


def run_4h_rotation_h4(ds: dict, fee_rt: float, exclude_syms: Optional[Set[str]] = None) -> dict:
    """H4: 12-Hour (every 3 bars of 4H) Cross-Sectional Relative Strength + Taker Flow."""
    T = ds["T"]
    ts_arr = ds["ts"]
    ser = ds["series"]
    btc = ser["BTCUSDT"]
    excl = exclude_syms or set()
    active_syms = [s for s in ds["syms"] if s not in excl]

    eq = [1.0] * T
    cash = 1.0
    held: Dict[str, dict] = {}
    trades = []
    start_idx = 50

    for i in range(start_idx, T):
        if i == T - 1 and held:
            for s, pos in list(held.items()):
                d = ser[s]
                px = d["c"][i] if d["valid"][i] else pos["entry_px"]
                net = (px / pos["entry_px"]) * (1.0 - fee_rt) - 1.0
                cash += pos["weight"] * (1.0 + net)
                trades.append({"sym": s.replace("USDT", ""), "entry_i": pos["entry_i"], "exit_i": i, "entry_ts": pos["entry_ts"], "exit_ts": ts_arr[i], "net": net, "reason": "END_CLOSE"})
            held.clear()

        mtm = cash
        for s, pos in held.items():
            d = ser[s]
            px = d["c"][i] if d["valid"][i] else pos["entry_px"]
            mtm += pos["weight"] * (px / pos["entry_px"])
        eq[i] = mtm

        # Rebalance every 3 bars (12H)
        if (i % 3 == 0) and i < T - 2:
            btc_bull = btc["c"][i] > btc["ema12"][i] and ds["dvol"][i] <= 68.0
            ranked = []
            if btc_bull:
                for s in active_syms:
                    d = ser[s]
                    if not d["valid"][i] or not d["valid"][i + 1]:
                        continue
                    # Skip last 4H bar: return from i-12 (48h ago) to i-1
                    r_skip1 = (d["c"][i - 1] - d["c"][i - 12]) / d["c"][i - 12] if d["c"][i - 12] > 0 else 0.0
                    score = r_skip1 / max(0.01, d["vol12"][i])
                    if r_skip1 > 0.02 and d["tbr3"][i] >= 0.53 and d["c"][i] > d["ema12"][i]:
                        ranked.append((score, s))
                ranked.sort(key=lambda x: x[0], reverse=True)

            top5 = {s for _, s in ranked[:5]}
            top2 = [s for _, s in ranked[:2]]

            for s in list(held.keys()):
                if not btc_bull or s not in top5:
                    pos = held.pop(s)
                    d = ser[s]
                    exit_px = d["o"][i + 1] if d["valid"][i + 1] else pos["entry_px"]
                    net = (exit_px / pos["entry_px"]) * (1.0 - fee_rt) - 1.0
                    cash += pos["weight"] * (1.0 + net)
                    trades.append({
                        "sym": s.replace("USDT", ""), "entry_i": pos["entry_i"], "exit_i": i + 1,
                        "entry_ts": pos["entry_ts"], "exit_ts": ts_arr[i + 1],
                        "net": net, "reason": "BTC_REGIME" if not btc_bull else "RANK_EXIT",
                    })

            if btc_bull and len(held) < 2:
                cur_mtm = cash + sum(p["weight"] for p in held.values())
                for s in top2:
                    if len(held) >= 2:
                        break
                    if s in held:
                        continue
                    d = ser[s]
                    entry_px = d["o"][i + 1]
                    if entry_px <= 0:
                        continue
                    alloc = min(cash, cur_mtm / 2.0)
                    if alloc <= 1e-6:
                        break
                    cash -= alloc
                    held[s] = {"entry_i": i + 1, "entry_ts": ts_arr[i + 1], "entry_px": entry_px, "weight": alloc}

    return {"eq": eq, "trades": trades}


# ==============================================================================
# 4. SIGNAL FUNCTIONS FOR H1, H2, H3
# ==============================================================================

def sig_h1_oi_breakout(s, d, i, ds):
    """H1: 4H Evolution of F3 (60h Breakout + Fresh OI + Whale Size + Breadth + Calm DVOL)."""
    btc = ds["series"]["BTCUSDT"]
    if btc["c"][i] <= btc["ema12"][i] or ds["breadth_ema"][i] < 0.50 or ds["dvol"][i] > 65.0:
        return None
    if d["c"][i] <= d["hh15"][i] or d["hh15"][i] <= 0:
        return None
    if d["oi"][i - 2] > 0:
        if (d["oi"][i] - d["oi"][i - 2]) / d["oi"][i - 2] < 0.015:
            return None
    if d["tbr"][i] < 0.55 or d["sz_ratio"][i] < 1.15 or d["qv"][i] < 1.3 * d["vol20"][i]:
        return None
    return (d["c"][i] / d["hh15"][i] - 1.0) + d["sz_ratio"][i]


def sig_h2_volcano_ignition(s, d, i, ds):
    """H2: Volcano Black-Box 4H Ignition (Extreme TBR >= 0.60 + Net Buy Flow + Vol Surge + Low/Neg FR)."""
    btc = ds["series"]["BTCUSDT"]
    btc_r1 = (btc["c"][i] - btc["o"][i]) / btc["o"][i] if btc["o"][i] > 0 else 0.0
    if btc_r1 < -0.015 or ds["dvol"][i] > 68.0:
        return None
    flow3 = d["net_flow"][i] + d["net_flow"][i - 1] + d["net_flow"][i - 2]
    if d["tbr"][i] < 0.60 or flow3 <= 0 or d["c"][i] <= d["o"][i]:
        return None
    if d["qv"][i] < 1.6 * d["vol20"][i]:
        return None
    if d["fund"][i] > 0.00015:  # Avoid overcrowded positive funding
        return None
    return d["tbr"][i] + (d["qv"][i] / max(1.0, d["vol20"][i])) * 0.2


def sig_h3_mid_dip_snap(s, d, i, ds):
    """H3: Moderate Dip Snap-Back (-5% to -11% 24h dip + 4H Taker Absorption + BTC Green Bar)."""
    btc = ds["series"]["BTCUSDT"]
    if btc["c"][i] <= btc["o"][i]:  # BTC 4H candle must be green (bounce confirmed)
        return None
    r6 = (d["l"][i - 1] - d["c"][i - 6]) / d["c"][i - 6] if d["c"][i - 6] > 0 else 0.0
    if r6 > -0.05 or r6 < -0.14:
        return None
    if d["c"][i] <= d["o"][i] or d["clv"][i] < 0.58 or d["tbr"][i] < 0.58:
        return None
    if d["qv"][i] < 1.2 * d["vol20"][i]:
        return None
    return d["tbr"][i] - r6


# ==============================================================================
# 5. METRICS & RUNNER
# ==============================================================================

def summarize(label: str, res: dict, ds: dict) -> dict:
    tr, eq = res["trades"], res["eq"]
    n = len(tr)
    split_idx = int(50 + (ds["T"] - 50) * 0.70)
    days = max(1.0, (ds["ts"][-1] - ds["ts"][50]) / 86400000.0)
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

    peak, max_dd, rets = eq[50], 0.0, []
    for k in range(51, len(eq)):
        if eq[k] > peak:
            peak = eq[k]
        dd = (eq[k] - peak) / peak
        if dd < max_dd:
            max_dd = dd
        if eq[k - 1] > 0:
            rets.append((eq[k] - eq[k - 1]) / eq[k - 1])
    mr = sum(rets) / max(1, len(rets))
    vr = sum((r - mr) ** 2 for r in rets) / max(1, len(rets))
    sh = (mr / math.sqrt(vr)) * math.sqrt(2190.0) if vr > 0 else 0.0  # 2190 4H bars/year
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
        f"{s['label']:24s} [{s['verdict']:5s}] N={s['n']:4d} ({s['per_mo']:4.0f}/m) "
        f"WR={s['wr']:4.1f}% PF={s['pf']:.2f} Ex5={s['ex5']:.2f} "
        f"OOS={s['oos_pf']:.2f}({s['oos_n']:3d}) Ret={s['ret']:+6.1f}% "
        f"DD={s['dd']:6.1f}% Sh={s['sh']:+5.2f} W/L={s['wl']:.2f}x"
    )


def run_all_4h(ds: dict, fee_rt: float, exclude_syms: Optional[Set[str]] = None) -> Dict[str, dict]:
    return {
        "H1_4H_OI_BREAKOUT": run_4h_event_engine(
            ds, fee_rt, 3, sig_h1_oi_breakout,
            {"stop_atr": 2.2, "be_trig": 0.030, "trail_atr": 2.5, "max_hold": 24, "q_btc": True, "oi_exhaust": True},
            exclude_syms,
        ),
        "H2_4H_VOLCANO_IGNITION": run_4h_event_engine(
            ds, fee_rt, 3, sig_h2_volcano_ignition,
            {"stop_atr": 2.2, "be_trig": 0.030, "trail_atr": 2.4, "max_hold": 18, "q_btc": True, "oi_exhaust": False},
            exclude_syms,
        ),
        "H3_4H_MID_DIP_SNAP": run_4h_event_engine(
            ds, fee_rt, 3, sig_h3_mid_dip_snap,
            {"stop_atr": 2.0, "be_trig": 0.025, "trail_atr": 2.2, "max_hold": 15, "q_btc": False, "use_swing_low2": True},
            exclude_syms,
        ),
        "H4_4H_CS_LEADER_ROT": run_4h_rotation_h4(ds, fee_rt, exclude_syms),
    }


def run_self_test() -> bool:
    T = 160
    t0 = int(dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)
    raw_k, raw_oi, raw_f, dv = {}, {}, {}, {}
    for s_idx, sym in enumerate(["BTCUSDT", "ETHUSDT", "SOLUSDT", "TIAUSDT"]):
        km, om, fm = {}, {}, {}
        px, oi = 100.0 * (s_idx + 1), 1e8
        for i in range(T):
            ts = t0 + i * 14400000
            wave = math.sin((i + s_idx) * 0.16) * 0.025 + (0.006 if 60 < i < 120 else -0.002)
            o = px
            c = max(5.0, o * (1.0 + wave))
            h, l = max(o, c) * 1.012, min(o, c) * 0.988
            qv = 4e6 * (2.0 if i % 4 == 0 else 1.0)
            km[ts] = {"t": ts, "o": o, "h": h, "l": l, "c": c, "qv": qv, "nt": 1000.0, "tbqv": qv * (0.64 if c > o else 0.36)}
            oi *= 1.01 if c > o else 0.99
            om[ts] = oi
            fm[ts] = -0.0001 if i % 5 == 0 else 0.00005
            if sym == "BTCUSDT":
                dv[ts] = 45.0
            px = c
        raw_k[sym], raw_oi[sym], raw_f[sym] = km, om, fm

    ds = build_dataset_4h(raw_k, raw_oi, raw_f, dv)
    checks = []
    checks.append(ds["T"] == T and len(ds["syms"]) == 4)
    btc = ds["series"]["BTCUSDT"]
    checks.append(abs(btc["hh15"][60] - max(btc["h"][45:60])) < 1e-9)
    res_map = run_all_4h(ds, 0.0026)
    checks.append(len(res_map) == 4)
    checks.append(all(math.isfinite(summarize(k, v, ds)["pf"]) for k, v in res_map.items()))
    # Gap-down + Next-Open check
    gap_k = {"BTCUSDT": {}}
    for i in range(80):
        ts = t0 + i * 14400000
        o, h, l, c = (82.0, 83.0, 81.0, 82.0) if i == 62 else (100.0, 101.0, 99.0, 100.5)
        gap_k["BTCUSDT"][ts] = {"t": ts, "o": o, "h": h, "l": l, "c": c, "qv": 2e6, "nt": 500.0, "tbqv": 1.2e6}
    gds = build_dataset_4h(gap_k, {}, {}, {})
    gres = run_4h_event_engine(gds, 0.0026, 1, lambda s, d, i, _ds: 1.0 if i == 60 else None, {"stop_atr": 2.0, "be_trig": 0.03, "trail_atr": 2.0, "max_hold": 10})
    checks.append(len(gres["trades"]) == 1 and gres["trades"][0]["entry_i"] == 61 and gres["trades"][0]["reason"] == "STOP_GAP")
    # Zero-trade safety
    zres = run_4h_event_engine(gds, 0.0026, 1, lambda s, d, i, _ds: None, {"stop_atr": 2.0, "be_trig": 0.03, "trail_atr": 2.0, "max_hold": 10})
    zst = summarize("ZERO", zres, gds)
    checks.append(zst["n"] == 0 and zst["pf"] == 0.0 and zst["verdict"] == "NO-GO")

    passed = sum(1 for c in checks if c)
    print(f"S7_4H_SELF_TEST: {passed}/{len(checks)} PASS")
    return passed == len(checks)


def main():
    ap = argparse.ArgumentParser(description="OMEGA S7 4H Swing & Volcano-Ignition Lab v1.0")
    ap.add_argument("--days", type=int, default=730)
    ap.add_argument("--out", type=str, default="OMEGA_S7_4H_SWING_REPORT.txt")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        sys.exit(0 if run_self_test() else 1)

    if requests is None:
        print("ERROR: requests required")
        sys.exit(1)

    end_dt = utc_now().replace(minute=0, second=0, microsecond=0)
    end_dt = end_dt.replace(hour=(end_dt.hour // 4) * 4) - dt.timedelta(hours=4)
    start_dt = end_dt - dt.timedelta(days=args.days)
    print(f"[OMEGA S7 4H v{VERSION}] Fetching {args.days}d 4H data ({start_dt:%Y-%m-%d} -> {end_dt:%Y-%m-%d})...")

    raw_k, raw_oi, raw_f = {}, {}, {}
    for idx, sym in enumerate(UNIVERSE_20, 1):
        print(f"  ({idx:02d}/{len(UNIVERSE_20)}) {sym}: spot 4H + futures OI + funding...")
        raw_k[sym] = fetch_spot_4h_klines(sym, start_dt, end_dt)
        raw_oi[sym] = fetch_futures_oi_4h(sym, start_dt, end_dt)
        raw_f[sym] = fetch_futures_funding_4h(sym, start_dt, end_dt)

    print("  Fetching Deribit BTC DVOL...")
    dvol_map = fetch_deribit_dvol_4h(start_dt, end_dt)
    if "BTCUSDT" not in raw_k or len(raw_k["BTCUSDT"]) < 200:
        print("ERROR: Insufficient BTCUSDT 4H bars.")
        sys.exit(1)

    ds = build_dataset_4h(raw_k, raw_oi, raw_f, dvol_map)
    btc_c = ds["series"]["BTCUSDT"]["c"]
    btc_ret = (btc_c[-1] / btc_c[50] - 1.0) * 100.0
    oi_cov = sum(1 for v in ds["series"]["BTCUSDT"]["oi"] if v > 0) / max(1, ds["T"]) * 100.0
    dv_cov = len(dvol_map) / max(1, ds["T"]) * 100.0

    lines = [
        "=" * 90,
        f"OMEGA S7 4H SWING & VOLCANO-IGNITION LAB v{VERSION} ({utc_now():%Y-%m-%d})",
        f"Universe: {len(ds['syms'])} coins | Bars: {ds['T']} (4H) | Span: {fmt_ts(ds['ts'][50])} .. {fmt_ts(ds['ts'][-1])}",
        f"BTC Buy&Hold: {btc_ret:+.1f}% | Coverage: OI={oi_cov:.0f}% DVOL={dv_cov:.0f}%",
        "Execution: Strict Next-Open, Stop-First (Gap-Protected), 70% ATR Floor, 70/30 IS/OOS",
        "=" * 90,
        "",
        "--- TABLE 1: 4H ENGINES AT 0.26% RT (NOBITEX USDT) ---",
    ]

    res_026 = run_all_4h(ds, 0.0026)
    stats_026 = [summarize(k, v, ds) for k, v in res_026.items()]
    stats_026.sort(key=lambda x: x["pf"], reverse=True)
    for s in stats_026:
        lines.append(fmt_row(s))

    lines.extend(["", "--- TABLE 2: 4H ENGINES AT 0.50% RT (NOBITEX IRT) ---"])
    res_050 = run_all_4h(ds, 0.0050)
    stats_050 = [summarize(k, v, ds) for k, v in res_050.items()]
    stats_050.sort(key=lambda x: x["pf"], reverse=True)
    for s in stats_050:
        lines.append(fmt_row(s))

    # Deep Dive on #1 Top Engine (Half-Year + Per-Coin + Leave-2-Out)
    best_name = stats_026[0]["label"]
    best_res = res_026[best_name]
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
    for sym, arr in coin_ranked:
        gp, gl = sum(v for v in arr if v > 0), sum(abs(v) for v in arr if v <= 0)
        pf = gp / gl if gl > 0 else 99.0
        wr = sum(1 for v in arr if v > 0) / len(arr) * 100.0
        lines.append(f"  {sym:6s}: N={len(arr):3d} WR={wr:4.1f}% PF={pf:.2f} SumNet={sum(arr)*100:+6.1f}%")

    top2 = {f"{coin_ranked[i][0]}USDT" for i in range(min(2, len(coin_ranked)))}
    l2_map = run_all_4h(ds, 0.0026, exclude_syms=top2)
    lines.append("")
    lines.append(f"  Dropped Top-2 Coins: {sorted(top2)}")
    lines.append("  " + fmt_row(summarize(f"{best_name}_L2O", l2_map[best_name], ds)))

    report = "\n".join(lines)
    print("\n" + report)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    print(f"\nSaved 4H report to {args.out}")


if __name__ == "__main__":
    main()
