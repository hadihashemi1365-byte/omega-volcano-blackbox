
#!/usr/bin/env python3
"""
OMEGA S7 INTRADAY 1H INSTITUTIONAL LAB v1.0
===========================================
1-Year Multi-Factor + Quality-Decay Exit A/B Torture Lab on 1H Spot Crypto.
Zero API keys required — uses 100% free public historical archives:
  1. Binance Spot 1H Klines (data.binance.vision monthly/daily ZIP + REST fallback)
     -> OHLCV, TakerBuyRatio (TBR), CVD, AvgTradeSizeRatio (QuoteVol/Trades)
  2. Binance Futures 5m Metrics (data.binance.vision/data/futures/um/daily/metrics)
     -> Open Interest (OI USD, dOI), Top-Trader L/S, Global L/S, Smart/Retail Divergence
  3. Binance Futures Premium Index & Funding Rate (data.binance.vision monthly ZIP)
     -> Perp-Spot Basis (bps), Funding Rate (forward-filled causally)
  4. Coinbase Exchange 1H BTC-USD Candles (api.exchange.coinbase.com)
     -> Coinbase Premium (US institutional spot demand in bps)
  5. Deribit DVOL 1H Implied Volatility Index (deribit.com/api/v2/public)
     -> Crypto VIX level & 6h/24h panic-change filter
  6. Cross-Sectional 1H Market Breadth (% coins > EMA24 and TBR > 0.52)

Execution Rigor:
  - 100% Spot-Only (Long-only, zero leverage, zero shorting)
  - Strict Next-Open execution (signal at bar t close -> fill at bar t+1 open)
  - Stop-First with Gap-Down protection + 70% ATR Floor (prevents quiet-vol whipsaw)
  - Causal Chandelier Trailing Stop (updated only after bar t closes)
  - 70/30 chronological IS/OOS split + ExTop5 PF + Sharpe + MaxDD + W/L Ratio
  - A/B test on every engine: BASE vs QDECAY (Quality-Decay early exits)
"""

import argparse
import concurrent.futures
import csv
import datetime as dt
import io
import math
import os
import sys
import time
import zipfile
from typing import Dict, List, Optional, Tuple

try:
    import requests
except ImportError:
    requests = None

VERSION = "1.0"
UNIVERSE_15 = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT",
    "LTCUSDT", "NEARUSDT", "TAOUSDT", "ZECUSDT", "AAVEUSDT",
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
    s.headers.update({"User-Agent": "OMEGA-S7-Lab/1.0"})
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
    cur = start_dt.date()
    end = end_dt.date()
    while cur <= end:
        out.append(cur)
        cur += dt.timedelta(days=1)
    return out


def _fetch_zip_csv_rows(sess, url: str, timeout: int = 20) -> List[List[str]]:
    try:
        r = sess.get(url, timeout=timeout)
        if r.status_code != 200 or not r.content:
            return []
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                text = f.read().decode("utf-8", errors="ignore")
                return list(csv.reader(io.StringIO(text)))
    except Exception:
        return []


# ==============================================================================
# 1. FREE HISTORICAL DATA FETCHERS
# ==============================================================================

def fetch_spot_1h_klines(sym: str, start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, dict]:
    """Fetch 1H spot klines from data.binance.vision monthly ZIPs + REST tail fill."""
    sess = _get_session()
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    bars: Dict[int, dict] = {}

    # Monthly archive ZIPs (fast & complete)
    for y, m in _month_list(start_dt, end_dt):
        url = f"{BINANCE_VISION}/spot/monthly/klines/{sym}/1h/{sym}-1h-{y:04d}-{m:02d}.zip"
        for row in _fetch_zip_csv_rows(sess, url):
            if len(row) < 11:
                continue
            try:
                ts = int(float(row[0]))
                if ts > 1e14:  # Binance microseconds after 2025-01
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

    # Fill recent current month via REST API
    last_ts = max(bars.keys()) + 3600000 if bars else start_ms
    if last_ts < end_ms - 3600000:
        for host in BINANCE_REST_HOSTS:
            try:
                cur = last_ts
                while cur < end_ms:
                    url = f"{host}/api/v3/klines?symbol={sym}&interval=1h&startTime={cur}&limit=1000"
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
                    nxt = int(data[-1][0]) + 3600000
                    if nxt <= cur or len(data) < 1000:
                        break
                    cur = nxt
                if bars:
                    break
            except Exception:
                continue
    return bars


def fetch_futures_metrics_1h(sym: str, start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, dict]:
    """Fetch 5m futures metrics daily ZIPs from data.binance.vision and aggregate to 1H."""
    sess = _get_session()
    hourly: Dict[int, dict] = {}
    days = _day_list(start_dt, end_dt)

    def _one_day(d: dt.date):
        url = f"{BINANCE_VISION}/futures/um/daily/metrics/{sym}/{sym}-metrics-{d:%Y-%m-%d}.zip"
        rows = _fetch_zip_csv_rows(sess, url, timeout=12)
        out = {}
        for row in rows:
            if len(row) < 8 or row[0] == "create_time":
                continue
            try:
                # create_time format: '2026-01-01 00:05:00'
                t_dt = dt.datetime.strptime(row[0][:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
                hr_ms = int(t_dt.replace(minute=0, second=0).timestamp() * 1000)
                oi_val = float(row[3]) if row[3] else 0.0
                top_pos = float(row[5]) if row[5] else 1.0
                glob_acc = float(row[6]) if row[6] else 1.0
                fut_tkr = float(row[7]) if row[7] else 1.0
                # Keep latest 5m print within each hour
                out[hr_ms] = {
                    "oi_usd": oi_val,
                    "top_ls": top_pos,
                    "glob_ls": glob_acc,
                    "fut_tkr": fut_tkr,
                }
            except Exception:
                continue
        return out

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        for res in ex.map(_one_day, days):
            hourly.update(res)
    return hourly


def fetch_futures_basis_and_funding(sym: str, start_dt: dt.datetime, end_dt: dt.datetime) -> Tuple[Dict[int, float], Dict[int, float]]:
    """Fetch 1H premiumIndexKlines (perp-spot basis) & fundingRate from data.binance.vision."""
    sess = _get_session()
    basis: Dict[int, float] = {}
    funding: Dict[int, float] = {}
    for y, m in _month_list(start_dt, end_dt):
        # Premium Index 1H
        p_url = f"{BINANCE_VISION}/futures/um/monthly/premiumIndexKlines/{sym}/1h/{sym}-1h-{y:04d}-{m:02d}.zip"
        for row in _fetch_zip_csv_rows(sess, p_url):
            if len(row) < 5 or not row[0][0].isdigit():
                continue
            try:
                ts = int(float(row[0]))
                if ts > 1e14:
                    ts //= 1000
                basis[ts] = float(row[4])  # close of premium index (e.g. -0.0005 = -5 bps)
            except Exception:
                continue
        # Funding Rate
        f_url = f"{BINANCE_VISION}/futures/um/monthly/fundingRate/{sym}/{sym}-fundingRate-{y:04d}-{m:02d}.zip"
        for row in _fetch_zip_csv_rows(sess, f_url):
            if len(row) < 3 or not row[0][0].isdigit():
                continue
            try:
                ts = int(float(row[0]))
                hr_ms = (ts // 3600000) * 3600000
                funding[hr_ms] = float(row[2])
            except Exception:
                continue
    return basis, funding


def fetch_coinbase_btc_1h(start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, float]:
    """Fetch Coinbase BTC-USD 1H close prices (free public API, 300 candles/req)."""
    sess = _get_session()
    out: Dict[int, float] = {}
    cur = start_dt
    step = dt.timedelta(hours=290)
    while cur < end_dt:
        nxt = min(end_dt, cur + step)
        url = (
            "https://api.exchange.coinbase.com/products/BTC-USD/candles"
            f"?granularity=3600&start={cur.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            f"&end={nxt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )
        try:
            r = sess.get(url, timeout=15)
            if r.status_code == 200:
                rows = r.json()
                if isinstance(rows, list):
                    for k in rows:
                        # [time_sec, low, high, open, close, volume]
                        if len(k) >= 5 and float(k[4]) > 0:
                            out[int(k[0]) * 1000] = float(k[4])
        except Exception:
            pass
        cur = nxt
        time.sleep(0.12)
    return out


def fetch_deribit_dvol_1h(start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, float]:
    """Fetch Deribit BTC DVOL 1H close (free public API)."""
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
                res = r.json().get("result", {})
                for row in res.get("data", []):
                    # [ts_ms, open, high, low, close]
                    if len(row) >= 5:
                        out[int(row[0])] = float(row[4])
        except Exception:
            pass
        cur_ms = nxt_ms
        time.sleep(0.12)
    return out


# ==============================================================================
# 2. CAUSAL FEATURE ENGINEERING (ZERO LOOK-AHEAD)
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


def calc_vwap(h: List[float], l: List[float], c: List[float], qv: List[float], length: int) -> List[float]:
    out = [0.0] * len(c)
    tp = [(h[i] + l[i] + c[i]) / 3.0 for i in range(len(c))]
    spv, sv = 0.0, 0.0
    for i in range(len(c)):
        v = qv[i] if qv[i] > 0 else 1.0
        spv += tp[i] * v
        sv += v
        if i >= length:
            ov = qv[i - length] if qv[i - length] > 0 else 1.0
            spv -= tp[i - length] * ov
            sv -= ov
        out[i] = spv / sv if sv > 0 else c[i]
    return out


def build_aligned_dataset(
    raw_klines: Dict[str, Dict[int, dict]],
    raw_metrics: Dict[str, Dict[int, dict]],
    raw_basis: Dict[str, Dict[int, float]],
    raw_funding: Dict[str, Dict[int, float]],
    cb_btc: Dict[int, float],
    dvol_btc: Dict[int, float],
) -> dict:
    """Align all series strictly onto BTCUSDT 1H timestamp grid with causal forward-fill."""
    btc_ts = sorted(raw_klines["BTCUSDT"].keys())
    T = len(btc_ts)
    syms = [s for s in raw_klines.keys() if len(raw_klines[s]) >= int(T * 0.7)]

    series = {}
    for s in syms:
        kmap = raw_klines[s]
        mmap = raw_metrics.get(s, {})
        bmap = raw_basis.get(s, {})
        fmap = raw_funding.get(s, {})

        o, h, l, c, qv, tbr, avg_sz = [], [], [], [], [], [], []
        oi, top_ls, glob_ls, basis, fund = [], [], [], [], []
        valid = []

        last_c = None
        last_oi, last_top, last_glob, last_b, last_f = 0.0, 1.0, 1.0, 0.0, 0.0
        for ts in btc_ts:
            bar = kmap.get(ts)
            if bar is not None:
                o.append(bar["o"]); h.append(bar["h"]); l.append(bar["l"]); c.append(bar["c"])
                qv.append(bar["qv"])
                rng = bar["h"] - bar["l"]
                clv = (bar["c"] - bar["l"]) / rng if rng > 0 else 0.5
                r_tbr = min(1.0, max(0.0, bar["tbqv"] / bar["qv"])) if bar["qv"] > 0 else clv
                tbr.append(r_tbr)
                avg_sz.append(bar["qv"] / max(1.0, bar["nt"]))
                last_c = bar["c"]
                valid.append(True)
            else:
                px = last_c if last_c is not None else 0.0
                o.append(px); h.append(px); l.append(px); c.append(px)
                qv.append(0.0); tbr.append(0.5); avg_sz.append(0.0)
                valid.append(False)

            mm = mmap.get(ts)
            if mm is not None:
                last_oi = mm["oi_usd"]
                last_top = mm["top_ls"]
                last_glob = mm["glob_ls"]
            oi.append(last_oi); top_ls.append(last_top); glob_ls.append(last_glob)
            if ts in bmap:
                last_b = bmap[ts]
            if ts in fmap:
                last_f = fmap[ts]
            basis.append(last_b); fund.append(last_f)

        ema12 = calc_ema(c, 12)
        ema24 = calc_ema(c, 24)
        ema48 = calc_ema(c, 48)
        ema72 = calc_ema(c, 72)
        atr14 = calc_atr(h, l, c, 14)
        atr48 = calc_sma(atr14, 48)
        vwap24 = calc_vwap(h, l, c, qv, 24)
        vol20 = calc_sma(qv, 20)
        avg_sz24 = calc_sma(avg_sz, 24)

        # Strictly prior 48h high (excludes bar i itself -> zero look-ahead)
        hh48 = [0.0] * T
        for i in range(T):
            s_idx = max(0, i - 48)
            hh48[i] = max(h[s_idx:i]) if i > 0 else h[0]

        smart_div = [top_ls[i] / max(0.1, glob_ls[i]) for i in range(T)]
        sz_ratio = [avg_sz[i] / max(1e-9, avg_sz24[i]) for i in range(T)]

        series[s] = {
            "o": o, "h": h, "l": l, "c": c, "qv": qv, "tbr": tbr,
            "valid": valid, "ema12": ema12, "ema24": ema24, "ema48": ema48, "ema72": ema72,
            "atr14": atr14, "atr48": atr48, "vwap24": vwap24, "vol20": vol20, "hh48": hh48,
            "oi": oi, "top_ls": top_ls, "glob_ls": glob_ls, "smart_div": smart_div,
            "basis": basis, "fund": fund, "sz_ratio": sz_ratio,
        }

    # Global macro 1H vectors: Coinbase Premium (bps), DVOL, Market Breadth
    btc_c = series["BTCUSDT"]["c"]
    cb_prem = [0.0] * T
    dvol = [50.0] * T
    last_cb, last_dv = 0.0, 50.0
    for i, ts in enumerate(btc_ts):
        if ts in cb_btc and btc_c[i] > 0:
            last_cb = (cb_btc[ts] / btc_c[i] - 1.0) * 10000.0  # bps
        cb_prem[i] = last_cb
        if ts in dvol_btc:
            last_dv = dvol_btc[ts]
        dvol[i] = last_dv

    breadth_ema = [0.5] * T
    breadth_tbr = [0.5] * T
    for i in range(T):
        cnt, above, buy_dom = 0, 0, 0
        for s in syms:
            if series[s]["valid"][i]:
                cnt += 1
                if series[s]["c"][i] > series[s]["ema24"][i]:
                    above += 1
                if series[s]["tbr"][i] > 0.52:
                    buy_dom += 1
        if cnt > 0:
            breadth_ema[i] = above / cnt
            breadth_tbr[i] = buy_dom / cnt

    return {
        "ts": btc_ts, "T": T, "syms": syms, "series": series,
        "cb_prem": cb_prem, "dvol": dvol,
        "breadth_ema": breadth_ema, "breadth_tbr": breadth_tbr,
    }


# ==============================================================================
# 3. SIMULATOR: STRICT NEXT-OPEN, STOP-FIRST (GAP-PROTECTED), ATR FLOOR, QDECAY
# ==============================================================================

def simulate_engine(ds: dict, fee_rt: float, max_slots: int, signal_fn, use_qdecay: bool) -> dict:
    T = ds["T"]
    ts_arr = ds["ts"]
    syms = ds["syms"]
    ser = ds["series"]
    btc = ser["BTCUSDT"]

    eq = [1.0] * T
    cash = 1.0
    open_pos = []  # dicts
    trades = []
    start_idx = 80

    for i in range(start_idx, T):
        # 1) Manage open positions on bar i
        for p in range(len(open_pos) - 1, -1, -1):
            pos = open_pos[p]
            d = ser[pos["sym"]]
            if not d["valid"][i]:
                continue

            exit_px = None
            reason = None

            # Stop-First with Gap-Down Protection
            if d["o"][i] <= pos["stop"]:
                exit_px = d["o"][i]
                reason = "TRAIL_GAP" if pos["be"] else "STOP_GAP"
            elif d["l"][i] <= pos["stop"]:
                exit_px = pos["stop"]
                reason = "TRAIL" if pos["be"] else "STOP"
            elif i - pos["entry_i"] >= pos["max_hold"]:
                exit_px = d["c"][i]
                reason = "TIME"
            elif i == T - 1:
                exit_px = d["c"][i]
                reason = "END_CLOSE"

            # Quality-Decay Early Exits (evaluated at bar i close -> exit at bar i close)
            if exit_px is None and use_qdecay:
                held_h = i - pos["entry_i"]
                cur_mfe = (max(pos["hh"], d["h"][i]) - pos["entry_px"]) / pos["entry_px"]
                cur_ret = (d["c"][i] - pos["entry_px"]) / pos["entry_px"]

                # Q1: No-Progress Time-Decay (stalled after 5h without +0.8% MFE)
                if held_h >= 5 and cur_mfe < 0.008 and cur_ret < 0.004:
                    exit_px = d["c"][i]
                    reason = "Q_STALL"
                # Q2: Order-Flow Flip (2 consecutive bars with TBR < 0.45)
                elif held_h >= 2 and d["tbr"][i] < 0.45 and d["tbr"][i - 1] < 0.45:
                    exit_px = d["c"][i]
                    reason = "Q_FLOW"
                # Q3: BTC Micro-Structure Break
                elif held_h >= 1 and btc["c"][i] < btc["ema12"][i] and btc["tbr"][i] < 0.45:
                    exit_px = d["c"][i]
                    reason = "Q_BTC"

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
                # Causal trailing update AFTER bar i closes (effective on bar i+1)
                if d["h"][i] > pos["hh"]:
                    pos["hh"] = d["h"][i]
                gain = (pos["hh"] - pos["entry_px"]) / pos["entry_px"]
                if not pos["be"] and gain >= 0.022:
                    pos["be"] = True
                    pos["stop"] = max(pos["stop"], pos["entry_px"] * (1.0 + fee_rt + 0.001))
                if pos["be"]:
                    eff_atr = max(d["atr14"][i], 0.70 * d["atr48"][i])
                    trail_mult = 2.4
                    # Q4: OI Exhaustion -> tighten trail in profit (> +2%) when OI drops > 1.5% in 3h
                    if use_qdecay and gain >= 0.02 and d["oi"][i - 3] > 0:
                        doi3 = (d["oi"][i] - d["oi"][i - 3]) / d["oi"][i - 3]
                        if doi3 < -0.015:
                            trail_mult = 1.2
                    trail_px = pos["hh"] - trail_mult * eff_atr
                    if trail_px > pos["stop"]:
                        pos["stop"] = trail_px

        # 2) Mark-to-market equity
        mtm = cash
        for pos in open_pos:
            d = ser[pos["sym"]]
            px = d["c"][i] if d["valid"][i] else pos["entry_px"]
            mtm += pos["weight"] * (px / pos["entry_px"])
        eq[i] = mtm

        # 3) Signals at bar i close -> strict Next-Open execution at bar i+1
        if i < T - 2 and len(open_pos) < max_slots:
            held_set = {x["sym"] for x in open_pos}
            cands = []
            for s in syms:
                if s in held_set:
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
                # Dynamic Stop with 70% ATR Floor (prevents quiet-vol stopouts)
                eff_atr = max(d["atr14"][i], 0.70 * d["atr48"][i])
                if eff_atr <= 0:
                    eff_atr = entry_px * 0.015
                stop_px = max(entry_px * 0.94, entry_px - 2.2 * eff_atr)
                alloc = min(cash, mtm / max_slots)
                if alloc <= 1e-6:
                    break
                cash -= alloc
                open_pos.append({
                    "sym": s, "entry_i": i + 1, "entry_px": entry_px,
                    "stop": stop_px, "be": False, "hh": entry_px,
                    "max_hold": 48, "weight": alloc,
                })

    return {"eq": eq, "trades": trades}


# ==============================================================================
# 4. FOUR MULTI-FACTOR 1H ENGINES
# ==============================================================================

def sig_f1_oi_liq_rebound(s, d, i, ds):
    """F1: Forced Liquidation Washout (Price drop + OI drop) + Whale & Coinbase Rebound."""
    r12 = (d["l"][i - 1] - d["c"][i - 12]) / d["c"][i - 12] if d["c"][i - 12] > 0 else 0.0
    if r12 > -0.035:
        return None
    # OI dropped (liquidation washout) if OI available
    if d["oi"][i - 12] > 0:
        doi12 = (d["oi"][i] - d["oi"][i - 12]) / d["oi"][i - 12]
        if doi12 > -0.015:
            return None
    # Panic DVOL not exploding upward right now
    if ds["dvol"][i] - ds["dvol"][i - 6] > 3.5:
        return None
    # Reversal green candle + whale taker absorption + Coinbase premium turning up
    cb_chg3 = ds["cb_prem"][i] - ds["cb_prem"][i - 3]
    if d["c"][i] <= d["o"][i] or d["tbr"][i] < 0.56 or d["sz_ratio"][i] < 1.15:
        return None
    if cb_chg3 < -2.0:
        return None
    return d["tbr"][i] + d["sz_ratio"][i] - r12


def sig_f2_basis_squeeze(s, d, i, ds):
    """F2: Perp Discount / Negative Funding + Smart/Retail Divergence + VWAP Reclaim."""
    btc = ds["series"]["BTCUSDT"]
    if btc["c"][i] <= btc["ema12"][i]:
        return None
    squeeze_fuel = (d["basis"][i] < -0.0004) or (d["fund"][i] < 0.0)
    if not squeeze_fuel:
        return None
    # Smart money (Top-Trader L/S) rising vs retail (Global L/S)
    div_chg = d["smart_div"][i] - d["smart_div"][i - 6]
    if div_chg < 0.0:
        return None
    if d["c"][i] <= d["vwap24"][i] or d["tbr"][i] < 0.54 or d["c"][i] <= d["o"][i]:
        return None
    return (-d["basis"][i] * 1000.0) + d["tbr"][i] + div_chg


def sig_f3_oi_fresh_trend(s, d, i, ds):
    """F3: Price Up + OI Up (Fresh Longs) + Whale Trade Size + High Breadth + Calm DVOL."""
    btc = ds["series"]["BTCUSDT"]
    if btc["c"][i] <= btc["ema24"][i]:
        return None
    if ds["breadth_ema"][i] < 0.55 or ds["dvol"][i] > 65.0:
        return None
    if d["c"][i] <= d["hh48"][i]:
        return None
    # Require fresh OI buildup (not just short covering) when OI exists
    if d["oi"][i - 6] > 0:
        doi6 = (d["oi"][i] - d["oi"][i - 6]) / d["oi"][i - 6]
        if doi6 < 0.012:
            return None
    if d["tbr"][i] < 0.54 or d["sz_ratio"][i] < 1.15 or d["qv"][i] < 1.3 * d["vol20"][i]:
        return None
    return (d["c"][i] / d["hh48"][i] - 1.0) + d["sz_ratio"][i]


def sig_f4_mtf_4h1h_sniper(s, d, i, ds):
    """F4: 4H/3D Macro Context + Active Session + 1H Whale/CB Sniper Entry."""
    hr = dt.datetime.fromtimestamp(ds["ts"][i] / 1000.0, tz=dt.timezone.utc).hour
    if hr < 9 or hr > 22:
        return None
    btc = ds["series"]["BTCUSDT"]
    # Higher-TF context: BTC above 72h EMA (~3D) and coin above 48h EMA (~2D)
    if btc["c"][i] <= btc["ema72"][i] or d["c"][i] <= d["ema48"][i]:
        return None
    if ds["cb_prem"][i] < -5.0 or ds["breadth_tbr"][i] < 0.45:
        return None
    # Pullback to EMA12 on 1H + whale absorption + smart/retail divergence >= 1.0
    if d["l"][i] > d["ema12"][i] * 1.003 or d["c"][i] <= d["ema12"][i] or d["c"][i] <= d["o"][i]:
        return None
    if d["tbr"][i] < 0.56 or d["sz_ratio"][i] < 1.10 or d["smart_div"][i] < 0.95:
        return None
    return d["tbr"][i] + d["sz_ratio"][i]


# ==============================================================================
# 5. METRICS & REPORT GENERATOR
# ==============================================================================

def calc_stats(name: str, res: dict, ds: dict, split_idx: int) -> dict:
    tr = res["trades"]
    eq = res["eq"]
    n = len(tr)
    days = max(1.0, (ds["ts"][-1] - ds["ts"][80]) / 86400000.0)
    per_mo = (n / days) * 30.0

    wins = [x["net"] for x in tr if x["net"] > 0]
    losses = [abs(x["net"]) for x in tr if x["net"] <= 0]
    gp, gl = sum(wins), sum(losses)
    wr = (len(wins) / n * 100.0) if n > 0 else 0.0
    pf = (gp / gl) if gl > 0 else (99.0 if gp > 0 else 0.0)
    wl = ((gp / len(wins)) / (gl / len(losses))) if (wins and losses) else 0.0

    srt = sorted([x["net"] for x in tr], reverse=True)[5:]
    egp = sum(v for v in srt if v > 0)
    egl = sum(abs(v) for v in srt if v <= 0)
    ex5 = (egp / egl) if egl > 0 else (99.0 if egp > 0 else 0.0)

    oos = [x["net"] for x in tr if x["entry_i"] >= split_idx]
    ogp = sum(v for v in oos if v > 0)
    ogl = sum(abs(v) for v in oos if v <= 0)
    oos_pf = (ogp / ogl) if ogl > 0 else (99.0 if ogp > 0 else 0.0)

    peak = eq[80]
    max_dd = 0.0
    rets = []
    for k in range(81, len(eq)):
        if eq[k] > peak:
            peak = eq[k]
        dd = (eq[k] - peak) / peak
        if dd < max_dd:
            max_dd = dd
        if eq[k - 1] > 0:
            rets.append((eq[k] - eq[k - 1]) / eq[k - 1])
    mean_r = sum(rets) / max(1, len(rets))
    var_r = sum((r - mean_r) ** 2 for r in rets) / max(1, len(rets))
    sharpe = (mean_r / math.sqrt(var_r)) * math.sqrt(8760.0) if var_r > 0 else 0.0
    net_ret = (eq[-1] - 1.0) * 100.0

    verdict = "NO-GO"
    if n >= 40 and pf >= 1.30 and oos_pf >= 1.25 and ex5 >= 1.15 and max_dd > -0.15 and net_ret > 0:
        verdict = "GO"
    elif n >= 25 and pf >= 1.15 and oos_pf >= 1.10 and net_ret > 0:
        verdict = "WATCH"

    return {
        "name": name, "verdict": verdict, "n": n, "per_mo": per_mo, "wr": wr,
        "pf": pf, "ex5": ex5, "oos_pf": oos_pf, "oos_n": len(oos),
        "net_ret": net_ret, "max_dd": max_dd * 100.0, "sharpe": sharpe, "wl": wl,
    }


def run_all_models(ds: dict, fee_rt: float) -> List[dict]:
    split_idx = int(80 + (ds["T"] - 80) * 0.70)
    engines = [
        ("F1_OI_LIQ_REBOUND", sig_f1_oi_liq_rebound),
        ("F2_BASIS_SQUEEZE", sig_f2_basis_squeeze),
        ("F3_OI_FRESH_TREND", sig_f3_oi_fresh_trend),
        ("F4_MTF_4H1H_SNIPER", sig_f4_mtf_4h1h_sniper),
    ]
    out = []
    for base_name, fn in engines:
        for mode, qflag in [("BASE", False), ("QDECAY", True)]:
            res = simulate_engine(ds, fee_rt, 3, fn, qflag)
            out.append(calc_stats(f"{base_name}_{mode}", res, ds, split_idx))
    return out


# ==============================================================================
# 6. BUILT-IN DETERMINISTIC SELF-TEST (RUNS OFFLINE IN SANDBOX)
# ==============================================================================

def run_self_test() -> bool:
    T = 260
    t0 = int(dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)
    raw_k, raw_m, raw_b, raw_f = {}, {}, {}, {}
    cb_btc, dvol_btc = {}, {}

    for s_idx, sym in enumerate(["BTCUSDT", "ETHUSDT", "SOLUSDT"]):
        km, mm, bm, fm = {}, {}, {}, {}
        px = 100.0 * (s_idx + 1)
        oi = 1e8
        for i in range(T):
            ts = t0 + i * 3600000
            wave = math.sin((i + s_idx) * 0.12) * 0.014 + (0.003 if 90 < i < 160 else -0.001)
            o = px
            c = max(5.0, o * (1.0 + wave))
            h = max(o, c) * 1.007
            l = min(o, c) * 0.993
            qv = 2e6 * (1.8 if i % 5 == 0 else 1.0)
            nt = 1000.0 if i % 5 == 0 else 1500.0
            tbqv = qv * (0.62 if c > o else 0.38)
            km[ts] = {"t": ts, "o": o, "h": h, "l": l, "c": c, "qv": qv, "nt": nt, "tbqv": tbqv}
            oi = max(1e7, oi * (1.0 + (0.006 if c > o else -0.008)))
            mm[ts] = {"oi_usd": oi, "top_ls": 1.35 if c > o else 0.95, "glob_ls": 1.05, "fut_tkr": 1.1}
            bm[ts] = -0.0007 if i % 9 == 0 else 0.0001
            fm[ts] = -0.0002 if i % 16 == 0 else 0.0001
            if sym == "BTCUSDT":
                cb_btc[ts] = c * 1.0006
                dvol_btc[ts] = 45.0
            px = c
        raw_k[sym], raw_m[sym], raw_b[sym], raw_f[sym] = km, mm, bm, fm

    ds = build_aligned_dataset(raw_k, raw_m, raw_b, raw_f, cb_btc, dvol_btc)
    checks = []
    # 1) Alignment & zero look-ahead hh48
    btc = ds["series"]["BTCUSDT"]
    checks.append(ds["T"] == T and len(ds["syms"]) == 3)
    checks.append(abs(btc["hh48"][100] - max(btc["h"][52:100])) < 1e-9)
    # 2) All 8 A/B models run cleanly with finite metrics
    stats = run_all_models(ds, 0.0026)
    checks.append(len(stats) == 8 and all(math.isfinite(x["pf"]) and math.isfinite(x["sharpe"]) for x in stats))
    # 3) Gap-down stop protection + Next-Open check
    gap_k = {"BTCUSDT": {}}
    for i in range(110):
        ts = t0 + i * 3600000
        o, h, l, c = (85.0, 86.0, 84.0, 85.0) if i == 92 else (100.0, 101.0, 99.0, 100.5)
        gap_k["BTCUSDT"][ts] = {"t": ts, "o": o, "h": h, "l": l, "c": c, "qv": 1e6, "nt": 500.0, "tbqv": 6e5}
    gds = build_aligned_dataset(gap_k, {}, {}, {}, {}, {})
    gres = simulate_engine(gds, 0.0026, 1, lambda s, d, i, _ds: 1.0 if i == 90 else None, False)
    checks.append(len(gres["trades"]) == 1 and gres["trades"][0]["entry_i"] == 91 and gres["trades"][0]["reason"] == "STOP_GAP")
    # 4) Quality-Decay Q_STALL check (flat price for 6 hours -> exits early on Q_STALL)
    flat_k = {"BTCUSDT": {}}
    for i in range(110):
        ts = t0 + i * 3600000
        flat_k["BTCUSDT"][ts] = {"t": ts, "o": 100.0, "h": 100.2, "l": 99.8, "c": 100.0, "qv": 1e6, "nt": 500.0, "tbqv": 5.5e5}
    fds = build_aligned_dataset(flat_k, {}, {}, {}, {}, {})
    fres = simulate_engine(fds, 0.0026, 1, lambda s, d, i, _ds: 1.0 if i == 85 else None, True)
    checks.append(len(fres["trades"]) == 1 and fres["trades"][0]["reason"] == "Q_STALL")

    passed = sum(1 for c in checks if c)
    print(f"SELF_TEST: {passed}/{len(checks)} PASS")
    return passed == len(checks)


def main():
    ap = argparse.ArgumentParser(description="OMEGA S7 1H Institutional Multi-Factor Lab v1.0")
    ap.add_argument("--days", type=int, default=365, help="Lookback days (default: 365)")
    ap.add_argument("--fee", type=float, default=0.0026, help="Round-trip fee (default: 0.0026)")
    ap.add_argument("--out", type=str, default="OMEGA_S7_1H_INSTITUTIONAL_REPORT.txt")
    ap.add_argument("--self-test", action="store_true", help="Run offline deterministic self-tests")
    args = ap.parse_args()

    if args.self_test:
        sys.exit(0 if run_self_test() else 1)

    if requests is None:
        print("ERROR: requests package is required. Run: pip install requests")
        sys.exit(1)

    end_dt = utc_now().replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=1)
    start_dt = end_dt - dt.timedelta(days=args.days)
    print(f"[OMEGA S7 v{VERSION}] Fetching {args.days}d 1H data ({start_dt:%Y-%m-%d} -> {end_dt:%Y-%m-%d})...")

    raw_k, raw_m, raw_b, raw_f = {}, {}, {}, {}
    for idx, sym in enumerate(UNIVERSE_15, 1):
        print(f"  ({idx:02d}/{len(UNIVERSE_15)}) {sym}: spot klines + futures metrics + basis/funding...")
        raw_k[sym] = fetch_spot_1h_klines(sym, start_dt, end_dt)
        raw_m[sym] = fetch_futures_metrics_1h(sym, start_dt, end_dt)
        b_map, f_map = fetch_futures_basis_and_funding(sym, start_dt, end_dt)
        raw_b[sym], raw_f[sym] = b_map, f_map

    print("  Fetching Coinbase BTC-USD 1H (Coinbase Premium) & Deribit BTC DVOL 1H...")
    cb_btc = fetch_coinbase_btc_1h(start_dt, end_dt)
    dvol_btc = fetch_deribit_dvol_1h(start_dt, end_dt)

    if "BTCUSDT" not in raw_k or len(raw_k["BTCUSDT"]) < 500:
        print("ERROR: Insufficient BTCUSDT 1H klines retrieved.")
        sys.exit(1)

    ds = build_aligned_dataset(raw_k, raw_m, raw_b, raw_f, cb_btc, dvol_btc)
    stats = run_all_models(ds, args.fee)
    stats.sort(key=lambda x: x["pf"], reverse=True)

    btc_c = ds["series"]["BTCUSDT"]["c"]
    btc_ret = (btc_c[-1] / btc_c[80] - 1.0) * 100.0
    m_cov = sum(1 for v in ds["series"]["BTCUSDT"]["oi"] if v > 0) / max(1, ds["T"]) * 100.0
    cb_cov = len(cb_btc) / max(1, ds["T"]) * 100.0
    dv_cov = len(dvol_btc) / max(1, ds["T"]) * 100.0

    lines = [
        "=" * 84,
        f"OMEGA S7 1H INSTITUTIONAL MULTI-FACTOR & QUALITY-DECAY LAB v{VERSION} ({utc_now():%Y-%m-%d})",
        f"Universe: {len(ds['syms'])} coins | Bars: {ds['T']} (1H) | Span: {fmt_ts(ds['ts'][80])} .. {fmt_ts(ds['ts'][-1])}",
        f"RT Fee: {args.fee*100:.2f}% | BTC Buy&Hold: {btc_ret:+.1f}% | Coverage: OI={m_cov:.0f}% CB={cb_cov:.0f}% DVOL={dv_cov:.0f}%",
        "Execution: Strict Next-Open, Stop-First (Gap-Protected), 70% ATR Floor, 70/30 IS/OOS",
        "=" * 84,
        "",
    ]
    for s in stats:
        lines.append(
            f"{s['name']:26s} [{s['verdict']:5s}] N={s['n']:4d} ({s['per_mo']:4.0f}/m) "
            f"WR={s['wr']:4.1f}% PF={s['pf']:.2f} Ex5={s['ex5']:.2f} "
            f"OOS={s['oos_pf']:.2f}({s['oos_n']:3d}) Ret={s['net_ret']:+6.1f}% "
            f"DD={s['max_dd']:6.1f}% Sh={s['sharpe']:+5.2f} W/L={s['wl']:.2f}x"
        )

    report = "\n".join(lines)
    print("\n" + report)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    print(f"\nSaved report to {args.out}")


if __name__ == "__main__":
    main()
