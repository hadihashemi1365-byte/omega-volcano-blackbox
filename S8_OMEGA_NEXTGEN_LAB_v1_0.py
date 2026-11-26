#!/usr/bin/env python3
"""
OMEGA S8 NEXT-GEN 1H/4H INSTITUTIONAL LAB v1.0 (2-YEAR, 120-COIN AUDIT)
=======================================================================
Synthesizes the best-in-class, zero-look-ahead architecture from all 10 AI
quant research reports:
  1. Schmitt-Trigger Dynamic Quarantine-to-Active Pool (DQP) across ~120 coins:
     5 anti-wash-trading & anti-spike gates (Med QV12 >= $150K/h, Sum QV24 >= $4M,
     Med Trades >= 350/h, Trade-Size Sanity [$80..$15K], Amihud <= 0.15,
     Anti-Spike VolConc12 <= 0.45), 6-bar promotion, 12-bar 40%-hysteresis demotion,
     and 48h punitive cooldown after >+12% blowoff / <-12% crash.
  2. 3-State Macro Bear Shield (Gamma in {0.0, 0.5, 1.0}) via Deribit DVOL Z-Score,
     Coinbase Premium Z-Score, and BTC 4H EMA50/EMA200.
  3. 3 Orthogonal Next-Gen Spot-Only Engines (strict Next-Open, S_min Law):
     - M1_RF_TCS_4H    : Residual Frog-in-the-Pan + Trend Clarity R^2 + KER + MaxBarShare
     - M2_PAAC_1H      : Post-Awakening Acceptance & Consolidation Shelf Breakout
     - M3_VR_PERSIST_1H: Lo-MacKinlay Variance Ratio VR(4) >= 1.28 Persistence
  4. Piecewise Volatility-Ratchet Exit (4 stages + Asymmetric Dead-Trade Exit)
     + Anti-Concentration Cap (max 3 trades/coin/30d, max 4 concurrent).
  5. Full Audit: 4 Fee Tiers (0.26%/0.36%/0.50%/0.65%), Strict Ablation
     (No-DQP, No-Macro, Fixed-Exit, Random-in-DQP Control), Half-Year Regimes,
     Leave-2-Out, ExTop5, and Leave-Best-Month-Out.
"""

import argparse
import concurrent.futures
import csv
import datetime as dt
import io
import math
import random
import statistics
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
UNIVERSE_120 = [
    # Core Large & Mid Caps
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT",
    "LINKUSDT", "SUIUSDT", "LTCUSDT", "NEARUSDT", "APTUSDT", "DOTUSDT", "TRXUSDT", "ATOMUSDT",
    "UNIUSDT", "AAVEUSDT", "ARBUSDT", "OPUSDT", "TAOUSDT", "FETUSDT", "ZECUSDT", "TONUSDT",
    "RENDERUSDT", "INJUSDT", "FILUSDT", "ETCUSDT", "XLMUSDT", "HBARUSDT", "ICPUSDT", "VETUSDT",
    "ALGOUSDT", "GRTUSDT", "SANDUSDT", "MANAUSDT", "AXSUSDT", "THETAUSDT", "RUNEUSDT", "STXUSDT",
    # Mid-Caps, DeFi, L2, AI, Gaming & Recent Nobitex Movers
    "SSVUSDT", "ZROUSDT", "LAYERUSDT", "VIRTUALUSDT", "SEIUSDT", "WIFUSDT", "MEUSDT", "MOVEUSDT",
    "TIAUSDT", "ENAUSDT", "STRKUSDT", "ONDOUSDT", "PENDLEUSDT", "JUPUSDT", "PYTHUSDT", "WLDUSDT",
    "EIGENUSDT", "ETHFIUSDT", "LDOUSDT", "CRVUSDT", "COMPUSDT", "SNXUSDT", "SUSHIUSDT", "DYDXUSDT",
    "GMXUSDT", "ENSUSDT", "IMXUSDT", "GALAUSDT", "CHZUSDT", "FLOWUSDT", "ROSEUSDT", "ONEUSDT",
    "SFPUSDT", "BATUSDT", "TRBUSDT", "TWTUSDT", "METISUSDT", "CGPTUSDT", "CAKEUSDT", "QNTUSDT",
    "DASHUSDT", "JSTUSDT", "BCHUSDT", "XTZUSDT", "EGLDUSDT", "KAVAUSDT", "MINAUSDT", "ZILUSDT",
    "ENJUSDT", "1INCHUSDT", "ANKRUSDT", "MASKUSDT", "LRCUSDT", "ZRXUSDT", "QTUMUSDT", "ONTUSDT",
    "ICXUSDT", "IOTAUSDT", "KSMUSDT", "CELOUSDT", "HOTUSDT", "RVNUSDT", "WOOUSDT", "BANDUSDT",
    "YFIUSDT", "UMAUSDT", "RSRUSDT", "STORJUSDT", "SCUSDT", "CFXUSDT", "ACHUSDT", "API3USDT",
    "COTIUSDT", "JASMYUSDT", "PEPEUSDT", "SHIBUSDT", "FLOKIUSDT", "BONKUSDT", "NOTUSDT", "ORDIUSDT",
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
    s.headers.update({"User-Agent": f"OMEGA-S8-NextGen/{VERSION}"})
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
# 1. FREE DATA FETCHERS (BINANCE VISION 1H + DERIBIT DVOL + COINBASE BTC-USD)
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


def fetch_deribit_dvol_1h(sess, start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, float]:
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
                        out[int(row[0])] = float(row[4])
        except Exception:
            pass
        cur_ms = nxt_ms
        time.sleep(0.08)
    return out


def fetch_coinbase_btc_1h(sess, start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, float]:
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
                        if len(k) >= 5 and float(k[4]) > 0:
                            out[int(k[0]) * 1000] = float(k[4])
        except Exception:
            pass
        cur = nxt
        time.sleep(0.08)
    return out


# ==============================================================================
# 2. CAUSAL INDICATORS, SCHMITT-TRIGGER DQP & MACRO BEAR SHIELD
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


def calc_std(arr: List[float], length: int) -> List[float]:
    out = [1.0] * len(arr)
    s, s2 = 0.0, 0.0
    for i, v in enumerate(arr):
        s += v
        s2 += v * v
        if i >= length:
            ov = arr[i - length]
            s -= ov
            s2 -= ov * ov
        cnt = min(i + 1, length)
        m = s / cnt
        out[i] = math.sqrt(max(1e-10, s2 / cnt - m * m))
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


def compute_schmitt_dqp_1h(o, h, l, c, qv, nt, valid) -> Tuple[List[int], List[int], List[bool]]:
    """
    Schmitt-Trigger 3-state Quarantine machine (0=Q, 1=ACTIVE, -1=COOLDOWN)
    computed strictly causally at bar i close (applied on i+1).
    Returns: (state, active_age_hrs, fresh_active_flag)
    """
    T = len(c)
    state = [0] * T
    active_age = [0] * T
    fresh_active = [False] * T

    cur_st = 0
    up_streak = 0
    dn_streak = 0
    q_dur = 0
    a_dur = 0
    cool_left = 0
    is_fresh = False

    for i in range(T):
        if i < 24 or not valid[i]:
            state[i] = 0
            q_dur += 1
            continue

        # Punitive Cooldown (-1) check on single-bar blowoff or crash
        r1 = (c[i] - o[i]) / o[i] if o[i] > 0 else 0.0
        rng = h[i] - l[i]
        up_wick = (h[i] - max(o[i], c[i])) / rng if rng > 0 else 0.0
        if (r1 > 0.12 and up_wick >= 0.65) or (r1 < -0.12):
            cur_st = -1
            cool_left = 48
            up_streak = 0
            dn_streak = 0
            a_dur = 0
            is_fresh = False
            state[i] = -1
            continue

        if cur_st == -1:
            cool_left -= 1
            if cool_left <= 0:
                cur_st = 0
                q_dur = 1
            state[i] = cur_st
            continue

        # 5 Schmitt-Trigger Gates on rolling 12h window [i-11..i]
        qv12 = qv[i - 11: i + 1]
        nt12 = nt[i - 11: i + 1]
        med_qv12 = statistics.median(qv12)
        sum_qv12 = sum(qv12)
        sum_qv24 = sum(qv[i - 23: i + 1])
        med_nt12 = statistics.median(nt12)
        sum_nt12 = sum(nt12)

        ats12 = sum_qv12 / max(1.0, sum_nt12)
        med_ats12 = statistics.median([qv12[k] / max(1.0, nt12[k]) for k in range(12)])
        amihud12 = statistics.median([
            abs((c[i - 11 + k] - c[i - 12 + k]) / max(1e-9, c[i - 12 + k])) / max(qv12[k] / 1e6, 1e-6)
            for k in range(12)
        ])
        vol_conc12 = max(qv12) / max(1.0, sum_qv12)

        pass_hi = (
            med_qv12 >= 150000.0
            and sum_qv24 >= 4000000.0
            and med_nt12 >= 350.0
            and 80.0 <= ats12 <= 15000.0
            and med_ats12 >= 60.0
            and amihud12 <= 0.15
            and vol_conc12 <= 0.45
        )
        below_lo = (med_qv12 < 60000.0) or (med_nt12 < 140.0)

        if cur_st == 0:
            q_dur += 1
            if pass_hi:
                up_streak += 1
            else:
                up_streak = 0
            if up_streak >= 6:
                cur_st = 1
                is_fresh = (q_dur >= 168)
                a_dur = 1
                q_dur = 0
                dn_streak = 0
        else:  # cur_st == 1 (ACTIVE)
            a_dur += 1
            if a_dur > 72:
                is_fresh = False
            if below_lo:
                dn_streak += 1
            else:
                dn_streak = 0
            if dn_streak >= 12:
                cur_st = 0
                q_dur = 1
                a_dur = 0
                up_streak = 0
                is_fresh = False

        state[i] = cur_st
        active_age[i] = a_dur
        fresh_active[i] = is_fresh

    return state, active_age, fresh_active


def build_datasets(raw_k: Dict[str, Dict[int, dict]], dvol_1h: Dict[int, float], cb_1h: Dict[int, float]) -> Tuple[dict, dict]:
    """Build strictly aligned 1H and 4H datasets with DQP + Macro Bear Shield."""
    btc_ts1 = sorted(raw_k["BTCUSDT"].keys())
    T1 = len(btc_ts1)
    syms = [s for s in raw_k.keys() if len(raw_k[s]) >= 400]

    # 1H aligned
    ser1 = {}
    for s in syms:
        km = raw_k[s]
        o, h, l, c, qv, nt, tbqv, tbr, valid = [], [], [], [], [], [], [], [], []
        last_c = None
        for ts in btc_ts1:
            b = km.get(ts)
            if b is not None:
                o.append(b["o"]); h.append(b["h"]); l.append(b["l"]); c.append(b["c"])
                qv.append(b["qv"]); nt.append(b["nt"]); tbqv.append(b["tbqv"])
                rng = b["h"] - b["l"]
                clv = (b["c"] - b["l"]) / rng if rng > 0 else 0.5
                tbr.append(min(1.0, max(0.0, b["tbqv"] / b["qv"])) if b["qv"] > 0 else clv)
                last_c = b["c"]
                valid.append(True)
            else:
                px = last_c if last_c is not None else 0.0
                o.append(px); h.append(px); l.append(px); c.append(px)
                qv.append(0.0); nt.append(0.0); tbqv.append(0.0); tbr.append(0.5)
                valid.append(False)

        dqp_st, dqp_age, fresh = compute_schmitt_dqp_1h(o, h, l, c, qv, nt, valid)
        atr6 = calc_atr(h, l, c, 6)
        atr14 = calc_atr(h, l, c, 14)
        atr24 = calc_atr(h, l, c, 24)
        qv_sma12 = calc_sma(qv, 12)
        qv_sma168 = calc_sma(qv, 168)
        log_r = [0.0] + [math.log(c[i] / c[i - 1]) if (c[i] > 0 and c[i - 1] > 0) else 0.0 for i in range(1, T1)]
        ser1[s] = {
            "o": o, "h": h, "l": l, "c": c, "qv": qv, "nt": nt, "tbqv": tbqv, "tbr": tbr,
            "valid": valid, "dqp": dqp_st, "age": dqp_age, "fresh": fresh,
            "atr6": atr6, "atr14": atr14, "atr24": atr24,
            "qv_sma12": qv_sma12, "qv_sma168": qv_sma168, "r": log_r,
        }

    # Build 4H aligned from 1H buckets (strictly 4 complete 1H bars per 4H bucket)
    buckets_btc = sorted({(ts // 14400000) * 14400000 for ts in btc_ts1})
    ts1_to_idx = {ts: i for i, ts in enumerate(btc_ts1)}
    valid_4h_ts = []
    b4_idx_groups = []
    for b_ts in buckets_btc:
        idxs = [ts1_to_idx[b_ts + k * 3600000] for k in range(4) if (b_ts + k * 3600000) in ts1_to_idx]
        if len(idxs) == 4:
            valid_4h_ts.append(b_ts)
            b4_idx_groups.append(idxs)

    T4 = len(valid_4h_ts)
    ser4 = {}
    for s in syms:
        d1 = ser1[s]
        o, h, l, c, qv, tbqv, tbr, valid, dqp = [], [], [], [], [], [], [], [], []
        for idxs in b4_idx_groups:
            if all(d1["valid"][k] for k in idxs):
                o.append(d1["o"][idxs[0]])
                h.append(max(d1["h"][k] for k in idxs))
                l.append(min(d1["l"][k] for k in idxs))
                c.append(d1["c"][idxs[-1]])
                sqv = sum(d1["qv"][k] for k in idxs)
                stb = sum(d1["tbqv"][k] for k in idxs)
                qv.append(sqv); tbqv.append(stb)
                tbr.append(min(1.0, max(0.0, stb / sqv)) if sqv > 0 else 0.5)
                valid.append(True)
                dqp.append(d1["dqp"][idxs[-1]])
            else:
                px = c[-1] if c else 0.0
                o.append(px); h.append(px); l.append(px); c.append(px)
                qv.append(0.0); tbqv.append(0.0); tbr.append(0.5)
                valid.append(False); dqp.append(0)
        atr14 = calc_atr(h, l, c, 14)
        ema50 = calc_ema(c, 50)
        ema200 = calc_ema(c, 200)
        log_r = [0.0] + [math.log(c[i] / c[i - 1]) if (c[i] > 0 and c[i - 1] > 0) else 0.0 for i in range(1, T4)]
        ser4[s] = {
            "o": o, "h": h, "l": l, "c": c, "qv": qv, "tbqv": tbqv, "tbr": tbr,
            "valid": valid, "dqp": dqp, "atr14": atr14, "ema50": ema50, "ema200": ema200, "r": log_r,
        }

    # 3-State Macro Bear Shield (Gamma_t on 4H, mapped causally to 1H)
    btc4 = ser4["BTCUSDT"]
    dv4, cb4 = [], []
    last_dv, last_cb = 50.0, 0.0
    for j, b_ts in enumerate(valid_4h_ts):
        end_hr = b_ts + 3 * 3600000
        if end_hr in dvol_1h:
            last_dv = dvol_1h[end_hr]
        if end_hr in cb_1h and btc4["c"][j] > 0:
            last_cb = math.log(cb_1h[end_hr] / btc4["c"][j])
        dv4.append(last_dv)
        cb4.append(last_cb)

    dv_ema = calc_ema(dv4, 180)
    dv_std = calc_std(dv4, 180)
    cb_ema = calc_ema(cb4, 84)
    cb_std = calc_std(cb4, 84)
    gamma4 = [1.0] * T4
    for j in range(T4):
        dv_z = (dv4[j] - dv_ema[j]) / max(1e-6, dv_std[j])
        cb_z = (cb4[j] - cb_ema[j]) / max(1e-6, cb_std[j])
        if dv_z > 2.0 and btc4["c"][j] < btc4["ema50"][j]:
            gamma4[j] = 0.0
        elif btc4["c"][j] < btc4["ema200"][j] and cb_z < -0.5:
            gamma4[j] = 0.5
        else:
            gamma4[j] = 1.0

    # Map last COMPLETED 4H Gamma to each 1H bar (zero look-ahead!)
    ts4_to_g = {valid_4h_ts[j]: gamma4[j] for j in range(T4)}
    gamma1 = [1.0] * T1
    cur_g = 1.0
    for i, ts in enumerate(btc_ts1):
        prev_4h = ((ts // 14400000) - 1) * 14400000
        if prev_4h in ts4_to_g:
            cur_g = ts4_to_g[prev_4h]
        gamma1[i] = cur_g

    ds1 = {"ts": btc_ts1, "T": T1, "syms": syms, "series": ser1, "gamma": gamma1}
    ds4 = {"ts": valid_4h_ts, "T": T4, "syms": syms, "series": ser4, "gamma": gamma4}
    return ds1, ds4


# ==============================================================================
# 3. THREE ORTHOGONAL NEXT-GEN SIGNALS (RF-TCS 4H, PAAC 1H, VR-PERSIST 1H)
# ==============================================================================

def _shrink_beta_resid(r_s: List[float], r_b: List[float], end_est: int, win: int) -> float:
    """Out-of-window OLS beta shrunk 0.7*beta + 0.3, clipped [0.20, 2.50]."""
    s0 = max(1, end_est - win)
    xs = r_b[s0:end_est]
    ys = r_s[s0:end_est]
    n = len(xs)
    if n < 20:
        return 1.0
    mx = sum(xs) / n
    my = sum(ys) / n
    vx = sum((x - mx) ** 2 for x in xs)
    cov = sum((xs[k] - mx) * (ys[k] - my) for k in range(n))
    b_raw = cov / (vx + 1e-10)
    return min(2.5, max(0.20, 0.7 * b_raw + 0.3))


def sig_m1_rf_tcs_4h(s: str, d: dict, i: int, ds4: dict, s_min: float, use_residual: bool = True) -> Optional[Tuple[float, float]]:
    """M1: Residual Frog-in-the-Pan + Trend Clarity R^2 + KER + MaxBarShare (4H)."""
    if s == "BTCUSDT" or i < 100:
        return None
    btc = ds4["series"]["BTCUSDT"]
    L = 12
    beta = _shrink_beta_resid(d["r"], btc["r"], i - L, 72) if use_residual else 0.0
    es = [d["r"][k] - beta * btc["r"][k] for k in range(i - L + 1, i + 1)]
    e_sum = sum(es)
    if e_sum <= 0:
        return None

    # 1) RMom Z-score >= 1.15
    em = e_sum / L
    e_var = sum((x - em) ** 2 for x in es)
    rmom = e_sum / math.sqrt(e_var + 1e-10)
    if rmom < 1.15:
        return None

    # 2) Trend Clarity R^2 >= 0.72 of cumulative residual path vs time 0..L
    cum = [0.0]
    for x in es:
        cum.append(cum[-1] + x)
    xs = list(range(L + 1))
    mx = L / 2.0
    my = sum(cum) / (L + 1)
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in cum)
    sxy = sum((xs[k] - mx) * (cum[k] - my) for k in range(L + 1))
    r2 = (sxy * sxy) / (sxx * syy + 1e-12)
    if r2 < 0.72:
        return None

    # 3) Frog-in-the-Pan (FIP ID <= -0.33 => >= 8/12 positive residual bars)
    pos_cnt = sum(1 for x in es if x > 0)
    neg_cnt = sum(1 for x in es if x < 0)
    fip_id = (neg_cnt - pos_cnt) / float(L)
    if fip_id > -0.33:
        return None

    # 4) Anti-Spike MaxBarShare <= 0.35 & Upper Wick <= 0.35 & KER >= 0.55
    max_share = max(es) / (e_sum + 1e-9)
    if max_share > 0.35:
        return None
    rng = d["h"][i] - d["l"][i]
    up_wick = (d["h"][i] - max(d["o"][i], d["c"][i])) / (rng + 1e-9)
    if up_wick > 0.35:
        return None
    ker = abs(e_sum) / (sum(abs(x) for x in es) + 1e-9)
    if ker < 0.55:
        return None

    # 5) Taker Buy Share >= 0.515 & Overextension guard
    sqv = sum(d["qv"][i - L + 1: i + 1])
    stb = sum(d["tbqv"][i - L + 1: i + 1])
    if sqv <= 0 or (stb / sqv) < 0.515:
        return None
    raw_ret = math.log(d["c"][i] / d["c"][i - L]) if d["c"][i - L] > 0 else 0.0
    atr_pct = d["atr14"][i] / max(1e-9, d["c"][i])
    if raw_ret > min(0.28, 3.5 * atr_pct * math.sqrt(L)):
        return None

    stop_dist = min(0.085, max(s_min, 2.2 * atr_pct))
    score = rmom * r2
    return (score, stop_dist)


def sig_m2_paac_1h(s: str, d: dict, i: int, ds1: dict, s_min: float) -> Optional[Tuple[float, float]]:
    """M2: Post-Awakening Acceptance & Consolidation Shelf Breakout (1H)."""
    if s == "BTCUSDT" or i < 200:
        return None
    # Require FreshActive OR strong 3x awakening ratio
    if not d["fresh"][i]:
        if d["qv_sma168"][i] <= 0 or (d["qv_sma12"][i] / d["qv_sma168"][i]) < 3.0:
            return None

    # Search for 4-bar impulse ending t0 in [i-18, i-6]
    best_t0 = None
    best_imp_ret = 0.0
    for t0 in range(i - 6, i - 19, -1):
        if t0 < 172 or d["o"][t0 - 3] <= 0:
            continue
        r_imp = math.log(d["c"][t0] / d["o"][t0 - 3])
        if 0.045 <= r_imp <= 0.25:
            best_t0 = t0
            best_imp_ret = r_imp
            break
    if best_t0 is None:
        return None

    t0 = best_t0
    h_imp = max(d["h"][t0 - 3: t0 + 1])
    l_imp = min(d["l"][t0 - 3: t0 + 1])
    if h_imp <= l_imp:
        return None

    # Impulse VWAP & shelf metrics over [t0+1 .. i-1]
    imp_qv = sum(d["qv"][t0 - 3: t0 + 1])
    vwap_imp = sum(d["c"][k] * d["qv"][k] for k in range(t0 - 3, t0 + 1)) / max(1.0, imp_qv)
    shelf_c = d["c"][t0 + 1: i]
    shelf_l = d["l"][t0 + 1: i]
    shelf_qv = d["qv"][t0 + 1: i]
    if len(shelf_c) < 4:
        return None

    min_l_shelf = min(shelf_l)
    rev_ratio = (h_imp - min_l_shelf) / (h_imp - l_imp)
    if rev_ratio > 0.45:
        return None
    if min(shelf_c) < max(vwap_imp, 0.5 * (h_imp + l_imp)):
        return None

    mean_sh_qv = sum(shelf_qv) / len(shelf_qv)
    pre_7d_qv = d["qv_sma168"][t0 - 4]
    if pre_7d_qv > 0 and mean_sh_qv < 2.2 * pre_7d_qv:
        return None
    if mean_sh_qv < 0.25 * (imp_qv / 4.0):
        return None

    # Volatility compression in shelf: ATR6 / ATR24 <= 0.70
    if d["atr24"][i] > 0 and (d["atr6"][i] / d["atr24"][i]) > 0.70:
        return None

    # Trigger bar i: breakout above shelf close high + TBR >= 0.54 + Vol >= 1.5x shelf
    if d["c"][i] <= max(shelf_c) or d["c"][i] < 0.985 * h_imp:
        return None
    if d["tbr"][i] < 0.54 or d["qv"][i] < 1.5 * mean_sh_qv:
        return None

    atr_pct = d["atr24"][i] / max(1e-9, d["c"][i])
    raw_stop = 1.0 - (min_l_shelf - 2.0 * d["atr24"][i]) / max(1e-9, d["c"][i])
    if raw_stop > 0.085:
        return None
    stop_dist = max(s_min, raw_stop, 2.0 * atr_pct)
    return (best_imp_ret + d["tbr"][i], stop_dist)


def sig_m3_vr_persist_1h(s: str, d: dict, i: int, ds1: dict, s_min: float, fee_rt: float) -> Optional[Tuple[float, float]]:
    """M3: Lo-MacKinlay Variance Ratio VR(q=4, n=24) >= 1.28 on 1H BTC-neutral residuals."""
    if s == "BTCUSDT" or i < 200:
        return None
    btc = ds1["series"]["BTCUSDT"]
    n, q = 24, 4
    beta = _shrink_beta_resid(d["r"], btc["r"], i - n, 168)
    es = [d["r"][k] - beta * btc["r"][k] for k in range(i - n + 1, i + 1)]
    mu = sum(es) / n
    var1 = sum((x - mu) ** 2 for x in es) / (n - 1)
    if var1 <= 1e-12:
        return None
    m = q * (n - q + 1) * (1.0 - q / n)
    q_sums = [sum(es[k: k + q]) - q * mu for k in range(n - q + 1)]
    varq = sum(v * v for v in q_sums) / max(1e-9, m)
    vr = varq / var1
    if vr < 1.28:
        return None

    e12 = es[-12:]
    s12 = sum(e12)
    if s12 <= 0:
        return None
    z12 = s12 / (math.sqrt(12.0 * var1) + 1e-10)
    if z12 < 1.80:
        return None
    if (max(e12) / (s12 + 1e-9)) > 0.40:
        return None
    if sum(d["tbr"][i - 2: i + 1]) / 3.0 < 0.53:
        return None

    atr_pct = d["atr24"][i] / max(1e-9, d["c"][i])
    stop_dist = min(0.08, max(s_min, 2.2 * atr_pct))
    if fee_rt / stop_dist > 0.12:  # Report #10 Cost/Stop <= 12% law!
        return None
    return (vr * z12, stop_dist)


# ==============================================================================
# 4. PORTFOLIO SIMULATOR (PIECEWISE VOLATILITY-RATCHET + ANTI-CONCENTRATION)
# ==============================================================================

def simulate_nextgen(
    ds: dict,
    fee_rt: float,
    signal_fn,
    is_4h: bool,
    use_dqp: bool = True,
    use_macro: bool = True,
    use_ratchet: bool = True,
    exclude_syms: Optional[Set[str]] = None,
    random_rate: float = 0.0,
) -> dict:
    T = ds["T"]
    ts_arr = ds["ts"]
    ser = ds["series"]
    gamma_arr = ds["gamma"]
    excl = exclude_syms or set()
    active_syms = [s for s in ds["syms"] if s not in excl and s != "BTCUSDT"]

    eq = [1.0] * T
    cash = 1.0
    open_pos = []
    trades = []
    start_idx = 110 if is_4h else 220
    max_slots = 4
    dead_bars = 12 if is_4h else 18
    max_hold = 36 if is_4h else 72
    cd_ms = 24 * 3600000
    roll30_ms = 30 * 86400000
    last_exit_ts: Dict[str, int] = {}
    coin_trade_ts: Dict[str, List[int]] = {}
    rng = random.Random(42)
    s_min = 0.042 if fee_rt >= 0.0045 else 0.030

    for i in range(start_idx, T):
        ts = ts_arr[i]

        # 1) Manage open positions (Strict Stop-First + Gap-Down Protection)
        for p in range(len(open_pos) - 1, -1, -1):
            pos = open_pos[p]
            d = ser[pos["sym"]]
            if not d["valid"][i]:
                continue

            exit_px, reason = None, None
            if d["o"][i] <= pos["stop"]:
                exit_px, reason = d["o"][i], "STOP_GAP"
            elif d["l"][i] <= pos["stop"]:
                exit_px, reason = pos["stop"], ("RATCHET" if pos["stage"] > 0 else "STOP")
            elif not use_ratchet and d["h"][i] >= pos["tp_fixed"]:
                exit_px, reason = pos["tp_fixed"], "FIXED_TP"
            elif i - pos["entry_i"] >= max_hold:
                exit_px, reason = d["c"][i], "TIME"
            elif i == T - 1:
                exit_px, reason = d["c"][i], "END_CLOSE"

            # Asymmetric Dead-Trade Exit (NEVER cuts winners!)
            if exit_px is None and use_ratchet:
                held = i - pos["entry_i"]
                if held >= dead_bars:
                    m_b = (max(pos["hh"], d["h"][i]) - pos["entry_px"]) / pos["r0"]
                    c_r = (d["c"][i] - pos["entry_px"]) / pos["r0"]
                    if m_b < 0.75 and c_r < 0.30:
                        exit_px, reason = d["c"][i], "DEAD_TIME"

            if exit_px is not None:
                gross = (exit_px - pos["entry_px"]) / pos["entry_px"]
                net = (1.0 + gross) * (1.0 - fee_rt) - 1.0
                cash += pos["weight"] * (1.0 + net)
                trades.append({
                    "sym": pos["sym"].replace("USDT", ""),
                    "entry_i": pos["entry_i"], "exit_i": i,
                    "entry_ts": ts_arr[pos["entry_i"]], "exit_ts": ts,
                    "net": net, "reason": reason,
                })
                last_exit_ts[pos["sym"]] = ts
                open_pos.pop(p)
            else:
                # Causal Piecewise Volatility-Ratchet update AFTER bar i closes
                if d["h"][i] > pos["hh"]:
                    pos["hh"] = d["h"][i]
                if use_ratchet:
                    m_b = (pos["hh"] - pos["entry_px"]) / pos["r0"]
                    atr_cur = d["atr14"][i] if is_4h else d["atr24"][i]
                    cand_stop = pos["stop"]
                    if m_b >= 4.5:
                        pos["stage"] = 3
                        cand_stop = pos["hh"] - 1.6 * atr_cur
                    elif m_b >= 2.5:
                        pos["stage"] = max(pos["stage"], 2)
                        cand_stop = pos["hh"] - 2.2 * atr_cur
                    elif m_b >= 1.2:
                        pos["stage"] = max(pos["stage"], 1)
                        cand_stop = max(pos["entry_px"] * (1.0 + fee_rt + 0.001), pos["hh"] - 2.6 * atr_cur)
                    if cand_stop > pos["stop"]:
                        pos["stop"] = cand_stop

        # 2) MTM equity
        mtm = cash
        for pos in open_pos:
            d = ser[pos["sym"]]
            px = d["c"][i] if d["valid"][i] else pos["entry_px"]
            mtm += pos["weight"] * (px / pos["entry_px"])
        eq[i] = mtm

        # 3) Entries at bar i close -> fill strictly at bar i+1 OPEN
        gamma = gamma_arr[i] if use_macro else 1.0
        if i < T - 2 and len(open_pos) < max_slots and gamma > 0.0:
            held = {x["sym"] for x in open_pos}
            cands = []
            for s in active_syms:
                if s in held:
                    continue
                d = ser[s]
                if not d["valid"][i] or not d["valid"][i + 1]:
                    continue
                if use_dqp and d["dqp"][i] != 1:
                    continue
                # Anti-Concentration: 24h cooldown + max 3 trades/coin/30d
                if (ts - last_exit_ts.get(s, 0)) < cd_ms:
                    continue
                recent_30 = [t for t in coin_trade_ts.get(s, []) if ts - t < roll30_ms]
                coin_trade_ts[s] = recent_30
                if len(recent_30) >= 3:
                    continue

                if random_rate > 0.0:
                    if rng.random() < random_rate:
                        atr_p = (d["atr14"][i] if is_4h else d["atr24"][i]) / max(1e-9, d["c"][i])
                        cands.append((rng.random(), s, min(0.085, max(s_min, 2.2 * atr_p))))
                else:
                    res_sig = signal_fn(s, d, i, ds)
                    if res_sig is not None:
                        cands.append((res_sig[0], s, res_sig[1]))

            cands.sort(key=lambda x: x[0], reverse=True)
            for _, s, s_dist in cands[: max_slots - len(open_pos)]:
                d = ser[s]
                entry_px = d["o"][i + 1]
                if entry_px <= 0:
                    continue
                r0 = entry_px * s_dist
                stop_px = entry_px - r0
                # Volatility-based risk sizing (0.75% * Gamma / (S + fee), max 20% NAV)
                w_target = min(0.20, (0.0075 * gamma) / (s_dist + fee_rt))
                alloc = min(cash, mtm * w_target)
                if alloc <= 1e-6:
                    break
                cash -= alloc
                coin_trade_ts.setdefault(s, []).append(ts)
                open_pos.append({
                    "sym": s, "entry_i": i + 1, "entry_px": entry_px,
                    "stop": stop_px, "r0": r0, "hh": entry_px, "stage": 0,
                    "tp_fixed": entry_px + 2.2 * r0, "weight": alloc,
                })

    return {"eq": eq, "trades": trades}


def summarize(label: str, res: dict, ds: dict, start_idx: int, bpy: float) -> dict:
    tr, eq = res["trades"], res["eq"]
    n = len(tr)
    split_idx = int(start_idx + (ds["T"] - start_idx) * 0.70)
    days = max(1.0, (ds["ts"][-1] - ds["ts"][start_idx]) / 86400000.0)
    per_yr = (n / days) * 365.0
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
    sh = (mr / math.sqrt(vr)) * math.sqrt(bpy) if vr > 0 else 0.0
    ret = (eq[-1] - 1.0) * 100.0

    verdict = "NO-GO"
    if n >= 50 and pf >= 1.30 and oos_pf >= 1.25 and ex5 >= 1.15 and max_dd > -0.18 and ret > 0:
        verdict = "GO"
    elif n >= 30 and pf >= 1.15 and oos_pf >= 1.10 and ret > 0:
        verdict = "WATCH"

    return {
        "label": label, "verdict": verdict, "n": n, "per_yr": per_yr, "wr": wr,
        "pf": pf, "ex5": ex5, "oos_pf": oos_pf, "oos_n": len(oos),
        "ret": ret, "dd": max_dd * 100.0, "sh": sh, "wl": wl,
    }


def fmt_row(s: dict) -> str:
    return (
        f"{s['label']:24s} [{s['verdict']:5s}] N={s['n']:4d} ({s['per_yr']:4.0f}/y) "
        f"WR={s['wr']:4.1f}% PF={s['pf']:.2f} Ex5={s['ex5']:.2f} "
        f"OOS={s['oos_pf']:.2f}({s['oos_n']:3d}) Ret={s['ret']:+6.1f}% "
        f"DD={s['dd']:6.1f}% Sh={s['sh']:+5.2f} W/L={s['wl']:.2f}x"
    )


# ==============================================================================
# 5. BUILT-IN OFFLINE SELF-TEST (6/6 DETERMINISTIC CHECKS)
# ==============================================================================

def run_self_test() -> bool:
    T = 560
    t0 = int(dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)
    raw_k, dv, cb = {}, {}, {}
    for s_idx, sym in enumerate(["BTCUSDT", "GOODUSDT", "WASHUSDT", "SPIKEUSDT"]):
        km = {}
        px = 100.0 * (s_idx + 1)
        for i in range(T):
            ts = t0 + i * 3600000
            chg = 0.008 if (sym == "GOODUSDT" and 300 <= i <= 420) else math.sin((i + s_idx) * 0.1) * 0.008
            o = px
            c = max(5.0, o * (1.0 + chg))
            h, l = max(o, c) * 1.004, min(o, c) * 0.996
            if sym == "WASHUSDT":
                qv, nt = 200000.0, 8000.0  # Avg trade size $25 -> MUST be blocked by [$80..$15K]!
            elif sym == "SPIKEUSDT" and i == 350:
                qv, nt = 5000000.0, 1000.0
                c = o * 1.15; h = o * 1.48  # +15% blowoff with 68% upper wick -> COOLDOWN (-1)!
            else:
                qv, nt = 350000.0, 800.0  # Avg trade size $437 -> clean liquid coin!
            km[ts] = {"t": ts, "o": o, "h": h, "l": l, "c": c, "qv": qv, "nt": nt, "tbqv": qv * 0.58}
            if sym == "BTCUSDT":
                dv[ts] = 45.0; cb[ts] = c * 1.0005
            px = c
        raw_k[sym] = km

    ds1, ds4 = build_datasets(raw_k, dv, cb)
    checks = []
    # 1) Schmitt DQP promotes GOODUSDT, blocks WASHUSDT ($25 avg trade), punishes SPIKEUSDT (-1)
    checks.append(ds1["series"]["GOODUSDT"]["dqp"][400] == 1)
    checks.append(ds1["series"]["WASHUSDT"]["dqp"][400] == 0)
    checks.append(ds1["series"]["SPIKEUSDT"]["dqp"][350] == -1)
    # 2) All 3 engines run cleanly with finite stats
    r1 = simulate_nextgen(ds4, 0.0026, lambda s, d, i, ds: sig_m1_rf_tcs_4h(s, d, i, ds, 0.030), True)
    r2 = simulate_nextgen(ds1, 0.0026, lambda s, d, i, ds: sig_m2_paac_1h(s, d, i, ds, 0.030), False)
    r3 = simulate_nextgen(ds1, 0.0026, lambda s, d, i, ds: sig_m3_vr_persist_1h(s, d, i, ds, 0.030, 0.0026), False)
    checks.append(all(math.isfinite(summarize("T", r, ds, si, bpy)["pf"]) for r, ds, si, bpy in [(r1, ds4, 110, 2190.0), (r2, ds1, 220, 8760.0), (r3, ds1, 220, 8760.0)]))
    # 3) Gap-Down Stop + Next-Open check
    gap_k = {"BTCUSDT": raw_k["BTCUSDT"], "GAPUSDT": {}}
    for i in range(T):
        ts = t0 + i * 3600000
        o, h, l, c = (82.0, 83.0, 81.0, 82.0) if i == 252 else (100.0, 101.0, 99.0, 100.5)
        gap_k["GAPUSDT"][ts] = {"t": ts, "o": o, "h": h, "l": l, "c": c, "qv": 350000.0, "nt": 800.0, "tbqv": 200000.0}
    gds1, _ = build_datasets(gap_k, dv, cb)
    gres = simulate_nextgen(gds1, 0.0026, lambda s, d, i, ds: (1.0, 0.03) if (s == "GAPUSDT" and i == 250) else None, False, use_dqp=False, use_macro=False)
    checks.append(len(gres["trades"]) == 1 and gres["trades"][0]["entry_i"] == 251 and gres["trades"][0]["reason"] == "STOP_GAP")
    # 4) Zero-trade safety
    zres = simulate_nextgen(gds1, 0.0026, lambda s, d, i, ds: None, False)
    zst = summarize("ZERO", zres, gds1, 220, 8760.0)
    checks.append(zst["n"] == 0 and zst["pf"] == 0.0 and zst["verdict"] == "NO-GO")

    passed = sum(1 for c in checks if c)
    print(f"S8_NEXTGEN_SELF_TEST: {passed}/{len(checks)} PASS")
    return passed == len(checks)


def main():
    ap = argparse.ArgumentParser(description="OMEGA S8 Next-Gen 1H/4H Institutional Lab v1.0")
    ap.add_argument("--days", type=int, default=730)
    ap.add_argument("--out", type=str, default="OMEGA_S8_NEXTGEN_REPORT.txt")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        sys.exit(0 if run_self_test() else 1)
    if requests is None:
        print("ERROR: requests required")
        sys.exit(1)

    end_dt = utc_now().replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=1)
    start_dt = end_dt - dt.timedelta(days=args.days)
    print(f"[OMEGA S8 NEXT-GEN v{VERSION}] Fetching {args.days}d 1H spot data for {len(UNIVERSE_120)} coins...")

    sess = _get_session()
    raw_k: Dict[str, Dict[int, dict]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        futs = {ex.submit(fetch_spot_1h_klines, sess, sym, start_dt, end_dt): sym for sym in UNIVERSE_120}
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
            if done % 20 == 0 or done == len(UNIVERSE_120):
                print(f"  Fetched {done}/{len(UNIVERSE_120)} coins (valid={len(raw_k)})")

    print("  Fetching Deribit BTC DVOL & Coinbase BTC-USD 1H...")
    dvol_1h = fetch_deribit_dvol_1h(sess, start_dt, end_dt)
    cb_1h = fetch_coinbase_btc_1h(sess, start_dt, end_dt)

    if "BTCUSDT" not in raw_k or len(raw_k["BTCUSDT"]) < 600:
        print("ERROR: Insufficient BTCUSDT 1H bars.")
        sys.exit(1)

    ds1, ds4 = build_datasets(raw_k, dvol_1h, cb_1h)
    btc_c = ds4["series"]["BTCUSDT"]["c"]
    btc_ret = (btc_c[-1] / btc_c[110] - 1.0) * 100.0
    avg_act = sum(sum(1 for s in ds1["syms"] if ds1["series"][s]["dqp"][i] == 1) for i in range(220, ds1["T"], 24)) / max(1, len(range(220, ds1["T"], 24)))
    g_zero_pct = sum(1 for g in ds4["gamma"][110:] if g == 0.0) / max(1, ds4["T"] - 110) * 100.0
    g_half_pct = sum(1 for g in ds4["gamma"][110:] if g == 0.5) / max(1, ds4["T"] - 110) * 100.0

    lines = [
        "=" * 94,
        f"OMEGA S8 NEXT-GEN 1H/4H INSTITUTIONAL LAB v{VERSION} ({utc_now():%Y-%m-%d})",
        f"Universe: {len(ds1['syms'])}/{len(UNIVERSE_120)} coins | Avg DQP ACTIVE/day: {avg_act:.1f} | Bars: {ds1['T']}(1H) / {ds4['T']}(4H)",
        f"Span: {fmt_ts(ds4['ts'][110])} .. {fmt_ts(ds4['ts'][-1])} | BTC B&H: {btc_ret:+.1f}% | Macro Shield: 0%={g_zero_pct:.1f}% 50%={g_half_pct:.1f}%",
        "Execution: Strict Next-Open, Stop-First (Gap-Protected), S_min>=3.0%/4.2%, Piecewise Ratchet, 70/30 IS/OOS",
        "=" * 94,
        "",
        "--- TABLE 1: 4-FEE-TIER STRESS MATRIX (0.26% / 0.36% / 0.50% / 0.65%) ---",
    ]

    primary_026 = {}
    for fee in [0.0026, 0.0036, 0.0050, 0.0065]:
        s_min = 0.042 if fee >= 0.0045 else 0.030
        r_m1 = simulate_nextgen(ds4, fee, lambda s, d, i, ds, sm=s_min: sig_m1_rf_tcs_4h(s, d, i, ds, sm, True), True)
        r_m2 = simulate_nextgen(ds1, fee, lambda s, d, i, ds, sm=s_min: sig_m2_paac_1h(s, d, i, ds, sm), False)
        r_m3 = simulate_nextgen(ds1, fee, lambda s, d, i, ds, sm=s_min, fr=fee: sig_m3_vr_persist_1h(s, d, i, ds, sm, fr), False)
        if fee == 0.0026:
            primary_026 = {
                "M1_RF_TCS_4H":     (r_m1, ds4, 110, 2190.0, True),
                "M2_PAAC_1H":       (r_m2, ds1, 220, 8760.0, False),
                "M3_VR_PERSIST_1H": (r_m3, ds1, 220, 8760.0, False),
            }
        lines.append(f"  [Fee {fee*100:.2f}% | S_min={s_min*100:.1f}%]")
        lines.append("  " + fmt_row(summarize("M1_RF_TCS_4H", r_m1, ds4, 110, 2190.0)))
        lines.append("  " + fmt_row(summarize("M2_PAAC_1H", r_m2, ds1, 220, 8760.0)))
        lines.append("  " + fmt_row(summarize("M3_VR_PERSIST_1H", r_m3, ds1, 220, 8760.0)))
        lines.append("")

    # Pick #1 engine at 0.26% for Deep Dive + Strict Ablation
    ranked = sorted([(summarize(k, v[0], v[1], v[2], v[3]), k) for k, v in primary_026.items()], key=lambda x: x[0]["pf"], reverse=True)
    best_st, best_name = ranked[0]
    best_res, best_ds, best_si, best_bpy, best_is4h = primary_026[best_name]

    lines.extend([f"--- TABLE 2: STRICT ABLATION & RANDOM CONTROL ON #1 ENGINE ({best_name}, 0.26% RT) ---"])
    def _run_best(use_dqp=True, use_macro=True, use_ratchet=True, use_res=True, excl=None, rand_rate=0.0):
        if best_name == "M1_RF_TCS_4H":
            fn = lambda s, d, i, ds: sig_m1_rf_tcs_4h(s, d, i, ds, 0.030, use_res)
        elif best_name == "M2_PAAC_1H":
            fn = lambda s, d, i, ds: sig_m2_paac_1h(s, d, i, ds, 0.030)
        else:
            fn = lambda s, d, i, ds: sig_m3_vr_persist_1h(s, d, i, ds, 0.030, 0.0026)
        return simulate_nextgen(best_ds, 0.0026, fn, best_is4h, use_dqp=use_dqp, use_macro=use_macro, use_ratchet=use_ratchet, exclude_syms=excl, random_rate=rand_rate)

    lines.append("  " + fmt_row(best_st))
    lines.append("  " + fmt_row(summarize("ABL_NO_DQP", _run_best(use_dqp=False), best_ds, best_si, best_bpy)))
    lines.append("  " + fmt_row(summarize("ABL_NO_MACRO_SHIELD", _run_best(use_macro=False), best_ds, best_si, best_bpy)))
    if best_name == "M1_RF_TCS_4H":
        lines.append("  " + fmt_row(summarize("ABL_NO_RESIDUAL_RAW", _run_best(use_res=False), best_ds, best_si, best_bpy)))
    lines.append("  " + fmt_row(summarize("ABL_FIXED_EXIT_2.2R", _run_best(use_ratchet=False), best_ds, best_si, best_bpy)))
    # Random-Entry Control inside DQP ACTIVE
    lines.append("  " + fmt_row(summarize("RANDOM_IN_DQP_CTRL", _run_best(rand_rate=0.003), best_ds, best_si, best_bpy)))

    # Deep Dive: Half-Year + Leave-Best-Month-Out + Per-Coin + Leave-2-Out
    lines.extend(["", f"--- TABLE 3: REGIMES, CONCENTRATION & LEAVE-2-OUT ({best_name}, 0.26% RT) ---"])
    by_half: Dict[str, List[float]] = {}
    by_month: Dict[str, List[float]] = {}
    by_coin: Dict[str, List[float]] = {}
    for x in best_res["trades"]:
        d_dt = dt.datetime.fromtimestamp(x["entry_ts"] / 1000.0, tz=dt.timezone.utc)
        by_half.setdefault(f"{d_dt.year}-H{1 if d_dt.month <= 6 else 2}", []).append(x["net"])
        by_month.setdefault(f"{d_dt.year}-{d_dt.month:02d}", []).append(x["net"])
        by_coin.setdefault(x["sym"], []).append(x["net"])

    for k in sorted(by_half.keys()):
        arr = by_half[k]
        gp, gl = sum(v for v in arr if v > 0), sum(abs(v) for v in arr if v <= 0)
        lines.append(f"  {k}: N={len(arr):3d} WR={sum(1 for v in arr if v > 0)/len(arr)*100:4.1f}% PF={gp/gl if gl>0 else 99.0:.2f} SumNet={sum(arr)*100:+6.1f}%")

    if by_month:
        best_m = max(by_month.items(), key=lambda kv: sum(kv[1]))[0]
        rem = [x["net"] for x in best_res["trades"] if dt.datetime.fromtimestamp(x["entry_ts"]/1000.0, tz=dt.timezone.utc).strftime("%Y-%m") != best_m]
        gp, gl = sum(v for v in rem if v > 0), sum(abs(v) for v in rem if v <= 0)
        lines.append(f"  Leave-Best-Month-Out ({best_m} dropped): N={len(rem)} PF={gp/gl if gl>0 else 99.0:.2f} SumNet={sum(rem)*100:+6.1f}%")

    coin_ranked = sorted(by_coin.items(), key=lambda kv: sum(kv[1]), reverse=True)
    lines.append("  TOP 8 COINS:")
    for sym, arr in coin_ranked[:8]:
        gp, gl = sum(v for v in arr if v > 0), sum(abs(v) for v in arr if v <= 0)
        lines.append(f"    {sym:8s}: N={len(arr):3d} PF={gp/gl if gl>0 else 99.0:.2f} SumNet={sum(arr)*100:+6.1f}%")

    top2 = {f"{coin_ranked[k][0]}USDT" for k in range(min(2, len(coin_ranked)))}
    l2_res = _run_best(excl=top2)
    lines.append(f"  Dropped Top-2 Coins: {sorted(top2)}")
    lines.append("  " + fmt_row(summarize(f"{best_name}_L2O", l2_res, best_ds, best_si, best_bpy)))

    report = "\n".join(lines)
    print("\n" + report)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    print(f"\nSaved S8 Next-Gen report to {args.out}")


if __name__ == "__main__":
    main()
