#!/usr/bin/env python3
"""
OMEGA S7 — F3_OI_FRESH_TREND_QDECAY 2-YEAR TORTURE & ABLATION AUDIT v1.0
========================================================================
Runs 4 institutional certification batteries on the 1H champion candidate
F3_OI_FRESH_TREND_QDECAY across 730 days (2 full years, ~17,520 1H bars):
  1. 3-Fee-Tier Stress Test (0.26% USDT / 0.50% IRT / 0.75% Stress) + Half-Year Regimes
  2. Exit Ablation Matrix (FULL vs NO_Q_STALL / NO_Q_FLOW / NO_Q_BTC / NO_OI_EXHAUST / BASE)
     + Per-Exit-Reason PnL Attribution
  3. Per-Coin Attribution + Leave-2-Out Universe Torture
  4. 3x3 Parameter Plateau (HH_LEN x OI_6H_MIN) Anti-Overfitting Grid
  5. Live Endpoint Feasibility Probe (Binance Vision / Binance FAPI / OKX / Deribit)

Zero API keys required. Strictly causal Next-Open, Stop-First (Gap-Protected), 70% ATR Floor.
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
    if HTTPAdapter is not None:
        ad = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=2)
        s.mount("https://", ad)
        s.mount("http://", ad)
    s.headers.update({"User-Agent": f"OMEGA-S7-F3-Audit/{VERSION}"})
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


def _fetch_zip_csv_rows(sess, url: str, timeout: int = 18) -> List[List[str]]:
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
# 1. FREE ARCHIVE FETCHERS (ONLY WHAT F3 NEEDS -> 2X FASTER FOR 730 DAYS!)
# ==============================================================================

def fetch_spot_1h_klines(sym: str, start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, dict]:
    sess = _get_session()
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

    # Tail fill current incomplete month via REST (data-api.binance.vision has no US geo-block)
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


def fetch_futures_oi_1h(sym: str, start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, float]:
    """Fetch 5m futures metrics daily ZIPs and extract hourly OI (USD)."""
    sess = _get_session()
    hourly: Dict[int, float] = {}
    days = _day_list(start_dt, end_dt)

    def _one_day(d: dt.date):
        url = f"{BINANCE_VISION}/futures/um/daily/metrics/{sym}/{sym}-metrics-{d:%Y-%m-%d}.zip"
        out = {}
        for row in _fetch_zip_csv_rows(sess, url, timeout=12):
            if len(row) < 4 or not row[0] or row[0] == "create_time":
                continue
            try:
                t_dt = dt.datetime.strptime(row[0][:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
                hr_ms = int(t_dt.replace(minute=0, second=0).timestamp() * 1000)
                if row[3]:
                    out[hr_ms] = float(row[3])
            except Exception:
                continue
        return out

    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as ex:
        for res in ex.map(_one_day, days):
            hourly.update(res)
    return hourly


def fetch_deribit_dvol_1h(start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[int, float]:
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
                        out[int(row[0])] = float(row[4])
        except Exception:
            pass
        cur_ms = nxt_ms
        time.sleep(0.10)
    return out


def probe_live_endpoints() -> List[str]:
    """Check which live OI/Klines/DVOL endpoints work directly from GitHub Actions runner."""
    sess = _get_session()
    probes = [
        ("Binance Vision Spot 1H", "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=2"),
        ("Binance FAPI Live OI",   "https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=2"),
        ("OKX Live OI Fallback",   "https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-history?instId=BTC-USDT-SWAP&period=1H&limit=2"),
        ("Deribit Live DVOL",      "https://www.deribit.com/api/v2/public/get_volatility_index_data?currency=BTC&resolution=3600&start_timestamp=1700000000000&end_timestamp=1700010000000"),
    ]
    lines = []
    for label, url in probes:
        try:
            r = sess.get(url, timeout=8)
            lines.append(f"  {label:24s}: HTTP {r.status_code} ({'OK' if r.status_code == 200 else 'BLOCKED/ERR'})")
        except Exception as e:
            lines.append(f"  {label:24s}: ERROR ({type(e).__name__})")
    return lines


# ==============================================================================
# 2. CAUSAL INDICATORS & DATASET BUILDER
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
    """Strictly prior N-bar high (excludes bar i -> zero look-ahead)."""
    T = len(h)
    out = [0.0] * T
    for i in range(T):
        out[i] = max(h[max(0, i - length):i]) if i > 0 else h[0]
    return out


def build_dataset(raw_k: Dict[str, Dict[int, dict]], raw_oi: Dict[str, Dict[int, float]], dvol_map: Dict[int, float]) -> dict:
    btc_ts = sorted(raw_k["BTCUSDT"].keys())
    T = len(btc_ts)
    syms = [s for s in raw_k.keys() if len(raw_k[s]) >= int(T * 0.65)]
    series = {}

    for s in syms:
        km = raw_k[s]
        om = raw_oi.get(s, {})
        o, h, l, c, qv, tbr, avg_sz, oi, valid = [], [], [], [], [], [], [], [], []
        last_c, last_oi = None, 0.0
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
            if ts in om:
                last_oi = om[ts]
            oi.append(last_oi)

        ema12 = calc_ema(c, 12)
        ema24 = calc_ema(c, 24)
        atr14 = calc_atr(h, l, c, 14)
        atr48 = calc_sma(atr14, 48)
        vol20 = calc_sma(qv, 20)
        avg_sz24 = calc_sma(avg_sz, 24)
        sz_ratio = [avg_sz[i] / max(1e-9, avg_sz24[i]) for i in range(T)]

        series[s] = {
            "o": o, "h": h, "l": l, "c": c, "qv": qv, "tbr": tbr, "valid": valid,
            "ema12": ema12, "ema24": ema24, "atr14": atr14, "atr48": atr48,
            "vol20": vol20, "oi": oi, "sz_ratio": sz_ratio,
            "hh36": calc_hh(h, 36), "hh48": calc_hh(h, 48), "hh60": calc_hh(h, 60),
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
                if series[s]["c"][i] > series[s]["ema24"][i]:
                    above += 1
        if cnt > 0:
            breadth_ema[i] = above / cnt

    return {"ts": btc_ts, "T": T, "syms": syms, "series": series, "dvol": dvol, "breadth_ema": breadth_ema}


# ==============================================================================
# 3. F3 CONFIGURABLE SIMULATOR (FOR ABLATION, LEAVE-2-OUT, AND PLATEAU)
# ==============================================================================

def run_f3(
    ds: dict,
    fee_rt: float = 0.0026,
    hh_key: str = "hh48",
    oi_6h_min: float = 0.012,
    q_stall: bool = True,
    q_flow: bool = True,
    q_btc: bool = True,
    q_oi_exhaust: bool = True,
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
    start_idx = 80
    max_slots = 3

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
            elif i - pos["entry_i"] >= 48:
                exit_px = d["c"][i]
                reason = "TIME"
            elif i == T - 1:
                exit_px = d["c"][i]
                reason = "END_CLOSE"

            if exit_px is None:
                held_h = i - pos["entry_i"]
                cur_mfe = (max(pos["hh"], d["h"][i]) - pos["entry_px"]) / pos["entry_px"]
                cur_ret = (d["c"][i] - pos["entry_px"]) / pos["entry_px"]
                if q_stall and held_h >= 5 and cur_mfe < 0.008 and cur_ret < 0.004:
                    exit_px, reason = d["c"][i], "Q_STALL"
                elif q_flow and held_h >= 2 and d["tbr"][i] < 0.45 and d["tbr"][i - 1] < 0.45:
                    exit_px, reason = d["c"][i], "Q_FLOW"
                elif q_btc and held_h >= 1 and btc["c"][i] < btc["ema12"][i] and btc["tbr"][i] < 0.45:
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
                if not pos["be"] and gain >= 0.022:
                    pos["be"] = True
                    pos["stop"] = max(pos["stop"], pos["entry_px"] * (1.0 + fee_rt + 0.001))
                if pos["be"]:
                    eff_atr = max(d["atr14"][i], 0.70 * d["atr48"][i])
                    trail_mult = 2.4
                    if q_oi_exhaust and gain >= 0.02 and d["oi"][i - 3] > 0:
                        if (d["oi"][i] - d["oi"][i - 3]) / d["oi"][i - 3] < -0.015:
                            trail_mult = 1.2
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

        # 3) F3 Entry at bar i close -> fill at bar i+1 OPEN
        if i < T - 2 and len(open_pos) < max_slots:
            if btc["c"][i] > btc["ema24"][i] and ds["breadth_ema"][i] >= 0.55 and ds["dvol"][i] <= 65.0:
                held_set = {x["sym"] for x in open_pos}
                cands = []
                for s in active_syms:
                    if s in held_set:
                        continue
                    d = ser[s]
                    if not d["valid"][i] or not d["valid"][i + 1]:
                        continue
                    hh_val = d[hh_key][i]
                    if d["c"][i] <= hh_val or hh_val <= 0:
                        continue
                    if d["oi"][i - 6] > 0:
                        if (d["oi"][i] - d["oi"][i - 6]) / d["oi"][i - 6] < oi_6h_min:
                            continue
                    if d["tbr"][i] < 0.54 or d["sz_ratio"][i] < 1.15 or d["qv"][i] < 1.3 * d["vol20"][i]:
                        continue
                    sc = (d["c"][i] / hh_val - 1.0) + d["sz_ratio"][i]
                    cands.append((sc, s))

                cands.sort(key=lambda x: x[0], reverse=True)
                for _, s in cands[: max_slots - len(open_pos)]:
                    d = ser[s]
                    entry_px = d["o"][i + 1]
                    if entry_px <= 0:
                        continue
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
                        "stop": stop_px, "be": False, "hh": entry_px, "weight": alloc,
                    })

    return {"eq": eq, "trades": trades}


def summarize(label: str, res: dict, ds: dict) -> dict:
    tr, eq = res["trades"], res["eq"]
    n = len(tr)
    split_idx = int(80 + (ds["T"] - 80) * 0.70)
    days = max(1.0, (ds["ts"][-1] - ds["ts"][80]) / 86400000.0)
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

    peak, max_dd, rets = eq[80], 0.0, []
    for k in range(81, len(eq)):
        if eq[k] > peak:
            peak = eq[k]
        dd = (eq[k] - peak) / peak
        if dd < max_dd:
            max_dd = dd
        if eq[k - 1] > 0:
            rets.append((eq[k] - eq[k - 1]) / eq[k - 1])
    mr = sum(rets) / max(1, len(rets))
    vr = sum((r - mr) ** 2 for r in rets) / max(1, len(rets))
    sh = (mr / math.sqrt(vr)) * math.sqrt(8760.0) if vr > 0 else 0.0
    ret = (eq[-1] - 1.0) * 100.0

    verdict = "NO-GO"
    if n >= 60 and pf >= 1.25 and oos_pf >= 1.20 and ex5 >= 1.10 and max_dd > -0.18 and ret > 0:
        verdict = "GO"
    elif n >= 40 and pf >= 1.12 and oos_pf >= 1.05 and ret > 0:
        verdict = "WATCH"

    return {
        "label": label, "verdict": verdict, "n": n, "per_mo": per_mo, "wr": wr,
        "pf": pf, "ex5": ex5, "oos_pf": oos_pf, "oos_n": len(oos),
        "ret": ret, "dd": max_dd * 100.0, "sh": sh, "wl": wl,
    }


def fmt_row(s: dict) -> str:
    return (
        f"{s['label']:22s} [{s['verdict']:5s}] N={s['n']:4d} ({s['per_mo']:4.0f}/m) "
        f"WR={s['wr']:4.1f}% PF={s['pf']:.2f} Ex5={s['ex5']:.2f} "
        f"OOS={s['oos_pf']:.2f}({s['oos_n']:3d}) Ret={s['ret']:+6.1f}% "
        f"DD={s['dd']:6.1f}% Sh={s['sh']:+5.2f} W/L={s['wl']:.2f}x"
    )


def run_self_test() -> bool:
    T = 240
    t0 = int(dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)
    raw_k, raw_oi, dv = {}, {}, {}
    for s_idx, sym in enumerate(["BTCUSDT", "ETHUSDT", "SOLUSDT", "ZECUSDT"]):
        km, om = {}, {}
        px, oi = 100.0 * (s_idx + 1), 1e8
        for i in range(T):
            ts = t0 + i * 3600000
            wave = math.sin((i + s_idx) * 0.11) * 0.015 + (0.004 if 90 < i < 180 else -0.001)
            o = px
            c = max(5.0, o * (1.0 + wave))
            h, l = max(o, c) * 1.008, min(o, c) * 0.992
            qv = 2e6 * (1.8 if i % 4 == 0 else 1.0)
            km[ts] = {"t": ts, "o": o, "h": h, "l": l, "c": c, "qv": qv, "nt": 900.0, "tbqv": qv * 0.60}
            oi *= 1.005 if c > o else 0.995
            om[ts] = oi
            if sym == "BTCUSDT":
                dv[ts] = 45.0
            px = c
        raw_k[sym], raw_oi[sym] = km, om

    ds = build_dataset(raw_k, raw_oi, dv)
    checks = []
    # 1) Dataset & strictly causal hh48
    checks.append(ds["T"] == T and len(ds["syms"]) == 4)
    btc = ds["series"]["BTCUSDT"]
    checks.append(abs(btc["hh48"][100] - max(btc["h"][52:100])) < 1e-9)
    # 2) Full F3 + ablation + Leave-2-Out + plateau all run cleanly
    r_full = run_f3(ds, fee_rt=0.0026)
    s_full = summarize("FULL", r_full, ds)
    checks.append(math.isfinite(s_full["pf"]) and math.isfinite(s_full["sh"]))
    r_l2 = run_f3(ds, fee_rt=0.0026, exclude_syms={"ETHUSDT", "ZECUSDT"})
    checks.append(all(x["sym"] not in {"ETH", "ZEC"} for x in r_l2["trades"]))
    r_p = run_f3(ds, fee_rt=0.0026, hh_key="hh60", oi_6h_min=0.016, q_stall=False)
    checks.append(len(r_p["eq"]) == T)
    # 3) Zero-trade safety
    r_z = run_f3(ds, fee_rt=0.0026, oi_6h_min=9.9)
    s_z = summarize("ZERO", r_z, ds)
    checks.append(s_z["n"] == 0 and s_z["pf"] == 0.0 and s_z["verdict"] == "NO-GO")

    passed = sum(1 for c in checks if c)
    print(f"F3_TORTURE_SELF_TEST: {passed}/{len(checks)} PASS")
    return passed == len(checks)


def main():
    ap = argparse.ArgumentParser(description="OMEGA S7 F3 2-Year Torture & Ablation Audit v1.0")
    ap.add_argument("--days", type=int, default=730)
    ap.add_argument("--out", type=str, default="OMEGA_S7_F3_TORTURE_REPORT.txt")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        sys.exit(0 if run_self_test() else 1)

    if requests is None:
        print("ERROR: requests required")
        sys.exit(1)

    end_dt = utc_now().replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=1)
    start_dt = end_dt - dt.timedelta(days=args.days)
    print(f"[OMEGA F3 TORTURE v{VERSION}] Fetching {args.days}d 1H data ({start_dt:%Y-%m-%d} -> {end_dt:%Y-%m-%d})...")

    raw_k, raw_oi = {}, {}
    for idx, sym in enumerate(UNIVERSE_15, 1):
        print(f"  ({idx:02d}/{len(UNIVERSE_15)}) {sym}: spot 1H + futures OI...")
        raw_k[sym] = fetch_spot_1h_klines(sym, start_dt, end_dt)
        raw_oi[sym] = fetch_futures_oi_1h(sym, start_dt, end_dt)

    print("  Fetching Deribit BTC DVOL 1H...")
    dvol_map = fetch_deribit_dvol_1h(start_dt, end_dt)
    ds = build_dataset(raw_k, raw_oi, dvol_map)

    btc_c = ds["series"]["BTCUSDT"]["c"]
    btc_ret = (btc_c[-1] / btc_c[80] - 1.0) * 100.0
    oi_cov = sum(1 for v in ds["series"]["BTCUSDT"]["oi"] if v > 0) / max(1, ds["T"]) * 100.0
    dv_cov = len(dvol_map) / max(1, ds["T"]) * 100.0

    lines = [
        "=" * 88,
        f"OMEGA S7 — F3_OI_FRESH_TREND_QDECAY 2-YEAR TORTURE AUDIT v{VERSION} ({utc_now():%Y-%m-%d})",
        f"Universe: {len(ds['syms'])} coins | Bars: {ds['T']} (1H) | Span: {fmt_ts(ds['ts'][80])} .. {fmt_ts(ds['ts'][-1])}",
        f"BTC Buy&Hold: {btc_ret:+.1f}% | Coverage: OI={oi_cov:.0f}% DVOL={dv_cov:.0f}%",
        "=" * 88,
        "",
        "--- BATTERY 1: 3-FEE-TIER STRESS TEST ---",
    ]

    base_res_026 = None
    for fee in [0.0026, 0.0050, 0.0075]:
        r = run_f3(ds, fee_rt=fee)
        if fee == 0.0026:
            base_res_026 = r
        lines.append(fmt_row(summarize(f"F3_QDECAY_{fee*100:.2f}%", r, ds)))

    # Half-Year Regime Breakdown on 0.26%
    lines.extend(["", "--- HALF-YEAR REGIME BREAKDOWN (0.26% RT) ---"])
    by_half: Dict[str, List[float]] = {}
    for x in base_res_026["trades"]:
        d = dt.datetime.fromtimestamp(x["entry_ts"] / 1000.0, tz=dt.timezone.utc)
        key = f"{d.year}-H{1 if d.month <= 6 else 2}"
        by_half.setdefault(key, []).append(x["net"])
    for key in sorted(by_half.keys()):
        arr = by_half[key]
        gp = sum(v for v in arr if v > 0)
        gl = sum(abs(v) for v in arr if v <= 0)
        pf = gp / gl if gl > 0 else 99.0
        wr = sum(1 for v in arr if v > 0) / len(arr) * 100.0
        lines.append(f"  {key}: N={len(arr):3d} WR={wr:4.1f}% PF={pf:.2f} SumNet={sum(arr)*100:+6.1f}%")

    # Battery 2: Exit Ablation + Per-Exit-Reason Breakdown
    lines.extend(["", "--- BATTERY 2: EXIT ABLATION MATRIX (0.26% RT) ---"])
    ablations = [
        ("FULL_QDECAY",   dict(q_stall=True,  q_flow=True,  q_btc=True,  q_oi_exhaust=True)),
        ("NO_Q_STALL",    dict(q_stall=False, q_flow=True,  q_btc=True,  q_oi_exhaust=True)),
        ("NO_Q_FLOW",     dict(q_stall=True,  q_flow=False, q_btc=True,  q_oi_exhaust=True)),
        ("NO_Q_BTC",      dict(q_stall=True,  q_flow=True,  q_btc=False, q_oi_exhaust=True)),
        ("NO_OI_EXHAUST", dict(q_stall=True,  q_flow=True,  q_btc=True,  q_oi_exhaust=False)),
        ("ONLY_Q_STALL",  dict(q_stall=True,  q_flow=False, q_btc=False, q_oi_exhaust=False)),
        ("ONLY_Q_BTC",    dict(q_stall=False, q_flow=False, q_btc=True,  q_oi_exhaust=False)),
        ("BASE_NO_QDECAY",dict(q_stall=False, q_flow=False, q_btc=False, q_oi_exhaust=False)),
    ]
    for lbl, kw in ablations:
        lines.append(fmt_row(summarize(lbl, run_f3(ds, fee_rt=0.0026, **kw), ds)))

    lines.extend(["", "--- PER-EXIT-REASON ATTRIBUTION (FULL_QDECAY 0.26%) ---"])
    by_reason: Dict[str, List[float]] = {}
    for x in base_res_026["trades"]:
        by_reason.setdefault(x["reason"], []).append(x["net"])
    for rsn, arr in sorted(by_reason.items(), key=lambda kv: sum(kv[1]), reverse=True):
        wr = sum(1 for v in arr if v > 0) / len(arr) * 100.0
        lines.append(f"  {rsn:12s}: N={len(arr):4d} WR={wr:4.1f}% Avg={sum(arr)/len(arr)*100:+5.2f}% SumNet={sum(arr)*100:+6.1f}%")

    # Battery 3: Per-Coin Attribution + Leave-2-Out
    lines.extend(["", "--- BATTERY 3: PER-COIN ATTRIBUTION & LEAVE-2-OUT (0.26% RT) ---"])
    by_coin: Dict[str, List[float]] = {}
    for x in base_res_026["trades"]:
        by_coin.setdefault(x["sym"], []).append(x["net"])
    coin_ranked = sorted(by_coin.items(), key=lambda kv: sum(kv[1]), reverse=True)
    for sym, arr in coin_ranked:
        gp = sum(v for v in arr if v > 0)
        gl = sum(abs(v) for v in arr if v <= 0)
        pf = gp / gl if gl > 0 else 99.0
        wr = sum(1 for v in arr if v > 0) / len(arr) * 100.0
        lines.append(f"  {sym:6s}: N={len(arr):3d} WR={wr:4.1f}% PF={pf:.2f} SumNet={sum(arr)*100:+6.1f}%")

    top2 = {f"{coin_ranked[i][0]}USDT" for i in range(min(2, len(coin_ranked)))}
    l2_res = run_f3(ds, fee_rt=0.0026, exclude_syms=top2)
    lines.append("")
    lines.append(f"  Dropped Top-2 Coins: {sorted(top2)}")
    lines.append("  " + fmt_row(summarize("LEAVE_2_OUT_0.26%", l2_res, ds)))

    # Battery 4: 3x3 Parameter Plateau
    lines.extend(["", "--- BATTERY 4: 3x3 PARAMETER PLATEAU (HH_LEN x OI_6H_MIN, 0.26% RT) ---"])
    pass_cnt = 0
    for hh_k in ["hh36", "hh48", "hh60"]:
        for oi_m in [0.008, 0.012, 0.016]:
            st = summarize(f"{hh_k}_OI{oi_m*100:.1f}%", run_f3(ds, fee_rt=0.0026, hh_key=hh_k, oi_6h_min=oi_m), ds)
            if st["pf"] >= 1.15 and st["ret"] > 0:
                pass_cnt += 1
            lines.append("  " + fmt_row(st))
    lines.append(f"  PLATEAU SCORE: {pass_cnt}/9 cells with PF>=1.15 & Ret>0")

    # Battery 5: Live Endpoint Feasibility Probe
    lines.extend(["", "--- BATTERY 5: LIVE BOT ENDPOINT FEASIBILITY PROBE ---"])
    lines.extend(probe_live_endpoints())

    report = "\n".join(lines)
    print("\n" + report)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    print(f"\nSaved torture audit report to {args.out}")


if __name__ == "__main__":
    main()
