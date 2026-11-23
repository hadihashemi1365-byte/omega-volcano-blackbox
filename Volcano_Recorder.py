#!/usr/bin/env python3
"""
OMEGA Volcano Black-Box Flight Recorder v2.0
Records 15-minute ultra-granular live microstructure, momentum, derivatives,
Nobitex orderbook, and free on-chain/macro snapshots across 66 coins
(4 Majors: BTC, ETH, SOL, BNB + 62 Speculative Altcoins).
Outputs:
  - data/blackbox_YYYY-MM-DD.csv (44 columns per symbol per 15m snapshot)
  - data/manifest.json (auto-index & daily explosion summary for live Dashboard)
Zero external dependencies (Python 3.10+ standard library only).
"""

import csv
import datetime
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

VERSION = "v2.0"
DATA_DIR = "data"
TIMEOUT_SEC = 10
MAX_WORKERS = 8

MAJORS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]

SPEC_32 = [
    "HUMAUSDT", "BANKUSDT", "DEXEUSDT", "TRBUSDT", "QNTUSDT", "TURBOUSDT", "WIFUSDT", "PEPEUSDT",
    "BONKUSDT", "FLOKIUSDT", "NOTUSDT", "DOGSUSDT", "TONUSDT", "RENDERUSDT", "FETUSDT", "TAOUSDT",
    "WLDUSDT", "ARKMUSDT", "PENDLEUSDT", "JUPUSDT", "PYTHUSDT", "STRKUSDT", "ENAUSDT", "EIGENUSDT",
    "ZROUSDT", "TNSRUSDT", "AGLDUSDT", "PROMUSDT", "API3USDT", "BANANAUSDT", "BOMEUSDT", "SUIUSDT",
]

SPEC_NEW_30 = [
    "ORDIUSDT", "1000SATSUSDT", "PEOPLEUSDT", "MEMEUSDT", "BICOUSDT", "MASKUSDT", "YGGUSDT", "GALAUSDT",
    "SANDUSDT", "IMXUSDT", "SUPERUSDT", "TIAUSDT", "SEIUSDT", "DYDXUSDT", "COMPUSDT", "CRVUSDT",
    "LDOUSDT", "ENSUSDT", "BLURUSDT", "PYRUSDT", "MAGICUSDT", "CELOUSDT", "STORJUSDT", "ANKRUSDT",
    "BANDUSDT", "UMAUSDT", "HIGHUSDT", "CYBERUSDT", "SUSHIUSDT", "GMTUSDT",
]

SYMBOLS = MAJORS + SPEC_32 + SPEC_NEW_30

CSV_HEADERS = [
    "ts_utc", "symbol", "is_major",
    "spot_px", "ret_15m_pct", "ret_1h_pct", "ret_4h_pct", "ret_24h_pct", "idiosync_1h_pct",
    "bar15m_clv", "bar15m_uwick", "bar15m_rng_pct",
    "vol_15m_usd", "vol_15m_surge_x", "tb_15m_ratio", "trd_15m_cnt", "trd_15m_surge_x",
    "qv_24h_usd", "trades_24h",
    "spread_bps", "bid_usd_05pct", "ask_usd_05pct", "bid_usd_1pct", "ask_usd_1pct",
    "bid_usd_2pct", "ask_usd_2pct", "obi_05pct", "obi_1pct", "ask_vacuum_2pct",
    "max_bid_wall_usd", "max_ask_wall_usd",
    "agg_tb_ratio", "agg_net_buy_usd", "agg_avg_usd", "agg_max_usd",
    "whale_5k_cnt", "bot_repeat_ratio", "trades_per_min",
    "deriv_src", "perp_px", "basis_bps", "funding_rate", "coin_oi", "usd_oi",
    "nbx_usdt_spread_bps", "nbx_usdt_ask_usd", "nbx_irt_spread_bps",
]


def http_get_json(url: str, timeout: int = TIMEOUT_SEC, post_body: dict = None):
    data_bytes = json.dumps(post_body).encode("utf-8") if post_body is not None else None
    headers = {"User-Agent": "OMEGA-BlackBox-Recorder/2.0", "Accept": "application/json"}
    if data_bytes is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data_bytes, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def safe_float(val, default=math.nan) -> float:
    try:
        x = float(val)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def fmt(x, d: int = 4) -> str:
    if x is None or (isinstance(x, float) and not math.isfinite(x)):
        return ""
    if isinstance(x, float):
        return f"{x:.{d}f}"
    return str(x)


def analyze_15m_klines(klines: list) -> dict:
    """
    Analyzes last ~18 closed/recent 15m klines to compute 15m/1h/4h momentum,
    candle microstructure (CLV, upper wick, range), volume surge, taker buy ratio, and trade count surge.
    Uses the LAST COMPLETED 15m bar (index -2 if current bar is open, or -1 if only 1 bar).
    """
    if not klines or len(klines) < 2:
        return {}
    # Use last completed 15m candle (klines[-2]) for zero partial-bar distortion
    idx = len(klines) - 2
    cur = klines[idx]
    o, h, l, c = safe_float(cur[1]), safe_float(cur[2]), safe_float(cur[3]), safe_float(cur[4])
    v, qv, trd, tbv = safe_float(cur[5], 0.0), safe_float(cur[7], 0.0), safe_float(cur[8], 0.0), safe_float(cur[9], 0.0)
    if not (o > 0 and c > 0):
        return {}

    ret_15m = (c / o - 1.0) * 100.0
    ret_1h = ((c / safe_float(klines[idx - 4][4]) - 1.0) * 100.0) if idx >= 4 and safe_float(klines[idx - 4][4], 0) > 0 else math.nan
    ret_4h = ((c / safe_float(klines[idx - 16][4]) - 1.0) * 100.0) if idx >= 16 and safe_float(klines[idx - 16][4], 0) > 0 else math.nan

    rng = max(h - l, c * 1e-6)
    clv = (c - l) / rng
    uwick = (h - max(o, c)) / rng
    rng_pct = ((h - l) / c) * 100.0
    tb_ratio = (tbv / v) if v > 0 else math.nan

    prev_bars = klines[max(0, idx - 16):idx]
    prev_qvs = [safe_float(b[7], 0.0) for b in prev_bars]
    prev_trds = [safe_float(b[8], 0.0) for b in prev_bars]
    mean_qv = (sum(prev_qvs) / len(prev_qvs)) if prev_qvs else 0.0
    mean_trd = (sum(prev_trds) / len(prev_trds)) if prev_trds else 0.0

    return {
        "ret_15m_pct": ret_15m,
        "ret_1h_pct": ret_1h,
        "ret_4h_pct": ret_4h,
        "bar15m_clv": clv,
        "bar15m_uwick": uwick,
        "bar15m_rng_pct": rng_pct,
        "vol_15m_usd": qv,
        "vol_15m_surge_x": (qv / mean_qv) if mean_qv > 0 else math.nan,
        "tb_15m_ratio": tb_ratio,
        "trd_15m_cnt": int(trd),
        "trd_15m_surge_x": (trd / mean_trd) if mean_trd > 0 else math.nan,
    }


def analyze_depth(depth_json: dict) -> dict:
    bids = depth_json.get("bids") or []
    asks = depth_json.get("asks") or []
    if not bids or not asks:
        return {}
    bb = safe_float(bids[0][0])
    ba = safe_float(asks[0][0])
    if not (bb > 0 and ba > 0):
        return {}
    mid = 0.5 * (bb + ba)
    spread_bps = ((ba - bb) / mid) * 10000.0

    bid05 = bid1 = bid2 = ask05 = ask1 = ask2 = 0.0
    max_bid_wall = max_ask_wall = 0.0
    for px_s, qty_s in bids:
        px, qty = safe_float(px_s, 0.0), safe_float(qty_s, 0.0)
        usd = px * qty
        if px >= mid * 0.995:
            bid05 += usd
        if px >= mid * 0.99:
            bid1 += usd
        if px >= mid * 0.98:
            bid2 += usd
            if usd > max_bid_wall:
                max_bid_wall = usd
    for px_s, qty_s in asks:
        px, qty = safe_float(px_s, 0.0), safe_float(qty_s, 0.0)
        usd = px * qty
        if px <= mid * 1.005:
            ask05 += usd
        if px <= mid * 1.01:
            ask1 += usd
        if px <= mid * 1.02:
            ask2 += usd
            if usd > max_ask_wall:
                max_ask_wall = usd

    obi05 = (bid05 - ask05) / (bid05 + ask05) if (bid05 + ask05) > 0 else math.nan
    obi1 = (bid1 - ask1) / (bid1 + ask1) if (bid1 + ask1) > 0 else math.nan
    ask_vac2 = ask2 / bid2 if bid2 > 0 else math.nan

    return {
        "spread_bps": spread_bps,
        "bid_usd_05pct": bid05, "ask_usd_05pct": ask05,
        "bid_usd_1pct": bid1, "ask_usd_1pct": ask1,
        "bid_usd_2pct": bid2, "ask_usd_2pct": ask2,
        "obi_05pct": obi05, "obi_1pct": obi1, "ask_vacuum_2pct": ask_vac2,
        "max_bid_wall_usd": max_bid_wall, "max_ask_wall_usd": max_ask_wall,
    }


def analyze_agg_trades(trades: list) -> dict:
    if not trades:
        return {}
    total_usd = tb_usd = ts_usd = max_usd = 0.0
    whale_5k = 0
    sizes = []
    t_min = t_max = None

    for tr in trades:
        px = safe_float(tr.get("p"), 0.0)
        qty = safe_float(tr.get("q"), 0.0)
        usd = px * qty
        if usd <= 0:
            continue
        total_usd += usd
        if tr.get("m") is False:
            tb_usd += usd
        else:
            ts_usd += usd
        if usd > max_usd:
            max_usd = usd
        if usd >= 5000.0:
            whale_5k += 1
        sizes.append(usd)
        ts = tr.get("T")
        if ts is not None:
            t_min = ts if t_min is None else min(t_min, ts)
            t_max = ts if t_max is None else max(t_max, ts)

    n = len(sizes)
    if n == 0:
        return {}
    repeats = sum(1 for i in range(1, n) if sizes[i - 1] > 0 and abs(sizes[i] - sizes[i - 1]) / sizes[i - 1] <= 0.05)
    bot_repeat = repeats / max(1, n - 1)
    elapsed_min = max((t_max - t_min) / 60000.0, 1.0 / 60.0) if (t_min is not None and t_max is not None) else math.nan

    return {
        "agg_tb_ratio": tb_usd / total_usd if total_usd > 0 else math.nan,
        "agg_net_buy_usd": tb_usd - ts_usd,
        "agg_avg_usd": total_usd / n,
        "agg_max_usd": max_usd,
        "whale_5k_cnt": whale_5k,
        "bot_repeat_ratio": bot_repeat,
        "trades_per_min": n / elapsed_min if math.isfinite(elapsed_min) else math.nan,
    }


def fetch_macro_onchain() -> dict:
    """Free zero-key macro & on-chain pulse: Fear&Greed, ETH Gas Gwei, BTC Mempool vMB, BTC Dominance."""
    pulse = {"fear_greed": None, "eth_gas_gwei": None, "btc_mempool_vmb": None, "btc_dom_pct": None, "mcap_chg24h": None}
    try:
        fg = http_get_json("https://api.alternative.me/fng/?limit=1", timeout=6)
        d = (fg.get("data") or [{}])[0]
        pulse["fear_greed"] = int(d.get("value")) if d.get("value") is not None else None
    except Exception:
        pass
    try:
        g = http_get_json("https://ethereum-rpc.publicnode.com", timeout=6, post_body={"jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id": 1})
        if g.get("result"):
            pulse["eth_gas_gwei"] = round(int(g["result"], 16) / 1e9, 2)
    except Exception:
        pass
    try:
        mp = http_get_json("https://mempool.space/api/mempool", timeout=6)
        if mp.get("vsize") is not None:
            pulse["btc_mempool_vmb"] = round(float(mp["vsize"]) / 1e6, 2)
    except Exception:
        pass
    try:
        cg = http_get_json("https://api.coingecko.com/api/v3/global", timeout=6)
        gd = cg.get("data") or {}
        if gd.get("market_cap_percentage", {}).get("btc") is not None:
            pulse["btc_dom_pct"] = round(float(gd["market_cap_percentage"]["btc"]), 2)
        if gd.get("market_cap_change_percentage_24h_usd") is not None:
            pulse["mcap_chg24h"] = round(float(gd["market_cap_change_percentage_24h_usd"]), 2)
    except Exception:
        pass
    return pulse


def fetch_spot_tickers_bulk() -> dict:
    sym_set = set(SYMBOLS)
    for url in ["https://data-api.binance.vision/api/v3/ticker/24hr", "https://api.binance.com/api/v3/ticker/24hr"]:
        try:
            data = http_get_json(url, timeout=15)
            if isinstance(data, list):
                return {d["symbol"]: d for d in data if d.get("symbol") in sym_set}
        except Exception:
            continue
    return {}


def fetch_derivatives_bulk() -> tuple:
    out = {}
    # 1. Binance FAPI
    try:
        prem = http_get_json("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=10)
        if isinstance(prem, list) and len(prem) > 20:
            sym_set = set(SYMBOLS)
            for row in prem:
                s = row.get("symbol")
                if s in sym_set:
                    out[s] = {"perp_px": safe_float(row.get("markPrice")), "funding_rate": safe_float(row.get("lastFundingRate")), "coin_oi": math.nan}

            def _get_oi(s):
                try:
                    j = http_get_json(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={s}", timeout=6)
                    return s, safe_float(j.get("openInterest"))
                except Exception:
                    return s, math.nan

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                for s, oi in ex.map(_get_oi, list(out.keys())):
                    out[s]["coin_oi"] = oi
            return "BINANCE_FAPI", out
    except Exception:
        pass

    # 2. OKX V5 Fallback
    try:
        tk = http_get_json("https://www.okx.com/api/v5/market/tickers?instType=SWAP", timeout=12)
        oi_resp = http_get_json("https://www.okx.com/api/v5/public/open-interest?instType=SWAP", timeout=12)
        tk_map = {d.get("instId"): safe_float(d.get("last")) for d in (tk.get("data") or [])}
        oi_map = {d.get("instId"): safe_float(d.get("oiCcy")) for d in (oi_resp.get("data") or [])}
        for s in SYMBOLS:
            base = "SATS" if s == "1000SATSUSDT" else s.replace("USDT", "")
            inst = f"{base}-USDT-SWAP"
            if inst in tk_map:
                out[s] = {"perp_px": tk_map.get(inst, math.nan), "funding_rate": math.nan, "coin_oi": oi_map.get(inst, math.nan)}
        if out:
            def _get_okx_fr(s):
                base = "SATS" if s == "1000SATSUSDT" else s.replace("USDT", "")
                try:
                    j = http_get_json(f"https://www.okx.com/api/v5/public/funding-rate?instId={base}-USDT-SWAP", timeout=6)
                    arr = j.get("data") or []
                    return s, safe_float(arr[0].get("fundingRate")) if arr else math.nan
                except Exception:
                    return s, math.nan

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                for s, fr in ex.map(_get_okx_fr, list(out.keys())):
                    out[s]["funding_rate"] = fr
            return "OKX_V5", out
    except Exception:
        pass
    return "NONE", {}


def parse_nobitex_orderbook_all(raw: dict) -> dict:
    out = {}
    if not isinstance(raw, dict):
        return out

    def _bm(book):
        if not isinstance(book, dict):
            return math.nan, math.nan
        bids, asks = book.get("bids") or [], book.get("asks") or []
        if not bids or not asks:
            return math.nan, math.nan
        bb, ba = safe_float(bids[0][0]), safe_float(asks[0][0])
        if not (bb > 0 and ba > 0):
            return math.nan, math.nan
        mid = 0.5 * (bb + ba)
        sp = ((ba - bb) / mid) * 10000.0
        ask_val = sum(safe_float(r[0], 0.0) * safe_float(r[1], 0.0) for r in asks[:15] if safe_float(r[0], 0.0) <= mid * 1.02)
        return sp, ask_val

    for s in SYMBOLS:
        base = s.replace("USDT", "")
        u_sp, u_ask = _bm(raw.get(f"{base}USDT"))
        i_sp, _ = _bm(raw.get(f"{base}IRT"))
        if math.isfinite(u_sp) or math.isfinite(i_sp):
            out[s] = {"nbx_usdt_spread_bps": u_sp, "nbx_usdt_ask_usd": u_ask, "nbx_irt_spread_bps": i_sp}
    return out


def fetch_nobitex_bulk() -> dict:
    try:
        return parse_nobitex_orderbook_all(http_get_json("https://apiv2.nobitex.ir/v3/orderbook/all", timeout=10))
    except Exception:
        return {}


def fetch_symbol_micro(sym: str) -> tuple:
    base = "https://data-api.binance.vision"
    k_info, d_info, a_info = {}, {}, {}
    try:
        k_json = http_get_json(f"{base}/api/v3/klines?symbol={sym}&interval=15m&limit=19", timeout=8)
        k_info = analyze_15m_klines(k_json)
    except Exception:
        pass
    try:
        d_json = http_get_json(f"{base}/api/v3/depth?symbol={sym}&limit=100", timeout=8)
        d_info = analyze_depth(d_json)
    except Exception:
        pass
    try:
        a_json = http_get_json(f"{base}/api/v3/aggTrades?symbol={sym}&limit=250", timeout=8)
        a_info = analyze_agg_trades(a_json)
    except Exception:
        pass
    return sym, k_info, d_info, a_info


def update_manifest(data_dir: str, ts_iso: str, deriv_src: str, deriv_cnt: int, nbx_cnt: int, macro: dict):
    files = sorted([f for f in os.listdir(data_dir) if f.startswith("blackbox_") and f.endswith(".csv")])
    dates = [f.replace("blackbox_", "").replace(".csv", "") for f in files]
    manifest = {
        "version": VERSION,
        "last_updated_utc": ts_iso,
        "total_days": len(dates),
        "dates": dates,
        "symbols_count": len(SYMBOLS),
        "columns_count": len(CSV_HEADERS),
        "deriv_src": deriv_src,
        "deriv_matched": deriv_cnt,
        "nobitex_matched": nbx_cnt,
        "macro_onchain": macro,
    }
    m_path = os.path.join(data_dir, "manifest.json")
    with open(m_path, "w", encoding="utf-8") as mf:
        json.dump(manifest, mf, ensure_ascii=False, indent=2)
    return m_path


def run_snapshot():
    t0 = time.time()
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    ts_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    day_str = now_utc.strftime("%Y-%m-%d")

    tickers = fetch_spot_tickers_bulk()
    deriv_src, deriv_map = fetch_derivatives_bulk()
    nbx_map = fetch_nobitex_bulk()
    macro = fetch_macro_onchain()

    active_syms = [s for s in SYMBOLS if s in tickers]
    micro_map = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(fetch_symbol_micro, s) for s in active_syms]
        for fut in as_completed(futs):
            sym, k_info, d_info, a_info = fut.result()
            micro_map[sym] = (k_info, d_info, a_info)

    btc_ret_1h = micro_map.get("BTCUSDT", ({}, {}, {}))[0].get("ret_1h_pct", math.nan)
    majors_set = set(MAJORS)

    os.makedirs(DATA_DIR, exist_ok=True)
    csv_path = os.path.join(DATA_DIR, f"blackbox_{day_str}.csv")
    write_header = not os.path.exists(csv_path)

    rows_written = 0
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(CSV_HEADERS)
        for s in active_syms:
            tk = tickers[s]
            spot_px = safe_float(tk.get("lastPrice"))
            ret24 = safe_float(tk.get("priceChangePercent"))
            qv24 = safe_float(tk.get("quoteVolume"))
            trd24 = tk.get("count", "")
            k_info, d_info, a_info = micro_map.get(s, ({}, {}, {}))
            r1h = k_info.get("ret_1h_pct", math.nan)
            idio1h = (r1h - btc_ret_1h) if (math.isfinite(r1h) and math.isfinite(btc_ret_1h)) else math.nan

            dv = deriv_map.get(s, {})
            perp_px = dv.get("perp_px", math.nan)
            coin_oi = dv.get("coin_oi", math.nan)
            usd_oi = (coin_oi * perp_px) if (math.isfinite(coin_oi) and math.isfinite(perp_px)) else math.nan
            basis_bps = ((perp_px - spot_px) / spot_px) * 10000.0 if (math.isfinite(perp_px) and spot_px > 0) else math.nan
            nbx = nbx_map.get(s, {})

            w.writerow([
                ts_iso, s, 1 if s in majors_set else 0,
                fmt(spot_px, 6), fmt(k_info.get("ret_15m_pct"), 3), fmt(r1h, 3), fmt(k_info.get("ret_4h_pct"), 3),
                fmt(ret24, 2), fmt(idio1h, 3),
                fmt(k_info.get("bar15m_clv"), 3), fmt(k_info.get("bar15m_uwick"), 3), fmt(k_info.get("bar15m_rng_pct"), 3),
                fmt(k_info.get("vol_15m_usd"), 1), fmt(k_info.get("vol_15m_surge_x"), 2), fmt(k_info.get("tb_15m_ratio"), 4),
                k_info.get("trd_15m_cnt", ""), fmt(k_info.get("trd_15m_surge_x"), 2),
                fmt(qv24, 1), trd24,
                fmt(d_info.get("spread_bps"), 2),
                fmt(d_info.get("bid_usd_05pct"), 1), fmt(d_info.get("ask_usd_05pct"), 1),
                fmt(d_info.get("bid_usd_1pct"), 1), fmt(d_info.get("ask_usd_1pct"), 1),
                fmt(d_info.get("bid_usd_2pct"), 1), fmt(d_info.get("ask_usd_2pct"), 1),
                fmt(d_info.get("obi_05pct"), 4), fmt(d_info.get("obi_1pct"), 4), fmt(d_info.get("ask_vacuum_2pct"), 4),
                fmt(d_info.get("max_bid_wall_usd"), 1), fmt(d_info.get("max_ask_wall_usd"), 1),
                fmt(a_info.get("agg_tb_ratio"), 4), fmt(a_info.get("agg_net_buy_usd"), 1),
                fmt(a_info.get("agg_avg_usd"), 1), fmt(a_info.get("agg_max_usd"), 1),
                a_info.get("whale_5k_cnt", ""), fmt(a_info.get("bot_repeat_ratio"), 4), fmt(a_info.get("trades_per_min"), 2),
                deriv_src if dv else "NONE", fmt(perp_px, 6), fmt(basis_bps, 2),
                fmt(dv.get("funding_rate"), 6), fmt(coin_oi, 2), fmt(usd_oi, 1),
                fmt(nbx.get("nbx_usdt_spread_bps"), 2), fmt(nbx.get("nbx_usdt_ask_usd"), 1), fmt(nbx.get("nbx_irt_spread_bps"), 2),
            ])
            rows_written += 1

    update_manifest(DATA_DIR, ts_iso, deriv_src, len(deriv_map), len(nbx_map), macro)
    dt = time.time() - t0
    print(f"[OMEGA BlackBox {VERSION}] {ts_iso} | rows={rows_written} | deriv={deriv_src}({len(deriv_map)}) | nbx={len(nbx_map)} | elapsed={dt:.1f}s")


def run_selftest():
    # T1: 15m kline momentum & surge
    kl = []
    for i in range(19):
        px = 100.0 if i < 17 else (106.0 if i == 17 else 107.0)
        vol = 1000.0 if i < 17 else 5000.0
        kl.append([i * 900000, str(100.0 if i == 17 else px), str(px * 1.01), str(px * 0.99), str(px), str(vol), 0, str(vol * px), 200 if i == 17 else 40, str(vol * 0.7)])
    k1 = analyze_15m_klines(kl)
    assert abs(k1["ret_15m_pct"] - 6.0) < 0.01 and k1["vol_15m_surge_x"] > 4.5 and k1["trd_15m_surge_x"] == 5.0, f"T1 fail: {k1}"

    # T2: Depth OBI, Ask Vacuum, and Walls
    d2 = analyze_depth({
        "bids": [["99.9", "100"], ["99.6", "200"], ["98.5", "500"]],
        "asks": [["100.1", "50"], ["100.6", "50"], ["101.5", "100"]],
    })
    assert d2["obi_05pct"] > 0.5 and d2["max_bid_wall_usd"] > 49000 and d2["ask_vacuum_2pct"] < 0.35, f"T2 fail: {d2}"

    # T3: aggTrades whale_5k, net_buy_usd, bot_repeat
    tr3 = [
        {"p": "10", "q": "600", "m": False, "T": 1000},   # $6000 whale buy
        {"p": "10", "q": "50",  "m": False, "T": 11000},  # $500 buy
        {"p": "10", "q": "51",  "m": False, "T": 21000},  # $510 repeat
        {"p": "10", "q": "20",  "m": True,  "T": 61000},  # $200 sell
    ]
    a3 = analyze_agg_trades(tr3)
    assert a3["whale_5k_cnt"] == 1 and abs(a3["agg_net_buy_usd"] - 6810.0) < 1e-4, f"T3 fail: {a3}"

    # T4: Nobitex parser
    nbx = parse_nobitex_orderbook_all({"BTCUSDT": {"bids": [["90000", "1"]], "asks": [["90100", "2"]]}})
    assert "BTCUSDT" in nbx and nbx["BTCUSDT"]["nbx_usdt_ask_usd"] > 180000, f"T4 fail: {nbx}"

    # T5: Disjoint 66 symbols (4 majors + 32 + 30)
    assert len(set(SPEC_32) & set(SPEC_NEW_30)) == 0 and len(SYMBOLS) == 66, "T5 fail"

    # T6: Exact 47 CSV headers
    assert len(CSV_HEADERS) == 47, f"T6 fail: {len(CSV_HEADERS)}"

    # T7: Manifest generation test in temp dir
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        open(os.path.join(td, "blackbox_2026-11-22.csv"), "w").close()
        mp = update_manifest(td, "2026-11-22T12:00:00Z", "OKX_V5", 50, 40, {"fear_greed": 55, "eth_gas_gwei": 12.3})
        mdata = json.load(open(mp))
        assert mdata["total_days"] == 1 and mdata["dates"] == ["2026-11-22"] and mdata["macro_onchain"]["fear_greed"] == 55, "T7 fail"

    # T8: NaN safe formatting
    assert fmt(math.nan) == "" and fmt(1.2345, 2) == "1.23", "T8 fail"

    print("SELFTEST: 8/8 PASS")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        run_selftest()
    else:
        run_snapshot()