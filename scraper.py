"""
FundScope MY - Scraper v3
No mstarpy dependency - pure direct HTTP to Morningstar
"""

import json, datetime, time, math, urllib.request, urllib.error
from pathlib import Path

FUNDS = [
    {"code": "PAGF",   "name": "Public Aggressive Growth Fund",      "secid": "0P0000A4GA"},
    {"code": "PGF",    "name": "Public Growth Fund",                  "secid": "0P0000A4GC"},
    {"code": "PEF",    "name": "Public Equity Fund",                  "secid": "0P0000A4GB"},
    {"code": "PSCF",   "name": "Public SmallCap Fund",               "secid": "0P0000A4GH"},
    {"code": "PSF",    "name": "Public Savings Fund",                 "secid": "0P0000A4GJ"},
    {"code": "PRSF",   "name": "Public Regular Savings Fund",        "secid": "0P0000A4GI"},
    {"code": "PEGF",   "name": "Public Enterprises Growth Fund",     "secid": "0P0000A4GE"},
    {"code": "PAIF",   "name": "Public Asia Ittikal Fund",           "secid": "0P0000A4GF"},
    {"code": "PIEF",   "name": "Public Islamic Equity Fund",         "secid": "0P0000A4GG"},
    {"code": "PIF",    "name": "Public Index Fund",                   "secid": "0P0000A4GD"},
    {"code": "PFESF",  "name": "Public Far-East Select Fund",        "secid": "0P0000BVPZ"},
    {"code": "PFEDF",  "name": "Public Far-East Dividend Fund",      "secid": "0P0000BVPY"},
    {"code": "PASF",   "name": "Public ASEAN Growth Fund",           "secid": "0P0000BVPX"},
    {"code": "PIGF",   "name": "Public Islamic Growth Fund",         "secid": "0P0000BVPW"},
    {"code": "PIOF",   "name": "Public Islamic Opportunities Fund",  "secid": "0P0000BVPV"},
    {"code": "PIDF",   "name": "Public Islamic Dividend Fund",       "secid": "0P0000BVPU"},
    {"code": "PCTF",   "name": "Public China Titans Fund",           "secid": "0P0000BVPT"},
    {"code": "PRSF2",  "name": "Public Regional Sector Fund",        "secid": "0P0000BVPS"},
    {"code": "PGSF",   "name": "Public Global Select Fund",          "secid": "0P0000BVPR"},
    {"code": "PGTF",   "name": "Public Global Titans Fund",          "secid": "0P0000BVPQ"},
    {"code": "PBGF",   "name": "PB Growth Fund",                     "secid": "0P0000BVPP"},
    {"code": "PBEF",   "name": "PB Equity Fund",                     "secid": "0P0000BVPO"},
    {"code": "PBAEF",  "name": "PB Asia Equity Fund",                "secid": "0P0000BVPN"},
    {"code": "PSEAS",  "name": "Public South-East Asia Select Fund", "secid": "0P0000BVPM"},
    {"code": "PEOF",   "name": "Public Emerging Opportunities Fund", "secid": "0P0000BVPL"},
    {"code": "PIAG",   "name": "Public Islamic ASEAN Growth Fund",   "secid": "0P0000BVPK"},
    {"code": "PIEEF",  "name": "Public Islamic Enterprises Equity",  "secid": "0P0000BVPJ"},
    {"code": "PEIS",   "name": "Public e-Islamic Sustainable",       "secid": "0P0000BVPI"},
    {"code": "PBAPDF", "name": "PB Asia Pacific Dividend Fund",      "secid": "0P0000BVPH"},
    {"code": "PFSF",   "name": "Public Focus Select Fund",           "secid": "0P0000BVPG"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.morningstar.com/",
    "Origin": "https://www.morningstar.com",
}

def fetch_url(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
    except Exception as e:
        print(f"    fetch error: {e}")
        return None

def fetch_nav_method1(secid):
    """Morningstar timeseries API - compact JSON format"""
    end   = datetime.date.today()
    start = end - datetime.timedelta(days=500)
    url = (
        f"https://lt.morningstar.com/api/rest.svc/timeseries_price/9vehuxllxs"
        f"?id={secid}%5D2%5D0%5DMYR"
        f"&currencyId=MYR&idtype=Morningstar&frequency=daily"
        f"&startDate={start.isoformat()}&endDate={end.isoformat()}"
        f"&outputType=COMPACTJSON"
    )
    data = fetch_url(url)
    if not data or not isinstance(data, list):
        return []
    try:
        history = data[0]["TimeSeries"]["Security"][0]["HistoryDetail"]
        return [{"date": h["EndDate"][:10], "nav": float(h["Value"])}
                for h in history if h.get("Value")]
    except Exception as e:
        print(f"    method1 parse error: {e}")
        return []

def fetch_nav_method2(secid):
    """Morningstar graph data API"""
    end   = datetime.date.today()
    start = end - datetime.timedelta(days=500)
    url = (
        f"https://api.morningstar.com/v2/security/historical-price"
        f"?id={secid}&currencyId=MYR&idtype=msid&frequency=daily"
        f"&startDate={start.isoformat()}&endDate={end.isoformat()}"
        f"&outputType=COMPACTJSON"
    )
    data = fetch_url(url)
    if not data or "d" not in data:
        return []
    try:
        return [{"date": row[0][:10], "nav": float(row[1])}
                for row in data["d"] if len(row) >= 2]
    except Exception as e:
        print(f"    method2 parse error: {e}")
        return []

def fetch_nav_method3(secid):
    """Morningstar fund quote API - gets at least current NAV"""
    url = (
        f"https://lt.morningstar.com/api/rest.svc/9vehuxllxs/security_details/{secid}"
        f"?viewId=FundQuickTake&currencyId=MYR&idtype=msid"
    )
    data = fetch_url(url)
    if not data:
        return []
    try:
        # Extract whatever NAV data exists
        results = []
        rows = data.get("fund", {}).get("navHistory", [])
        for row in rows:
            results.append({"date": row["d"][:10], "nav": float(row["v"])})
        return results
    except Exception:
        return []

def fetch_nav(fund):
    secid = fund["secid"]
    name  = fund["name"]
    print(f"  {name}")

    for method_fn, label in [
        (fetch_nav_method1, "timeseries"),
        (fetch_nav_method2, "graph API"),
        (fetch_nav_method3, "quote API"),
    ]:
        result = method_fn(secid)
        if result:
            print(f"    ✓ {len(result)} records via {label}")
            return result

    print(f"    ✗ all methods failed")
    return []

# --- Indicators ---

def rsi(prices, period=14):
    if len(prices) < period + 1: return None
    gains, losses = [], []
    for i in range(1, len(prices)):
        d = prices[i] - prices[i-1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag*(period-1) + gains[i]) / period
        al = (al*(period-1) + losses[i]) / period
    if al == 0: return 100.0
    return round(100 - 100/(1 + ag/al), 2)

def ma(prices, p):
    return round(sum(prices[-p:])/p, 4) if len(prices) >= p else None

def pct(prices, days):
    if len(prices) < days+1: return None
    old = prices[-(days+1)]
    return round((prices[-1]-old)/old*100, 2) if old else None

def vol(prices, p=20):
    if len(prices) < p+1: return None
    rets = [(prices[i]-prices[i-1])/prices[i-1] for i in range(len(prices)-p, len(prices))]
    mean = sum(rets)/len(rets)
    return round(math.sqrt(sum((r-mean)**2 for r in rets)/len(rets))*100, 2)

def rsi_label(v):
    if v is None: return "N/A"
    if v >= 70: return "OVERBOUGHT"
    if v <= 30: return "OVERSOLD"
    if v >= 55: return "BULLISH"
    if v <= 45: return "BEARISH"
    return "NEUTRAL"

def trend(price, ma20, ma50):
    if not ma20 or not ma50: return "N/A"
    if price > ma20 > ma50: return "UPTREND"
    if price < ma20 < ma50: return "DOWNTREND"
    return "SIDEWAYS"

def indicators(fund, history):
    if not history or len(history) < 5: return None
    h = sorted(history, key=lambda x: x["date"])
    p = [float(x["nav"]) for x in h]
    r = rsi(p); m20 = ma(p,20); m50 = ma(p,50)
    return {
        "fund_code": fund["code"], "fund_name": fund["name"], "secid": fund["secid"],
        "date": h[-1]["date"], "nav": p[-1],
        "rsi_14": r, "rsi_signal": rsi_label(r),
        "ma5": ma(p,5), "ma20": m20, "ma50": m50, "trend": trend(p[-1],m20,m50),
        "pct_1d": pct(p,1), "pct_1w": pct(p,5), "pct_1m": pct(p,21),
        "pct_3m": pct(p,63), "pct_1y": pct(p,252), "volatility_20d": vol(p),
        "data_points": len(p),
    }

def flow_proxy(ind_list):
    valid = [f for f in ind_list if f and f.get("pct_1m") is not None]
    if not valid: return {}
    median = sorted(f["pct_1m"] for f in valid)[len(valid)//2]
    out = {}
    for f in valid:
        diff = f["pct_1m"] - median
        out[f["fund_code"]] = {
            "signal": "INFLOW" if diff>1.5 else "OUTFLOW" if diff<-1.5 else "NEUTRAL",
            "vs_category": round(diff,2),
            "note": f"{'+' if diff>=0 else ''}{diff:.1f}% vs category median"
        }
    return out

def rank(ind_list):
    scored = []
    for f in ind_list:
        if not f: continue
        s = (f.get("pct_1w") or 0)*0.4 + (f.get("pct_1m") or 0)*0.4
        if f.get("rsi_14"): s += ((f["rsi_14"]-50)/50)*20*0.2
        scored.append({"code": f["fund_code"], "score": round(s,4)})
    scored.sort(key=lambda x: x["score"], reverse=True)
    return {x["code"]: {"rank": i+1, "score": x["score"]} for i,x in enumerate(scored)}

# --- Main ---

def run():
    print(f"\n{'='*50}")
    print(f"FundScope MY — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')} MYT")
    print(f"{'='*50}\n")

    data_dir = Path("./data")
    data_dir.mkdir(exist_ok=True)

    nav_file = data_dir / "nav_history.json"
    all_nav = json.loads(nav_file.read_text()) if nav_file.exists() else {}

    all_ind = []
    for fund in FUNDS:
        nav_data = fetch_nav(fund)
        if nav_data:
            existing = {r["date"]: r["nav"] for r in all_nav.get(fund["code"], [])}
            for r in nav_data: existing[r["date"]] = r["nav"]
            all_nav[fund["code"]] = [{"date":d,"nav":v} for d,v in sorted(existing.items())]
        ind = indicators(fund, all_nav.get(fund["code"], []))
        if ind: all_ind.append(ind)
        time.sleep(1)

    print(f"\n{len(all_ind)}/{len(FUNDS)} funds loaded\n")

    fp = flow_proxy(all_ind)
    rk = rank(all_ind)

    funds_out = []
    for ind in all_ind:
        code = ind["fund_code"]
        funds_out.append({
            "code": code, "name": ind["fund_name"], "secid": ind["secid"],
            "nav": ind["nav"], "date": ind["date"],
            "pct_1d": ind.get("pct_1d"), "pct_1w": ind.get("pct_1w"),
            "pct_1m": ind.get("pct_1m"), "pct_3m": ind.get("pct_3m"),
            "pct_1y": ind.get("pct_1y"), "rsi_14": ind.get("rsi_14"),
            "rsi_signal": ind.get("rsi_signal"), "ma5": ind.get("ma5"),
            "ma20": ind.get("ma20"), "ma50": ind.get("ma50"),
            "trend": ind.get("trend"), "volatility": ind.get("volatility_20d"),
            "flow_signal": fp.get(code,{}).get("signal","N/A"),
            "flow_vs_category": fp.get(code,{}).get("vs_category"),
            "flow_note": fp.get(code,{}).get("note",""),
            "momentum_rank": rk.get(code,{}).get("rank"),
            "momentum_score": rk.get(code,{}).get("score"),
            "top_holdings": [],
        })

    funds_out.sort(key=lambda x: x.get("momentum_rank") or 999)

    dashboard = {
        "last_updated": datetime.datetime.now().isoformat(),
        "total_funds": len(funds_out), "funds": funds_out,
        "top_gainers_1d": sorted([f for f in funds_out if f.get("pct_1d")], key=lambda x: x["pct_1d"], reverse=True)[:5],
        "top_losers_1d":  sorted([f for f in funds_out if f.get("pct_1d")], key=lambda x: x["pct_1d"])[:5],
        "top_momentum": funds_out[:5],
        "oversold_funds": [f for f in funds_out if f.get("rsi_14") and f["rsi_14"]<=35],
        "inflow_signals": [f for f in funds_out if f.get("flow_signal")=="INFLOW"],
    }

    nav_file.write_text(json.dumps(all_nav))
    (data_dir/"indicators.json").write_text(json.dumps(all_ind, indent=2))
    (data_dir/"dashboard.json").write_text(json.dumps(dashboard, indent=2))

    print(f"✅ Saved! {len(funds_out)} funds → data/dashboard.json")
    if dashboard["top_gainers_1d"]:
        t = dashboard["top_gainers_1d"][0]
        print(f"   Best today: {t['name']} ({t.get('pct_1d',0):+.2f}%)")

if __name__ == "__main__":
    run()
