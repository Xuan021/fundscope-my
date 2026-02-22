"""
Public Mutual Fund Intelligence - Fixed Scraper v2
Uses direct Morningstar SecIDs - much more reliable than name search
"""

import json
import datetime
import time
import math
import urllib.request
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.morningstar.com/",
}

def fetch_url(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"    HTTP error: {e}")
        return None

def fetch_nav_with_mstarpy(secid):
    try:
        import mstarpy
        fund = mstarpy.Funds(term=secid, country="my", pageSize=1)
        end_date = datetime.datetime.today()
        start_date = end_date - datetime.timedelta(days=400)
        nav_data = fund.nav(start_date, end_date)
        if nav_data:
            return [{"date": r["date"][:10], "nav": float(r["nav"])} for r in nav_data]
    except Exception as e:
        print(f"    mstarpy error: {e}")
    return []

def fetch_nav_direct(secid):
    end = datetime.date.today()
    start = end - datetime.timedelta(days=400)
    url = (
        f"https://lt.morningstar.com/api/rest.svc/timeseries_price/9vehuxllxs"
        f"?id={secid}%5D2%5D0%5DMYR&currencyId=MYR&idtype=Morningstar"
        f"&frequency=daily&startDate={start.isoformat()}"
        f"&endDate={end.isoformat()}&outputType=COMPACTJSON"
    )
    data = fetch_url(url)
    if data and isinstance(data, list) and len(data) > 0:
        try:
            series = data[0].get("TimeSeries", {}).get("Security", [])
            if series:
                return [
                    {"date": item["EndDate"][:10], "nav": float(item["Value"])}
                    for item in series[0].get("HistoryDetail", [])
                    if item.get("Value")
                ]
        except Exception:
            pass
    return []

def fetch_nav(fund):
    secid = fund["secid"]
    print(f"  {fund['name']}")
    
    result = fetch_nav_with_mstarpy(secid)
    if result:
        print(f"    ✓ {len(result)} NAV records (mstarpy)")
        return result
    
    result = fetch_nav_direct(secid)
    if result:
        print(f"    ✓ {len(result)} NAV records (direct API)")
        return result
    
    print(f"    ✗ No data retrieved")
    return []

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(prices)):
        delta = prices[i] - prices[i-1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period-1) + gains[i]) / period
        avg_loss = (avg_loss * (period-1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_gain/avg_loss)), 2)

def calculate_ma(prices, period):
    if len(prices) < period:
        return None
    return round(sum(prices[-period:]) / period, 4)

def calculate_pct_change(prices, days):
    if len(prices) < days + 1:
        return None
    old = prices[-(days+1)]
    if old == 0:
        return None
    return round((prices[-1] - old) / old * 100, 2)

def calculate_volatility(prices, period=20):
    if len(prices) < period + 1:
        return None
    returns = [(prices[i]-prices[i-1])/prices[i-1] for i in range(len(prices)-period, len(prices))]
    mean = sum(returns) / len(returns)
    variance = sum((r-mean)**2 for r in returns) / len(returns)
    return round(math.sqrt(variance) * 100, 2)

def rsi_signal(rsi):
    if rsi is None: return "N/A"
    if rsi >= 70: return "OVERBOUGHT"
    elif rsi <= 30: return "OVERSOLD"
    elif rsi >= 55: return "BULLISH"
    elif rsi <= 45: return "BEARISH"
    return "NEUTRAL"

def trend_signal(price, ma20, ma50):
    if not ma20 or not ma50: return "N/A"
    if price > ma20 > ma50: return "UPTREND"
    elif price < ma20 < ma50: return "DOWNTREND"
    return "SIDEWAYS"

def compute_indicators(fund, nav_history):
    if not nav_history or len(nav_history) < 5:
        return None
    sorted_nav = sorted(nav_history, key=lambda x: x['date'])
    prices = [float(x['nav']) for x in sorted_nav]
    rsi = calculate_rsi(prices)
    ma20 = calculate_ma(prices, 20)
    ma50 = calculate_ma(prices, 50)
    return {
        "fund_code": fund["code"],
        "fund_name": fund["name"],
        "secid": fund["secid"],
        "date": sorted_nav[-1]['date'],
        "nav": prices[-1],
        "rsi_14": rsi,
        "rsi_signal": rsi_signal(rsi),
        "ma5": calculate_ma(prices, 5),
        "ma20": ma20,
        "ma50": ma50,
        "trend": trend_signal(prices[-1], ma20, ma50),
        "pct_1d": calculate_pct_change(prices, 1),
        "pct_1w": calculate_pct_change(prices, 5),
        "pct_1m": calculate_pct_change(prices, 21),
        "pct_3m": calculate_pct_change(prices, 63),
        "pct_1y": calculate_pct_change(prices, 252),
        "volatility_20d": calculate_volatility(prices),
        "data_points": len(prices),
    }

def compute_flow_proxy(indicators_list):
    valid = [f for f in indicators_list if f and f.get("pct_1m") is not None]
    if not valid: return {}
    returns = sorted([f["pct_1m"] for f in valid])
    median = returns[len(returns)//2]
    result = {}
    for f in valid:
        diff = f["pct_1m"] - median
        result[f["fund_code"]] = {
            "signal": "INFLOW" if diff > 1.5 else "OUTFLOW" if diff < -1.5 else "NEUTRAL",
            "vs_category": round(diff, 2),
            "note": f"{'+' if diff >= 0 else ''}{diff:.1f}% vs category median"
        }
    return result

def rank_funds(indicators_list):
    scored = []
    for f in indicators_list:
        if not f: continue
        score = (f.get("pct_1w") or 0) * 0.4 + (f.get("pct_1m") or 0) * 0.4
        if f.get("rsi_14"):
            score += ((f["rsi_14"] - 50) / 50) * 20 * 0.2
        scored.append({"fund_code": f["fund_code"], "score": round(score, 4)})
    scored.sort(key=lambda x: x["score"], reverse=True)
    return {item["fund_code"]: {"rank": i+1, "score": item["score"]} for i, item in enumerate(scored)}

def run_daily_pipeline():
    print(f"\n{'='*55}")
    print(f"FundScope MY — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}\n")

    data_dir = Path("./data")
    data_dir.mkdir(exist_ok=True)

    nav_file = data_dir / "nav_history.json"
    all_nav = json.loads(nav_file.read_text()) if nav_file.exists() else {}

    all_indicators = []

    for fund in FUNDS:
        nav_data = fetch_nav(fund)
        if nav_data:
            existing = {r["date"]: r["nav"] for r in all_nav.get(fund["code"], [])}
            for r in nav_data:
                existing[r["date"]] = r["nav"]
            all_nav[fund["code"]] = [{"date": d, "nav": v} for d, v in sorted(existing.items())]
        
        ind = compute_indicators(fund, all_nav.get(fund["code"], []))
        if ind:
            all_indicators.append(ind)
        time.sleep(1.5)

    print(f"\n{len(all_indicators)}/{len(FUNDS)} funds loaded successfully\n")

    flow = compute_flow_proxy(all_indicators)
    ranks = rank_funds(all_indicators)

    funds_out = []
    for ind in all_indicators:
        code = ind["fund_code"]
        funds_out.append({
            **{k: ind.get(k) for k in ["fund_code","fund_name","secid","nav","date",
               "pct_1d","pct_1w","pct_1m","pct_3m","pct_1y",
               "rsi_14","rsi_signal","ma5","ma20","ma50","trend","volatility_20d"]},
            "code": code,
            "name": ind["fund_name"],
            "volatility": ind.get("volatility_20d"),
            "flow_signal": flow.get(code, {}).get("signal", "N/A"),
            "flow_vs_category": flow.get(code, {}).get("vs_category"),
            "flow_note": flow.get(code, {}).get("note", ""),
            "momentum_rank": ranks.get(code, {}).get("rank"),
            "momentum_score": ranks.get(code, {}).get("score"),
            "top_holdings": [],
        })

    funds_out.sort(key=lambda x: x.get("momentum_rank") or 999)

    dashboard = {
        "last_updated": datetime.datetime.now().isoformat(),
        "total_funds": len(funds_out),
        "funds": funds_out,
        "top_gainers_1d": sorted([f for f in funds_out if f.get("pct_1d")], key=lambda x: x["pct_1d"], reverse=True)[:5],
        "top_losers_1d": sorted([f for f in funds_out if f.get("pct_1d")], key=lambda x: x["pct_1d"])[:5],
        "top_momentum": funds_out[:5],
        "oversold_funds": [f for f in funds_out if f.get("rsi_14") and f["rsi_14"] <= 35],
        "inflow_signals": [f for f in funds_out if f.get("flow_signal") == "INFLOW"],
    }

    nav_file.write_text(json.dumps(all_nav))
    (data_dir / "indicators.json").write_text(json.dumps(all_indicators, indent=2))
    (data_dir / "dashboard.json").write_text(json.dumps(dashboard, indent=2))

    print(f"✅ Done! {len(funds_out)} funds saved to data/dashboard.json")
    if dashboard["top_gainers_1d"]:
        top = dashboard["top_gainers_1d"][0]
        print(f"   Top gainer: {top['name']} ({top.get('pct_1d',0):+.2f}%)")

if __name__ == "__main__":
    run_daily_pipeline()
