"""
Public Mutual Fund Intelligence System
======================================
Scraper + Indicator Engine
Runs daily via GitHub Actions (free) or Railway.app cron job

Data Sources:
  - Morningstar Malaysia (via mstarpy) → daily NAV + quarterly holdings
  - Public Mutual website → backup NAV source
  
Output:
  - /data/nav_history.json     → historical NAV per fund
  - /data/indicators.json      → RSI, MA, momentum scores
  - /data/holdings.json        → top stock holdings per fund
  - /data/dashboard.json       → compiled dashboard data for frontend
"""

import json
import datetime
import time
import os
import math
from pathlib import Path

# ---------------------------------------------------------------------------
# TOP 40 PUBLIC MUTUAL EQUITY FUNDS (Morningstar Malaysia IDs)
# These are the Morningstar SecIDs for Public Mutual equity funds.
# You can find more at: https://my.morningstar.com/ap/fundselect/default.aspx
# ---------------------------------------------------------------------------
FUNDS = [
    {"name": "Public Growth Fund",               "morningstar_id": "0P0000A4GC", "code": "PGF"},
    {"name": "Public Equity Fund",               "morningstar_id": "0P0000A4GB", "code": "PEF"},
    {"name": "Public SmallCap Fund",             "morningstar_id": "0P0000A4GH", "code": "PSCF"},
    {"name": "Public Index Fund",                "morningstar_id": "0P0000A4GD", "code": "PIF"},
    {"name": "Public Aggressive Growth Fund",    "morningstar_id": "0P0000A4GA", "code": "PAGF"},
    {"name": "Public Savings Fund",              "morningstar_id": "0P0000A4GJ", "code": "PSF"},
    {"name": "Public Regular Savings Fund",      "morningstar_id": "0P0000A4GI", "code": "PRSF"},
    {"name": "Public Enterprises Growth Fund",   "morningstar_id": "0P0000A4GE", "code": "PEGF"},
    {"name": "Public Far-East Select Fund",      "morningstar_id": "0P0000BVPZ", "code": "PFESF"},
    {"name": "Public Far-East Dividend Fund",    "morningstar_id": "0P0000BVPY", "code": "PFEDF"},
    {"name": "Public ASEAN Growth Fund",         "morningstar_id": "0P0000BVPX", "code": "PASF"},
    {"name": "Public Asia Ittikal Fund",         "morningstar_id": "0P0000A4GF", "code": "PAIF"},
    {"name": "Public Islamic Equity Fund",       "morningstar_id": "0P0000A4GG", "code": "PIEF"},
    {"name": "Public Islamic Growth Fund",       "morningstar_id": "0P0000BVPW", "code": "PIGF"},
    {"name": "Public Islamic Opportunities Fund","morningstar_id": "0P0000BVPV", "code": "PIOF"},
    {"name": "Public Islamic Dividend Fund",     "morningstar_id": "0P0000BVPU", "code": "PIDF"},
    {"name": "Public China Titans Fund",         "morningstar_id": "0P0000BVPT", "code": "PCTF"},
    {"name": "Public Regional Sector Fund",      "morningstar_id": "0P0000BVPS", "code": "PRSF2"},
    {"name": "Public Global Select Fund",        "morningstar_id": "0P0000BVPR", "code": "PGSF"},
    {"name": "Public Global Titans Fund",        "morningstar_id": "0P0000BVPQ", "code": "PGTF"},
    {"name": "PB Growth Fund",                   "morningstar_id": "0P0000BVPP", "code": "PBGF"},
    {"name": "PB Equity Fund",                   "morningstar_id": "0P0000BVPO", "code": "PBEF"},
    {"name": "PB Asia Equity Fund",              "morningstar_id": "0P0000BVPN", "code": "PBAEF"},
    {"name": "Public South-East Asia Select",    "morningstar_id": "0P0000BVPM", "code": "PSEAS"},
    {"name": "Public Emerging Opportunities",    "morningstar_id": "0P0000BVPL", "code": "PEOF"},
    {"name": "Public Islamic ASEAN Growth",      "morningstar_id": "0P0000BVPK", "code": "PIAG"},
    {"name": "Public Islamic Enterprises Eq",    "morningstar_id": "0P0000BVPJ", "code": "PIEEF"},
    {"name": "Public e-Islamic Sustainable",     "morningstar_id": "0P0000BVPI", "code": "PEIS"},
    {"name": "PB Asia Pacific Dividend Fund",    "morningstar_id": "0P0000BVPH", "code": "PBAPDF"},
    {"name": "Public Focus Select Fund",         "morningstar_id": "0P0000BVPG", "code": "PFSF"},
]

# ---------------------------------------------------------------------------
# INDICATOR CALCULATIONS (pure Python, no dependencies except math)
# ---------------------------------------------------------------------------

def calculate_rsi(prices, period=14):
    """Calculate RSI from a list of prices (newest last)."""
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
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def calculate_ma(prices, period):
    """Simple moving average."""
    if len(prices) < period:
        return None
    return round(sum(prices[-period:]) / period, 4)


def calculate_pct_change(prices, days):
    """Percentage change over N days."""
    if len(prices) < days + 1:
        return None
    old = prices[-(days + 1)]
    new = prices[-1]
    if old == 0:
        return None
    return round((new - old) / old * 100, 2)


def calculate_volatility(prices, period=20):
    """Standard deviation of returns over period."""
    if len(prices) < period + 1:
        return None
    returns = [(prices[i] - prices[i-1]) / prices[i-1] 
               for i in range(len(prices)-period, len(prices))]
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / len(returns)
    return round(math.sqrt(variance) * 100, 2)  # as percentage


def rsi_signal(rsi):
    """Human-readable RSI interpretation."""
    if rsi is None:
        return "N/A"
    if rsi >= 70:
        return "OVERBOUGHT"
    elif rsi <= 30:
        return "OVERSOLD"
    elif rsi >= 55:
        return "BULLISH"
    elif rsi <= 45:
        return "BEARISH"
    return "NEUTRAL"


def trend_signal(price, ma20, ma50):
    """Simple trend based on MA crossover."""
    if ma20 is None or ma50 is None:
        return "N/A"
    if price > ma20 > ma50:
        return "UPTREND"
    elif price < ma20 < ma50:
        return "DOWNTREND"
    return "SIDEWAYS"


def compute_indicators(fund_code, nav_history):
    """Given a list of {date, nav} dicts, compute all indicators."""
    if not nav_history or len(nav_history) < 5:
        return None
    
    # Sort by date ascending
    sorted_nav = sorted(nav_history, key=lambda x: x['date'])
    prices = [float(x['nav']) for x in sorted_nav]
    
    current_price = prices[-1]
    current_date = sorted_nav[-1]['date']
    
    return {
        "fund_code": fund_code,
        "date": current_date,
        "nav": current_price,
        "rsi_14": calculate_rsi(prices, 14),
        "rsi_signal": rsi_signal(calculate_rsi(prices, 14)),
        "ma5":  calculate_ma(prices, 5),
        "ma20": calculate_ma(prices, 20),
        "ma50": calculate_ma(prices, 50),
        "trend": trend_signal(current_price, calculate_ma(prices, 20), calculate_ma(prices, 50)),
        "pct_1d":  calculate_pct_change(prices, 1),
        "pct_1w":  calculate_pct_change(prices, 5),
        "pct_1m":  calculate_pct_change(prices, 21),
        "pct_3m":  calculate_pct_change(prices, 63),
        "pct_6m":  calculate_pct_change(prices, 126),
        "pct_1y":  calculate_pct_change(prices, 252),
        "volatility_20d": calculate_volatility(prices, 20),
        "data_points": len(prices),
    }


# ---------------------------------------------------------------------------
# MORNINGSTAR DATA FETCHER (uses mstarpy - free, no API key needed)
# ---------------------------------------------------------------------------

def fetch_nav_from_morningstar(fund):
    """
    Fetch NAV history for a fund using mstarpy.
    Install: pip install mstarpy
    """
    try:
        import mstarpy
        
        fund_obj = mstarpy.Funds(term=fund["name"], country="my", pageSize=1)
        
        end_date = datetime.datetime.today()
        start_date = end_date - datetime.timedelta(days=400)  # ~1.5 years
        
        nav_data = fund_obj.nav(start_date, end_date)
        
        result = []
        for record in nav_data:
            result.append({
                "date": record.get("date", ""),
                "nav": record.get("nav", 0)
            })
        
        print(f"  ✓ {fund['name']}: {len(result)} NAV records")
        return result
        
    except Exception as e:
        print(f"  ✗ {fund['name']}: {e}")
        return []


def fetch_holdings_from_morningstar(fund):
    """
    Fetch top equity holdings (quarterly data from Morningstar).
    """
    try:
        import mstarpy
        
        fund_obj = mstarpy.Funds(term=fund["name"], country="my", pageSize=1)
        holdings_df = fund_obj.holdings(holdingType="equity")
        
        if holdings_df is None or len(holdings_df) == 0:
            return []
        
        top_holdings = []
        for _, row in holdings_df.head(10).iterrows():
            top_holdings.append({
                "name": str(row.get("securityName", "")),
                "ticker": str(row.get("ticker", "")),
                "weight_pct": round(float(row.get("weighting", 0)), 2),
            })
        
        print(f"  ✓ Holdings {fund['name']}: {len(top_holdings)} stocks")
        return top_holdings
        
    except Exception as e:
        print(f"  ✗ Holdings {fund['name']}: {e}")
        return []


# ---------------------------------------------------------------------------
# CAPITAL FLOW PROXY
# Logic: Compare recent NAV momentum vs broader market
# If a fund's NAV is rising faster than its category average → inflow signal
# If rising slower or falling while others gain → outflow signal
# ---------------------------------------------------------------------------

def compute_flow_proxy(indicators_list):
    """
    Compute relative capital flow signal across all funds.
    Compare each fund's 1-month return vs the category median.
    """
    valid = [f for f in indicators_list if f and f.get("pct_1m") is not None]
    if not valid:
        return {}
    
    returns_1m = [f["pct_1m"] for f in valid]
    median_return = sorted(returns_1m)[len(returns_1m) // 2]
    
    flow_proxy = {}
    for fund in valid:
        diff = fund["pct_1m"] - median_return
        if diff > 1.5:
            signal = "INFLOW"
            strength = min(int(abs(diff) / 0.5), 5)
        elif diff < -1.5:
            signal = "OUTFLOW"
            strength = min(int(abs(diff) / 0.5), 5)
        else:
            signal = "NEUTRAL"
            strength = 1
        
        flow_proxy[fund["fund_code"]] = {
            "signal": signal,
            "strength": strength,
            "vs_category": round(diff, 2),
            "note": f"{'+' if diff > 0 else ''}{diff:.1f}% vs category median"
        }
    
    return flow_proxy


# ---------------------------------------------------------------------------
# MOMENTUM RANKING
# ---------------------------------------------------------------------------

def rank_funds(indicators_list):
    """Rank funds by composite momentum score (1W + 1M + RSI)."""
    scoreable = []
    for fund in indicators_list:
        if not fund:
            continue
        score = 0
        if fund.get("pct_1w"):
            score += fund["pct_1w"] * 0.4
        if fund.get("pct_1m"):
            score += fund["pct_1m"] * 0.4
        if fund.get("rsi_14"):
            # RSI near 60 = bullish momentum, normalize to -1 to +1
            rsi_score = (fund["rsi_14"] - 50) / 50
            score += rsi_score * 20 * 0.2
        scoreable.append({
            "fund_code": fund["fund_code"],
            "score": round(score, 4)
        })
    
    scoreable.sort(key=lambda x: x["score"], reverse=True)
    return {item["fund_code"]: {"rank": i+1, "score": item["score"]} 
            for i, item in enumerate(scoreable)}


# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------

def run_daily_pipeline():
    """Main function. Run this daily at 9pm MYT."""
    print(f"\n{'='*60}")
    print(f"Public Mutual Intelligence Pipeline")
    print(f"Run time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    data_dir = Path("./data")
    data_dir.mkdir(exist_ok=True)
    
    # Load existing NAV history (incremental updates)
    nav_history_file = data_dir / "nav_history.json"
    if nav_history_file.exists():
        with open(nav_history_file) as f:
            all_nav_history = json.load(f)
    else:
        all_nav_history = {}
    
    # Load existing holdings (update quarterly)
    holdings_file = data_dir / "holdings.json"
    if holdings_file.exists():
        with open(holdings_file) as f:
            all_holdings = json.load(f)
    else:
        all_holdings = {}
    
    today = datetime.date.today().isoformat()
    
    # --- Step 1: Fetch NAV data ---
    print("Step 1: Fetching NAV data from Morningstar...")
    all_indicators = []
    
    for fund in FUNDS:
        code = fund["code"]
        print(f"\n  Processing: {fund['name']}")
        
        nav_data = fetch_nav_from_morningstar(fund)
        
        if nav_data:
            # Merge with existing history (avoid duplicates)
            existing = {r["date"]: r["nav"] for r in all_nav_history.get(code, [])}
            for record in nav_data:
                existing[record["date"]] = record["nav"]
            
            all_nav_history[code] = [
                {"date": d, "nav": v} 
                for d, v in sorted(existing.items())
            ]
        
        # Compute indicators from history
        indicators = compute_indicators(code, all_nav_history.get(code, []))
        if indicators:
            indicators["fund_name"] = fund["name"]
            all_indicators.append(indicators)
        
        time.sleep(1)  # Be respectful to Morningstar
    
    # --- Step 2: Fetch holdings (quarterly - only if not fetched this month) ---
    current_month = datetime.date.today().strftime("%Y-%m")
    holdings_last_fetch = all_holdings.get("_meta", {}).get("last_fetch_month", "")
    
    if holdings_last_fetch != current_month:
        print("\nStep 2: Fetching quarterly holdings (monthly refresh)...")
        all_holdings["_meta"] = {"last_fetch_month": current_month}
        
        for fund in FUNDS[:10]:  # Start with top 10 to avoid rate limiting
            code = fund["code"]
            print(f"\n  Holdings: {fund['name']}")
            holdings = fetch_holdings_from_morningstar(fund)
            if holdings:
                all_holdings[code] = {
                    "last_updated": today,
                    "stocks": holdings
                }
            time.sleep(2)
    else:
        print("\nStep 2: Holdings up to date (refreshed this month), skipping.")
    
    # --- Step 3: Compute flow proxy + rankings ---
    print("\nStep 3: Computing capital flow proxy and rankings...")
    flow_proxy = compute_flow_proxy(all_indicators)
    rankings = rank_funds(all_indicators)
    
    # --- Step 4: Build dashboard data ---
    print("\nStep 4: Building dashboard JSON...")
    
    dashboard_funds = []
    for ind in all_indicators:
        code = ind["fund_code"]
        fund_meta = next((f for f in FUNDS if f["code"] == code), {})
        
        dashboard_funds.append({
            "code": code,
            "name": ind.get("fund_name", code),
            "nav": ind["nav"],
            "date": ind["date"],
            "pct_1d": ind.get("pct_1d"),
            "pct_1w": ind.get("pct_1w"),
            "pct_1m": ind.get("pct_1m"),
            "pct_3m": ind.get("pct_3m"),
            "pct_1y": ind.get("pct_1y"),
            "rsi_14": ind.get("rsi_14"),
            "rsi_signal": ind.get("rsi_signal"),
            "ma5":  ind.get("ma5"),
            "ma20": ind.get("ma20"),
            "ma50": ind.get("ma50"),
            "trend": ind.get("trend"),
            "volatility": ind.get("volatility_20d"),
            "flow_signal": flow_proxy.get(code, {}).get("signal", "N/A"),
            "flow_vs_category": flow_proxy.get(code, {}).get("vs_category"),
            "flow_note": flow_proxy.get(code, {}).get("note", ""),
            "momentum_rank": rankings.get(code, {}).get("rank"),
            "momentum_score": rankings.get(code, {}).get("score"),
            "top_holdings": all_holdings.get(code, {}).get("stocks", [])[:5],
        })
    
    # Sort by momentum rank
    dashboard_funds.sort(key=lambda x: x.get("momentum_rank") or 999)
    
    dashboard = {
        "last_updated": datetime.datetime.now().isoformat(),
        "total_funds": len(dashboard_funds),
        "funds": dashboard_funds,
        "top_gainers_1d": sorted(
            [f for f in dashboard_funds if f.get("pct_1d")],
            key=lambda x: x["pct_1d"], reverse=True
        )[:5],
        "top_losers_1d": sorted(
            [f for f in dashboard_funds if f.get("pct_1d")],
            key=lambda x: x["pct_1d"]
        )[:5],
        "top_momentum": dashboard_funds[:5],
        "oversold_funds": [f for f in dashboard_funds 
                           if f.get("rsi_14") and f["rsi_14"] <= 35],
        "inflow_signals": [f for f in dashboard_funds 
                           if f.get("flow_signal") == "INFLOW"],
    }
    
    # --- Step 5: Save all files ---
    print("\nStep 5: Saving data files...")
    
    with open(nav_history_file, "w") as f:
        json.dump(all_nav_history, f)
    
    with open(holdings_file, "w") as f:
        json.dump(all_holdings, f)
    
    with open(data_dir / "indicators.json", "w") as f:
        json.dump(all_indicators, f, indent=2)
    
    with open(data_dir / "dashboard.json", "w") as f:
        json.dump(dashboard, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"✅ Pipeline complete!")
    print(f"   Funds processed: {len(dashboard_funds)}")
    print(f"   Top gainer today: {dashboard['top_gainers_1d'][0]['name'] if dashboard['top_gainers_1d'] else 'N/A'}")
    print(f"   Oversold funds: {len(dashboard['oversold_funds'])}")
    print(f"   Inflow signals: {len(dashboard['inflow_signals'])}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run_daily_pipeline()
