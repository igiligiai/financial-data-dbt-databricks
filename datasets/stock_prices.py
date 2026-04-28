import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import yfinance as yf

logger = logging.getLogger(__name__)

NASDAQ_100_SYMBOLS = [
    "AAPL", "ABNB", "ADBE", "ADI", "ADP", "ADSK", "AEP", "AMAT", "AMD",
    "AMGN", "AMZN", "ANSS", "APP", "ARM", "ASML", "AVGO", "AZN", "BIIB",
    "BKNG", "BKR", "CDNS", "CDW", "CEG", "CHTR", "CMCSA", "COIN", "COST",
    "CPRT", "CRWD", "CSGP", "CSCO", "CTAS", "CTSH", "DASH", "DDOG",
    "DLTR", "DXCM", "EA", "EXC", "FANG", "FAST", "FTNT", "GEHC", "GFS",
    "GILD", "GOOG", "GOOGL", "HON", "IDXX", "ILMN", "INTC", "INTU",
    "ISRG", "KDP", "KHC", "KLAC", "LIN", "LRCX", "LULU", "MAR", "MCHP",
    "MDB", "MDLZ", "MELI", "META", "MNST", "MRNA", "MRVL", "MSFT", "MU",
    "NFLX", "NVDA", "NXPI", "ODFL", "ON", "ORLY", "PANW", "PAYX", "PCAR",
    "PDD", "PEP", "PLTR", "PYPL", "QCOM", "REGN", "ROP", "ROST", "SBUX",
    "SMCI", "SNPS", "TEAM", "TMUS", "TSLA", "TTD", "TTWO", "TXN", "VRSK",
    "VRTX", "WBD", "WDAY", "XEL", "ZS",
]


def _safe_round(value, decimals: int = 2):
    """Round a numeric value safely, returning None for non-numeric inputs."""
    try:
        return round(float(value), decimals)
    except (TypeError, ValueError):
        return None


def _timestamp_iso(epoch) -> str | None:
    """Convert an epoch timestamp to ISO 8601 UTC string."""
    if isinstance(epoch, (int, float)):
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
    return None


def get_hourly_stock_data(symbol: str) -> Dict:
    """
    Extract hourly price snapshot for a single symbol.

    Captures time-sensitive market data meant to be collected once per hour:
    current price, day high/low, open, previous close, volume, and market cap.

    Returns:
        Dict with hourly price fields or an error payload.
    """
    try:
        symbol = symbol.upper().strip()
        if not symbol:
            return {"error": "Invalid Symbol", "message": "Symbol cannot be empty"}

        info = yf.Ticker(symbol).info or {}
        return {
            "symbol": symbol,
            "extracted_at": datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S"),
            "current_price": _safe_round(info.get("currentPrice")),
            "day_high": _safe_round(info.get("dayHigh") or info.get("regularMarketDayHigh")),
            "day_low": _safe_round(info.get("dayLow") or info.get("regularMarketDayLow")),
            "previous_close": _safe_round(info.get("previousClose") or info.get("regularMarketPreviousClose")),
            "volume": info.get("volume") or info.get("regularMarketVolume"),
            "market_cap": info.get("marketCap"),
            "fifty_day_average": _safe_round(info.get("fiftyDayAverage")),
            "two_hundred_day_average": _safe_round(info.get("twoHundredDayAverage")),
            "fifty_two_week_high": _safe_round(info.get("fiftyTwoWeekHigh")),
            "fifty_two_week_low": _safe_round(info.get("fiftyTwoWeekLow")),
        }
    except Exception as e:
        logger.error("Hourly data extraction failed for %s: %s", symbol, e)
        return {"error": "Lookup Error", "symbol": symbol, "message": str(e)}


def get_company_info(symbol: str) -> Dict:
    """
    Extract general (mostly static) company information for a single symbol.

    This data changes infrequently and is suited for a daily or on-demand refresh.

    Returns:
        Dict with company profile fields or an error payload.
    """
    try:
        symbol = symbol.upper().strip()
        if not symbol:
            return {"error": "Invalid Symbol", "message": "Symbol cannot be empty"}

        info = yf.Ticker(symbol).info or {}

        return {
            # Identity & profile
            "symbol": symbol,
            "company_name": info.get("longName") or info.get("shortName", symbol),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "quote_type": info.get("quoteType"),
            "long_business_summary": info.get("longBusinessSummary"),

            # Location & contact
            "country": info.get("country"),
            "city": info.get("city"),
            "state": info.get("state"),
            "address": info.get("address1"),
            "zip_code": info.get("zip"),
            "phone": info.get("phone"),
            "website": info.get("website"),
            "ir_website": info.get("irWebsite"),

            # Workforce
            "full_time_employees": info.get("fullTimeEmployees"),

            # Exchange & market
            "exchange": info.get("exchange"),
            "full_exchange_name": info.get("fullExchangeName"),
            "currency": info.get("financialCurrency") or info.get("currency"),
            "regular_market_time": _timestamp_iso(info.get("regularMarketTime")),
            "regular_market_open": _safe_round(info.get("open") or info.get("regularMarketOpen")),
            "first_trade_date": _timestamp_iso(
                info.get("firstTradeDateMilliseconds", 0) / 1000
                if info.get("firstTradeDateMilliseconds")
                else None
            ),
            "last_split_date": _timestamp_iso(info.get("lastSplitDate")),
            "last_split_factor": info.get("lastSplitFactor"),

            # Share structure
            "shares_outstanding": info.get("sharesOutstanding"),
            "float_shares": info.get("floatShares"),
            "held_percent_insiders": _safe_round(info.get("heldPercentInsiders"), 4),
            "held_percent_institutions": _safe_round(info.get("heldPercentInstitutions"), 4),

            # Risk & governance
            "beta": _safe_round(info.get("beta"), 3),
            "audit_risk": info.get("auditRisk"),
            "board_risk": info.get("boardRisk"),
            "compensation_risk": info.get("compensationRisk"),
            "share_holder_rights_risk": info.get("shareHolderRightsRisk"),
            "overall_risk": info.get("overallRisk"),

            # Dividends
            "dividend_rate": _safe_round(info.get("dividendRate")),
            "dividend_yield": _safe_round(info.get("dividendYield"), 4),

            # Analyst sentiment
            "number_of_analyst_opinions": info.get("numberOfAnalystOpinions"),
            "recommendation_key": info.get("recommendationKey"),

            # Financials (latest snapshot)
            "total_revenue": info.get("totalRevenue"),
            "total_debt": info.get("totalDebt"),
            "total_cash": info.get("totalCash"),
            "free_cashflow": info.get("freeCashflow"),
            "book_value": _safe_round(info.get("bookValue")),
            "revenue_growth": _safe_round(info.get("revenueGrowth"), 4),
            "earnings_growth": _safe_round(info.get("earningsGrowth"), 4),
            "profit_margins": _safe_round(info.get("profitMargins"), 4),
            "gross_margins": _safe_round(info.get("grossMargins"), 4),
            "operating_margins": _safe_round(info.get("operatingMargins"), 4),
            "return_on_assets": _safe_round(info.get("returnOnAssets"), 4),
            "return_on_equity": _safe_round(info.get("returnOnEquity"), 4),
            "trailing_pe": _safe_round(info.get("trailingPE")),
            "forward_pe": _safe_round(info.get("forwardPE")),
            "peg_ratio": _safe_round(info.get("pegRatio")),
            "trailing_eps": _safe_round(info.get("trailingEps")),
            "forward_eps": _safe_round(info.get("forwardEps")),

            "extracted_at": datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception as e:
        logger.error("Company info error for %s: %s", symbol, e)
        return {"error": "Lookup Error", "symbol": symbol, "message": str(e)}


def extract_hourly_prices(symbols: List[str] | None = None) -> List[Dict]:
    """Fetch hourly price snapshots for all given symbols (default: NASDAQ 100)."""
    symbols = symbols or NASDAQ_100_SYMBOLS
    results = []
    for sym in symbols:
        logger.info("Fetching hourly data for %s", sym)
        results.append(get_hourly_stock_data(sym))
    return results


def extract_company_info(symbols: List[str] | None = None) -> List[Dict]:
    """Fetch company info for all given symbols (default: NASDAQ 100)."""
    symbols = symbols or NASDAQ_100_SYMBOLS
    results = []
    for sym in symbols:
        logger.info("Fetching company info for %s", sym)
        results.append(get_company_info(sym))
    return results


def write_json(data: List[Dict], filename: str, path: Path) -> Path:
    """Write a list of dicts to a timestamped JSON file under DATA_DIR."""
    path.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    path_str = path / f"{filename}_{ts}0000.json"
    with open(path_str, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info("Wrote %d records to %s", len(data), path_str)
    return path_str
