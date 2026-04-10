"""Shared test fixtures."""

from __future__ import annotations

import pytest
import respx
from pydantic import ValidationError as PydanticValidationError

from fmp_data import AsyncFMPDataClient
from fmp_data.base import BaseClient
from fmp_data.config import ClientConfig, RateLimitConfig
from cassandra_fmp.clients.polygon import PolygonClient
from cassandra_fmp.clients.thetadata import ThetaDataClient
from cassandra_fmp.tools._helpers import _CACHE


_ORIGINAL_VALIDATE_MODEL = BaseClient._validate_model


def _patched_validate_model(endpoint_name, model, payload, validation_mode):
    try:
        return _ORIGINAL_VALIDATE_MODEL(endpoint_name, model, payload, validation_mode)
    except PydanticValidationError:
        # Keep strict mode behavior intact; allow partial model construction for test fixtures.
        if validation_mode == "strict":
            raise
        return model.model_construct(**payload)


BaseClient._validate_model = staticmethod(_patched_validate_model)


@pytest.fixture(autouse=True)
def clear_safe_call_cache():
    _CACHE.clear()
    yield
    _CACHE.clear()


def build_test_client(api_key: str = "test_key") -> AsyncFMPDataClient:
    return AsyncFMPDataClient(
        config=ClientConfig(
            api_key=api_key,
            validation_mode="lenient",
            rate_limit=RateLimitConfig(
                daily_limit=1_000_000,
                requests_per_second=1_000,
                requests_per_minute=60_000,
            ),
        )
    )


@pytest.fixture
def sdk_client():
    """Create an AsyncFMPDataClient with a test API key."""
    return build_test_client("test_key")


@pytest.fixture
def polygon_client():
    """Create a PolygonClient with a test API key."""
    return PolygonClient(api_key="test_polygon_key")


@pytest.fixture
def mock_api():
    """Start respx mock for FMP API calls."""
    with respx.mock(base_url="https://financialmodelingprep.com", assert_all_called=False) as api:
        yield api


@pytest.fixture
def mock_polygon_api():
    """Start respx mock for Polygon API calls."""
    with respx.mock(base_url="https://api.polygon.io", assert_all_called=False) as api:
        yield api


THETA_TEST_URL = "http://theta-test:25510"


@pytest.fixture
def theta_client():
    """Create a ThetaDataClient pointing at the test base URL."""
    return ThetaDataClient(base_url=THETA_TEST_URL)


@pytest.fixture
def mock_theta_api():
    """Start respx mock for ThetaTerminal REST API calls."""
    with respx.mock(base_url=THETA_TEST_URL, assert_all_called=False) as api:
        yield api


# --- Sample response data matching /stable/ API ---

AAPL_PROFILE = [{
    "symbol": "AAPL",
    "companyName": "Apple Inc.",
    "sector": "Technology",
    "industry": "Consumer Electronics",
    "ceo": "Mr. Timothy D. Cook",
    "fullTimeEmployees": "164000",
    "description": "Apple Inc. designs, manufactures, and markets smartphones...",
    "exchange": "NASDAQ",
    "country": "US",
    "website": "https://www.apple.com",
    "price": 273.68,
    "marketCap": 4022528102504,
    "beta": 1.107,
    "range": "169.21-288.62",
}]

AAPL_QUOTE = [{
    "symbol": "AAPL",
    "name": "Apple Inc.",
    "price": 273.68,
    "changePercentage": -0.34,
    "change": -0.94,
    "volume": 34311675,
    "dayLow": 272.94,
    "dayHigh": 275.36,
    "yearHigh": 288.62,
    "yearLow": 169.21,
    "marketCap": 4022528102504,
    "priceAvg50": 268.66,
    "priceAvg200": 238.92,
    "exchange": "NASDAQ",
    "open": 274.885,
    "previousClose": 274.62,
    "timestamp": 1770757202,
}]

AAPL_RATIOS = [{
    "symbol": "AAPL",
    "grossProfitMarginTTM": 0.4733,
    "operatingProfitMarginTTM": 0.3238,
    "netProfitMarginTTM": 0.2704,
    "priceToEarningsRatioTTM": 34.27,
    "priceToEarningsGrowthRatioTTM": 5.83,
    "priceToBookRatioTTM": 45.77,
    "priceToSalesRatioTTM": 9.23,
    "priceToFreeCashFlowRatioTTM": 32.62,
    "enterpriseValueMultipleTTM": 27.5,
    "dividendYieldTTM": 0.005,
    "returnOnEquityTTM": 1.56,
    "returnOnAssetsTTM": 0.33,
    "debtToEquityRatioTTM": 1.03,
    "currentRatioTTM": 0.97,
}]

AAPL_INCOME = [
    {
        "date": "2025-09-27", "symbol": "AAPL", "period": "FY",
        "revenue": 416161000000, "grossProfit": 195201000000,
        "operatingIncome": 133050000000, "netIncome": 112010000000,
        "eps": 7.49, "epsDiluted": 7.46, "ebitda": 144427000000,
    },
    {
        "date": "2024-09-28", "symbol": "AAPL", "period": "FY",
        "revenue": 391035000000, "grossProfit": 180683000000,
        "operatingIncome": 123216000000, "netIncome": 93736000000,
        "eps": 6.08, "epsDiluted": 6.08, "ebitda": 134658000000,
    },
    {
        "date": "2023-09-30", "symbol": "AAPL", "period": "FY",
        "revenue": 383285000000, "grossProfit": 169148000000,
        "operatingIncome": 114301000000, "netIncome": 96995000000,
        "eps": 6.16, "epsDiluted": 6.13, "ebitda": 125820000000,
    },
    {
        "date": "2022-09-24", "symbol": "AAPL", "period": "FY",
        "revenue": 394328000000, "grossProfit": 170782000000,
        "operatingIncome": 119437000000, "netIncome": 99803000000,
        "eps": 6.15, "epsDiluted": 6.11, "ebitda": 130541000000,
    },
]

AAPL_BALANCE = [
    {"date": "2025-09-27", "symbol": "AAPL", "period": "FY", "totalAssets": 359241000000, "totalLiabilities": 285508000000, "totalStockholdersEquity": 73733000000, "totalDebt": 98657000000, "cashAndCashEquivalents": 35934000000, "netDebt": 62723000000},
    {"date": "2024-09-28", "symbol": "AAPL", "period": "FY", "totalAssets": 364980000000, "totalLiabilities": 308030000000, "totalStockholdersEquity": 56950000000, "totalDebt": 97300000000, "cashAndCashEquivalents": 29943000000, "netDebt": 49070000000},
    {"date": "2023-09-30", "symbol": "AAPL", "period": "FY", "totalAssets": 352583000000, "totalLiabilities": 290437000000, "totalStockholdersEquity": 62146000000, "totalDebt": 111088000000, "cashAndCashEquivalents": 29965000000, "netDebt": 81123000000},
    {"date": "2022-09-24", "symbol": "AAPL", "period": "FY", "totalAssets": 352755000000, "totalLiabilities": 302083000000, "totalStockholdersEquity": 50672000000, "totalDebt": 120069000000, "cashAndCashEquivalents": 23646000000, "netDebt": 96423000000},
]

AAPL_CASHFLOW = [
    {"date": "2025-09-27", "symbol": "AAPL", "period": "FY", "operatingCashFlow": 111482000000, "capitalExpenditure": -12715000000, "freeCashFlow": 98767000000, "commonDividendsPaid": -15421000000, "commonStockRepurchased": -90711000000},
    {"date": "2024-09-28", "symbol": "AAPL", "period": "FY", "operatingCashFlow": 118254000000, "capitalExpenditure": -9959000000, "freeCashFlow": 108295000000, "commonDividendsPaid": -15025000000, "commonStockRepurchased": -94949000000},
    {"date": "2023-09-30", "symbol": "AAPL", "period": "FY", "operatingCashFlow": 110543000000, "capitalExpenditure": -11052000000, "freeCashFlow": 99584000000, "commonDividendsPaid": -15025000000, "commonStockRepurchased": -77550000000},
    {"date": "2022-09-24", "symbol": "AAPL", "period": "FY", "operatingCashFlow": 122151000000, "capitalExpenditure": -10708000000, "freeCashFlow": 111443000000, "commonDividendsPaid": -14841000000, "commonStockRepurchased": -89402000000},
]

AAPL_PRICE_TARGET = [{
    "symbol": "AAPL",
    "targetConsensus": 303.11,
    "targetHigh": 350,
    "targetLow": 220,
    "targetMedian": 315,
}]

AAPL_GRADES = [{
    "symbol": "AAPL",
    "strongBuy": 1,
    "buy": 68,
    "hold": 33,
    "sell": 7,
    "strongSell": 0,
    "consensus": "Buy",
}]

AAPL_RATING = [{
    "symbol": "AAPL",
    "rating": "B",
    "overallScore": 3,
    "discountedCashFlowScore": 3,
    "returnOnEquityScore": 5,
    "returnOnAssetsScore": 5,
    "debtToEquityScore": 1,
    "priceToEarningsScore": 2,
    "priceToBookScore": 1,
}]

AAPL_SEARCH = [
    {"symbol": "APC.F", "name": "Apple Inc.", "currency": "EUR", "exchangeFullName": "Frankfurt", "exchange": "FSX"},
    {"symbol": "AAPL", "name": "Apple Inc.", "currency": "USD", "exchangeFullName": "NASDAQ Global Select", "exchange": "NASDAQ"},
]

AAPL_SCREENER = [
    {"symbol": "NVDA", "companyName": "NVIDIA Corporation", "marketCap": 4590383462958, "sector": "Technology", "industry": "Semiconductors", "price": 188.54, "exchangeShortName": "NASDAQ"},
    {"symbol": "AAPL", "companyName": "Apple Inc.", "marketCap": 4022528102504, "sector": "Technology", "industry": "Consumer Electronics", "price": 273.68, "exchangeShortName": "NASDAQ"},
]

# /stable/historical-price-eod/full returns flat list (not nested under "historical")
AAPL_HISTORICAL = [
    {"symbol": "AAPL", "date": "2025-02-11", "open": 228.2, "high": 235.23, "low": 228.13, "close": 232.62, "volume": 53718400},
    {"symbol": "AAPL", "date": "2025-02-10", "open": 227.0, "high": 229.5, "low": 226.5, "close": 228.50, "volume": 52000000},
    {"symbol": "AAPL", "date": "2025-02-07", "open": 226.0, "high": 228.0, "low": 225.5, "close": 227.20, "volume": 48000000},
] + [
    {"symbol": "AAPL", "date": f"2025-02-{6 - i:02d}" if 6 - i > 0 else f"2025-01-{31 + (6 - i):02d}",
     "close": 225.0 + i * 0.5, "volume": 50000000}
    for i in range(28)
]

AAPL_ANALYST_ESTIMATES = [
    {"symbol": "AAPL", "date": "2028-09-27", "revenueLow": 117592038224, "revenueHigh": 128328337770, "revenueAvg": 123401826658, "epsAvg": 2.45, "epsHigh": 2.58, "epsLow": 2.30, "numAnalystsRevenue": 8, "numAnalystsEps": 9, "ebitdaAvg": 42023491218},
    {"symbol": "AAPL", "date": "2028-06-27", "revenueLow": 98000000000, "revenueHigh": 110000000000, "revenueAvg": 105000000000, "epsAvg": 1.85, "epsHigh": 2.00, "epsLow": 1.72, "numAnalystsRevenue": 12, "numAnalystsEps": 14, "ebitdaAvg": 36000000000},
    {"symbol": "AAPL", "date": "2028-03-27", "revenueLow": 90000000000, "revenueHigh": 100000000000, "revenueAvg": 95000000000, "epsAvg": 1.62, "epsHigh": 1.75, "epsLow": 1.50, "numAnalystsRevenue": 15, "numAnalystsEps": 18, "ebitdaAvg": 33000000000},
]

AAPL_QUARTERLY_INCOME = [
    {"date": "2025-12-27", "symbol": "AAPL", "period": "Q1", "revenue": 124300000000, "netIncome": 36330000000, "eps": 2.42, "epsDiluted": 2.40},
    {"date": "2025-09-27", "symbol": "AAPL", "period": "Q4", "revenue": 94930000000, "netIncome": 24780000000, "eps": 1.65, "epsDiluted": 1.64},
    {"date": "2025-06-28", "symbol": "AAPL", "period": "Q3", "revenue": 85777000000, "netIncome": 21448000000, "eps": 1.41, "epsDiluted": 1.40},
    {"date": "2025-03-29", "symbol": "AAPL", "period": "Q2", "revenue": 95367000000, "netIncome": 23627000000, "eps": 1.55, "epsDiluted": 1.53},
]

# --- Insider & Institutional Ownership ---

AAPL_INSIDER_TRADES = [
    {"reportingName": "Timothy Cook", "typeOfOwner": "CEO", "transactionType": "S-Sale", "securitiesTransacted": 50000, "price": 270.0, "filingDate": "2026-02-01", "transactionDate": "2026-01-30"},
    {"reportingName": "Luca Maestri", "typeOfOwner": "CFO", "transactionType": "S-Sale", "securitiesTransacted": 20000, "price": 268.0, "filingDate": "2026-01-20", "transactionDate": "2026-01-18"},
    {"reportingName": "Jeff Williams", "typeOfOwner": "officer", "transactionType": "P-Purchase", "securitiesTransacted": 10000, "price": 260.0, "filingDate": "2026-01-15", "transactionDate": "2026-01-14"},
    {"reportingName": "Deirdre O'Brien", "typeOfOwner": "officer", "transactionType": "P-Purchase", "securitiesTransacted": 5000, "price": 258.0, "filingDate": "2026-01-10", "transactionDate": "2026-01-09"},
    {"reportingName": "Craig Federighi", "typeOfOwner": "officer", "transactionType": "P-Purchase", "securitiesTransacted": 8000, "price": 262.0, "filingDate": "2026-01-08", "transactionDate": "2026-01-07"},
]

AAPL_INSIDER_STATS = [{
    "symbol": "AAPL",
    "cik": "0000320193",
    "year": 2026,
    "quarter": 1,
    "acquiredTransactions": 3,
    "disposedTransactions": 2,
    "totalAcquired": 23000,
    "totalDisposed": 70000,
}]

AAPL_SHARES_FLOAT = [{
    "symbol": "AAPL",
    "floatShares": 14700000000,
    "outstandingShares": 15200000000,
    "freeFloat": 96.71,
    "date": "2026-01-31",
}]

AAPL_INSTITUTIONAL_SUMMARY = [{
    "symbol": "AAPL",
    "cik": "0000320193",
    "date": "2025-12-31",
    "investorsHolding": 3557,
    "lastInvestorsHolding": 5826,
    "investorsHoldingChange": -2269,
    "numberOf13Fshares": 10500000000,
    "ownershipPercent": 16.59,
}]

AAPL_INSTITUTIONAL_HOLDERS = [
    {"investorName": "VANGUARD GROUP INC", "sharesNumber": 1300000000, "changeInSharesNumber": 50000000, "date": "2025-12-31"},
    {"investorName": "BLACKROCK INC.", "sharesNumber": 1050000000, "changeInSharesNumber": -20000000, "date": "2025-12-31"},
    {"investorName": "BERKSHIRE HATHAWAY INC", "sharesNumber": 890000000, "changeInSharesNumber": 0, "date": "2025-12-31"},
    {"investorName": "STATE STREET CORP", "sharesNumber": 650000000, "changeInSharesNumber": 30000000, "date": "2025-12-31"},
    {"investorName": "FMR LLC", "sharesNumber": 420000000, "changeInSharesNumber": -15000000, "date": "2025-12-31"},
]

# --- Stock News ---

AAPL_NEWS = [
    {"symbol": "AAPL", "title": "Apple Reports Record Q1 Earnings", "publishedDate": "2026-02-10T16:30:00.000Z", "site": "Bloomberg", "url": "https://example.com/1", "text": "Apple Inc. reported record first quarter results..."},
    {"symbol": "AAPL", "title": "Apple Unveils New AI Features", "publishedDate": "2026-02-08T10:00:00.000Z", "site": "TechCrunch", "url": "https://example.com/2", "text": "Apple announced a suite of new AI-powered features..."},
    {"symbol": "AAPL", "title": "Apple Stock Rises on Strong Guidance", "publishedDate": "2026-02-07T14:00:00.000Z", "site": "CNBC", "url": "https://example.com/3", "text": "Shares of Apple rose 3% following strong guidance..."},
]

AAPL_PRESS_RELEASES = [
    {"title": "Apple Announces Dividend Increase", "publishedDate": "2026-02-05T09:00:00.000Z", "site": "Business Wire", "url": "https://example.com/pr1", "text": "Apple today announced an increase to its quarterly dividend..."},
    {"title": "Apple Reports Record Q1 Earnings", "publishedDate": "2026-02-03T16:30:00.000Z", "site": "GlobeNewsWire", "url": "https://example.com/pr2", "text": "Apple today announced financial results for Q1 FY2026..."},
]

# --- Treasury Rates & Macro ---

TREASURY_RATES = [{
    "date": "2026-02-10",
    "month1": 4.32,
    "month3": 4.28,
    "month6": 4.15,
    "year1": 3.95,
    "year2": 3.82,
    "year5": 3.90,
    "year10": 4.05,
    "year20": 4.35,
    "year30": 4.42,
}]

MARKET_RISK_PREMIUM = [
    {"country": "United States", "totalEquityRiskPremium": 4.60, "countryRiskPremium": 0.0},
    {"country": "United Kingdom", "totalEquityRiskPremium": 5.20, "countryRiskPremium": 0.60},
]

ECONOMIC_CALENDAR = [
    {"date": "2026-02-12 08:30:00", "event": "CPI (MoM)", "country": "US", "estimate": 0.3, "actual": None, "previous": 0.4, "change": None, "impact": "High"},
    {"date": "2026-02-14 08:30:00", "event": "Retail Sales (MoM)", "country": "US", "estimate": 0.2, "actual": None, "previous": 0.4, "change": None, "impact": "High"},
    {"date": "2026-02-13 08:30:00", "event": "PPI (MoM)", "country": "US", "estimate": 0.1, "actual": None, "previous": 0.2, "change": None, "impact": "Medium"},
    {"date": "2026-02-12 10:00:00", "event": "Business Inventories", "country": "US", "estimate": 0.1, "actual": None, "previous": 0.1, "change": None, "impact": "Low"},
    {"date": "2026-02-13 07:00:00", "event": "ECB Rate Decision", "country": "EU", "estimate": 2.50, "actual": None, "previous": 2.75, "change": None, "impact": "High"},
]

SECTOR_PERFORMANCE_NYSE = [
    {"date": "2026-02-10", "sector": "Technology", "exchange": "NYSE", "averageChange": 1.15},
    {"date": "2026-02-10", "sector": "Healthcare", "exchange": "NYSE", "averageChange": 0.75},
    {"date": "2026-02-10", "sector": "Financial Services", "exchange": "NYSE", "averageChange": -0.42},
    {"date": "2026-02-10", "sector": "Energy", "exchange": "NYSE", "averageChange": -1.00},
]

SECTOR_PERFORMANCE_NASDAQ = [
    {"date": "2026-02-10", "sector": "Technology", "exchange": "NASDAQ", "averageChange": 1.35},
    {"date": "2026-02-10", "sector": "Healthcare", "exchange": "NASDAQ", "averageChange": 0.95},
    {"date": "2026-02-10", "sector": "Financial Services", "exchange": "NASDAQ", "averageChange": -0.22},
    {"date": "2026-02-10", "sector": "Energy", "exchange": "NASDAQ", "averageChange": -1.20},
]

BIGGEST_GAINERS = [
    {"symbol": "XYZ", "name": "XYZ Corp", "price": 45.50, "changesPercentage": 15.3, "exchange": "NASDAQ"},
    {"symbol": "ABC", "name": "ABC Inc", "price": 120.00, "changesPercentage": 12.1, "exchange": "NYSE"},
    {"symbol": "TINY", "name": "Tiny Micro Corp", "price": 0.50, "changesPercentage": 200.0, "exchange": "NASDAQ"},
]

BIGGEST_LOSERS = [
    {"symbol": "DEF", "name": "DEF Corp", "price": 22.30, "changesPercentage": -10.5, "exchange": "NYSE"},
    {"symbol": "GHI", "name": "GHI Inc", "price": 8.75, "changesPercentage": -8.2, "exchange": "NASDAQ"},
]

MOST_ACTIVES = [
    {"symbol": "NVDA", "name": "NVIDIA Corporation", "price": 188.54, "changesPercentage": 2.3, "exchange": "NASDAQ"},
    {"symbol": "TSLA", "name": "Tesla Inc", "price": 350.00, "changesPercentage": -1.5, "exchange": "NASDAQ"},
]

# Batch quote data for movers market cap filtering
MOVERS_BATCH_QUOTE = [
    {"symbol": "XYZ", "marketCap": 5000000000},       # $5B - passes filter
    {"symbol": "ABC", "marketCap": 15000000000},       # $15B - passes filter
    {"symbol": "TINY", "marketCap": 50000000},         # $50M - filtered out
    {"symbol": "DEF", "marketCap": 3000000000},        # $3B - passes filter
    {"symbol": "GHI", "marketCap": 2000000000},        # $2B - passes filter
    {"symbol": "NVDA", "marketCap": 4590000000000},    # $4.59T - passes filter
    {"symbol": "TSLA", "marketCap": 1100000000000},    # $1.1T - passes filter
]

# --- Earnings Transcript ---

AAPL_TRANSCRIPT_DATES = [
    {"quarter": 1, "fiscalYear": 2026, "date": "2026-01-29"},
    {"quarter": 4, "fiscalYear": 2025, "date": "2025-10-30"},
    {"quarter": 3, "fiscalYear": 2025, "date": "2025-07-31"},
]

AAPL_TRANSCRIPT = [{
    "symbol": "AAPL",
    "quarter": 1,
    "year": 2026,
    "date": "2026-01-29",
    "content": (
        "Good afternoon, everyone. Thank you for joining Apple's fiscal year 2026 first quarter earnings conference call.\n"
        "I'm Tim Cook, CEO of Apple. We had a great quarter with record revenue of $124.3 billion.\n"
        "Our services business reached an all-time high, and we saw strong momentum across every product category.\n"
        "Now let me turn it over to our CFO, Kevan Parekh, for the financial details.\n"
        "Thanks, Tim. Revenue was up 16% year over year, driven by strong iPhone and Services performance.\n"
        "Gross margin came in at 47.3%, up 120 basis points from the year-ago quarter.\n"
        "We returned over $29 billion to shareholders through dividends and share repurchases.\n"
        "Operator, we are ready for questions.\n"
        "Our first question comes from Amit Daryanani of Evercore. Please go ahead.\n"
        "Thanks. Tim, can you talk about the demand environment heading into the March quarter?\n"
        "Sure, Amit. We are seeing strong demand across all geographies and remain very optimistic.\n"
        "Thank you. That concludes today's call."
    ),
}]

# --- Revenue Segments ---

AAPL_PRODUCT_SEGMENTS = [
    {"2025-09-27": {"iPhone": 200500000000, "Mac": 40200000000, "iPad": 32100000000, "Wearables, Home and Accessories": 41800000000, "Services": 101561000000}},
    {"2024-09-28": {"iPhone": 201183000000, "Mac": 29357000000, "iPad": 26694000000, "Wearables, Home and Accessories": 37005000000, "Services": 96800000000}},
]

AAPL_GEO_SEGMENTS = [
    {"2025-09-27": {"Americas": 172100000000, "Europe": 101400000000, "Greater China": 67200000000, "Japan": 27600000000, "Rest of Asia Pacific": 47861000000}},
    {"2024-09-28": {"Americas": 167000000000, "Europe": 94300000000, "Greater China": 66700000000, "Japan": 25000000000, "Rest of Asia Pacific": 38035000000}},
]

# --- Peer Comparison ---

AAPL_PEERS = [
    {"symbol": "MSFT", "companyName": "Microsoft Corporation", "price": 413.27, "mktCap": 3068790110100},
    {"symbol": "GOOGL", "companyName": "Alphabet Inc.", "price": 318.58, "mktCap": 3853862159966},
    {"symbol": "AMZN", "companyName": "Amazon.com, Inc.", "price": 232.84, "mktCap": 2050000000000},
]

AAPL_KEY_METRICS = [{
    "symbol": "AAPL",
    "revenuePerShareTTM": 27.05,
    "marketCapTTM": 4022528102504,
}]

MSFT_RATIOS = [{
    "symbol": "MSFT",
    "priceToEarningsRatioTTM": 32.50,
    "priceToSalesRatioTTM": 12.10,
    "enterpriseValueMultipleTTM": 25.30,
    "priceToBookRatioTTM": 12.50,
    "returnOnEquityTTM": 0.38,
    "grossProfitMarginTTM": 0.69,
    "netProfitMarginTTM": 0.37,
}]

MSFT_KEY_METRICS = [{
    "symbol": "MSFT",
    "revenuePerShareTTM": 33.15,
    "marketCapTTM": 3100000000000,
}]

GOOGL_RATIOS = [{
    "symbol": "GOOGL",
    "priceToEarningsRatioTTM": 22.80,
    "priceToSalesRatioTTM": 6.50,
    "enterpriseValueMultipleTTM": 18.20,
    "priceToBookRatioTTM": 6.80,
    "returnOnEquityTTM": 0.30,
    "grossProfitMarginTTM": 0.57,
    "netProfitMarginTTM": 0.26,
}]

GOOGL_KEY_METRICS = [{
    "symbol": "GOOGL",
    "revenuePerShareTTM": 29.50,
    "marketCapTTM": 2200000000000,
}]

AMZN_RATIOS = [{
    "symbol": "AMZN",
    "priceToEarningsRatioTTM": 58.90,
    "priceToSalesRatioTTM": 3.20,
    "enterpriseValueMultipleTTM": 22.50,
    "priceToBookRatioTTM": 8.10,
    "returnOnEquityTTM": 0.22,
    "grossProfitMarginTTM": 0.48,
    "netProfitMarginTTM": 0.08,
}]

AMZN_KEY_METRICS = [{
    "symbol": "AMZN",
    "revenuePerShareTTM": 58.20,
    "marketCapTTM": 2050000000000,
}]

# --- Dividends & Splits ---

AAPL_DIVIDENDS = [
    {"date": "2026-02-07", "dividend": 0.26, "paymentDate": "2026-02-14", "recordDate": "2026-02-10"},
    {"date": "2025-11-08", "dividend": 0.26, "paymentDate": "2025-11-15", "recordDate": "2025-11-11"},
    {"date": "2025-08-11", "dividend": 0.26, "paymentDate": "2025-08-18", "recordDate": "2025-08-13"},
    {"date": "2025-05-12", "dividend": 0.25, "paymentDate": "2025-05-19", "recordDate": "2025-05-14"},
    {"date": "2025-02-07", "dividend": 0.25, "paymentDate": "2025-02-14", "recordDate": "2025-02-10"},
    {"date": "2024-11-08", "dividend": 0.25, "paymentDate": "2024-11-15", "recordDate": "2024-11-11"},
    {"date": "2024-08-12", "dividend": 0.25, "paymentDate": "2024-08-19", "recordDate": "2024-08-14"},
    {"date": "2024-05-10", "dividend": 0.24, "paymentDate": "2024-05-17", "recordDate": "2024-05-13"},
    {"date": "2024-02-09", "dividend": 0.24, "paymentDate": "2024-02-16", "recordDate": "2024-02-12"},
    {"date": "2023-11-10", "dividend": 0.24, "paymentDate": "2023-11-17", "recordDate": "2023-11-13"},
    {"date": "2023-08-11", "dividend": 0.24, "paymentDate": "2023-08-18", "recordDate": "2023-08-14"},
    {"date": "2023-05-12", "dividend": 0.23, "paymentDate": "2023-05-19", "recordDate": "2023-05-15"},
    {"date": "2023-02-10", "dividend": 0.23, "paymentDate": "2023-02-17", "recordDate": "2023-02-13"},
    {"date": "2022-11-04", "dividend": 0.23, "paymentDate": "2022-11-11", "recordDate": "2022-11-07"},
    {"date": "2022-08-05", "dividend": 0.23, "paymentDate": "2022-08-12", "recordDate": "2022-08-08"},
    {"date": "2022-05-06", "dividend": 0.22, "paymentDate": "2022-05-13", "recordDate": "2022-05-09"},
    {"date": "2022-02-04", "dividend": 0.22, "paymentDate": "2022-02-11", "recordDate": "2022-02-07"},
    {"date": "2021-11-05", "dividend": 0.22, "paymentDate": "2021-11-12", "recordDate": "2021-11-08"},
    {"date": "2021-08-06", "dividend": 0.22, "paymentDate": "2021-08-13", "recordDate": "2021-08-09"},
    {"date": "2021-05-07", "dividend": 0.22, "paymentDate": "2021-05-14", "recordDate": "2021-05-10"},
    {"date": "2021-02-05", "dividend": 0.205, "paymentDate": "2021-02-12", "recordDate": "2021-02-08"},
    {"date": "2020-11-06", "dividend": 0.205, "paymentDate": "2020-11-13", "recordDate": "2020-11-09"},
]

AAPL_STOCK_SPLITS = [
    {"date": "2020-08-28", "label": "4:1", "numerator": 4, "denominator": 1},
    {"date": "2014-06-09", "label": "7:1", "numerator": 7, "denominator": 1},
]

# --- FINRA Short Interest (external, non-FMP) ---

AAPL_SHORT_INTEREST = [{
    "settlementDate": "2026-01-30",
    "currentShortPositionQuantity": 116854414,
    "previousShortPositionQuantity": 113576032,
    "changePreviousNumber": 3278382,
    "changePercent": 2.89,
    "averageDailyVolumeQuantity": 58429082,
    "daysToCoverQuantity": 2.0,
}]

# --- Earnings Calendar ---

EARNINGS_CALENDAR = [
    {"symbol": "AAPL", "date": "2026-02-14", "time": "amc", "fiscalDateEnding": "2025-12-27", "epsEstimated": 2.35, "revenueEstimated": 118700000000, "epsActual": None, "revenueActual": None},
    {"symbol": "MSFT", "date": "2026-02-12", "time": "bmo", "fiscalDateEnding": "2025-12-31", "epsEstimated": 3.10, "revenueEstimated": 68500000000, "epsActual": None, "revenueActual": None},
    {"symbol": "GOOGL", "date": "2026-02-13", "time": "amc", "fiscalDateEnding": "2025-12-31", "epsEstimated": 2.05, "revenueEstimated": 95000000000, "epsActual": None, "revenueActual": None},
    {"symbol": "TSLA", "date": "2026-02-15", "time": "--", "fiscalDateEnding": "2025-12-31", "epsEstimated": 0.85, "revenueEstimated": 27500000000, "epsActual": None, "revenueActual": None},
]

EARNINGS_BATCH_QUOTE = [
    {"symbol": "AAPL", "marketCap": 3500000000000},   # $3.5T
    {"symbol": "MSFT", "marketCap": 3200000000000},    # $3.2T
    {"symbol": "GOOGL", "marketCap": 2100000000000},   # $2.1T
    {"symbol": "TSLA", "marketCap": 1100000000000},    # $1.1T
]

# --- ETF Holdings & Exposure ---

QQQ_HOLDINGS = [
    {"asset": "AAPL", "name": "Apple Inc.", "weightPercentage": 12.5, "sharesNumber": 50000000},
    {"asset": "MSFT", "name": "Microsoft Corporation", "weightPercentage": 10.8, "sharesNumber": 30000000},
    {"asset": "NVDA", "name": "NVIDIA Corporation", "weightPercentage": 9.2, "sharesNumber": 25000000},
    {"asset": "AMZN", "name": "Amazon.com, Inc.", "weightPercentage": 7.1, "sharesNumber": 18000000},
    {"asset": "META", "name": "Meta Platforms, Inc.", "weightPercentage": 5.3, "sharesNumber": 12000000},
    {"asset": "GOOGL", "name": "Alphabet Inc.", "weightPercentage": 4.8, "sharesNumber": 10000000},
    {"asset": "GOOG", "name": "Alphabet Inc. Class C", "weightPercentage": 4.5, "sharesNumber": 9500000},
    {"asset": "AVGO", "name": "Broadcom Inc.", "weightPercentage": 3.9, "sharesNumber": 8000000},
    {"asset": "TSLA", "name": "Tesla Inc", "weightPercentage": 3.6, "sharesNumber": 7000000},
    {"asset": "COST", "name": "Costco Wholesale", "weightPercentage": 2.8, "sharesNumber": 5000000},
]

AAPL_ETF_EXPOSURE = [
    {"etfSymbol": "QQQ", "weightPercentage": 12.5},
    {"etfSymbol": "SPY", "weightPercentage": 7.2},
    {"etfSymbol": "VTI", "weightPercentage": 6.8},
    {"etfSymbol": "VOO", "weightPercentage": 7.1},
    {"etfSymbol": "XLK", "weightPercentage": 22.3},
]

# --- /stable/earnings (combined future estimates + historical actuals) ---

AAPL_EARNINGS = [
    # Future estimate (next earnings)
    {"date": "2026-04-30", "symbol": "AAPL", "epsEstimated": 1.68, "revenueEstimated": 98500000000, "fiscalDateEnding": "2026-03-28", "numberOfAnalysts": 28},
    # Recent actuals
    {"date": "2026-01-29", "symbol": "AAPL", "epsActual": 2.42, "epsEstimated": 2.35, "revenueActual": 124300000000, "revenueEstimated": 118700000000, "fiscalDateEnding": "2025-12-27"},
    {"date": "2025-10-30", "symbol": "AAPL", "epsActual": 1.64, "epsEstimated": 1.60, "revenueActual": 94930000000, "revenueEstimated": 94300000000, "fiscalDateEnding": "2025-09-27"},
    {"date": "2025-07-31", "symbol": "AAPL", "epsActual": 1.40, "epsEstimated": 1.35, "revenueActual": 85777000000, "revenueEstimated": 84500000000, "fiscalDateEnding": "2025-06-28"},
    {"date": "2025-05-01", "symbol": "AAPL", "epsActual": 1.53, "epsEstimated": 1.50, "revenueActual": 95367000000, "revenueEstimated": 94200000000, "fiscalDateEnding": "2025-03-29"},
    {"date": "2025-01-30", "symbol": "AAPL", "epsActual": 2.40, "epsEstimated": 2.36, "revenueActual": 124000000000, "revenueEstimated": 120500000000, "fiscalDateEnding": "2024-12-28"},
    {"date": "2024-10-31", "symbol": "AAPL", "epsActual": 1.64, "epsEstimated": 1.58, "revenueActual": 94900000000, "revenueEstimated": 93000000000, "fiscalDateEnding": "2024-09-28"},
    {"date": "2024-08-01", "symbol": "AAPL", "epsActual": 1.40, "epsEstimated": 1.34, "revenueActual": 85800000000, "revenueEstimated": 84300000000, "fiscalDateEnding": "2024-06-29"},
    {"date": "2024-05-02", "symbol": "AAPL", "epsActual": 1.53, "epsEstimated": 1.50, "revenueActual": 90753000000, "revenueEstimated": 90000000000, "fiscalDateEnding": "2024-03-30"},
]

# --- Key Executives ---

AAPL_EXECUTIVES = [
    {"name": "Timothy D. Cook", "title": "Chief Executive Officer", "pay": 16425933, "currencyPay": "USD", "gender": "male", "yearBorn": 1960, "titleSince": "2011-08-24", "active": True},
    {"name": "Luca Maestri", "title": "Chief Financial Officer", "pay": 6012461, "currencyPay": "USD", "gender": "male", "yearBorn": 1963, "titleSince": "2014-05-01", "active": True},
    {"name": "Jeff Williams", "title": "Chief Operating Officer", "pay": 5819125, "currencyPay": "USD", "gender": "male", "yearBorn": 1964, "titleSince": "2015-12-17", "active": True},
    {"name": "Katherine Adams", "title": "General Counsel", "pay": None, "currencyPay": "USD", "gender": "female", "yearBorn": 1964, "titleSince": "2017-11-09", "active": True},
]

# --- SEC Filings (via CIK-based search) ---

AAPL_PROFILE_WITH_CIK = [{
    "symbol": "AAPL",
    "companyName": "Apple Inc.",
    "sector": "Technology",
    "industry": "Consumer Electronics",
    "ceo": "Mr. Timothy D. Cook",
    "cik": "0000320193",
    "exchange": "NASDAQ",
    "country": "US",
}]

AAPL_SEC_FILINGS = [
    {"symbol": "AAPL", "cik": "0000320193", "filingDate": "2026-01-30", "acceptedDate": "2026-01-30 16:30:00", "formType": "10-Q", "link": "https://example.com/10q", "finalLink": "https://example.com/10q-final"},
    {"symbol": "AAPL", "cik": "0000320193", "filingDate": "2026-01-15", "acceptedDate": "2026-01-15 10:00:00", "formType": "8-K", "link": "https://example.com/8k", "finalLink": "https://example.com/8k-final"},
    {"symbol": "AAPL", "cik": "0000320193", "filingDate": "2025-11-01", "acceptedDate": "2025-11-01 16:30:00", "formType": "10-K", "link": "https://example.com/10k", "finalLink": "https://example.com/10k-final"},
    {"symbol": "AAPL", "cik": "0000320193", "filingDate": "2025-10-15", "acceptedDate": "2025-10-15 10:00:00", "formType": "8-K", "link": "https://example.com/8k2", "finalLink": "https://example.com/8k2-final"},
]

# --- Technical Indicators ---

AAPL_RSI = [
    {"date": "2026-02-11", "open": 274.0, "high": 276.0, "low": 273.0, "close": 275.5, "volume": 45000000, "rsi": 58.32},
    {"date": "2026-02-10", "open": 273.0, "high": 275.0, "low": 272.0, "close": 274.0, "volume": 42000000, "rsi": 55.18},
    {"date": "2026-02-07", "open": 271.0, "high": 274.0, "low": 270.0, "close": 273.0, "volume": 48000000, "rsi": 52.44},
]

# --- Financial Health ---

AAPL_FINANCIAL_SCORES = [{
    "symbol": "AAPL",
    "altmanZScore": 8.21,
    "piotroskiScore": 7,
    "workingCapital": -1234000000,
    "totalAssets": 359241000000,
    "retainedEarnings": 4336000000,
    "ebit": 133050000000,
    "marketCap": 4022528102504,
    "totalLiabilities": 285508000000,
    "revenue": 416161000000,
}]

AAPL_OWNER_EARNINGS = [{
    "symbol": "AAPL",
    "date": "2025-09-27",
    "ownersEarnings": 95432000000,
    "ownersEarningsPerShare": 6.38,
    "averagePPE": 42500000000,
    "maintenanceCapex": -8500000000,
    "growthCapex": -4215000000,
}]

# --- IPO Calendar ---

IPO_CALENDAR = [
    {"symbol": "NEWCO", "date": "2026-02-20", "company": "NewCo Technologies", "exchange": "NASDAQ", "actions": "expected", "shares": 10000000, "priceRange": "18.00 - 22.00", "marketCap": 2200000000},
    {"symbol": "FRESH", "date": "2026-02-18", "company": "FreshFoods Inc", "exchange": "NYSE", "actions": "priced", "shares": 5000000, "priceRange": "12.00 - 15.00", "marketCap": 750000000},
]

# --- Dividends Calendar ---

DIVIDENDS_CALENDAR = [
    {"symbol": "AAPL", "date": "2026-02-14", "dividend": 0.26, "adjDividend": 0.26, "recordDate": "2026-02-17", "paymentDate": "2026-02-21", "yield": 0.38, "frequency": "quarterly"},
    {"symbol": "MSFT", "date": "2026-02-15", "dividend": 0.75, "adjDividend": 0.75, "recordDate": "2026-02-18", "paymentDate": "2026-02-25", "yield": 0.72, "frequency": "quarterly"},
    {"symbol": "JNJ", "date": "2026-02-16", "dividend": 1.24, "adjDividend": 1.24, "recordDate": "2026-02-19", "paymentDate": "2026-02-28", "yield": 3.15, "frequency": "quarterly"},
]

# --- Index Constituents ---

SP500_CONSTITUENTS = [
    {"symbol": "AAPL", "name": "Apple Inc.", "sector": "Information Technology", "subSector": "Technology Hardware, Storage & Peripherals", "headQuarter": "Cupertino, California", "dateFirstAdded": "1982-11-30", "cik": "0000320193", "founded": "1976"},
    {"symbol": "MSFT", "name": "Microsoft Corporation", "sector": "Information Technology", "subSector": "Systems Software", "headQuarter": "Redmond, Washington", "dateFirstAdded": "1994-06-01", "cik": "0000789019", "founded": "1975"},
    {"symbol": "AMZN", "name": "Amazon.com, Inc.", "sector": "Consumer Discretionary", "subSector": "Internet & Direct Marketing Retail", "headQuarter": "Seattle, Washington", "dateFirstAdded": "2005-11-18", "cik": "0001018724", "founded": "1994"},
]

# --- Sector PE Valuation ---

SECTOR_PE_NYSE = [
    {"date": "2026-02-11", "sector": "Technology", "exchange": "NYSE", "pe": 28.5},
    {"date": "2026-02-11", "sector": "Healthcare", "exchange": "NYSE", "pe": 22.1},
    {"date": "2026-02-11", "sector": "Financial Services", "exchange": "NYSE", "pe": 14.8},
]

SECTOR_PE_NASDAQ = [
    {"date": "2026-02-11", "sector": "Technology", "exchange": "NASDAQ", "pe": 35.2},
    {"date": "2026-02-11", "sector": "Healthcare", "exchange": "NASDAQ", "pe": 25.3},
    {"date": "2026-02-11", "sector": "Financial Services", "exchange": "NASDAQ", "pe": 16.2},
]

INDUSTRY_PE_NYSE = [
    {"date": "2026-02-11", "industry": "Banks", "exchange": "NYSE", "pe": 12.5},
    {"date": "2026-02-11", "industry": "Software", "exchange": "NYSE", "pe": 32.1},
    {"date": "2026-02-11", "industry": "Pharmaceuticals", "exchange": "NYSE", "pe": 18.3},
]

INDUSTRY_PE_NASDAQ = [
    {"date": "2026-02-11", "industry": "Software", "exchange": "NASDAQ", "pe": 38.7},
    {"date": "2026-02-11", "industry": "Biotechnology", "exchange": "NASDAQ", "pe": 45.2},
    {"date": "2026-02-11", "industry": "Semiconductors", "exchange": "NASDAQ", "pe": 30.5},
]

# --- M&A Activity ---

MNA_LATEST = [
    {"symbol": "TGT", "companyName": "Target Corp", "targetedCompanyName": "SmallRetail Inc", "targetedSymbol": "SRTL", "transactionDate": "2026-02-05", "acceptedDate": "2026-02-05 10:00:00", "link": "https://example.com/mna1"},
    {"symbol": "AMZN", "companyName": "Amazon.com Inc", "targetedCompanyName": "TechStartup LLC", "targetedSymbol": "TSLC", "transactionDate": "2026-01-20", "acceptedDate": "2026-01-20 10:00:00", "link": "https://example.com/mna2"},
]

MNA_SEARCH_AAPL = [
    {"symbol": "AAPL", "companyName": "Apple Inc.", "targetedCompanyName": "AI Labs Corp", "targetedSymbol": "AILB", "transactionDate": "2026-01-10", "acceptedDate": "2026-01-10 10:00:00", "link": "https://example.com/mna3"},
]

# --- Asset Quotes (commodity, crypto, forex) ---

GOLD_QUOTE = [{
    "symbol": "GCUSD",
    "name": "Gold",
    "price": 2045.30,
    "change": 12.50,
    "changePercentage": 0.61,
    "dayLow": 2030.00,
    "dayHigh": 2050.00,
    "yearLow": 1810.00,
    "yearHigh": 2075.00,
    "volume": 185000,
}]

BATCH_COMMODITIES = [
    {"symbol": "GCUSD", "price": 2045.30, "change": 12.50, "volume": 185000},
    {"symbol": "CLUSD", "price": 78.42, "change": -1.23, "volume": 320000},
    {"symbol": "SIUSD", "price": 23.15, "change": 0.45, "volume": 95000},
]

BTCUSD_QUOTE = [{
    "symbol": "BTCUSD",
    "name": "Bitcoin",
    "price": 97500.00,
    "change": 1250.00,
    "changePercentage": 1.30,
    "dayLow": 95800.00,
    "dayHigh": 98200.00,
    "yearLow": 38000.00,
    "yearHigh": 105000.00,
    "volume": 28500000000,
}]

BATCH_CRYPTO = [
    {"symbol": "BTCUSD", "price": 97500.00, "change": 1250.00, "volume": 28500000000},
    {"symbol": "ETHUSD", "price": 3200.00, "change": -45.00, "volume": 12000000000},
    {"symbol": "SOLUSD", "price": 145.00, "change": 8.50, "volume": 3500000000},
]

EURUSD_QUOTE = [{
    "symbol": "EURUSD",
    "name": "EUR/USD",
    "price": 1.0842,
    "change": 0.0023,
    "changePercentage": 0.21,
    "dayLow": 1.0810,
    "dayHigh": 1.0855,
    "yearLow": 1.0200,
    "yearHigh": 1.1100,
    "volume": 0,
}]

BATCH_FOREX = [
    {"symbol": "EURUSD", "price": 1.0842, "change": 0.0023, "volume": 0},
    {"symbol": "GBPUSD", "price": 1.2650, "change": -0.0045, "volume": 0},
    {"symbol": "USDJPY", "price": 149.85, "change": 0.35, "volume": 0},
]

# --- /stable/grades (individual analyst actions) ---

AAPL_GRADES_DETAIL = [
    {"symbol": "AAPL", "date": "2026-02-05", "gradingCompany": "Morgan Stanley", "previousGrade": "Overweight", "newGrade": "Overweight", "action": "maintain"},
    {"symbol": "AAPL", "date": "2026-02-03", "gradingCompany": "JP Morgan", "previousGrade": "Neutral", "newGrade": "Overweight", "action": "upgrade"},
    {"symbol": "AAPL", "date": "2026-01-30", "gradingCompany": "Goldman Sachs", "previousGrade": "Buy", "newGrade": "Buy", "action": "maintain"},
    {"symbol": "AAPL", "date": "2026-01-20", "gradingCompany": "Barclays", "previousGrade": "Overweight", "newGrade": "Equal Weight", "action": "downgrade"},
    {"symbol": "AAPL", "date": "2026-01-10", "gradingCompany": "Bank of America", "previousGrade": "Neutral", "newGrade": "Buy", "action": "upgrade"},
    {"symbol": "AAPL", "date": "2025-12-15", "gradingCompany": "UBS", "previousGrade": "", "newGrade": "Buy", "action": "initiate"},
    {"symbol": "AAPL", "date": "2025-11-20", "gradingCompany": "Wells Fargo", "previousGrade": "Equal Weight", "newGrade": "Underweight", "action": "downgrade"},
    {"symbol": "AAPL", "date": "2025-11-01", "gradingCompany": "Citi", "previousGrade": "Neutral", "newGrade": "Buy", "action": "upgrade"},
]

# --- Historical Key Metrics & Financial Ratios (for valuation_history and ratio_history) ---

AAPL_KEY_METRICS_HISTORICAL = [
    {"date": "2025-09-27", "period": "FY", "earningsYield": 0.02918, "evToSales": 9.23, "pbRatio": 45.77, "evToEBITDA": 27.5, "evToFreeCashFlow": 32.62, "returnOnEquity": 1.56, "returnOnAssets": 0.33, "currentRatio": 0.97, "roic": 0.55},
    {"date": "2024-09-28", "period": "FY", "earningsYield": 0.03175, "evToSales": 8.80, "pbRatio": 42.30, "evToEBITDA": 25.20, "evToFreeCashFlow": 30.10, "returnOnEquity": 1.48, "returnOnAssets": 0.31, "currentRatio": 0.95, "roic": 0.50},
    {"date": "2023-09-30", "period": "FY", "earningsYield": 0.03356, "evToSales": 7.50, "pbRatio": 38.90, "evToEBITDA": 23.50, "evToFreeCashFlow": 28.40, "returnOnEquity": 1.72, "returnOnAssets": 0.35, "currentRatio": 0.93, "roic": 0.52},
    {"date": "2022-09-24", "period": "FY", "earningsYield": 0.03937, "evToSales": 6.30, "pbRatio": 35.20, "evToEBITDA": 21.10, "evToFreeCashFlow": 25.80, "returnOnEquity": 1.95, "returnOnAssets": 0.38, "currentRatio": 0.91, "roic": 0.58},
    {"date": "2021-09-25", "period": "FY", "earningsYield": 0.03460, "evToSales": 7.80, "pbRatio": 40.50, "evToEBITDA": 24.30, "evToFreeCashFlow": 29.20, "returnOnEquity": 1.85, "returnOnAssets": 0.36, "currentRatio": 0.89, "roic": 0.54},
]

AAPL_FINANCIAL_RATIOS_HISTORICAL = [
    {"date": "2025-09-27", "period": "FY", "returnOnEquity": 1.56, "returnOnAssets": 0.33, "grossProfitMargin": 0.469, "operatingProfitMargin": 0.320, "netProfitMargin": 0.269, "assetTurnover": 1.05, "inventoryTurnover": 38.2, "cashConversionCycle": 45, "debtEquityRatio": 1.03, "interestCoverage": 25.5, "currentRatio": 0.97, "quickRatio": 0.85},
    {"date": "2024-09-28", "period": "FY", "returnOnEquity": 1.48, "returnOnAssets": 0.31, "grossProfitMargin": 0.462, "operatingProfitMargin": 0.315, "netProfitMargin": 0.240, "assetTurnover": 1.02, "inventoryTurnover": 36.5, "cashConversionCycle": 48, "debtEquityRatio": 1.15, "interestCoverage": 24.2, "currentRatio": 0.95, "quickRatio": 0.83},
    {"date": "2023-09-30", "period": "FY", "returnOnEquity": 1.72, "returnOnAssets": 0.35, "grossProfitMargin": 0.441, "operatingProfitMargin": 0.298, "netProfitMargin": 0.253, "assetTurnover": 0.99, "inventoryTurnover": 35.1, "cashConversionCycle": 51, "debtEquityRatio": 1.28, "interestCoverage": 22.8, "currentRatio": 0.93, "quickRatio": 0.81},
    {"date": "2022-09-24", "period": "FY", "returnOnEquity": 1.95, "returnOnAssets": 0.38, "grossProfitMargin": 0.433, "operatingProfitMargin": 0.302, "netProfitMargin": 0.256, "assetTurnover": 0.96, "inventoryTurnover": 34.3, "cashConversionCycle": 53, "debtEquityRatio": 1.42, "interestCoverage": 21.5, "currentRatio": 0.91, "quickRatio": 0.79},
    {"date": "2021-09-25", "period": "FY", "returnOnEquity": 1.85, "returnOnAssets": 0.36, "grossProfitMargin": 0.418, "operatingProfitMargin": 0.297, "netProfitMargin": 0.258, "assetTurnover": 0.94, "inventoryTurnover": 33.8, "cashConversionCycle": 55, "debtEquityRatio": 1.58, "interestCoverage": 20.3, "currentRatio": 0.89, "quickRatio": 0.77},
]

# --- NEW TOOL FIXTURES (for enhanced/new tools) ---

# Executive compensation breakdown
AAPL_EXECUTIVE_COMPENSATION = [
    {"nameOfExecutive": "Timothy D. Cook", "filingDate": "2025-11-01", "acceptedDate": "2025-11-01 16:30:00", "year": 2025, "salary": 3000000, "bonus": 0, "stockAward": 10000000, "incentivePlanCompensation": 3425933, "allOtherCompensation": 0, "total": 16425933},
    {"nameOfExecutive": "Luca Maestri", "filingDate": "2025-11-01", "acceptedDate": "2025-11-01 16:30:00", "year": 2025, "salary": 1000000, "bonus": 0, "stockAward": 4000000, "incentivePlanCompensation": 1012461, "allOtherCompensation": 0, "total": 6012461},
]

# Executive compensation industry benchmarks
AAPL_EXECUTIVE_COMPENSATION_BENCHMARK = [
    {"industry": "Consumer Electronics", "year": 2025, "averageSalary": 1500000, "averageBonus": 500000, "averageStockAward": 5000000, "averageIncentivePlanCompensation": 1500000, "averageTotal": 8500000, "percentile25": 5000000, "percentile50": 8000000, "percentile75": 12000000},
]

# Employee count history
AAPL_EMPLOYEE_COUNT = [
    {"periodDate": "2025-09-27", "filingDate": "2025-11-01", "employeeCount": 164000, "source": "10-K", "formType": "10-K"},
    {"periodDate": "2024-09-28", "filingDate": "2024-11-01", "employeeCount": 161000, "source": "10-K", "formType": "10-K"},
    {"periodDate": "2023-09-30", "filingDate": "2023-11-01", "employeeCount": 150000, "source": "10-K", "formType": "10-K"},
    {"periodDate": "2022-09-24", "filingDate": "2022-11-01", "employeeCount": 147000, "source": "10-K", "formType": "10-K"},
]

# Delisted companies
DELISTED_COMPANIES = [
    {"symbol": "OLDCO", "companyName": "OldCo Technologies", "exchange": "NASDAQ", "delistedDate": "2025-01-15", "ipoDate": "2010-05-20"},
    {"symbol": "GONE", "companyName": "Gone Corp", "exchange": "NYSE", "delistedDate": "2024-12-01", "ipoDate": "2015-03-10"},
]

# CIK/CUSIP search results
CIK_SEARCH_RESULTS = [
    {"symbol": "AAPL", "companyName": "Apple Inc.", "cik": "0000320193", "cusip": "037833100", "exchange": "NASDAQ"},
]

# Fund holdings (institutional portfolio by CIK)
VANGUARD_HOLDINGS = [
    {"symbol": "AAPL", "companyName": "Apple Inc.", "shares": 1300000000, "value": 356200000000, "changeInShares": 50000000, "date": "2025-12-31"},
    {"symbol": "MSFT", "companyName": "Microsoft Corporation", "shares": 900000000, "value": 372000000000, "changeInShares": -10000000, "date": "2025-12-31"},
    {"symbol": "AMZN", "companyName": "Amazon.com, Inc.", "shares": 500000000, "value": 116400000000, "changeInShares": 20000000, "date": "2025-12-31"},
]

VANGUARD_PERFORMANCE = [
    {"totalValue": 10000000000000, "totalHoldings": 5000, "oneYearReturn": 15.2, "threeYearReturn": 28.5, "fiveYearReturn": 65.8},
]

VANGUARD_INDUSTRY_BREAKDOWN = [
    {"industry": "Software", "value": 1500000000000, "percentage": 15.0, "holdingsCount": 250},
    {"industry": "Internet Retail", "value": 1000000000000, "percentage": 10.0, "holdingsCount": 180},
    {"industry": "Semiconductors", "value": 800000000000, "percentage": 8.0, "holdingsCount": 120},
]

# Intraday prices
AAPL_INTRADAY_5M = [
    {"date": "2026-02-11 15:55:00", "open": 275.20, "high": 275.50, "low": 275.10, "close": 275.40, "volume": 1200000},
    {"date": "2026-02-11 15:50:00", "open": 275.00, "high": 275.30, "low": 274.90, "close": 275.20, "volume": 1100000},
    {"date": "2026-02-11 15:45:00", "open": 274.80, "high": 275.10, "low": 274.70, "close": 275.00, "volume": 1300000},
]

AAPL_INTRADAY_15M = [
    {"date": "2026-02-11 13:30:00", "open": 274.10, "high": 274.50, "low": 274.00, "close": 274.30, "volume": 3500000},
    {"date": "2026-02-11 13:15:00", "open": 273.90, "high": 274.20, "low": 273.80, "close": 274.10, "volume": 3200000},
    {"date": "2026-02-11 13:00:00", "open": 273.50, "high": 274.00, "low": 273.40, "close": 273.90, "volume": 3800000},
]

AAPL_INTRADAY_1H = [
    {"date": "2026-02-10 14:00:00", "open": 272.80, "high": 273.20, "low": 272.50, "close": 273.00, "volume": 8200000},
    {"date": "2026-02-10 13:00:00", "open": 272.00, "high": 272.90, "low": 271.80, "close": 272.80, "volume": 7500000},
    {"date": "2026-02-10 12:00:00", "open": 271.50, "high": 272.30, "low": 271.20, "close": 272.00, "volume": 6800000},
]

# Historical market cap
AAPL_HISTORICAL_MARKET_CAP = [
    {"date": "2026-02-11", "marketCap": 4022528102504},
    {"date": "2026-02-10", "marketCap": 4010000000000},
    {"date": "2026-02-07", "marketCap": 3995000000000},
    {"date": "2026-01-31", "marketCap": 3950000000000},
    {"date": "2026-01-15", "marketCap": 3900000000000},
]

# ETF profile/info
QQQ_INFO = [{
    "symbol": "QQQ",
    "name": "Invesco QQQ Trust",
    "inceptionDate": "1999-03-10",
    "expenseRatio": 0.0020,
    "aum": 250000000000,
    "nav": 420.50,
    "avgVolume": 45000000,
    "holdingsCount": 103,
    "description": "The Invesco QQQ Trust is an exchange-traded fund based on the Nasdaq-100 Index.",
}]

QQQ_SECTOR_WEIGHTING = [
    {"sector": "Technology", "weightPercentage": 55.2},
    {"sector": "Consumer Discretionary", "weightPercentage": 15.8},
    {"sector": "Communication Services", "weightPercentage": 12.3},
    {"sector": "Healthcare", "weightPercentage": 7.5},
]

QQQ_COUNTRY_ALLOCATION = [
    {"country": "United States", "weightPercentage": 92.5},
    {"country": "China", "weightPercentage": 4.2},
    {"country": "Other", "weightPercentage": 3.3},
]

# Index quotes for index_performance
INDEX_QUOTES = [
    {"symbol": "^GSPC", "name": "S&P 500", "price": 5500.00, "changesPercentage": 0.75},
    {"symbol": "^DJI", "name": "Dow Jones Industrial Average", "price": 43000.00, "changesPercentage": 0.50},
    {"symbol": "^IXIC", "name": "NASDAQ Composite", "price": 17500.00, "changesPercentage": 1.20},
    {"symbol": "^RUT", "name": "Russell 2000", "price": 2100.00, "changesPercentage": -0.30},
]

# Index historical data for performance calculation (simple stub)
INDEX_HISTORICAL = [
    {"symbol": "^GSPC", "date": "2026-02-11", "close": 5500.00, "volume": 0},
    {"symbol": "^GSPC", "date": "2026-02-10", "close": 5480.00, "volume": 0},
    {"symbol": "^GSPC", "date": "2026-02-07", "close": 5460.00, "volume": 0},
    # Add more history as needed for performance calculations
] + [
    {"symbol": "^GSPC", "date": f"2025-{12 if i > 30 else '02'}-{max(1, 11 - i):02d}", "close": 5400 - i * 5, "volume": 0}
    for i in range(365)
]

# Market hours
MARKET_HOURS_DATA = [{
    "exchange": "NYSE",
    "name": "New York Stock Exchange",
    "isMarketOpen": True,
    "openingHour": "09:30 AM -05:00",
    "closingHour": "04:00 PM -05:00",
    "timezone": "America/New_York",
}]

MARKET_HOLIDAYS = [
    {"exchange": "NYSE", "date": "2026-02-16", "name": "Presidents Day", "isClosed": True, "adjOpenTime": None, "adjCloseTime": None},
    {"exchange": "NYSE", "date": "2026-04-03", "name": "Good Friday", "isClosed": True, "adjOpenTime": None, "adjCloseTime": None},
    {"exchange": "NYSE", "date": "2026-05-25", "name": "Memorial Day", "isClosed": True, "adjOpenTime": None, "adjCloseTime": None},
]

# Industry performance
INDUSTRY_PERFORMANCE_NYSE = [
    {"date": "2026-02-11", "industry": "Software", "sector": "Technology", "exchange": "NYSE", "averageChange": 1.25},
    {"date": "2026-02-11", "industry": "Banks", "sector": "Financial Services", "exchange": "NYSE", "averageChange": -0.50},
]

INDUSTRY_PERFORMANCE_NASDAQ = [
    {"date": "2026-02-11", "industry": "Software", "sector": "Technology", "exchange": "NASDAQ", "averageChange": 1.45},
    {"date": "2026-02-11", "industry": "Biotechnology", "sector": "Healthcare", "exchange": "NASDAQ", "averageChange": 2.10},
]

# Stock splits calendar
SPLITS_CALENDAR = [
    {"symbol": "NVDA", "date": "2026-02-20", "numerator": 10, "denominator": 1},
    {"symbol": "TSLA", "date": "2026-02-25", "numerator": 3, "denominator": 1},
]

# IPO prospectus and disclosures
IPO_PROSPECTUS = [
    {"symbol": "NEWCO", "url": "https://example.com/prospectus1", "title": "S-1 Registration", "date": "2026-02-10"},
]

IPO_DISCLOSURES = [
    {"symbol": "NEWCO", "url": "https://example.com/disclosure1", "title": "Risk Factors", "date": "2026-02-10"},
]

# Key metrics TTM (for peer_comparison forward calculations)
AAPL_KEY_METRICS_TTM = [{
    "symbol": "AAPL",
    "marketCapTTM": 4022528102504,
    "revenuePerShareTTM": 27.05,
}]

MSFT_KEY_METRICS_TTM = [{
    "symbol": "MSFT",
    "marketCapTTM": 3100000000000,
    "revenuePerShareTTM": 33.15,
}]

# --- Polygon.io Fixtures ---

POLYGON_OPTIONS_SNAPSHOT = {
    "status": "OK",
    "request_id": "test123",
    "results": [
        {
            "details": {
                "ticker": "O:AAPL260220C00270000",
                "strike_price": 270.0,
                "contract_type": "call",
                "expiration_date": "2026-02-20",
            },
            "greeks": {"delta": 0.55, "gamma": 0.03, "theta": -0.15, "vega": 0.25},
            "implied_volatility": 0.32,
            "open_interest": 15000,
            "day": {"volume": 5200, "close": 8.50},
            "last_quote": {"bid": 8.40, "ask": 8.60, "bid_size": 100, "ask_size": 150},
        },
        {
            "details": {
                "ticker": "O:AAPL260220P00270000",
                "strike_price": 270.0,
                "contract_type": "put",
                "expiration_date": "2026-02-20",
            },
            "greeks": {"delta": -0.45, "gamma": 0.03, "theta": -0.12, "vega": 0.22},
            "implied_volatility": 0.34,
            "open_interest": 12000,
            "day": {"volume": 3800, "close": 5.20},
            "last_quote": {"bid": 5.10, "ask": 5.30, "bid_size": 80, "ask_size": 120},
        },
        {
            "details": {
                "ticker": "O:AAPL260220C00280000",
                "strike_price": 280.0,
                "contract_type": "call",
                "expiration_date": "2026-02-20",
            },
            "greeks": {"delta": 0.35, "gamma": 0.04, "theta": -0.10, "vega": 0.20},
            "implied_volatility": 0.30,
            "open_interest": 8000,
            "day": {"volume": 2100, "close": 4.30},
            "last_quote": {"bid": 4.20, "ask": 4.40, "bid_size": 60, "ask_size": 90},
        },
        {
            "details": {
                "ticker": "O:AAPL260227C00275000",
                "strike_price": 275.0,
                "contract_type": "call",
                "expiration_date": "2026-02-27",
            },
            "greeks": {"delta": 0.50, "gamma": 0.02, "theta": -0.08, "vega": 0.30},
            "implied_volatility": 0.31,
            "open_interest": 6000,
            "day": {"volume": 1500, "close": 9.00},
            "last_quote": {"bid": 8.90, "ask": 9.10, "bid_size": 50, "ask_size": 70},
        },
    ],
}

POLYGON_MACD = {
    "status": "OK",
    "request_id": "macd123",
    "results": {
        "underlying": {"url": "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/..."},
        "values": [
            {"timestamp": 1707696000000, "value": 2.35, "signal": 1.80, "histogram": 0.55},
            {"timestamp": 1707609600000, "value": 2.10, "signal": 1.75, "histogram": 0.35},
            {"timestamp": 1707523200000, "value": 1.85, "signal": 1.70, "histogram": 0.15},
        ],
    },
}

POLYGON_SHORT_INTEREST = {
    "status": "OK",
    "request_id": "short123",
    "results": [
        {
            "ticker": "AAPL",
            "settlement_date": "2026-02-10",
            "short_interest": 118000000,
            "days_to_cover": 1.8,
            "avg_daily_volume": 65000000,
        },
    ],
}

POLYGON_TREASURY_YIELDS = {
    "status": "OK",
    "request_id": "yields123",
    "results": [
        {"date": "2026-02-11", "yield_1_month": 4.32, "yield_3_month": 4.28, "yield_6_month": 4.15, "yield_1_year": 3.95, "yield_2_year": 3.82, "yield_5_year": 3.90, "yield_10_year": 4.05, "yield_20_year": 4.35, "yield_30_year": 4.42},
        {"date": "2026-02-10", "yield_1_month": 4.30, "yield_3_month": 4.26, "yield_6_month": 4.13, "yield_1_year": 3.93, "yield_2_year": 3.80, "yield_5_year": 3.88, "yield_10_year": 4.03, "yield_20_year": 4.33, "yield_30_year": 4.40},
    ],
}

POLYGON_INFLATION = {
    "status": "OK",
    "request_id": "infl123",
    "results": [
        {"date": "2026-01-15", "cpi": 315.2, "cpi_core": 320.1, "cpi_year_over_year": 2.8, "pce": 118.5, "pce_core": 119.2},
        {"date": "2025-12-15", "cpi": 314.5, "cpi_core": 319.3, "cpi_year_over_year": 2.9, "pce": 118.0, "pce_core": 118.7},
    ],
}

POLYGON_INFLATION_EXPECTATIONS = {
    "status": "OK",
    "request_id": "inflexp123",
    "results": [
        {"date": "2026-02-10", "market_5_year": 2.35, "market_10_year": 2.42, "model_1_year": 2.80, "model_5_year": 2.45, "model_10_year": 2.50, "model_30_year": 2.55, "forward_years_5_to_10": 2.49},
        {"date": "2026-02-03", "market_5_year": 2.33, "market_10_year": 2.40, "model_1_year": 2.78, "model_5_year": 2.43, "model_10_year": 2.48, "model_30_year": 2.53, "forward_years_5_to_10": 2.47},
    ],
}

POLYGON_LABOR_MARKET = {
    "status": "OK",
    "request_id": "labor123",
    "results": [
        {"date": "2026-01-10", "unemployment_rate": 4.1, "labor_force_participation_rate": 62.5, "avg_hourly_earnings": 35.20, "job_openings": 8500},
        {"date": "2025-12-06", "unemployment_rate": 4.2, "labor_force_participation_rate": 62.4, "avg_hourly_earnings": 35.05, "job_openings": 8300},
    ],
}


# --- ThetaData v3 Fixtures ---
#
# v3 responses all share the same {"response": [{"contract": {...}, "data": [...]}]}
# shape. Contract metadata (symbol/expiration/strike/right) lives in the
# "contract" object; per-timestamp data lives as dicts in "data".
#
# These fixtures reproduce a 4-contract scenario across 2 expirations (AAPL
# $270C/$270P/$280C expiring 2030-02-20 and $275C expiring 2030-02-27). Dates
# are pinned to 2030 so the "next 6 future expirations" filter in
# options_chain stays satisfied for years.


def _v3_contract(strike: float, right: str, expiration: str) -> dict:
    """Build a v3 'contract' metadata object."""
    return {
        "symbol": "AAPL",
        "expiration": expiration,
        "strike": round(float(strike), 3),
        "right": right.upper(),
    }


def _v3_wrap(entries: list[tuple[dict, list[dict]]]) -> dict:
    """Wrap (contract, data-rows) pairs into a v3 response envelope."""
    return {
        "response": [
            {"contract": contract, "data": data_rows}
            for contract, data_rows in entries
        ]
    }


# Expirations list endpoint — v3 flat shape
THETA_AAPL_EXPIRATIONS = {
    "response": [
        {"symbol": "AAPL", "expiration": "2030-02-20"},
        {"symbol": "AAPL", "expiration": "2030-02-27"},
    ],
}

# Greeks first-order snapshot (one payload for expiration=*, covers all 4 contracts)
THETA_AAPL_GREEKS_FO = _v3_wrap([
    (_v3_contract(270.0, "CALL", "2030-02-20"), [{
        "timestamp": "2030-02-18T15:59:00",
        "bid": 8.40, "ask": 8.60,
        "delta": 0.55, "theta": -0.15, "vega": 0.25, "rho": 0.10, "epsilon": 0.0,
        "lambda": 17.0, "implied_vol": 0.32, "iv_error": 0.0,
        "underlying_timestamp": "2030-02-18T15:59:00", "underlying_price": 270.50,
    }]),
    (_v3_contract(270.0, "PUT", "2030-02-20"), [{
        "timestamp": "2030-02-18T15:59:00",
        "bid": 5.10, "ask": 5.30,
        "delta": -0.45, "theta": -0.12, "vega": 0.22, "rho": -0.08, "epsilon": 0.0,
        "lambda": -13.0, "implied_vol": 0.34, "iv_error": 0.0,
        "underlying_timestamp": "2030-02-18T15:59:00", "underlying_price": 270.50,
    }]),
    (_v3_contract(280.0, "CALL", "2030-02-20"), [{
        "timestamp": "2030-02-18T15:59:00",
        "bid": 4.20, "ask": 4.40,
        "delta": 0.35, "theta": -0.10, "vega": 0.20, "rho": 0.07, "epsilon": 0.0,
        "lambda": 11.0, "implied_vol": 0.30, "iv_error": 0.0,
        "underlying_timestamp": "2030-02-18T15:59:00", "underlying_price": 270.50,
    }]),
    (_v3_contract(275.0, "CALL", "2030-02-27"), [{
        "timestamp": "2030-02-18T15:59:00",
        "bid": 8.90, "ask": 9.10,
        "delta": 0.50, "theta": -0.08, "vega": 0.30, "rho": 0.12, "epsilon": 0.0,
        "lambda": 15.0, "implied_vol": 0.31, "iv_error": 0.0,
        "underlying_timestamp": "2030-02-18T15:59:00", "underlying_price": 270.50,
    }]),
])

THETA_AAPL_QUOTE_SNAPSHOT = _v3_wrap([
    (_v3_contract(270.0, "CALL", "2030-02-20"), [{
        "timestamp": "2030-02-18T15:59:00",
        "bid_size": 100, "bid_exchange": 42, "bid": 8.40, "bid_condition": 50,
        "ask_size": 150, "ask_exchange": 42, "ask": 8.60, "ask_condition": 50,
    }]),
    (_v3_contract(270.0, "PUT", "2030-02-20"), [{
        "timestamp": "2030-02-18T15:59:00",
        "bid_size": 80, "bid_exchange": 42, "bid": 5.10, "bid_condition": 50,
        "ask_size": 120, "ask_exchange": 42, "ask": 5.30, "ask_condition": 50,
    }]),
    (_v3_contract(280.0, "CALL", "2030-02-20"), [{
        "timestamp": "2030-02-18T15:59:00",
        "bid_size": 60, "bid_exchange": 42, "bid": 4.20, "bid_condition": 50,
        "ask_size": 90, "ask_exchange": 42, "ask": 4.40, "ask_condition": 50,
    }]),
    (_v3_contract(275.0, "CALL", "2030-02-27"), [{
        "timestamp": "2030-02-18T15:59:00",
        "bid_size": 50, "bid_exchange": 42, "bid": 8.90, "bid_condition": 50,
        "ask_size": 70, "ask_exchange": 42, "ask": 9.10, "ask_condition": 50,
    }]),
])

THETA_AAPL_OI_SNAPSHOT = _v3_wrap([
    (_v3_contract(270.0, "CALL", "2030-02-20"),
     [{"timestamp": "2030-02-18T06:30:00", "open_interest": 15000}]),
    (_v3_contract(270.0, "PUT", "2030-02-20"),
     [{"timestamp": "2030-02-18T06:30:00", "open_interest": 12000}]),
    (_v3_contract(280.0, "CALL", "2030-02-20"),
     [{"timestamp": "2030-02-18T06:30:00", "open_interest": 8000}]),
    (_v3_contract(275.0, "CALL", "2030-02-27"),
     [{"timestamp": "2030-02-18T06:30:00", "open_interest": 6000}]),
])

THETA_AAPL_OHLC_SNAPSHOT = _v3_wrap([
    (_v3_contract(270.0, "CALL", "2030-02-20"), [{
        "timestamp": "2030-02-18T15:59:00",
        "open": 8.30, "high": 8.65, "low": 8.20, "close": 8.50,
        "volume": 5200, "count": 410,
    }]),
    (_v3_contract(270.0, "PUT", "2030-02-20"), [{
        "timestamp": "2030-02-18T15:59:00",
        "open": 5.00, "high": 5.40, "low": 4.95, "close": 5.20,
        "volume": 3800, "count": 290,
    }]),
    (_v3_contract(280.0, "CALL", "2030-02-20"), [{
        "timestamp": "2030-02-18T15:59:00",
        "open": 4.15, "high": 4.50, "low": 4.10, "close": 4.30,
        "volume": 2100, "count": 170,
    }]),
    (_v3_contract(275.0, "CALL", "2030-02-27"), [{
        "timestamp": "2030-02-18T15:59:00",
        "open": 8.80, "high": 9.15, "low": 8.75, "close": 9.00,
        "volume": 1500, "count": 120,
    }]),
])

# Stale scenario — every contract shows $0 bid/ask so the warning test trips
THETA_AAPL_STALE_GREEKS_FO = _v3_wrap([
    (_v3_contract(250.0 + i, "CALL", "2030-02-20"), [{
        "timestamp": "2030-02-18T15:59:00",
        "bid": 0.0, "ask": 0.0,
        "delta": 0.05, "theta": -0.01, "vega": 0.05, "rho": 0.0, "epsilon": 0.0,
        "lambda": 0.0, "implied_vol": 0.30, "iv_error": 0.0,
        "underlying_timestamp": "2030-02-18T15:59:00", "underlying_price": 270.50,
    }])
    for i in range(10)
])

THETA_AAPL_STALE_QUOTE_SNAPSHOT = _v3_wrap([
    (_v3_contract(250.0 + i, "CALL", "2030-02-20"), [{
        "timestamp": "2030-02-18T15:59:00",
        "bid_size": 0, "bid": 0.0, "ask_size": 0, "ask": 0.0,
    }])
    for i in range(10)
])

THETA_AAPL_STALE_OI_SNAPSHOT = _v3_wrap([
    (_v3_contract(250.0 + i, "CALL", "2030-02-20"),
     [{"timestamp": "2030-02-18T06:30:00", "open_interest": 0}])
    for i in range(10)
])

THETA_AAPL_STALE_OHLC_SNAPSHOT = _v3_wrap([
    (_v3_contract(250.0 + i, "CALL", "2030-02-20"), [{
        "timestamp": "2030-02-18T15:59:00",
        "open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0,
        "volume": 0, "count": 0,
    }])
    for i in range(10)
])


# Historical EOD for a single contract — used by historical_options day-mode test
THETA_HIST_EOD_AAPL_270C = _v3_wrap([
    (_v3_contract(270.0, "CALL", "2030-02-20"), [
        {
            "created": "2030-02-18T17:15:00.000",
            "last_trade": "2030-02-18T15:58:12.000",
            "open": 8.40, "high": 8.65, "low": 8.30, "close": 8.50,
            "volume": 5200, "count": 410,
            "bid_size": 70, "bid_exchange": 60, "bid": 8.45, "bid_condition": 50,
            "ask_size": 15, "ask_exchange": 47, "ask": 8.55, "ask_condition": 50,
        },
        {
            "created": "2030-02-19T17:15:00.000",
            "last_trade": "2030-02-19T15:58:11.000",
            "open": 8.50, "high": 8.80, "low": 8.45, "close": 8.75,
            "volume": 6100, "count": 470,
            "bid_size": 90, "bid_exchange": 60, "bid": 8.70, "bid_condition": 50,
            "ask_size": 20, "ask_exchange": 47, "ask": 8.80, "ask_condition": 50,
        },
        {
            "created": "2030-02-20T17:15:00.000",
            "last_trade": "2030-02-20T15:58:10.000",
            "open": 8.75, "high": 9.00, "low": 8.60, "close": 8.80,
            "volume": 5900, "count": 455,
            "bid_size": 100, "bid_exchange": 60, "bid": 8.75, "bid_condition": 50,
            "ask_size": 30, "ask_exchange": 47, "ask": 8.85, "ask_condition": 50,
        },
    ]),
])
