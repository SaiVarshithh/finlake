"""
FinLake — reference data for the Bronze ingestion pipeline.

NSE_TICKERS is the source of truth for which tickers this pipeline pulls and
what metadata backs `dim_tickers`. yfinance requires an exchange suffix on
Indian symbols (.NS for NSE, .BO for BSE) — the base `ticker` field is what
appears in Iceberg tables and dashboards; `yfinance_symbol` is only used when
calling the yfinance API.
"""

NSE_TICKERS = [
    {"ticker": "RELIANCE",   "company_name": "Reliance Industries Ltd",         "exchange": "NSE", "yfinance_symbol": "RELIANCE.NS"},
    {"ticker": "TCS",        "company_name": "Tata Consultancy Services Ltd",   "exchange": "NSE", "yfinance_symbol": "TCS.NS"},
    {"ticker": "HDFCBANK",   "company_name": "HDFC Bank Ltd",                   "exchange": "NSE", "yfinance_symbol": "HDFCBANK.NS"},
    {"ticker": "ICICIBANK",  "company_name": "ICICI Bank Ltd",                  "exchange": "NSE", "yfinance_symbol": "ICICIBANK.NS"},
    {"ticker": "INFY",       "company_name": "Infosys Ltd",                     "exchange": "NSE", "yfinance_symbol": "INFY.NS"},
    {"ticker": "HINDUNILVR", "company_name": "Hindustan Unilever Ltd",          "exchange": "NSE", "yfinance_symbol": "HINDUNILVR.NS"},
    {"ticker": "ITC",        "company_name": "ITC Ltd",                        "exchange": "NSE", "yfinance_symbol": "ITC.NS"},
    {"ticker": "SBIN",       "company_name": "State Bank of India",             "exchange": "NSE", "yfinance_symbol": "SBIN.NS"},
    {"ticker": "BHARTIARTL", "company_name": "Bharti Airtel Ltd",               "exchange": "NSE", "yfinance_symbol": "BHARTIARTL.NS"},
    {"ticker": "KOTAKBANK",  "company_name": "Kotak Mahindra Bank Ltd",         "exchange": "NSE", "yfinance_symbol": "KOTAKBANK.NS"},
    {"ticker": "LT",         "company_name": "Larsen & Toubro Ltd",             "exchange": "NSE", "yfinance_symbol": "LT.NS"},
    {"ticker": "AXISBANK",   "company_name": "Axis Bank Ltd",                   "exchange": "NSE", "yfinance_symbol": "AXISBANK.NS"},
    {"ticker": "BAJFINANCE", "company_name": "Bajaj Finance Ltd",               "exchange": "NSE", "yfinance_symbol": "BAJFINANCE.NS"},
    {"ticker": "ASIANPAINT", "company_name": "Asian Paints Ltd",                "exchange": "NSE", "yfinance_symbol": "ASIANPAINT.NS"},
    {"ticker": "MARUTI",     "company_name": "Maruti Suzuki India Ltd",         "exchange": "NSE", "yfinance_symbol": "MARUTI.NS"},
    {"ticker": "SUNPHARMA",  "company_name": "Sun Pharmaceutical Industries",   "exchange": "NSE", "yfinance_symbol": "SUNPHARMA.NS"},
    {"ticker": "TITAN",      "company_name": "Titan Company Ltd",               "exchange": "NSE", "yfinance_symbol": "TITAN.NS"},
    {"ticker": "ULTRACEMCO", "company_name": "UltraTech Cement Ltd",            "exchange": "NSE", "yfinance_symbol": "ULTRACEMCO.NS"},
    {"ticker": "WIPRO",      "company_name": "Wipro Ltd",                       "exchange": "NSE", "yfinance_symbol": "WIPRO.NS"},
    {"ticker": "NESTLEIND",  "company_name": "Nestle India Ltd",                "exchange": "NSE", "yfinance_symbol": "NESTLEIND.NS"},
]
