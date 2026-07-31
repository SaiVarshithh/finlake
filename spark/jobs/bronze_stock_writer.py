"""
FinLake Bronze Stock Writer.

Pulls daily OHLCV data for configured NSE/BSE symbols through yfinance and
merges it into `nessie.finlake_bronze.raw_stock_prices`.

Environment variables:
    FINLAKE_TICKERS          Comma-separated base tickers, e.g. RELIANCE,TCS.
                             If unset, the default reference list is used.
    LOOKBACK_DAYS            Rolling calendar-day window for normal DAG runs.
                             Default: 20.
    BACKFILL_START           Optional explicit start date, YYYY-MM-DD.
    BACKFILL_END             Optional explicit end date, YYYY-MM-DD, exclusive.
    FINLAKE_MARKET_TZ        Timezone used to resolve "today". Default:
                             Asia/Kolkata.
    MIN_SUCCESSFUL_TICKERS   Minimum distinct tickers that must land. Default:
                             min(20, requested ticker count).
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from zoneinfo import ZoneInfo

import yfinance as yf

from spark.handler import build_spark, iceberg_initialisation
from spark.model.constants import NSE_TICKERS
from spark.model.models import DimTickers, RawStockPrices


def parse_positive_int(value: str | None, default: int, name: str) -> int:
    if value is None or str(value).strip() == "":
        return default
    try:
        parsed = int(str(value).strip())
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {value!r}") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be >= 1, got {parsed}")
    return parsed


def parse_date(value: str, name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD, got {value!r}") from exc


def resolve_date_range() -> tuple[date, date]:
    start_override = os.getenv("BACKFILL_START", "").strip()
    end_override = os.getenv("BACKFILL_END", "").strip()
    if start_override or end_override:
        if not start_override or not end_override:
            raise ValueError("BACKFILL_START and BACKFILL_END must be provided together")
        start_date = parse_date(start_override, "BACKFILL_START")
        end_date = parse_date(end_override, "BACKFILL_END")
    else:
        lookback_days = parse_positive_int(os.getenv("LOOKBACK_DAYS"), 20, "LOOKBACK_DAYS")
        market_tz = ZoneInfo(os.getenv("FINLAKE_MARKET_TZ", "Asia/Kolkata"))
        end_date = datetime.now(market_tz).date() + timedelta(days=1)
        start_date = end_date - timedelta(days=lookback_days)

    if start_date >= end_date:
        raise ValueError(f"Invalid yfinance date range: start={start_date}, end={end_date}")
    return start_date, end_date


def resolve_tickers() -> list[dict[str, str]]:
    override = os.getenv("FINLAKE_TICKERS", "").strip()
    if not override:
        return NSE_TICKERS

    requested = [ticker.strip().upper() for ticker in override.split(",") if ticker.strip()]
    requested_set = set(requested)
    known_by_ticker = {ticker["ticker"]: ticker for ticker in NSE_TICKERS}
    resolved = [known_by_ticker[ticker] for ticker in requested if ticker in known_by_ticker]
    missing = requested_set - set(known_by_ticker)
    if missing:
        print(f"[bronze_stock_writer] WARNING: unknown tickers skipped: {sorted(missing)}")
    return resolved


def to_decimal(value) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None


def get_symbol_frame(raw, yf_symbol: str, requested_symbols: list[str]):
    if hasattr(raw.columns, "nlevels") and raw.columns.nlevels > 1:
        top_level_symbols = set(raw.columns.get_level_values(0))
        if yf_symbol in top_level_symbols:
            return raw[yf_symbol].dropna(how="all")
        second_level_symbols = set(raw.columns.get_level_values(1))
        if yf_symbol in second_level_symbols:
            return raw.xs(yf_symbol, axis=1, level=1).dropna(how="all")
        return None

    if len(requested_symbols) == 1:
        return raw.dropna(how="all")
    return None


def download_prices(yf_symbols: list[str], start_date: date, end_date: date):
    print(f"[bronze_stock_writer] yfinance range: {start_date} -> {end_date} (end exclusive)")
    return yf.download(
        yf_symbols,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=True,
    )


@iceberg_initialisation
def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    tickers = resolve_tickers()
    if not tickers:
        raise ValueError("No valid tickers resolved. Check FINLAKE_TICKERS.")

    start_date, end_date = resolve_date_range()
    symbol_to_meta = {ticker["yfinance_symbol"]: ticker for ticker in tickers}
    yf_symbols = list(symbol_to_meta)

    print("=" * 80)
    print("FinLake Bronze Stock Writer")
    print(f"Target table       : {RawStockPrices.TABLE_NAME}")
    print(f"Ticker count       : {len(tickers)}")
    print(f"Date range         : {start_date} -> {end_date} (end exclusive)")
    print("=" * 80)

    raw = download_prices(yf_symbols, start_date, end_date)
    if raw.empty:
        raise ValueError("yfinance returned no data for the requested tickers/date range.")

    airflow_run_id = os.getenv("AIRFLOW_RUN_ID", "").strip()
    batch_id = airflow_run_id or f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
    ingestion_ts = datetime.now(timezone.utc)

    rows = []
    successful_tickers = set()
    required_columns = ["Open", "High", "Low", "Close", "Volume"]

    for yf_symbol in yf_symbols:
        meta = symbol_to_meta[yf_symbol]
        ticker_df = get_symbol_frame(raw, yf_symbol, yf_symbols)
        if ticker_df is None or ticker_df.empty:
            print(f"[bronze_stock_writer] WARNING: no data returned for {yf_symbol}, skipping")
            continue

        for trade_date, record in ticker_df.iterrows():
            if any(column not in record.index for column in required_columns):
                continue
            if record[required_columns].isnull().any():
                continue

            open_price = to_decimal(record["Open"])
            high_price = to_decimal(record["High"])
            low_price = to_decimal(record["Low"])
            close_price = to_decimal(record["Close"])
            adj_close = to_decimal(record["Adj Close"]) if "Adj Close" in record.index else None
            if None in (open_price, high_price, low_price, close_price):
                continue

            rows.append(
                (
                    meta["ticker"],
                    meta["exchange"],
                    trade_date.date(),
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    adj_close,
                    int(record["Volume"]),
                    ingestion_ts,
                    batch_id,
                )
            )
            successful_tickers.add(meta["ticker"])

    if not rows:
        raise ValueError("No valid rows parsed from the yfinance response.")

    min_successful_tickers = parse_positive_int(
        os.getenv("MIN_SUCCESSFUL_TICKERS"),
        min(20, len(tickers)),
        "MIN_SUCCESSFUL_TICKERS",
    )
    if len(successful_tickers) < min_successful_tickers:
        raise RuntimeError(
            "Only "
            f"{len(successful_tickers)} tickers landed, below required minimum "
            f"{min_successful_tickers}. Successful tickers: {sorted(successful_tickers)}"
        )

    df = spark.createDataFrame(rows, schema=RawStockPrices.get_schema())
    print(
        "[bronze_stock_writer] Parsed "
        f"{df.count()} rows across {len(successful_tickers)} successful tickers."
    )

    RawStockPrices().write_raw_stock_prices_df(df)
    print(f"[bronze_stock_writer] Merged rows into {RawStockPrices.TABLE_NAME} (batch_id={batch_id})")

    added_date = datetime.now(timezone.utc).date()
    dim_rows = [
        (ticker["ticker"], ticker["company_name"], ticker["exchange"], ticker["yfinance_symbol"], added_date)
        for ticker in NSE_TICKERS
    ]
    dim_df = spark.createDataFrame(dim_rows, schema=DimTickers.get_schema())
    DimTickers().write_dim_tickers_df(dim_df)
    print(f"[bronze_stock_writer] Refreshed {DimTickers.TABLE_NAME} ({dim_df.count()} tickers)")

    spark.stop()


if __name__ == "__main__":
    main()
