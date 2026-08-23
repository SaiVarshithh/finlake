select
    upper(trim(ticker)) as ticker,
    upper(trim(exchange)) as exchange,
    trade_date,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    ingestion_ts,
    source_batch_id
from {{ source('bronze', 'raw_stock_prices') }}

