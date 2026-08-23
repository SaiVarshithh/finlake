with ranked_prices as (
    select
        ticker,
        exchange,
        trade_date,
        open,
        high,
        low,
        close,
        adj_close,
        volume,
        ingestion_ts,
        source_batch_id,
        row_number() over (
            partition by ticker, trade_date
            order by ingestion_ts desc, source_batch_id desc
        ) as ingestion_rank
    from {{ ref('stg_stock_prices') }}
)

select
    ticker,
    exchange,
    trade_date,
    open,
    high,
    low,
    close,
    adj_close,
    volume
from ranked_prices
where ingestion_rank = 1

