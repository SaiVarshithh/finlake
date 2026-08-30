with lagged_prices as (
    select
        ticker,
        exchange,
        trade_date,
        close,
        lag(close) over (
            partition by ticker
            order by trade_date
        ) as previous_close
    from {{ ref('clean_prices') }}
)

select
    ticker,
    exchange,
    trade_date,
    close,
    previous_close,
    case
        when previous_close is null then null
        else (close - previous_close) / nullif(previous_close, 0)
    end as daily_return,
    case
        when previous_close is null then null
        else 100.0 * (close - previous_close) / nullif(previous_close, 0)
    end as daily_return_pct
from lagged_prices
