with rolling_prices as (
    select
        ticker,
        exchange,
        trade_date,
        close,
        count(*) over (
            partition by ticker
            order by trade_date
            rows between 19 preceding and current row
        ) as observations_20d,
        count(*) over (
            partition by ticker
            order by trade_date
            rows between 49 preceding and current row
        ) as observations_50d,
        avg(close) over (
            partition by ticker
            order by trade_date
            rows between 19 preceding and current row
        ) as partial_moving_avg_20d,
        avg(close) over (
            partition by ticker
            order by trade_date
            rows between 49 preceding and current row
        ) as partial_moving_avg_50d
    from {{ ref('clean_prices') }}
)

select
    ticker,
    exchange,
    trade_date,
    close,
    observations_20d,
    observations_50d,
    case
        when observations_20d = 20 then partial_moving_avg_20d
    end as moving_avg_20d,
    case
        when observations_50d = 50 then partial_moving_avg_50d
    end as moving_avg_50d
from rolling_prices
