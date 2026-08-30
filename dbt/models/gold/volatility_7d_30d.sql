with rolling_returns as (
    select
        ticker,
        exchange,
        trade_date,
        daily_return,
        count(daily_return) over (
            partition by ticker
            order by trade_date
            rows between 6 preceding and current row
        ) as return_observations_7d,
        count(daily_return) over (
            partition by ticker
            order by trade_date
            rows between 29 preceding and current row
        ) as return_observations_30d,
        stddev_samp(daily_return) over (
            partition by ticker
            order by trade_date
            rows between 6 preceding and current row
        ) as partial_volatility_7d,
        stddev_samp(daily_return) over (
            partition by ticker
            order by trade_date
            rows between 29 preceding and current row
        ) as partial_volatility_30d
    from {{ ref('daily_returns') }}
)

select
    ticker,
    exchange,
    trade_date,
    daily_return,
    return_observations_7d,
    return_observations_30d,
    case
        when return_observations_7d = 7 then partial_volatility_7d
    end as volatility_7d,
    case
        when return_observations_30d = 30 then partial_volatility_30d
    end as volatility_30d
from rolling_returns
