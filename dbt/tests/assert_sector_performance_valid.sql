select *
from {{ ref('sector_performance') }}
where
    ticker_count <= 0
    or advancing_tickers < 0
    or declining_tickers < 0
    or advancing_tickers + declining_tickers > ticker_count
    or minimum_daily_return > average_daily_return
    or maximum_daily_return < average_daily_return
