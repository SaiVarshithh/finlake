select
    returns.trade_date,
    sectors.sector,
    count(*) as ticker_count,
    avg(returns.daily_return) as average_daily_return,
    100.0 * avg(returns.daily_return) as average_daily_return_pct,
    min(returns.daily_return) as minimum_daily_return,
    max(returns.daily_return) as maximum_daily_return,
    sum(case when returns.daily_return > 0 then 1 else 0 end) as advancing_tickers,
    sum(case when returns.daily_return < 0 then 1 else 0 end) as declining_tickers
from {{ ref('daily_returns') }} as returns
inner join {{ ref('sector_mapping') }} as sectors
    on returns.ticker = sectors.ticker
where returns.daily_return is not null
group by returns.trade_date, sectors.sector
