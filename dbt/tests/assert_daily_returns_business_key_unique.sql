select
    ticker,
    trade_date,
    count(*) as row_count
from {{ ref('daily_returns') }}
group by ticker, trade_date
having count(*) > 1
