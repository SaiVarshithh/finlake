select
    ticker,
    trade_date,
    count(*) as row_count
from {{ ref('moving_avg_20_50') }}
group by ticker, trade_date
having count(*) > 1
