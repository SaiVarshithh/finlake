select
    ticker,
    trade_date,
    count(*) as row_count
from {{ ref('clean_prices') }}
group by ticker, trade_date
having count(*) > 1

