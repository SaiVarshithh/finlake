select
    ticker,
    trade_date,
    count(*) as row_count
from {{ ref('volatility_7d_30d') }}
group by ticker, trade_date
having count(*) > 1
