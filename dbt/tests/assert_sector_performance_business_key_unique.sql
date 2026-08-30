select
    trade_date,
    sector,
    count(*) as row_count
from {{ ref('sector_performance') }}
group by trade_date, sector
having count(*) > 1
