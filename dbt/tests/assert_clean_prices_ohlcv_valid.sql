select *
from {{ ref('clean_prices') }}
where
    open <= 0
    or high <= 0
    or low <= 0
    or close <= 0
    or volume < 0
    or high < greatest(open, close, low)
    or low > least(open, close, high)
    or trade_date > current_date

