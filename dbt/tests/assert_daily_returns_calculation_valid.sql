select *
from {{ ref('daily_returns') }}
where
    (previous_close is null and daily_return is not null)
    or (
        previous_close is not null
        and (
            daily_return is null
            or abs(daily_return - ((close - previous_close) / previous_close)) > 0.000000001
            or abs(daily_return_pct - (100.0 * daily_return)) > 0.0000001
        )
    )
