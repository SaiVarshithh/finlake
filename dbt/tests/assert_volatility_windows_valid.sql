select *
from {{ ref('volatility_7d_30d') }}
where
    return_observations_7d not between 0 and 7
    or return_observations_30d not between 0 and 30
    or (return_observations_7d < 7 and volatility_7d is not null)
    or (return_observations_7d = 7 and (volatility_7d is null or volatility_7d < 0))
    or (return_observations_30d < 30 and volatility_30d is not null)
    or (return_observations_30d = 30 and (volatility_30d is null or volatility_30d < 0))
