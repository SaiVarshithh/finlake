select *
from {{ ref('moving_avg_20_50') }}
where
    observations_20d not between 1 and 20
    or observations_50d not between 1 and 50
    or (observations_20d < 20 and moving_avg_20d is not null)
    or (observations_20d = 20 and (moving_avg_20d is null or moving_avg_20d <= 0))
    or (observations_50d < 50 and moving_avg_50d is not null)
    or (observations_50d = 50 and (moving_avg_50d is null or moving_avg_50d <= 0))
