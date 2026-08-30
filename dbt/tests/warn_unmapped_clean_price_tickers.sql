{{ config(severity='warn') }}

select distinct prices.ticker
from {{ ref('clean_prices') }} as prices
left join {{ ref('sector_mapping') }} as sectors
    on prices.ticker = sectors.ticker
where sectors.ticker is null
