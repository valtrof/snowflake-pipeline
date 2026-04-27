-- Custom singular test: max temp must always be >= min temp
-- Any rows returned = test failure

select
    city,
    observation_date,
    temp_max_c,
    temp_min_c
from {{ ref('stg_weather') }}
where temp_max_c is not null
  and temp_min_c is not null
  and temp_max_c < temp_min_c
