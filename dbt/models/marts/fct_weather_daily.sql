-- Mart: analytics-ready fact table, one row per city per day
-- Partitioned by month, clustered by city for efficient analytical queries

{{
    config(
        materialized='incremental',
        unique_key=['city', 'observation_date'],
        cluster_by=['city', 'year', 'month'],
        on_schema_change='sync_all_columns'
    )
}}

with source as (
    select * from {{ ref('int_weather_enriched') }}
    where is_complete_record = true

    {% if is_incremental() %}
        and observation_date > (select max(observation_date) from {{ this }})
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'observation_date']) }} as weather_id,
    city,
    observation_date,
    year,
    month,
    season,
    day_of_week,
    temp_max_c,
    temp_min_c,
    temp_avg_c,
    temp_range_c,
    precipitation_mm,
    windspeed_max_kmh,
    is_rainy_day,
    loaded_at
from source
