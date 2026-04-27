-- Intermediate: add derived fields — temperature range, season, comfort index, quality flags

with base as (
    select * from {{ ref('stg_weather') }}
    where observation_date is not null
),

enriched as (
    select
        city,
        observation_date,
        temp_max_c,
        temp_min_c,
        precipitation_mm,
        windspeed_max_kmh,

        -- derived
        round(temp_max_c - temp_min_c, 2)                       as temp_range_c,
        round((temp_max_c + temp_min_c) / 2, 2)                 as temp_avg_c,

        case
            when month(observation_date) in (12, 1, 2)  then 'Winter'
            when month(observation_date) in (3, 4, 5)   then 'Spring'
            when month(observation_date) in (6, 7, 8)   then 'Summer'
            else 'Fall'
        end                                                      as season,

        case
            when precipitation_mm > 0 then true
            else false
        end                                                      as is_rainy_day,

        case
            when temp_max_c is null
              or temp_min_c is null
              or precipitation_mm is null
              or windspeed_max_kmh is null then false
            else true
        end                                                      as is_complete_record,

        year(observation_date)                                   as year,
        month(observation_date)                                  as month,
        dayofweek(observation_date)                              as day_of_week,
        loaded_at
    from base
)

select * from enriched
