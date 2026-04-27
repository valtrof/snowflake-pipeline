-- Staging: cast all columns from raw strings to proper types, rename for clarity

with source as (
    select * from {{ source('raw', 'raw_weather_daily') }}
),

cleaned as (
    select
        city,
        try_to_date(date, 'YYYY-MM-DD')         as observation_date,
        try_to_decimal(temp_max_c, 10, 2)        as temp_max_c,
        try_to_decimal(temp_min_c, 10, 2)        as temp_min_c,
        try_to_decimal(precipitation_mm, 10, 2)  as precipitation_mm,
        try_to_decimal(windspeed_max_kmh, 10, 2) as windspeed_max_kmh,
        loaded_at
    from source
    where date is not null
)

select * from cleaned
