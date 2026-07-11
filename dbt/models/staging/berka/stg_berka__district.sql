-- dbt/models/staging/berka/stg_berka__district.sql

with source as (
    select * from {{ source('berka', 'district') }}
),

renamed as (
    select
        district_id,
        name            as district_name,
        region,
        num_inhabitants,
        avg_salary,
        unemployment_rate_95,
        unemployment_rate_96,
        num_crimes_95,
        num_crimes_96
    from source
)

select * from renamed