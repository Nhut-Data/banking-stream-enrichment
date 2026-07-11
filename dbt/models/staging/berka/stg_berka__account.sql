-- dbt/models/staging/berka/stg_berka__account.sql

with source as (
    select * from {{ source('berka', 'account') }}
),

renamed as (
    select
        account_id,
        district_id,
        frequency,
        to_date(
            case
                when date::text like '9%' then '19' || date::text
                else '19' || date::text
            end,
            'YYYYMMDD'
        ) as account_open_date
    from source
)

select * from renamed