-- dbt/models/staging/berka/stg_berka__account.sql

with source as (
    select * from {{ source('berka', 'account') }}
),

renamed as (
    select
        account_id,
        district_id,
        frequency,
        PARSE_DATE('%Y%m%d',
            CASE
                WHEN CAST(date AS STRING) LIKE '9%' THEN CONCAT('19', CAST(date AS STRING))
                ELSE CONCAT('20', CAST(date AS STRING))
            END
        ) as account_open_date
    from source
)

select * from renamed