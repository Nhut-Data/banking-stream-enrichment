-- dbt/models/staging/berka/stg_berka__disposition.sql

with source as (
    select * from {{ source('berka', 'disposition') }}
),

renamed as (
    select
        disp_id,
        client_id,
        account_id,
        type as disposition_type  -- 'OWNER' hoặc 'DISPONENT'
    from source
)

select * from renamed