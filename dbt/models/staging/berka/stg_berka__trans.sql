-- dbt/models/staging/berka/stg_berka__trans.sql
-- Staging: cast types, rename, không có business logic
-- Dedupe theo trans_id — bắt Duplicate corruption đã tiêm

with source as (
    select * from {{ source('berka', 'trans') }}
),

-- Dedupe: giữ 1 bản trong số các duplicate
-- Duplicate được tiêm có chủ đích để mô phỏng Kafka producer retry
-- dbt test unique(trans_id) sẽ bắt nếu dedupe này bị bỏ qua
deduplicated as (
    select *
    from (
        select *,
            row_number() over (
                partition by trans_id
                order by trans_id
            ) as rn
        from source
    ) ranked
    where rn = 1
),

renamed as (
    select
        -- Keys
        trans_id,
        account_id,

        -- Date: convert YYMMDD integer sang DATE
        -- Berka dùng format YYMMDD nén thành INTEGER (vd: 930101 = 1993-01-01)
        -- Thêm prefix '19' vì data là 1993-1998
        to_date(
            case
                when date::text like '9%' then '19' || date::text
                when date::text like '0%' then '20' || date::text  -- synthetic data 1999+
                else '19' || date::text
            end,
            'YYYYMMDD'
        ) as transaction_date,

        -- Transaction details
        type            as transaction_type,
        operation,
        amount,
        balance,

        -- k_symbol: purpose code
        -- Empty string ('') và NULL là 2 giá trị khác nhau trong Berka
        nullif(k_symbol, '')    as k_symbol,

        -- Bank transfer fields
        bank,
        account         as counterpart_account

    from deduplicated
)

select * from renamed