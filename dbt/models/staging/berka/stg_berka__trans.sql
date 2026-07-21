-- dbt/models/staging/berka/stg_berka__trans.sql
-- Staging: cast types, rename, không có business logic
-- Dedupe theo trans_id — bắt Duplicate corruption đã tiêm
with source as (
    select * from {{ source('berka', 'trans') }}
),
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
        trans_id,
        account_id,
        -- Date: convert YYMMDD integer sang DATE
        -- LPAD bắt buộc trước — cột date là INTEGER nên số 0 đầu bị mất
        -- (vd: "001017" bị lưu thành số 1017), gây sai lệch nếu không phục hồi lại
        -- trước khi áp dụng logic phân biệt thế kỷ dựa trên ký tự đầu
        PARSE_DATE('%Y%m%d',
            case
                when LPAD(CAST(date AS STRING), 6, '0') like '0%' then '20' || LPAD(CAST(date AS STRING), 6, '0')
                when LPAD(CAST(date AS STRING), 6, '0') like '9%' then '19' || LPAD(CAST(date AS STRING), 6, '0')
                else CONCAT('19', LPAD(CAST(date AS STRING), 6, '0'))
            end
        ) as transaction_date,
        type            as transaction_type,
        operation,
        amount,
        balance,
        nullif(k_symbol, '')    as k_symbol,
        bank,
        account         as counterpart_account
    from deduplicated
)
select * from renamed
