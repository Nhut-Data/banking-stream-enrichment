-- dbt/models/marts/fct_enriched_transactions.sql
-- Fact table: transaction + customer profile
-- Đây là output cuối của Batch Layer, tương đương với enriched records
-- của Speed Layer nhưng đầy đủ hơn (có dedupe + referential check)

with transactions as (
    select * from {{ ref('stg_berka__trans') }}
),

profiles as (
    select * from {{ ref('int_customer_profile') }}
),

enriched as (
    select
        -- Transaction
        t.trans_id,
        t.account_id,
        t.transaction_date,
        t.transaction_type,
        t.operation,
        t.amount,
        t.balance,
        t.k_symbol,

        -- Customer profile
        p.client_id,
        p.gender,
        p.birth_date,
        p.age_at_end_of_dataset,
        p.frequency         as account_frequency,
        p.account_open_date,

        -- Geographic
        p.district_name,
        p.region,
        p.avg_salary,
        p.unemployment_rate_96,

        -- Derived metrics
        t.amount / nullif(p.avg_salary, 0)  as amount_to_avg_salary_ratio

    from transactions t
    -- INNER JOIN: loại bỏ referential violations
    -- Các trans_id không tìm thấy account sẽ không có trong mart
    -- Đây là intentional — Batch Layer ưu tiên correctness over completeness
    inner join profiles p
        on t.account_id = p.account_id
)

select * from enriched