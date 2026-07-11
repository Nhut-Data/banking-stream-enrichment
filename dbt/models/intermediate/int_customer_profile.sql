-- dbt/models/intermediate/int_customer_profile.sql
-- Join 4 bảng profile thành 1 view phẳng
-- Chỉ lấy OWNER disposition — người chủ tài khoản chính

with accounts as (
    select * from {{ ref('stg_berka__account') }}
),

clients as (
    select * from {{ ref('stg_berka__client') }}
),

dispositions as (
    select * from {{ ref('stg_berka__disposition') }}
    where disposition_type = 'OWNER'
),

districts as (
    select * from {{ ref('stg_berka__district') }}
),

joined as (
    select
        -- Account
        a.account_id,
        a.district_id       as account_district_id,
        a.frequency,
        a.account_open_date,

        -- Client
        c.client_id,
        c.gender,
        c.birth_date,
        c.age_at_end_of_dataset,
        c.district_id       as client_district_id,

        -- District (của client)
        d.district_name,
        d.region,
        d.num_inhabitants,
        d.avg_salary,
        d.unemployment_rate_95,
        d.unemployment_rate_96,
        d.num_crimes_95,
        d.num_crimes_96

    from accounts a
    inner join dispositions disp
        on a.account_id = disp.account_id
    inner join clients c
        on disp.client_id = c.client_id
    left join districts d
        on c.district_id = d.district_id
)

select * from joined