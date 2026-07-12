-- dbt/models/staging/berka/stg_berka__client.sql

with source as (
    select * from {{ source('berka', 'client') }}
),

renamed as (
    select
        client_id,
        district_id,

        -- birth_number encode cả ngày sinh lẫn giới tính:
        --   Nam:  YYMMDD
        --   Nữ:   YY(MM+50)DD  — tháng cộng thêm 50
        -- Decode gender từ tháng
        birth_number,

        CASE
            WHEN MOD(CAST(birth_number AS INT64), 10000) / 100 > 50 THEN 'F'
            ELSE 'M'
        END as gender,

        -- Decode ngày sinh
        -- Nữ: trừ 5000 để lấy lại YYMMDD thật
        PARSE_DATE('%Y%m%d',
            CONCAT('19', LPAD(
                CAST(
                    CASE
                        WHEN MOD(CAST(birth_number AS INT64), 10000) / 100 > 50
                        THEN birth_number - 5000
                        ELSE birth_number
                    END
                AS STRING),
            6, '0'))
        ) as birth_date,

        -- Tính tuổi tại thời điểm cuối dataset (1998-12-31)
        DATE_DIFF(DATE '1998-12-31', 
            PARSE_DATE('%Y%m%d',
                CONCAT('19', LPAD(
                    CAST(
                        CASE
                            WHEN MOD(CAST(birth_number AS INT64), 10000) / 100 > 50
                            THEN birth_number - 5000
                            ELSE birth_number
                        END
                    AS STRING),
                6, '0'))
            ),
        YEAR) as age_at_end_of_dataset

    from source
)

select * from renamed