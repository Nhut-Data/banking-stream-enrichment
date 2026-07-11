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

        case
            when (birth_number % 10000 / 100) > 50 then 'F'
            else 'M'
        end as gender,

        -- Decode ngày sinh
        -- Nữ: trừ 5000 để lấy lại YYMMDD thật
        to_date(
            '19' || lpad(
                case
                    when (birth_number % 10000 / 100) > 50
                    then (birth_number - 5000)::text
                    else birth_number::text
                end,
                6, '0'
            ),
            'YYYYMMDD'
        ) as birth_date,

        -- Tính tuổi tại thời điểm cuối dataset (1998-12-31)
        date_part('year', age(
            date '1998-12-31',
            to_date(
                '19' || lpad(
                    case
                        when (birth_number % 10000 / 100) > 50
                        then (birth_number - 5000)::text
                        else birth_number::text
                    end,
                    6, '0'
                ),
                'YYYYMMDD'
            )
        ))::integer as age_at_end_of_dataset

    from source
)

select * from renamed