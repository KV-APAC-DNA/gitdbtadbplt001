with source as(
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_customer_sales_rep_mapping') }}
),
union1 as(
    select distinct
        cast(year_code AS INTEGER) as year,
        cast(month_code AS INTEGER) as month,
        customer_code::varchar(100) as customer_code,
        customer_name_code::varchar(255) as customer_name,
        s_name::varchar(255) as customers_name,
        psr_code_01_code::varchar(100) as psr_code,
        psr_code_01_name::varchar(255) as psr_name,
        ec_code::varchar(255) as ec_code,
        offtake_inc_code::varchar(255) as offtake_inc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM source
),
union2 as(
    select distinct
        cast(year_code AS INTEGER) as year,
        cast(month_code AS INTEGER) as month,
        customer_code::varchar(100) as customer_code,
        customer_name_code::varchar(255) as customer_name,
        s_name::varchar(255) as customers_name,
        psr_code_02_code::varchar(100) as psr_code,
        psr_code_02_name::varchar(255) as psr_name,
        ec_code::varchar(255) as ec_code,
        offtake_inc_code::varchar(255) as offtake_inc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM source
),
union3 as(
    select distinct
        cast(year_code AS INTEGER) as year,
        cast(month_code AS INTEGER) as month,
        customer_code::varchar(100) as customer_code,
        customer_name_code::varchar(255) as customer_name,
        s_name::varchar(255) as customers_name,
        psr_code_03_code::varchar(100) as psr_code,
        psr_code_03_name::varchar(255) as psr_name,
        ec_code::varchar(255) as ec_code,
        offtake_inc_code::varchar(255) as offtake_inc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM source
),
union4 as(
    select distinct
        cast(year_code AS INTEGER) as year,
        cast(month_code AS INTEGER) as month,
        customer_code::varchar(100) as customer_code,
        customer_name_code::varchar(255) as customer_name,
        s_name::varchar(255) as customers_name,
        psr_code_04_code::varchar(100) as psr_code,
        psr_code_04_name::varchar(255) as psr_name,
        ec_code::varchar(255) as ec_code,
        offtake_inc_code::varchar(255) as offtake_inc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM source
),
union5 as(
    select distinct
        cast(year_code AS INTEGER) as year,
        cast(month_code AS INTEGER) as month,
        customer_code::varchar(100) as customer_code,
        customer_name_code::varchar(255) as customer_name,
        s_name::varchar(255) as customers_name,
        psr_code_05_code::varchar(100) as psr_code,
        psr_code_05_name::varchar(255) as psr_name,
        ec_code::varchar(255) as ec_code,
        offtake_inc_code::varchar(255) as offtake_inc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM source
),
final as(
    select * from union1
    union all
    select * from union2
    union all
    select * from union3
    union all
    select * from union4
    union all
    select * from union5
)
select * from final