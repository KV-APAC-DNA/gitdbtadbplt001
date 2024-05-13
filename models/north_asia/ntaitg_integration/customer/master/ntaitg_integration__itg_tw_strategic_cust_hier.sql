with source as(
    select * from {{ source('ntasdl_raw', 'sdl_tw_strategic_cust_hier') }}
),
final as(
    select 
        CAST(year_code AS INTEGER) as year,
        CAST(month_code AS INTEGER) as month,
        month_name::varchar(200) as month_name,
        ec_code::varchar(200) as ec_code,
        ec_name::varchar(200) as ec_name,
        offtake_inc_code::varchar(200) as offtake_inc,
        offtake_inc_name::varchar(200) as offtake_inc_name,
        customer_code::varchar(200) as customer_code,
        customer_name_code::varchar(200) as customer_name,
        customer_name_local::varchar(200) as customer_c_name,
        s_name::varchar(200) as customer_s_name,
        psr_code_01_code::varchar(200) as psr_code01,
        psr_code_01_name::varchar(200) as psr_name01,
        psr_code_02_code::varchar(200) as psr_code02,
        psr_code_02_name::varchar(200) as psr_name02,
        psr_code_03_code::varchar(200) as psr_code03,
        psr_code_03_name::varchar(200) as psr_name03,
        psr_code_04_code::varchar(200) as psr_code04,
        psr_code_04_name::varchar(200) as psr_name04,
        psr_code_05_code::varchar(200) as psr_code05,
        psr_code_05_name::varchar(200) as psr_name05,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM source
)
select * from final