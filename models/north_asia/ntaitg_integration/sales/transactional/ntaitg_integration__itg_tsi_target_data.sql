{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['filename'],
        pre_hook = "delete from {{this}} where year_month in ( select distinct date from {{ source('ntasdl_raw', 'sdl_tsi_target_data') }});"
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_tsi_target_data') }}
),
final as
(    
    SELECT 
        date::number(18,0) as year_month,
        ec::varchar(10) as ec,
        offtake::varchar(10) as offtake,
        customer_code::varchar(50) as customer_code,
        customer_name::varchar(100) as customer_name,
        customer_cname::varchar(100) as customer_cname,
        customer_sname::varchar(100) as customer_sname,
        nts::number(34,8) as nts,
        "offtake(sell_out)"::number(34,8) as "offtake/sellout",
        gts::number(34,8) as gts,
        pre_sales::number(34,8) as pre_sales,
        prs_code_01::varchar(50) as prs_code_01,
        prs_code_02::varchar(50) as prs_code_02,
        prs_code_03::varchar(50) as prs_code_03,
        prs_code_04::varchar(50) as prs_code_04,
        prs_code_05::varchar(50) as prs_code_05,
        pre_sales - NTS::number(34,8) as TP,
        ((pre_sales - NTS) / nullif(pre_sales, 0)) * 100::float as TP_Percent,
        filename::varchar(100) as filename,
        crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final