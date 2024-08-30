
{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="delete from {{this}} 
        where md5(coalesce(upper(item_no),'N/A') || coalesce(upper(customer_code),'N/A') || coalesce(upper(customer_name),'N/A')) in (select md5(coalesce(upper(item_no),'N/A') || coalesce(upper(customer_code),'N/A') || coalesce(upper(customer_name),'N/A')) 
        from {{source('thasdl_raw','sdl_mym_gt_sales_report_fact')}}
        where filename not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_mym_gt_sales_report_fact__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_mym_gt_sales_report_fact__duplicate_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_mym_gt_sales_report_fact__test_format') }}
    )
        
         )"
    )
}}

with source as
(
    select * from {{source('thasdl_raw','sdl_mym_gt_sales_report_fact')}}
    where filename not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_mym_gt_sales_report_fact__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_mym_gt_sales_report_fact__duplicate_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_mym_gt_sales_report_fact__test_format') }}
    )
),
final as
(
    SELECT 
        item_no::varchar(50) as item_no,
        description::varchar(200) as description,
        qty_sold::number(18,4) as qty_sold,
        foc_qty::number(18,4) as foc_qty,
        total::number(18,4) as total_mmk,
        try_to_date(substring(trim(period), 1, 10), 'dd/mm/yyyy')::date as period,
        customer_group::varchar(100) as customer_group,
        customer_code::varchar(50) as customer_code,
        customer_name::varchar(100) as customer_name,
        filename::varchar(50) as filename,
        run_id::varchar(14) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final