{{
    config(
        materialized="incremental",
        incremental_strategy = "append"
    )
}}

with source as
(
    select * from {{source('thasdl_raw','sdl_mym_gt_sales_report_fact')}}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_mym_gt_sales_report_fact__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_mym_gt_sales_report_fact__duplicate_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_mym_gt_sales_report_fact__test_format') }}
    )
),

final as
(   
    select 
        item_no,
        description,
        qty_sold,
        foc_qty,
        total,
        period,
        customer_group,
        customer_code,
        customer_name,
        filename as file_name,
        run_id,
        crt_dttm
    from source
    {% if is_incremental() %}
        where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif%}
)
select * from final