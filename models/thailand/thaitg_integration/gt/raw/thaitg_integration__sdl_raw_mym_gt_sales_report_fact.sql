{{
    config(
        materialized="incremental",
        incremental_strategy = "append"
    )
}}

with source as
(
    select * from {{source('thasdl_raw','sdl_mym_gt_sales_report_fact')}}
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
        filename,
        run_id,
        crt_dttm
    from source
    {% if is_incremental() %}
        where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif%}
)
select * from final