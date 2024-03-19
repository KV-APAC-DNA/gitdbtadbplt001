
{{
    config(
        materialized="incremental",
        incremental_strategy = "delete+insert",
        unique_key=["hash_key"]
    )
}}

with source as
(
    select * from {{source('thasdl_raw','sdl_mym_gt_sales_report_fact')}}
),
final as
(
    SELECT 
        item_no::varchar(50) as item_no,
        md5(
            coalesce(UPPER(item_no), 'N/A') || 
            coalesce(upper (customer_code), 'N/A') || 
            coalesce(upper (customer_name), 'N/A')
        ) as hash_key,
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
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
    from source
)
select * from final