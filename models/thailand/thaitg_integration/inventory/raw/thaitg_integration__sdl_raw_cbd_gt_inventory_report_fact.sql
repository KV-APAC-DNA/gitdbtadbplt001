{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}


with sdl_cbd_gt_inventory_report_fact as (
    select * from {{ source('thasdl_raw', 'sdl_cbd_gt_inventory_report_fact') }}
    where filename not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_cbd_gt_inventory_report_fact__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_cbd_gt_inventory_report_fact__duplicate_test') }}
    )

),
final as (
SELECT
    date,
    clientcd_name,
    product_code,
    product_name,
    baseUOM,
    expired,
    "1-90days",
    "91-180days",
    "181-365days",
    ">365days",
    total_qty,
    filename as filename,
    run_id,
    crt_dttm
FROM
   sdl_cbd_gt_inventory_report_fact
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)
select * from final 