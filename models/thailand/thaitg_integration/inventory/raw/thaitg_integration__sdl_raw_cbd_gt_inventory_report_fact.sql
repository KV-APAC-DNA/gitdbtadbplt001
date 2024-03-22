
with sdl_cbd_gt_inventory_report_fact as (
    select * from {{ source('thasdl_raw', 'sdl_cbd_gt_inventory_report_fact') }}

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
    filename,
    run_id,
    crt_dttm
FROM
   sdl_cbd_gt_inventory_report_fact
)
select * from final 