with sdl_cbd_gt_inventory_report_fact as (
    select * from {{ source('thasdl_raw', 'sdl_cbd_gt_inventory_report_fact') }}
),
final as (
 select 
 date::date as date,
clientcd_name::varchar(200) as clientcd_name,
product_code::varchar(50) as product_code,
product_name::varchar(200) as product_name,
baseuom::varchar(10) as baseuom,
 cast (expired as NUMERIC(18,4)) as expired,
 cast ("1-90days" as NUMERIC(18,4)) as "1-90days",
 cast ("91-180days" as NUMERIC(18,4)) as "91-180days",
 cast ("181-365days" as NUMERIC(18,4)) as "181-365days",
 cast (">365days" as NUMERIC(18,4)) as ">365days",
 cast (total_qty as NUMERIC(18,4)) as total_qty,
 filename::varchar(50) as filename,
 run_id::varchar(14) as run_id,
 crt_dttm::timestamp_ntz(9) as crt_dttm,
 current_timestamp()::timestamp_ntz(9) AS updt_dttm
  from sdl_cbd_gt_inventory_report_fact
)
select * from final