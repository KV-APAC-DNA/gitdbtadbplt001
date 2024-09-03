{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ["branch_id","pro_id","ord_no","line_ref"]
    )
}}

with source as 
(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_order_promotion') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_order_promotion__duplicate_test')}}
    )
),
final as 
(
select 
    branch_id::varchar(30) as branch_id,
    pro_id::varchar(50) as pro_id,
    ord_no::varchar(20) as ord_no,
    line_ref::varchar(10) as line_ref,
    trim(disc_type)::varchar(1) as disc_type,
    trim(break_by)::varchar(1) as break_by,
    disc_break_line_ref::varchar(10) as disc_break_line_ref,
    cast(disct_bl_amt as numeric)::number(15,4) as disct_bl_amt,
    cast(disct_bl_qty as int)::number(18,0) as disct_bl_qty,
    free_item_code::varchar(20) as free_item_code,
    cast(free_item_qty as int)::number(18,0) as free_item_qty,
    cast(disc_amt as numeric)::number(15,4) as disc_amt,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
    run_id::number(14,0) as run_id,
    file_name::varchar(255) as file_name
from source
)
select * from final