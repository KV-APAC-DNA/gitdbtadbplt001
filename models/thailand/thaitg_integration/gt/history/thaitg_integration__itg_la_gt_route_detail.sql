
{{
    config(
        materialized="incremental",
        incremental_strategy='append',
        pre_hook="delete from {{this}} where (coalesce(upper(trim(saleunit)),'N/A')) in (select distinct coalesce(upper(trim(saleunit)),'N/A')
                                                  from {{ source('thasdl_raw', 'sdl_la_gt_route_detail') }}) and   upper(flag) in ('I','U');"
    )
}}
with wks_la_gt_route_detail_pre_load as (
    select * from {{ ref('thawks_integration__wks_la_gt_route_detail_pre_load') }}
),
final as (
SELECT 
route_id::varchar(50) as route_id,
customer_id::varchar(50) as customer_id,
route_no::varchar(10) as route_no,
saleunit::varchar(100) as saleunit,
ship_to::varchar(50) as ship_to,
contact_person::varchar(100) as contact_person,
created_date::date as created_date,
file_upload_date::date as file_upload_date,
last_modified_date::date as last_modified_date,
flag::varchar(5) as flag,
filename::varchar(50) as filename,
run_id::varchar(14) as run_id,
crt_dttm::timestamp_ntz(9) as crt_dttm,
current_timestamp()::timestamp_ntz(9) as updt_dttm,
FROM wks_la_gt_route_detail_pre_load )
select * from final