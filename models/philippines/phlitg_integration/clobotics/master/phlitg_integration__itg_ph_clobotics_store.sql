{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['filename'],
        pre_hook= "delete from {{this}} where split_part(filename, '_', 1) in (
        select distinct split_part(filename, '_', 1) as filename from {{ source('phlsdl_raw', 'sdl_ph_clobotics_store_raw_data') }});"
    )
}}

with sdl_ph_clobotics_store_raw_data as 
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_clobotics_store_raw_data') }}
),
itg_ph_clobotics_store_mapping as 
(
    select * from {{ source('phlitg_integration', 'itg_ph_clobotics_store_mapping') }} ----to be confirmed
),
trans as
(
    select 
    project,
    period,
    planid,
    plan_status,
    plan_finish_time,
    username,
    display_username,
    store_code,
    store_name,
    city,
    shop_front_images,
    channel,
    plan_start_time,
    plan_upload_time,
    sku_id,
    sku_name,
    category,
    sub_category,
    brand,
    manufacturer,
    kpi,
    value,
    filename,
    run_id,
    from sdl_ph_clobotics_store_raw_data 
),
final as 
(
select 
    a.project::varchar(255) as project,
	a.period::varchar(255) as period,
	a.planid::varchar(255) as planid,
	a.plan_status::varchar(255) as plan_status,
	a.plan_finish_time::timestamp_ntz(9) as plan_finish_time,
	a.username::varchar(200) as username,
	a.display_username::varchar(200) as display_username,
	trim(b.store_code)::varchar(255) as store_code,
	a.store_name::varchar(255) as store_name,
	a.city::varchar(255) as city,
	a.shop_front_images::varchar(255) as shop_front_images,
	a.channel::varchar(255) as channel,
	a.plan_start_time::timestamp_ntz(9) as plan_start_time,
	a.plan_upload_time::timestamp_ntz(9) as plan_upload_time,
	a.sku_id::varchar(100) as sku_id,
	a.sku_name::varchar(255) as sku_name,
	a.category::varchar(255) as category,
	a.sub_category::varchar(255) as sub_category,
	a.brand::varchar(255) as brand,
	a.manufacturer::varchar(255) as manufacturer,
	a.kpi::varchar(255) as kpi,
	a.value::number(10,0) as value,
	a.filename::varchar(200) as filename,
	a.run_id::varchar(14) as run_id,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
from trans as a 
    left join itg_ph_clobotics_store_mapping b on 
    trim(a.store_code) = trim(b.old_store_code)
    and split_part(trim(a.store_name), ' ', 1) = split_part(trim(b.store_name), ' ', 1)
)
select * from final