{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['filename'],
        pre_hook= "{% if is_incremental() %}
                delete from {{this}} where split_part(filename, '_', 1) in (
                select distinct split_part(filename, '_', 1) as filename from {{ source('phlsdl_raw', 'sdl_ph_clobotics_task_raw_data') }}
                where filename not in (
                select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_clobotics_task_raw_data__null_test')}}
                union all
                select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_clobotics_task_raw_data__format_test1')}}
                union all
                select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_clobotics_task_raw_data__format_test2')}}
                union all
                select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_clobotics_task_raw_data__format_test3')}}
            )
                );
        {%endif%}"
    )
}}

with sdl_ph_clobotics_task_raw_data as 
(
    select *, dense_rank() over (partition by null order by filename desc) as rnk
    from {{ source('phlsdl_raw', 'sdl_ph_clobotics_task_raw_data') }}
    where filename not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_clobotics_task_raw_data__null_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_clobotics_task_raw_data__format_test1')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_clobotics_task_raw_data__format_test2')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_clobotics_task_raw_data__format_test3')}}
    ) qualify rnk =1
),
itg_ph_clobotics_store_mapping as 
(
    select * from {{ source('phlitg_integration', 'itg_ph_clobotics_store_mapping') }} 
),
trans as
(
    select 
    project,
    period,
    taskid,
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
    task_name,
    task_status,
    create_time,
    task_action_time,
    stitched_image_url,
    image_quality,
    sku_id,
    sku_name,
    category,
    sub_category,
    brand,
    manufacturer,
    kpi,
    value,
    filename,
    run_id
    from sdl_ph_clobotics_task_raw_data 
),
final as 
(
    select 
    a.project::varchar(100) as project,
	a.period::varchar(200) as period,
	a.taskid::varchar(200) as taskid,
	a.planid::varchar(100) as planid,
	a.plan_status::varchar(50) as plan_status,
    cast(a.plan_finish_time as timestamp_ntz(9)) as plan_finish_time,
    a.username::varchar(200) as username,
	a.display_username::varchar(200) as display_username,
    trim(b.store_code)::varchar(255) as store_code,
    a.store_name::varchar(255) as store_name,
	a.city::varchar(255) as city,
	a.shop_front_images::varchar(255) as shop_front_images,
	a.channel::varchar(255) as channel,
	a.task_name::varchar(200) as task_name,
	a.task_status::varchar(100) as task_status,
    cast(a.create_time as timestamp_ntz(9)) as create_time,
    cast(a.task_action_time as timestamp_ntz(9)) as task_action_time,
    a.stitched_image_url::varchar(255) as stitched_image_url,
	a.image_quality::varchar(100) as image_quality,
	a.sku_id::varchar(100) as sku_id,
	a.sku_name::varchar(255) as sku_name,
	a.category::varchar(255) as category,
	a.sub_category::varchar(255) as sub_category,
	a.brand::varchar(255) as brand,
	a.manufacturer::varchar(255) as manufacturer,
	a.kpi::varchar(255) as kpi,
	a.value::number(10,1) as value,
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