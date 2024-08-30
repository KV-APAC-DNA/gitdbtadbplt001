{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['from_cycle','to_cycle','customer_code']
    )
}}

with source as (
    select *, dense_rank() over (partition by from_cycle,to_cycle,customer_code order by file_name desc) rnk 
    from {{ source('vnmsdl_raw','sdl_vn_gt_topdoor_target') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_gt_topdoor_target__duplicate_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_gt_topdoor_target__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_gt_topdoor_target__format_test_from_cycle')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_gt_topdoor_target__format_test_to_cycle')}}
    ) qualify rnk = 1
),

final as
(
    select
    td_id::varchar(100) as td_id,
	td_zone::varchar(50) as td_zone,
	from_cycle::varchar(10) as from_cycle,
	to_cycle::varchar(10) as to_cycle,
	distributor_code::varchar(100) as distributor_code,
	customer_code::varchar(100) as customer_code,
	customer_name::varchar(200) as customer_name,
	shoptype::varchar(100) as shoptype,
	target_value::number(18,2) as target_value,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	file_name::varchar(200) as file_name
    from source
)
select * from final