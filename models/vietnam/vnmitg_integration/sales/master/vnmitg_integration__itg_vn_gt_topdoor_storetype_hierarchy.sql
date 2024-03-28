with source as 
(
    select * from {{ source('vnmsdl_raw','sdl_mds_vn_topdoor_storetype_mapping') }}
),

final as
(
    select
    storetype::varchar(100) as storetype,
    group_hierarchy::varchar(100) as group_hierarchy,
    top_door_group::varchar(100) as top_door_group,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	null::varchar(40) as run_id
    from source
)

select * from final