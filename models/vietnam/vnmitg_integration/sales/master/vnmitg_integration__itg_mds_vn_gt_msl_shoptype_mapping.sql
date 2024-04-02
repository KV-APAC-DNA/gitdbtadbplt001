{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['shoptype_code','shoptype_name','channel','subchannel','msl_subchannel','active']
    )
}}

with source as (
    select * from {{ source('vnmsdl_raw','sdl_mds_vn_gt_msl_shoptype_mapping') }}
),

final as
(
    select
    shoptype_code::varchar(200) as shoptype_code ,
	shoptype_name::varchar(200) as 	shoptype_name ,
	channel::varchar(200) as channel ,
	subchannel::varchar(200) as subchannel,
	msl_subchannel::varchar(500) as msl_subchannel,
	active::number(31,0) as active,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	to_char(current_timestamp(),'yyyymmddhhmiss')::varchar(40) as run_id
    from source
)
select * from final