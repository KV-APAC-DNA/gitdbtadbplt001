{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['dstrbtr_id']
    )
}}

with source as (
    select * from {{ source('vnmsdl_raw','sdl_vn_dms_distributor_dim') }}
),

final as
(
select
    territory_dist::varchar(100) as territory_dist,
    mapped_spk::varchar(30) as mapped_spk,
    dstrbtr_id::varchar(30) as dstrbtr_id,
    dstrbtr_type::varchar(100) as dstrbtr_type,
    dstrbtr_name::varchar(100) as dstrbtr_name,
    dstrbtr_address::varchar(500) as dstrbtr_address,
    trim(longitude, ',')::varchar(20) as longitude,
    trim(latitude, ',')::varchar(20) as latitude,
    region::varchar(20) as region,
    province::varchar(100) as province,
    trim(active, ',')::varchar(5) as active,
    trim(asm_id)::varchar(50) as asm_id,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    null::number(14,0) as run_id    
from source
)
select * from final
   