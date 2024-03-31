with sdl_mds_vn_ecom_target as (
    select * from {{ source('vnmsdl_raw', 'sdl_mds_vn_ecom_target') }}
),
final as (
select
    cycle::number(31,0) as cycle,
    platform::varchar(200) as platform,
    target::number(31,1) as target,
    null::varchar(14) as run_id,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
from sdl_mds_vn_ecom_target
)
select * from final 