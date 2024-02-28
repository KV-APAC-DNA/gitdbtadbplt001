
with sdl_mds_th_ref_region as (
-- select * from DEV_DNA_LOAD.SNAPOSESDL_RAW.SDL_MDS_TH_REF_REGION
select * from {{ source('thasdl_raw', 'sdl_mds_th_ref_region') }}
),
transformed as (
select
  trim(code) as region,
  trim(name) as region_desc,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as curr_date,
  current_timestamp() as updt_dttm
from sdl_mds_th_ref_region),
final as (
    select
    region::varchar(20) as region,
    region_desc::varchar(100) as region_desc,
    cdl_dttm::varchar(255) as cdl_dttm,
    curr_date::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final

