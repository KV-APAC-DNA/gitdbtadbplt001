
with sdl_mds_th_ref_region as (
select * from {{ source('thasdl_raw', 'sdl_mds_th_ref_region') }}
),
final as (
select
  trim(code)::varchar(20) as region,
  trim(name)::varchar(100) as region_desc,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3')::varchar(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as curr_date,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from sdl_mds_th_ref_region)
select * from final

