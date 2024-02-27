with source as(
    select * from {{ source('thasdl_raw','sdl_mds_th_ref_distributor_item_unit') }}
),
trans as (
select
  trim(code)::varchar(10) as code,
  trim(name)::varchar(50) as name1,
  trim(name2)::varchar(50) as name2,
  null as msrepl_tran_ver,
  to_char(cast(current_timestamp() as timestampntz), 'yyyymmddhh24missms')::varchar(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
),

final as (
select 
  code,
  name1,
  name2,
  msrepl_tran_ver,
  cdl_dttm,
  crtd_dttm,
  updt_dttm
from trans
)

select * from final