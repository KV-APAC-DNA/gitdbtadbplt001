with source as(
    select * from {{ source('thasdl_raw','sdl_mds_th_ref_distributor_item_unit') }}
),
final as (
select
  trim(code)::varchar(10) as code,
  trim(name)::varchar(50) as name1,
  trim(name2)::varchar(50) as name2,
  null::varchar(50) as msrepl_tran_ver,
  to_char(cast(current_timestamp() as timestampntz), 'yyyymmddhh24missff3')::varchar(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)

select * from final