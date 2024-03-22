with source as(
    select * from {{ source('thasdl_raw','sdl_mds_th_cop') }}
),
final as (
select
  trim(code)::varchar(20) as dist_id,
  cast(trim(year) as int)::number(18,0) as year,
  cast(trim(month) as int)::number(18,0) as month,
  cast(trim(no_of_store) as int)::number(18,0) as str_count,
  to_char(cast(current_timestamp() as timestampntz), 'yyyymmddhh24missff3')::varchar(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)

select * from final