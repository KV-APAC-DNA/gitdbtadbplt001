with source as(
    select * from {{ source('thasdl_raw','sdl_mds_th_ref_distributor_customer_group') }}
),
trans as (
select
  trim(code)::varchar(10) as ar_typ_cd,
  trim(name)::varchar(100) as grp_nm,
  trim(code)::varchar(10) as ar_typ_grp,
  trim(name)::varchar(50) as typ_grp_nm,
  to_char(cast(current_timestamp() as timestampntz(9)), 'yyyymmddhh24missms')::varchar(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
),

final as (
select
  ar_typ_cd,
  grp_nm,
  ar_typ_grp,
  typ_grp_nm,
  cdl_dttm,
  crtd_dttm,
  updt_dttm
from trans
)

select * from final