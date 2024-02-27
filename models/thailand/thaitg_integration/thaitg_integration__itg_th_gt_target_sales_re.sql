with source as(
    select * from {{ source('thasdl_raw','sdl_mds_th_distributor_target_sales_re') }}
),
trans as (
select
  'TH'::varchar(5) as cntry_cd,
  'THD'::varchar(5) as crncy_cd,
  upper(trim(distributorid))::varchar(10) as distributor_id,
  upper(trim(re_code))::varchar(20) as re_code,
  trim(re_name)::varchar(50) as re_name,
  trim(period)::varchar(10) as period,
  cast(trim(target) as decimal(18, 6)) as target,
  to_char(cast(current_timestamp() as timestampntz), 'yyyymmddhh24missms')::varchar(100) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
),

final as (
select
  cntry_cd,
  crncy_cd,
  distributor_id,
  re_code,
  re_name,
  period,
  target,
  cdl_dttm,
  crtd_dttm,
  updt_dttm
from trans
)

select * from final