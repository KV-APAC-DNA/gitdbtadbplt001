with sdl_mds_th_distributor_list as (
select * from {{ source('thasdl_raw', 'sdl_mds_th_distributor_list') }}
),
final as (
select
  trim(code)::varchar(10) as dstrbtr_id,
  trim(name)::varchar(100) as dist_nm,
  null::number(18,0) as cost_lvl,
  trim(status_name)::varchar(10) as status,
  trim(region_code)::varchar(20) as region,
  'TH'::varchar(20) as cntry,
  null::varchar(10) as curnt_dist,
  null::number(3,0) as inv_day,
  trim(sap_customer_code)::varchar(255) as dstrbtr_cd,
  null::float as dstrbtr_fee,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3')::varchar(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as curr_date,
  current_timestamp()::timestamp_ntz(9) as updt_dttm,
  trim(ship_to)::varchar(200) as ship_to_code
from sdl_mds_th_distributor_list)
select * from final

