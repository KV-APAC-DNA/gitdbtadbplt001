with sdl_mds_th_distributor_list as (
select * from dev_dna_load.snaposesdl_raw.sdl_mds_th_distributor_list
),
transformed as (
select
  trim(code) as dstrbtr_id,
  trim(name) as dist_nm,
  null as cost_lvl,
  trim(status_name) as status,
  trim(region_code) as region,
  'TH' as cntry,
  null as curnt_dist,
  null as inv_day,
  trim(sap_customer_code) as dstrbtr_cd,
  null as dstrbtr_fee,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as curr_date,
  current_timestamp() as updt_dttm,
  trim(ship_to) as ship_to_code
from sdl_mds_th_distributor_list),
final as (
    select
    dstrbtr_id::varchar(10) as dstrbtr_id,
    dist_nm::varchar(100) as dist_nm,
    cost_lvl::number(18,0) as cost_lvl,
    status::varchar(10) as status,
    region::varchar(20) as region,
    cntry::varchar(20) as cntry,
    curnt_dist::varchar(10) as curnt_dist,
    inv_day::number(3,0) as inv_day,
    dstrbtr_cd::varchar(255) as dstrbtr_cd,
    dstrbtr_fee::float as dstrbtr_fee,
    cdl_dttm::varchar(255) as cdl_dttm,
    curr_date::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    ship_to_code::varchar(200) as ship_to_code
    from transformed
)
select * from final

