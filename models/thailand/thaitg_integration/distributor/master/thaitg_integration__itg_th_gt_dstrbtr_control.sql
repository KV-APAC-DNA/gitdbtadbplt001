with sdl_mds_th_gt_scope as (
--   select * from dev_dna_load.snaposesdl_raw.sdl_mds_th_gt_scope
select * from {{ source('thasdl_raw', 'sdl_mds_th_gt_scope') }}
  ),
transformed as (
  select
    'TH' as cntry_cd,
    upper(trim(code)) as distributor_id,
    upper(trim(sellout)) as sellout_flag,
    upper(trim(msl)) as msl_flag,
    upper(trim(call)) as call_flag,
    upper(trim(otif)) as otif_flag,
    upper(trim(stores)) as stores_flag,
    upper(trim(chana_cust_load)) as chana_cust_load_flag,
    to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
    current_timestamp() as crtd_dttm,
    current_timestamp() as updt_dttm
  from sdl_mds_th_gt_scope),
final as (
    select
    cntry_cd::varchar(5) as cntry_cd,
    distributor_id::varchar(10) as distributor_id,
    sellout_flag::varchar(2) as sellout_flag,
    msl_flag::varchar(2) as msl_flag,
    call_flag::varchar(2) as call_flag,
    otif_flag::varchar(2) as otif_flag,
    stores_flag::varchar(2) as stores_flag,
    chana_cust_load_flag::varchar(2) as chana_cust_load_flag,
    cdl_dttm::varchar(100) as cdl_dttm,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,	
    updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final 
