with sdl_mds_th_gt_scope as (
select * from {{ source('thasdl_raw', 'sdl_mds_th_gt_scope') }}
  ),
final as (
  select
    'TH'::varchar(5) as cntry_cd,
    upper(trim(code))::varchar(10) as distributor_id,
    upper(trim(sellout))::varchar(2) as sellout_flag,
    upper(trim(msl))::varchar(2) as msl_flag,
    upper(trim(call))::varchar(2) as call_flag,
    upper(trim(otif))::varchar(2) as otif_flag,
    upper(trim(stores))::varchar(2) as stores_flag,
    upper(trim(chana_cust_load))::varchar(2) as chana_cust_load_flag,
    to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3')::varchar(100) as cdl_dttm,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from sdl_mds_th_gt_scope)
select * from final 
