{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['dstrbtr_id','sls_office','sls_grp','period']
    )
}}
with sdl_mds_th_distributor_target_sales as (
select * from {{ source('thasdl_raw', 'sdl_mds_th_distributor_target_sales') }}
),
final as (
select
  trim(distributorid)::varchar(10) as dstrbtr_id,
  trim(saleoffice::varchar(10)) as sls_office,
  trim(salegroup)::varchar(10) as sls_grp,
  trim(target)::number(18,0) as target,
  trim(period)::varchar(6) as period,
  to_date(period::varchar, 'YYYYMM')::timestamp_ntz(9) as tgt_date,
  null::timestamp_ntz(9) as crt_date,
  to_char(cast(current_timestamp() as timestampntz), 'yyyymmddhh24missms')::varchar(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as curr_date,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from sdl_mds_th_distributor_target_sales)
select * from final
