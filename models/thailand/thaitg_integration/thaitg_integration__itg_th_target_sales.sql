with sdl_th_target_sales as (
select * from dev_dna_load.snaposesdl_raw.sdl_th_target_sales
),
transformed as (
select
  dstrbtr_id,
  sls_office,
  sls_grp,
  target,
  period,
  tgt_date,
  crt_date,
  cdl_dttm,
  current_timestamp() as curr_date,
  current_timestamp() as updt_dttm
from sdl_th_target_sales),
final as (
    select
    dstrbtr_id::varchar(10) as  dstrbtr_id,
    sls_office::varchar(10) as sls_office,
    sls_grp::varchar(10) as sls_grp,
    target::number(18,0) as target,
    period::varchar(6) as period,
    tgt_date::timestamp_ntz(9) as tgt_date,
    crt_date::timestamp_ntz(9) as crt_date,
    cdl_dttm::varchar(255) as cdl_dttm,
    curr_date::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final
