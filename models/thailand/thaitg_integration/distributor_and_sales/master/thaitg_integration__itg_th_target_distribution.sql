{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['dstrbtr_id']
    )
}}
with source as(
    select * from {{ source('thasdl_raw','sdl_th_target_distribution') }}
),
final as (
select
  dstrbtr_id::varchar(10) as dstrbtr_id,
  period::varchar(6) as period,
  target::number(18,0) as target,
  updt_date::timestamp_ntz(9) as updt_date,
  prod_nm::varchar(50) as prod_nm,
  cdl_dttm::varchar(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm,

from source
)

select * from final