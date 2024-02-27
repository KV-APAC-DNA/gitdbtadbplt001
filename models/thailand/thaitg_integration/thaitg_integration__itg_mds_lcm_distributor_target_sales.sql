{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid','re','target','period']
    )
}}

with source as(
    select * from {{ source('thasdl_raw','sdl_mds_lcm_distributor_target_sales_re') }}
),
trans as 
(
select
    upper(trim(distributorid))::varchar(10) as distributorid,
    upper(trim(re))::varchar(20) as re,
    target::number(18,6) as target,
    trim(period)::varchar(10) as period,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
),

final as (
select
  distributorid,
  re,
  target,
  period,
  crt_dttm,
  updt_dttm
from trans
)

select * from final