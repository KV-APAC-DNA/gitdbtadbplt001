{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid','re','target','period']
    )
}}


with sdl_mds_lcm_distributor_target_sales_re as (
-- select * from dev_dna_load.snaposesdl_raw.sdl_mds_lcm_distributor_target_sales_re
select * from {{ source('thasdl_raw', 'sdl_mds_lcm_distributor_target_sales_re') }}
),
transformed as (
select
  upper(trim(distributorid)) as distributorid,
  upper(trim(re)) as re,
  target,
  trim(period) as period,
  convert_timezone('Asia/Singapore', current_timestamp()) as crt_dttm,
  convert_timezone('Asia/Singapore', current_timestamp()) as updt_dttm
from sdl_mds_lcm_distributor_target_sales_re),
final as (
    select
    distributorid::varchar(10) as distributorid,
    re::varchar(20) as re,
    target::number(18,6) as target,
    period::varchar(10) as period,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final 