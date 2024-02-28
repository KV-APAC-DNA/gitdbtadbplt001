with sdl_mds_th_market_share_distribution as (
-- select * from dev_dna_load.snaposesdl_raw.sdl_mds_th_market_share_distribution
select * from {{ source('thasdl_raw', 'sdl_mds_th_market_share_distribution') }}
),
transformed as (
select
  trim(measure) as measure,
  trim(category) as category,
  cast(trim(year_month) as int) as year_month,
  cast(trim(value) as decimal(20, 5)) as value,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') As cdl_dttm,
  current_timestamp() as curr_date,
  current_timestamp() as updt_dttm
from sdl_mds_th_market_share_distribution),
final as (
    select
    measure::varchar(100) as measure,
    category::varchar(256) as category,
    year_month::number(18,0) as year_month,
    value::number(20,5) as value,
    cdl_dttm::varchar(255) as cdl_dttm,
    curr_date::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final

