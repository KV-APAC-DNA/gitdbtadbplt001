with sdl_mds_th_market_share_distribution as (
select * from {{ source('thasdl_raw', 'sdl_mds_th_market_share_distribution') }}
),
final as (
select
  trim(measure)::varchar(100) as measure,
  trim(category)::varchar(256) as category,
  trim(year_month)::number(18,0)  as year_month,
  trim(value)::number(20,5) as value,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3')::varchar(255) As cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as curr_date,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from sdl_mds_th_market_share_distribution)
select * from final

