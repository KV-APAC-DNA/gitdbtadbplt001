with source as(
    select * from {{ source('thasdl_raw','sdl_mds_th_ps_weights') }}
),
trans as (
select
  channel::varchar(100) as channel,
  re::varchar(100) as re,
  kpi::varchar(100) as kpi,
  weight::number(20,4) as weight,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm
from source
),

final as (
    select 
    channel,
    re,
    kpi,
    weight
    crtd_dttm
from trans

)

select * from final