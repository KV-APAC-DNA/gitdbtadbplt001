
with source as (
    select * from {{ source('sgpsdl_raw','sdl_mds_sg_ps_weights') }}
),
final as (
select
  channel::varchar(100) as channel,
  re::varchar(100) as retail_env,
  kpi::varchar(100) as kpi,
  weight::numeric(20,4) as weight,
  cast(current_timestamp() as timestamp_ntz(9)) as crtd_dttm
from source
)
select * from final

