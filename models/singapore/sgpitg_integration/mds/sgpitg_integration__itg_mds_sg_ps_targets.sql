
with source as (
    select * from {{ source('sgpsdl_raw','sdl_mds_sg_ps_targets') }}
),

final as (
select
  tgt.channel::varchar(100) as channel,
  tgt.re::varchar(100) as retail_env,
  tgt.kpi::varchar(100) as kpi,
  tgt.attribute_1::varchar(100) as attribute_1,
  tgt.attribute_2::varchar(100) as attribute_2,
  tgt.value::numeric(20,4) as target,
cast(current_timestamp() as timestamp_ntz(9)) as crtd_dttm
from source as tgt
)
select * from final