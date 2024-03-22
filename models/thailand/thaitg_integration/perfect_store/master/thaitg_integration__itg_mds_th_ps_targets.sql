with sdl_mds_th_ps_targets as (
  select * from {{ source('thasdl_raw', 'sdl_mds_th_ps_targets') }}
  ),
  final as (
  select
    tgt.channel::varchar(100) as channel,
    tgt.re::varchar(100) as re,
    tgt.kpi::varchar(100) as kpi,
    tgt.attribute_1::varchar(100) as attribute_1,
    tgt.attribute_2::varchar(100) as attribute_2,
    tgt.value::number(20,4) as value,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm,
    tgt.valid_from::timestamp_ntz(9) as valid_from,
    tgt.valid_to::timestamp_ntz(9) as valid_to
  from sdl_mds_th_ps_targets as tgt)
  select * from final