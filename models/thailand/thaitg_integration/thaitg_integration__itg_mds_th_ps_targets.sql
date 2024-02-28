with sdl_mds_th_ps_targets as (
  select * from {{ source('thasdl_raw', 'sdl_mds_th_ps_targets') }}
  ),
  transformed as (
  select
    tgt.channel as channel,
    tgt.re as re,
    tgt.kpi as kpi,
    tgt.attribute_1 as attribute_1,
    tgt.attribute_2 as attribute_2,
    tgt.value as value,
    current_timestamp() as crtd_dttm,
    tgt.valid_from,
    tgt.valid_to
  from sdl_mds_th_ps_targets as tgt),
final as (
    select
    channel::varchar(100) as channel,
    re::varchar(100) as retail_env,
    kpi::varchar(100) as kpi,
    attribute_1::varchar(100) as attribute_1,
    attribute_2::varchar(100) as attribute_2,
    value::number(20,4) as target,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    valid_from::timestamp_ntz(9) as valid_from,
    valid_to::timestamp_ntz(9) as valid_to
    from transformed
)
  select * from final