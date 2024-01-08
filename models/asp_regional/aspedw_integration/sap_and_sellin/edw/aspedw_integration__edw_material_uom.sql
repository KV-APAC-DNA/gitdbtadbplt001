{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
    )
}}


with source as(
    select * from {{ ref('aspitg_integration__itg_material_uom') }}
),
final as(
  select
  material,
  unit,
  base_uom,
  record_mode,
  cast(uomz1d as decimal(20, 4)) as uomz1d,
  cast(uomn1d as decimal(20, 4)) as uomn1d,
  cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)
select * from final