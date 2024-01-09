{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
    )
}}


with source as(
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_material_uom') }}
),
final as(
  select
  material,
  unit,
  base_uom,
  recordmode as record_mode,
  CAST(uomz1d as decimal(20, 4)) as uomz1d,
  CAST(uomn1d as decimal(20, 4)) as uomn1d,
  CAST(cdl_dttm AS TEXT) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm,
  file_name
  from source
)

select * from final