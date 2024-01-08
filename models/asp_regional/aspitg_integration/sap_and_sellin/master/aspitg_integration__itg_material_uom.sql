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
  CAST(uomz1d AS DECIMAL(20, 4)) AS uomz1d,
  CAST(uomn1d AS DECIMAL(20, 4)) AS uomn1d,
  cdl_dttm,
  current_timestamp()::TIMESTAMP_NTZ(9) AS crtd_dttm,
  current_timestamp()::TIMESTAMP_NTZ(9) AS updt_dttm,
  file_name
  from source
)

select * from final