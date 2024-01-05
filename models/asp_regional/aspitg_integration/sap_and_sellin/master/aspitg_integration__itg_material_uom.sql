with source as(
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_material_uom') }}
),
final as(
    SELECT
  MATERIAL,
  UNIT,
  BASE_UOM,
  RECORDMODE as RECORD_MODE,
  CAST(UOMZ1D AS DECIMAL(20, 4)) AS UOMZ1D,
  CAST(UOMN1D AS DECIMAL(20, 4)) AS UOMN1D,
  CDL_DTTM,
  CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS CRTD_DTTM,
  CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS UPDT_DTTM,
  file_name
FROM source
)

select * from final