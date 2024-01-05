with source as(
    select * from {{ source('BWA_ACCESS', 'BWA_MATERIAL_UOM') }}
),
final as(
    SELECT
  material,
  unit,
  base_uom,
  recordmode,
  uomz1d,
  uomn1d,
  _INGESTIONTIMESTAMP_ as CDL_DTTM,
   CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) as CURR_DT,
  NULL as File_Name
FROM source
)
select * from final