with source as(
    select * from {{ source('bwa_access', 'bwa_material_uom') }}
),
final as(
    SELECT
  material,
  unit,
  base_uom,
  recordmode,
  uomz1d,
  uomn1d,
  _ingestiontimestamp_ as cdl_dttm,
   current_timestamp()::timestamp_ntz(9) as curr_dt,
  NULL as File_Name
FROM source
)
select * from final