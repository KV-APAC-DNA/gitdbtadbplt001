with source as(
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_material_uom') }}
),
final as(
  select
  material::VARCHAR(50) as material,
  unit::VARCHAR(20) as unit,
  base_uom::VARCHAR(20) as base_uom,
  recordmode::VARCHAR(100) as record_mode,
  CAST(uomz1d as number(20, 4)) as uomz1d,
  CAST(uomn1d as number(20, 4)) as uomn1d,
  cdl_dttm::VARCHAR(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm,
  file_name::VARCHAR(255) as file_name
  from source
)

select * from final