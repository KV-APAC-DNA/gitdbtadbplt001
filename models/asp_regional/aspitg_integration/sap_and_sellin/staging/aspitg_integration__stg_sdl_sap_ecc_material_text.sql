with sources as(
    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_material_text') }}
)

select * from sources