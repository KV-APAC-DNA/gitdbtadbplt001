with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_material_plant_text') }}
),

final as (
    select
        plant,
        mat_plant,
        langu,
        txtmd,
        updt_dttm
    from source

)

select * from final
