with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_material_plant_text') }}

),

final as (

    select
        plant,
        mat_plant,
        langu,
        txtmd,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
