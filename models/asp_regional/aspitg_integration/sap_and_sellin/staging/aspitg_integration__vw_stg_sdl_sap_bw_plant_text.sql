with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_plant_text') }}

),

final as (

    select
        plant,
        txtmd,
        txtlg,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
