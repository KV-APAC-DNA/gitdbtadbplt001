with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_material_sales_text') }}

),

final as (

    select
        salesorg,
        distr_chan,
        mat_sales,
        langu,
        txtmd,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
