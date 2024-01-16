with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_material_sales_text') }}
),

final as (
    select
        salesorg,
        distr_chan,
        mat_sales,
        langu,
        txtmd,
        updt_dttm
    from source

)

select * from final
