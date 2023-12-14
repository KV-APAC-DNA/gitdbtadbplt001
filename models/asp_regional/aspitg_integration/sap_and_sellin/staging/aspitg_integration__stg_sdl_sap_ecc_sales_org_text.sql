{{
    config(
        materialized="view",
        alias="stg_sdl_sap_ecc_sales_org_text",
        tags=[""]
    )
}}

with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_sales_org_text') }}

),

final as (

    select
        mandt,
        spras,
        vkorg,
        vtext,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
