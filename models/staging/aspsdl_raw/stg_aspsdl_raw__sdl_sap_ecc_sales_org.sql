{{
    config(
        materialized='view',
        alias='stg_sdl_sap_ecc_sales_org',
        tags=['']
    )
}}

with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_sales_org') }}

),

final as (

    select
        mandt,
        vkorg,
        waers,
        bukrs,
        kunnr,
        land1,
        waers1,
        periv,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
