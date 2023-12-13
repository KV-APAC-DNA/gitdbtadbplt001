{{
    config(
        alias="sdl_sap_ecc_company_code_text",
        materialized="view"
    )
}}
with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_company_code_text') }}

),

final as (

    select
        mandt,
        bukrs,
        txtmd,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
