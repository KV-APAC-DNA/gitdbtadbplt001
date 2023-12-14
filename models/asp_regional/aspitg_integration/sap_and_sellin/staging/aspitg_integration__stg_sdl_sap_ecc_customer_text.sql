{{
    config(
        alias="stg_sdl_sap_ecc_customer_text",
        materialized="view"
    )
}}
with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_customer_text') }}

),

final as (

    select
        mandt,
        kunnr,
        txtmd,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
