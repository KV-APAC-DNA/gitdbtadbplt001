{{
    config(
        alias="stg_sdl_sap_bw_country_code_text",
        materialized="view"
    )
}}
with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_country_code_text') }}

),

final as (

    select
        country,
        langu,
        txtsh,
        txtmd,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
