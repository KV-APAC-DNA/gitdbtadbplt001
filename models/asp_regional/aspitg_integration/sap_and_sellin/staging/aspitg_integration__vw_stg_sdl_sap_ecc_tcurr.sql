{{
    config(
        alias="vw_stg_sdl_sap_ecc_tcurr",
        materialized="view",
        tags=["daily","sap_ecc"]
    )
}}
with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_tcurr') }}

),

final as (

    select
        mandt,
        kurst,
        fcurr,
        tcurr,
        gdatu,
        ukurs,
        ffact,
        tfact,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
