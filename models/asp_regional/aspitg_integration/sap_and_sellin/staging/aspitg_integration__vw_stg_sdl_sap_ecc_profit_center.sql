{{
    config(
        materialized="view",
        alias="vw_stg_sdl_sap_ecc_profit_center",
        tags=["daily","sap_ecc"]
    )
}}

with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_profit_center') }}

),

final as (

    select
        kokrs,
        prctr,
        dateto,
        datefrom,
        verak,
        waers,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
