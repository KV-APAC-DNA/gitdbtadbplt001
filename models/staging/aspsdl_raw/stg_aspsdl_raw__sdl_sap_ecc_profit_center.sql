{{
    config(
        materialized="view",
        alias="stg_sdl_sap_ecc_profit_center",
        tags=[""]
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
