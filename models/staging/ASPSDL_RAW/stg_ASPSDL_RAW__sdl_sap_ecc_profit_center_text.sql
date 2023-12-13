{{
    config(
        materialized='view',
        alias='stg_sdl_sap_ecc_profit_center_text'
    )
}}

with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_profit_center_text') }}

),

final as (

    select
        langu,
        kokrs,
        prctr,
        dateto,
        datefrom,
        txtsh,
        txtmd,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
