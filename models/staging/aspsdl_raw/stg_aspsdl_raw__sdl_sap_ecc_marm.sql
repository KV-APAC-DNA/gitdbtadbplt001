{{
    config(
        materialized='view',
        alias='stg_sdl_sap_ecc_marm',
        tags=['']
    )
}}

with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_marm') }}

),

final as (

    select
        matnr,
        meinh,
        umrez,
        umren,
        eannr,
        ean11,
        numtp,
        laeng,
        breit,
        hoehe,
        meabm,
        volum,
        voleh,
        brgew,
        gewei,
        mesub,
        atinn,
        mesrt,
        xfhdw,
        xbeww,
        kzwso,
        msehi,
        bflme_marm,
        gtin_variant,
        nest_ftr,
        max_stack,
        capause,
        ty2tq,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
