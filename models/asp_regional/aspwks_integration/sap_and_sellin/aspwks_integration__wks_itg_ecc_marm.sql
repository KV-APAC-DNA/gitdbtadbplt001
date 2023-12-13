{{
    config(
        alias="wks_itg_ecc_marm",
        tags=[""]
    )
}}

with 

source as (

    select * from {{ ref('stg_aspsdl_raw__sdl_sap_ecc_marm') }}
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
    -- tgt.crt_dttm as tgt_crt_dttm,
    updt_dttm as updt_dttm
  from source
)

select * from final