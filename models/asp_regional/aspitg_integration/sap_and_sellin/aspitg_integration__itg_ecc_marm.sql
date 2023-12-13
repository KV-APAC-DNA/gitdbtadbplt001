{{
    config(
        alias="itg_ecc_marm",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["matl_no","alt_unt"],
        merge_exclude_columns = ["crt_dttm"],
        tags=[""]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_ecc_marm') }}
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
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final