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
    matnr as matl_no,
    meinh as alt_unt,
    umrez as numrtr,
    umren as denomtr,
    eannr as ean_nr,
    ean11 as ean_11,
    numtp as ctgry,
    laeng as length,
    breit as width,
    hoehe as height,
    meabm as unit,
    volum as volum,
    voleh as vol_unt,
    brgew as gross_wt,
    gewei as wt_unt,
    mesub as lowrlvl_unt,
    atinn as intrnl_char,
    mesrt as uom_srtno,
    xfhdw as leadng_unt,
    xbeww as valutn_unt,
    kzwso as unts_use,
    msehi as uom,
    bflme_marm as lg_vrnt,
    gtin_variant as ean_vrnt,
    nest_ftr as remng_vol,
    max_stack as max_stack,
    capause as capause,
    ty2tq as uom_ctgry,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final