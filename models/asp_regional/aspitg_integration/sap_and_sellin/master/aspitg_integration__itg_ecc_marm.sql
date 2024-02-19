{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["matl_no","alt_unt"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_ecc_marm') }}
),

trans as (
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
),

final as(
    select
        matl_no::varchar(18) as matl_no,
        alt_unt::varchar(3) as alt_unt,
        numrtr::number(5,0) as numrtr,
        denomtr::number(5,0) as denomtr,
        ean_nr::varchar(13) as ean_nr,
        ean_11::varchar(18) as ean_11,
        ctgry::varchar(2) as ctgry,
        length::number(13,3) as length,
        width::number(13,3) as width,
        height::number(13,3) as height,
        unit::varchar(3) as unit,
        volum::varchar(14) as volum,
        vol_unt::varchar(3) as vol_unt,
        gross_wt::number(13,3) as gross_wt,
        wt_unt::varchar(3) as wt_unt,
        lowrlvl_unt::varchar(3) as lowrlvl_unt,
        intrnl_char::number(10,0) as intrnl_char,
        uom_srtno::number(2,0) as uom_srtno,
        leadng_unt::varchar(1) as leadng_unt,
        valutn_unt::varchar(1) as valutn_unt,
        unts_use::varchar(1) as unts_use,
        uom::varchar(3) as uom,
        lg_vrnt::varchar(1) as lg_vrnt,
        ean_vrnt::varchar(2) as ean_vrnt,
        remng_vol::number(3,0) as remng_vol,
        max_stack::number(3,0) as max_stack,
        capause::number(15,0) as capause,
        uom_ctgry::varchar(1) as uom_ctgry,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)

select * from final