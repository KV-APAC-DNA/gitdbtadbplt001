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

    select * from {{ ref('aspitg_integration__itg_ecc_marm') }}
   
),

final as (
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
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final