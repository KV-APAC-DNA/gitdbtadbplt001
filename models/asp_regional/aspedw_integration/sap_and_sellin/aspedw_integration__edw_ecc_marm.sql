{{
    config(
        alias="edw_ecc_marm",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["matl_no","alt_unt"],
        merge_exclude_columns = ["crt_dttm"],
        tags=["daily"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_edw_ecc_marm') }}
),

final as (
    select
    matl_no,
    alt_unt,
    numrtr,
    denomtr,
    ean_nr,
    ean_11,
    ctgry,
    length,
    width,
    height,
    unit,
    volum,
    vol_unt,
    gross_wt,
    wt_unt,
    lowrlvl_unt,
    intrnl_char,
    uom_srtno,
    leadng_unt,
    valutn_unt,
    unts_use,
    uom,
    lg_vrnt,
    ean_vrnt,
    remng_vol,
    max_stack,
    capause,
    uom_ctgry,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final