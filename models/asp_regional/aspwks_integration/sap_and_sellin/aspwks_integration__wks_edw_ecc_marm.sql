{{
    config(
        alias="wks_edw_ecc_marm",
        tags=["daily"]
    )
}}

with 

source as (

    select * from {{ ref('aspitg_integration__itg_ecc_marm') }}
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
    -- null as tgt_crt_dttm,
    updt_dttm as updt_dttm
    --null as chng_flg
  from source
)

select * from final