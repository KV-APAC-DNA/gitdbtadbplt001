with edw_ecc_marm as(
    select * from {{ ref('aspedw_integration__edw_ecc_marm') }}
),
final as (
    Select matl_no as "matl_no",
    alt_unt as "alt_unt",
    numrtr as "numrtr",
    denomtr as "denomtr",
    ean_nr as "ean_nr",
    ean_11 as "ean_11",
    ctgry as "ctgry",
    length as "length",
    width as "width",
    height as "height",
    unit as "unit",
    volum as "volum",
    vol_unt as "vol_unt",
    gross_wt as "gross_wt",
    wt_unt as "wt_unt",
    lowrlvl_unt as "lowrlvl_unt",
    intrnl_char as "intrnl_char",
    uom_srtno as "uom_srtno",
    leadng_unt as "leadng_unt",
    valutn_unt as "valutn_unt",
    unts_use as "unts_use",
    uom as "uom",
    lg_vrnt as "lg_vrnt",
    ean_vrnt as "ean_vrnt",
    remng_vol as "remng_vol",
    max_stack as "max_stack",
    capause as "capause",
    uom_ctgry as "uom_ctgry",
    crt_dttm as "crt_dttm",
    updt_dttm as "updt_dttm"
    From edw_ecc_marm
)

select * from final