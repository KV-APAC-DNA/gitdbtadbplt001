with source as (
    select * from {{ ref('phledw_integration__edw_vw_ph_pos_wm_competitor_analysis') }}
),
final as
(
    select 
        jj_mnth_id as "jj_mnth_id",
        brnch_cd as "brnch_cd",
        brnch_nm as "brnch_nm",
        brand_ctgry_cd as "brand_ctgry_cd",
        brand_ctgry_nm as "brand_ctgry_nm",
        vendor_cd as "vendor_cd",
        vendor_nm as "vendor_nm",
        item_cd as "item_cd",
        item_nm as "item_nm",
        pos_qty as "pos_qty",
        pos_gts as "pos_gts"
    from source
)
select * from final