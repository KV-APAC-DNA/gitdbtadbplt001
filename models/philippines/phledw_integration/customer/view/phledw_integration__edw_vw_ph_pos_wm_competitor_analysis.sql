with 
itg_ph_pos_wm_competitor as
(
    select * from {{ ref('phlitg_integration__itg_ph_pos_wm_competitor') }}
),
final as
(
    SELECT 
    itg_ph_pos_wm_competitor.jj_mnth_id,
    itg_ph_pos_wm_competitor.brnch_cd,
    itg_ph_pos_wm_competitor.brnch_nm,
    itg_ph_pos_wm_competitor.brand_ctgry_cd,
    itg_ph_pos_wm_competitor.brand_ctgry_nm,
    itg_ph_pos_wm_competitor.vendor_cd,
    itg_ph_pos_wm_competitor.vendor_nm,
    itg_ph_pos_wm_competitor.item_cd,
    itg_ph_pos_wm_competitor.item_nm,
    itg_ph_pos_wm_competitor.pos_qty,
    itg_ph_pos_wm_competitor.pos_gts
FROM itg_ph_pos_wm_competitor
)
select * from final