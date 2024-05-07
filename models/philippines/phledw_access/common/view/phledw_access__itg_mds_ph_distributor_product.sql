with source as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_distributor_product') }}
),
final as
(
    select 
        promo_reg_nm AS "promo_reg_nm",
        promo_strt_period AS "promo_strt_period",
        effective_to AS "effective_to",
        dstrbtr_item_cd AS "dstrbtr_item_cd",
        effective_from AS "effective_from",
        last_chg_datetime AS "last_chg_datetime",
        promo_end_period AS "promo_end_period",
        dstrbtr_item_nm AS "dstrbtr_item_nm",
        dstrbtr_grp_cd AS "dstrbtr_grp_cd",
        dstrbtr_grp_nm AS "dstrbtr_grp_nm",
        updt_dttm AS "updt_dttm",
        active AS "active",
        sap_item_nm AS "sap_item_nm",
        promo_reg_ind AS "promo_reg_ind",
        sap_item_cd AS "sap_item_cd",
        crtd_dttm AS "crtd_dttm"
    from source
)
select * from final