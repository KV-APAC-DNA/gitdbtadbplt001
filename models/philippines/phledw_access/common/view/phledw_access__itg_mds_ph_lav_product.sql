with source as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_lav_product') }}
),
final as
(
    select item_cd as "item_cd",
        item_nm as "item_nm",
        ims_otc_tag as "ims_otc_tag",
        ims_otc_tag_nm as "ims_otc_tag_nm",
        npi_strt_period as "npi_strt_period",
        price_lst_period as "price_lst_period",
        promo_strt_period as "promo_strt_period",
        promo_end_period as "promo_end_period",
        promo_reg_ind as "promo_reg_ind",
        promo_reg_nm as "promo_reg_nm",
        hero_sku_ind as "hero_sku_ind",
        hero_sku_nm as "hero_sku_nm",
        rpt_grp_1 as "rpt_grp_1",
        rpt_grp_1_desc as "rpt_grp_1_desc",
        rpt_grp_2 as "rpt_grp_2",
        rpt_grp_2_desc as "rpt_grp_2_desc",
        rpt_grp_3 as "rpt_grp_3",
        rpt_grp_3_desc as "rpt_grp_3_desc",
        rpt_grp_4 as "rpt_grp_4",
        rpt_grp_4_desc as "rpt_grp_4_desc",
        rpt_grp_5 as "rpt_grp_5",
        rpt_grp_5_desc as "rpt_grp_5_desc",
        scard_brand_cd as "scard_brand_cd",
        scard_brand_desc as "scard_brand_desc",
        scard_franchise_cd as "scard_franchise_cd",
        scard_franchise_desc as "scard_franchise_desc",
        scard_put_up_cd as "scard_put_up_cd",
        scard_put_up_desc as "scard_put_up_desc",
        scard_varient_cd as "scard_varient_cd",
        scard_varient_desc as "scard_varient_desc",
        last_chg_datetime as "last_chg_datetime",
        effective_from as "effective_from",
        effective_to as "effective_to",
        active as "active",
        crtd_dttm as "crtd_dttm",
        updt_dttm as "updt_dttm"
    from source
)
select * from final