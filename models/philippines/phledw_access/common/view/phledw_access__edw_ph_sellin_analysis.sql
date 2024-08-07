with source as (
    select * from {{ ref('phledw_integration__edw_ph_sellin_analysis') }}
),
final as
(
    select jj_year as "jj_year",
        jj_qtr as "jj_qtr",
        jj_mnth_id as "jj_mnth_id",
        jj_mnth_no as "jj_mnth_no",
        jj_wk as "jj_wk",
        jj_mnth_wk_no as "jj_mnth_wk_no",
        sellin_bill_type as "sellin_bill_type",
        sls_grp_cd as "sls_grp_cd",
        sls_grp_desc as "sls_grp_desc",
        cntry_nm as "cntry_nm",
        region as "region",
        sap_state_cd as "sap_state_cd",
        sold_to as "sold_to",
        sold_to_nm as "sold_to_nm",
        sap_sls_org as "sap_sls_org",
        sap_cmp_id as "sap_cmp_id",
        sap_cntry_cd as "sap_cntry_cd",
        sap_cntry_nm as "sap_cntry_nm",
        sap_addr as "sap_addr",
        sap_region as "sap_region",
        sap_city as "sap_city",
        sap_post_cd as "sap_post_cd",
        sap_chnl_cd as "sap_chnl_cd",
        sap_chnl_desc as "sap_chnl_desc",
        sap_sls_office_cd as "sap_sls_office_cd",
        sap_sls_office_desc as "sap_sls_office_desc",
        sap_sls_grp_cd as "sap_sls_grp_cd",
        sap_sls_grp_desc as "sap_sls_grp_desc",
        sap_curr_cd as "sap_curr_cd",
        gch_region as "gch_region",
        gch_cluster as "gch_cluster",
        gch_subcluster as "gch_subcluster",
        gch_market as "gch_market",
        gch_retail_banner as "gch_retail_banner",
        chnl_cd as "chnl_cd",
        chnl_desc as "chnl_desc",
        sub_chnl_cd as "sub_chnl_cd",
        sub_chnl_desc as "sub_chnl_desc",
        sku as "sku",
        sku_desc as "sku_desc",
        global_mat_region as "global_mat_region",
        global_prod_franchise as "global_prod_franchise",
        global_prod_brand as "global_prod_brand",
        global_prod_variant as "global_prod_variant",
        global_prod_put_up_cd as "global_prod_put_up_cd",
        global_put_up_desc as "global_put_up_desc",
        sap_prod_mnr_cd as "sap_prod_mnr_cd",
        sap_prod_mnr_desc as "sap_prod_mnr_desc",
        sap_prod_hier_cd as "sap_prod_hier_cd",
        sap_prod_hier_desc as "sap_prod_hier_desc",
        global_prod_sub_brand as "global_prod_sub_brand",
        global_prod_need_state as "global_prod_need_state",
        global_prod_category as "global_prod_category",
        global_prod_subcategory as "global_prod_subcategory",
        global_prod_segment as "global_prod_segment",
        global_prod_subsegment as "global_prod_subsegment",
        global_prod_size as "global_prod_size",
        global_prod_size_uom as "global_prod_size_uom",
        parent_customer_cd as "parent_customer_cd",
        parent_customer as "parent_customer",
        account_grp as "account_grp",
        trade_type as "trade_type",
        is_reg as "is_reg",
        is_promo as "is_promo",
        local_mat_promo_strt_period as "local_mat_promo_strt_period",
        is_npi as "is_npi",
        is_hero as "is_hero",
        is_mcl as "is_mcl",
        local_mat_npi_strt_period as "local_mat_npi_strt_period",
        local_mat_listpriceunit as "local_mat_listpriceunit",
        sls_qty as "sls_qty",
        ret_qty as "ret_qty",
        sls_less_rtn_qty as "sls_less_rtn_qty",
        gts_val as "gts_val",
        ret_val as "ret_val",
        gts_less_rtn_val as "gts_less_rtn_val",
        nts_qty as "nts_qty",
        nts_val as "nts_val",
        tp_val as "tp_val",
        target_type as "target_type",
        iop_trgt_val as "iop_trgt_val",
        bp_trgt_val as "bp_trgt_val",
        le_trgt_val as "le_trgt_val",
        last_updt_dt as "last_updt_dt",
        iop_gts_trgt_val as "iop_gts_trgt_val",
        pka_productkey as "pka_productkey"
    from source
)
select * from final