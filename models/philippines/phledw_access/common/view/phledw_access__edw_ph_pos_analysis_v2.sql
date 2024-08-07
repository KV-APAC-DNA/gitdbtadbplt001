with source as (
    select * from {{ ref('phledw_integration__edw_ph_pos_analysis_v2') }}
),
final as
(
    select jj_year as "jj_year",
        jj_qtr as "jj_qtr",
        jj_mnth_id as "jj_mnth_id",
        jj_mnth_no as "jj_mnth_no",
        cntry_nm as "cntry_nm",
        cust_cd as "cust_cd",
        cust_brnch_cd as "cust_brnch_cd",
        mt_cust_brnch_nm as "mt_cust_brnch_nm",
        region_cd as "region_cd",
        region_nm as "region_nm",
        prov_cd as "prov_cd",
        prov_nm as "prov_nm",
        mncplty_cd as "mncplty_cd",
        mncplty_nm as "mncplty_nm",
        city_cd as "city_cd",
        city_nm as "city_nm",
        ae_nm as "ae_nm",
        ash_no as "ash_no",
        ash_nm as "ash_nm",
        pms_nm as "pms_nm",
        item_cd as "item_cd",
        mt_item_nm as "mt_item_nm",
        sold_to as "sold_to",
        sold_to_nm as "sold_to_nm",
        region as "region",
        chnl_cd as "chnl_cd",
        chnl_desc as "chnl_desc",
        sub_chnl_cd as "sub_chnl_cd",
        sub_chnl_desc as "sub_chnl_desc",
        parent_customer_cd as "parent_customer_cd",
        parent_customer as "parent_customer",
        account_grp as "account_grp",
        trade_type as "trade_type",
        sls_grp_desc as "sls_grp_desc",
        sap_state_cd as "sap_state_cd",
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
        sku as "sku",
        sku_desc as "sku_desc",
        sap_mat_type_cd as "sap_mat_type_cd",
        sap_mat_type_desc as "sap_mat_type_desc",
        sap_base_uom_cd as "sap_base_uom_cd",
        sap_prchse_uom_cd as "sap_prchse_uom_cd",
        sap_prod_sgmt_cd as "sap_prod_sgmt_cd",
        sap_prod_sgmt_desc as "sap_prod_sgmt_desc",
        sap_base_prod_cd as "sap_base_prod_cd",
        sap_base_prod_desc as "sap_base_prod_desc",
        sap_mega_brnd_cd as "sap_mega_brnd_cd",
        sap_mega_brnd_desc as "sap_mega_brnd_desc",
        sap_brnd_cd as "sap_brnd_cd",
        sap_brnd_desc as "sap_brnd_desc",
        sap_vrnt_cd as "sap_vrnt_cd",
        sap_vrnt_desc as "sap_vrnt_desc",
        sap_put_up_cd as "sap_put_up_cd",
        sap_put_up_desc as "sap_put_up_desc",
        sap_grp_frnchse_cd as "sap_grp_frnchse_cd",
        sap_grp_frnchse_desc as "sap_grp_frnchse_desc",
        sap_frnchse_cd as "sap_frnchse_cd",
        sap_frnchse_desc as "sap_frnchse_desc",
        sap_prod_frnchse_cd as "sap_prod_frnchse_cd",
        sap_prod_frnchse_desc as "sap_prod_frnchse_desc",
        sap_prod_mjr_cd as "sap_prod_mjr_cd",
        sap_prod_mjr_desc as "sap_prod_mjr_desc",
        sap_prod_mnr_cd as "sap_prod_mnr_cd",
        sap_prod_mnr_desc as "sap_prod_mnr_desc",
        sap_prod_hier_cd as "sap_prod_hier_cd",
        sap_prod_hier_desc as "sap_prod_hier_desc",
        global_mat_region as "global_mat_region",
        global_prod_franchise as "global_prod_franchise",
        global_prod_brand as "global_prod_brand",
        global_prod_variant as "global_prod_variant",
        global_prod_put_up_cd as "global_prod_put_up_cd",
        global_put_up_desc as "global_put_up_desc",
        global_prod_sub_brand as "global_prod_sub_brand",
        global_prod_need_state as "global_prod_need_state",
        global_prod_category as "global_prod_category",
        global_prod_subcategory as "global_prod_subcategory",
        global_prod_segment as "global_prod_segment",
        global_prod_subsegment as "global_prod_subsegment",
        global_prod_size as "global_prod_size",
        global_prod_size_uom as "global_prod_size_uom",
        is_reg as "is_reg",
        is_promo as "is_promo",
        local_mat_promo_strt_period as "local_mat_promo_strt_period",
        is_npi as "is_npi",
        is_hero as "is_hero",
        is_mcl as "is_mcl",
        local_mat_npi_strt_period as "local_mat_npi_strt_period",
        pos_qty as "pos_qty",
        pos_gts as "pos_gts",
        pos_item_prc as "pos_item_prc",
        pos_tax as "pos_tax",
        pos_nts as "pos_nts",
        conv_factor as "conv_factor",
        jj_qty_pc as "jj_qty_pc",
        jj_item_prc_per_pc as "jj_item_prc_per_pc",
        jj_gts as "jj_gts",
        jj_vat_amt as "jj_vat_amt",
        jj_nts as "jj_nts"
    from source
)
select * from final