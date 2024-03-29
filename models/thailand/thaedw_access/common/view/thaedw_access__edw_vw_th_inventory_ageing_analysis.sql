with source as (
    select * from {{ ref('thaedw_integration__edw_vw_th_inventory_ageing_analysis') }}
),
final as (
    select
        typ as "typ",
        cntry_cd as "cntry_cd",
        cntry_nm as "cntry_nm",
        bill_date as "bill_date",
        dstrbtr_grp_cd as "dstrbtr_grp_cd",
        dstrbtr_nm as "dstrbtr_nm",
        dstrbtr_matl_num as "dstrbtr_matl_num",
        warehse_cd as "warehse_cd",
        warehse_grp as "warehse_grp",
        batch_expiry_date as "batch_expiry_date",
        root_code as "root_code",
        inv_qty as "inv_qty",
        inv_amt as "inv_amt",
        sls_prc2 as "sls_prc2",
        unit_per_case as "unit_per_case",
        cases as "cases",
        eaches as "eaches",
        sls_amt as "sls_amt",
        total_bht as "total_bht",
        "year" as "year",
        qrtr as "qrtr",
        mnth_id as "mnth_id",
        mnth_no as "mnth_no",
        wk as "wk",
        mnth_wk_no as "mnth_wk_no",
        cal_date as "cal_date",
        sku_description as "sku_description",
        franchise as "franchise",
        brand as "brand",
        variant as "variant",
        segment as "segment",
        put_up_description as "put_up_description",
        prod_sub_brand as "prod_sub_brand",
        prod_subsegment as "prod_subsegment",
        prod_category as "prod_category",
        prod_subcategory as "prod_subcategory",
        sap_cust_id as "sap_cust_id",
        sap_cust_nm as "sap_cust_nm",
        sap_sls_org as "sap_sls_org",
        sap_cmp_id as "sap_cmp_id",
        sap_cntry_cd as "sap_cntry_cd",
        sap_cntry_nm as "sap_cntry_nm",
        sap_addr as "sap_addr",
        sap_region as "sap_region",
        sap_state_cd as "sap_state_cd",
        sap_city as "sap_city",
        sap_post_cd as "sap_post_cd",
        sap_chnl_cd as "sap_chnl_cd",
        sap_chnl_desc as "sap_chnl_desc",
        sap_sls_office_cd as "sap_sls_office_cd",
        sap_sls_office_desc as "sap_sls_office_desc",
        sap_sls_grp_cd as "sap_sls_grp_cd",
        sap_sls_grp_desc as "sap_sls_grp_desc",
        sap_prnt_cust_key as "sap_prnt_cust_key",
        sap_prnt_cust_desc as "sap_prnt_cust_desc",
        sap_cust_chnl_key as "sap_cust_chnl_key",
        sap_cust_chnl_desc as "sap_cust_chnl_desc",
        sap_cust_sub_chnl_key as "sap_cust_sub_chnl_key",
        sap_sub_chnl_desc as "sap_sub_chnl_desc",
        sap_go_to_mdl_key as "sap_go_to_mdl_key",
        sap_go_to_mdl_desc as "sap_go_to_mdl_desc",
        sap_bnr_key as "sap_bnr_key",
        sap_bnr_desc as "sap_bnr_desc",
        sap_bnr_frmt_key as "sap_bnr_frmt_key",
        sap_bnr_frmt_desc as "sap_bnr_frmt_desc",
        retail_env as "retail_env"
    from source
)
select * from final