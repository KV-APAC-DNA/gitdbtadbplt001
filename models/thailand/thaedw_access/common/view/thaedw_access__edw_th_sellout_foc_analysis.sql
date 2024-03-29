with source as (
    select * from {{ ref('thaedw_integration__edw_th_sellout_foc_analysis') }}
),
final as (
    select
        segment as "segment",
        distributor_id as "distributor_id",
        ar_type_code as "ar_type_code",
        sap_sls_office_cd as "sap_sls_office_cd",
        prom_cd3 as "prom_cd3",
        retail_env as "retail_env",
        salesman_name as "salesman_name",
        sap_prnt_cust_desc as "sap_prnt_cust_desc",
        sap_bnr_frmt_desc as "sap_bnr_frmt_desc",
        sku_code as "sku_code",
        bar_code as "bar_code",
        sap_state_cd as "sap_state_cd",
        sap_cust_id as "sap_cust_id",
        sap_cust_sub_chnl_key as "sap_cust_sub_chnl_key",
        credit_note_amount as "credit_note_amount",
        cn_reason_description as "cn_reason_description",
        line_discount as "line_discount",
        sap_city as "sap_city",
        quantity_dz as "quantity_dz",
        distributor_name as "distributor_name",
        year as "year",
        order_date as "order_date",
        return_quantity as "return_quantity",
        sap_go_to_mdl_key as "sap_go_to_mdl_key",
        iscancel as "iscancel",
        sap_sls_grp_cd as "sap_sls_grp_cd",
        tot_bf_discount as "tot_bf_discount",
        sales_group as "sales_group",
        sku_description as "sku_description",
        country_name as "country_name",
        grp_cd as "grp_cd",
        city as "city",
        sap_bnr_frmt_key as "sap_bnr_frmt_key",
        year_quarter as "year_quarter",
        region_desc as "region_desc",
        net_invoice as "net_invoice",
        brand as "brand",
        sap_chnl_cd as "sap_chnl_cd",
        sap_region as "sap_region",
        year_week_number as "year_week_number",
        prod_category as "prod_category",
        prod_subcategory as "prod_subcategory",
        sales_office_name as "sales_office_name",
        sap_bnr_key as "sap_bnr_key",
        target_sales as "target_sales",
        sap_go_to_mdl_desc as "sap_go_to_mdl_desc",
        sap_cust_chnl_key as "sap_cust_chnl_key",
        prod_subsegment as "prod_subsegment",
        sap_post_cd as "sap_post_cd",
        cn_damaged_goods as "cn_damaged_goods",
        prod_sub_brand as "prod_sub_brand",
        variant as "variant",
        channel as "channel",
        channel_code as "channel_code",
        cluster as "cluster",
        sap_sls_office_desc as "sap_sls_office_desc",
        salesman_code as "salesman_code",
        ar_type_name as "ar_type_name",
        cn_reason_code as "cn_reason_code",
        franchise as "franchise",
        sap_sub_chnl_desc as "sap_sub_chnl_desc",
        country_code as "country_code",
        sap_chnl_desc as "sap_chnl_desc",
        put_up_description as "put_up_description",
        bottom_line_discount as "bottom_line_discount",
        sap_cust_nm as "sap_cust_nm",
        sap_cmp_id as "sap_cmp_id",
        sap_cust_chnl_desc as "sap_cust_chnl_desc",
        prom_cd2 as "prom_cd2",
        ar_name as "ar_name",
        prom_cd1 as "prom_cd1",
        sap_sls_grp_desc as "sap_sls_grp_desc",
        sales_office_code as "sales_office_code",
        order_no as "order_no",
        month_week_number as "month_week_number",
        sap_prnt_cust_key as "sap_prnt_cust_key",
        gross_trade_sales as "gross_trade_sales",
        sales_quantity as "sales_quantity",
        ar_code as "ar_code",
        sap_cntry_nm as "sap_cntry_nm",
        sap_addr as "sap_addr",
        month_number as "month_number",
        sap_sls_org as "sap_sls_org",
        sap_cntry_cd as "sap_cntry_cd",
        sap_bnr_desc as "sap_bnr_desc",
        price as "price",
        district as "district",
        target_calls as "target_calls",
        month_year as "month_year"
    from source
)
select * from final