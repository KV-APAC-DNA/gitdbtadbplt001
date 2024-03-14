with source as (
    select * from {{ ref('mysitg_integration__itg_my_sellout_sales_fact') }}
),
final as (
    select
        dstrbtr_id as "dstrbtr_id",
        sls_ord_num as "sls_ord_num",
        sls_ord_dt as "sls_ord_dt",
        type as "type",
        cust_cd as "cust_cd",
        dstrbtr_wh_id as "dstrbtr_wh_id",
        item_cd as "item_cd",
        dstrbtr_prod_cd as "dstrbtr_prod_cd",
        ean_num as "ean_num",
        dstrbtr_prod_desc as "dstrbtr_prod_desc",
        grs_prc as "grs_prc",
        qty as "qty",
        uom as "uom",
        qty_pc as "qty_pc",
        qty_aft_conv as "qty_aft_conv",
        subtotal_1 as "subtotal_1",
        discount as "discount",
        subtotal_2 as "subtotal_2",
        bottom_line_dscnt as "bottom_line_dscnt",
        total_amt_aft_tax as "total_amt_aft_tax",
        total_amt_bfr_tax as "total_amt_bfr_tax",
        sls_emp as "sls_emp",
        custom_field1 as "custom_field1",
        custom_field2 as "custom_field2",
        sap_matl_num as "sap_matl_num",
        filename as "filename",
        cdl_dttm as "cdl_dttm",
        crtd_dttm as "crtd_dttm",
        updt_dttm as "updt_dttm"
    from source
)
select * from final