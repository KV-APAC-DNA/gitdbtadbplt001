with source as (
    select * from {{ ref('phledw_integration__edw_ph_npi') }}
),
final as
(
    SELECT 
        nts_val AS "nts_val",
        pipeline AS "pipeline",
        sold_to AS "sold_to",
        sls_grp_desc AS "sls_grp_desc",
        subsource_type AS "subsource_type",
        brand AS "brand",
        trade_type AS "trade_type",
        parent_customer AS "parent_customer",
        dstrbtr_cust_cd AS "dstrbtr_cust_cd",
        jj_mnth_id AS "jj_mnth_id",
        sls_grping AS "sls_grping",
        sku_desc AS "sku_desc",
        peg_itemdesc AS "peg_itemdesc",
        chnl_desc AS "chnl_desc",
        peg_itemcode AS "peg_itemcode",
        sap_sls_office_desc AS "sap_sls_office_desc",
        sold_to_nm AS "sold_to_nm",
        parent_customer_cd AS "parent_customer_cd",
        sub_chnl_desc AS "sub_chnl_desc",
        sales_cycle AS "sales_cycle",
        account_name AS "account_name",
        sku AS "sku"
    FROM  source
)
select * from final