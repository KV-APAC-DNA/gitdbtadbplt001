with itg_vn_dms_sales_stock_fact as(
    select * from {{ ref('vnmitg_integration__itg_vn_dms_sales_stock_fact') }} 
),
itg_vn_dms_product_dim as(
    select * from {{ ref('vnmitg_integration__itg_vn_dms_product_dim') }} 
),
transformed as(
    select 
        itg_vn_dms_sales_stock_fact.cntry_code AS cntry_cd
        ,'Vietnam' AS cntry_nm
        ,itg_vn_dms_sales_stock_fact.wh_code AS warehse_cd
        ,itg_vn_dms_sales_stock_fact.dstrbtr_id AS dstrbtr_grp_cd
        ,NULL AS dstrbtr_soldto_code
        ,itg_vn_dms_sales_stock_fact.material_code AS dstrbtr_matl_num
        ,prod.productcodesap AS sap_matl_num
        ,NULL AS bar_cd
        ,itg_vn_dms_sales_stock_fact.DATE AS inv_dt
        ,itg_vn_dms_sales_stock_fact.quantity AS soh
        ,itg_vn_dms_sales_stock_fact.amount AS soh_val
        ,itg_vn_dms_sales_stock_fact.amount AS jj_soh_val
        ,0 AS beg_stock_qty
        ,itg_vn_dms_sales_stock_fact.quantity AS end_stock_qty
        ,0 AS beg_stock_val
        ,itg_vn_dms_sales_stock_fact.amount AS end_stock_val
        ,0 AS jj_beg_stock_qty
        ,0 AS jj_end_stock_qty
        ,0 AS jj_beg_stock_val
        ,0 AS jj_end_stock_val
    from (
        itg_vn_dms_sales_stock_fact LEFT JOIN itg_vn_dms_product_dim prod 
        ON ((((itg_vn_dms_sales_stock_fact.material_code)::TEXT = (prod.product_code)::TEXT)))
        )
)
select * from transformed
