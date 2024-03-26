with wks_vietnam_siso_propagate_final as (

),
edw_material_dim as (

),
EDW_GCH_PRODUCTHIERARCHY as (

),
edw_material_sales_dim as (

),
final as (
    SELECT month,
    TRIM(NVL(NULLIF(T4.GPH_PROD_BRND, ''), 'NA')) AS BRAND,
    TRIM(NVL(NULLIF(T4.GPH_PROD_VRNT, ''), 'NA')) AS VARIANT,
    TRIM(NVL(NULLIF(T4.GPH_PROD_SGMNT, ''), 'NA')) AS SEGMENT,
    TRIM(NVL(NULLIF(T4.GPH_PROD_CTGRY, ''), 'NA')) AS PROD_CATEGORY,
    TRIM(NVL(NULLIF(T4.pka_size_desc, ''), 'NA')) AS pka_size_desc,
    TRIM(NVL(NULLIF(T4.pka_product_key, ''), 'NA')) AS pka_product_key,
    NVL(sap_parent_customer_key, 'NA') AS SAP_PARENT_CUSTOMER_KEY,
    sum(last_3months_so_value) as last_3months_so_val,
    sum(last_6months_so_value) as last_6months_so_val,
    sum(last_12months_so_value) as last_12months_so_val,
    sum(last_36months_so_value) as last_36months_so_val,
    CASE
        WHEN COALESCE(last_36months_so_val, 0) > 0
        and COALESCE(last_12months_so_val, 0) <= 0 THEN 'N'
        ELSE 'Y'
    END AS healthy_inventory
FROM wks_vietnam_siso_propagate_final SISO,
    (
        SELECT DISTINCT EMD.matl_num AS SAP_MATL_NUM,
            EMD.pka_product_key as pka_product_key,
            EMD.pka_size_desc as pka_size_desc,
            EGPH.GCPH_BRAND AS GPH_PROD_BRND,
            EGPH.GCPH_VARIANT AS GPH_PROD_VRNT,
            EGPH.GCPH_CATEGORY AS GPH_PROD_CTGRY,
            EGPH.GCPH_SEGMENT AS GPH_PROD_SGMNT
        FROM edw_material_dim EMD,
            EDW_GCH_PRODUCTHIERARCHY EGPH
        WHERE LTRIM(EMD.MATL_NUM, '0') = ltrim(EGPH.MATERIALNUMBER(+), 0)
            AND EMD.PROD_HIER_CD <> ''
            AND LTRIM(EMD.MATL_NUM, '0') IN (
                SELECT DISTINCT LTRIM(MATL_NUM, '0')
                FROM edw_material_sales_dim
                WHERE sls_org in ('260S')
            )
    ) T4
WHERE LTRIM(T4.SAP_MATL_NUM(+), '0') = SISO.matl_num
GROUP BY month,
    GPH_PROD_BRND,
    GPH_PROD_VRNT,
    GPH_PROD_SGMNT,
    GPH_PROD_CTGRY,
    pka_size_desc,
    pka_product_key,
    sap_parent_customer_key
)
select * from final