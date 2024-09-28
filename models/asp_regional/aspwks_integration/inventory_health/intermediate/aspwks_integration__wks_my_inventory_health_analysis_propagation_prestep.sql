with wks_my_siso_propagate_final
as (
    select *
    from PROD_DNA_CORE.myswks_integration.wks_my_siso_propagate_final
    ),
edw_vw_os_customer_dim
as (
    select *
    from PROD_DNA_CORE.mysedw_integration.edw_vw_my_customer_dim
    ),
itg_my_dstrbtrr_dim
as (
    select *
    from PROD_DNA_CORE.mysitg_integration.itg_my_dstrbtrr_dim
    ),
vw_edw_reg_exch_rate
as (
    select *
    from PROD_DNA_CORE.aspedw_integration.vw_edw_reg_exch_rate
    ),
edw_my_siso_analysis
as (
    select *
    from PROD_DNA_CORE.mysedw_integration.edw_my_siso_analysis
    ),
edw_vw_my_si_pos_inv_analysis
as (
    select *
    from PROD_DNA_CORE.mysedw_integration.edw_vw_my_si_pos_inv_analysis
    ),
edw_material_dim
as (
    select *
    from PROD_DNA_CORE.aspedw_integration.edw_material_dim
    ),
edw_vw_os_time_dim
as (
    select *
    from PROD_DNA_CORE.sgpedw_integration.edw_vw_os_time_dim
    ),
ONSESEA
AS (
    SELECT CAST(TIME.YEAR AS VARCHAR(10)) AS YEAR,
        CAST(TIME.QRTR_NO AS VARCHAR(14)) AS QRTR_NO,
        CAST(TIME.MNTH_ID AS VARCHAR(21)) AS MONTH_YEAR,
        CAST(TIME.MNTH_NO AS VARCHAR(10)) AS mnth_no,
        CAST('Malaysia' AS VARCHAR) AS COUNTRY_NAME,
        TRIM(NVL(NULLIF(SISO.DSTRBTR_GRP_CD, ''), 'NA')) AS DSTRBTR_GRP_CD,
        TRIM(NVL(NULLIF(SISO.distributor, ''), 'NA')) AS distributor_id_name,
        TRIM(NVL(NULLIF(product.GLOBAL_PROD_FRANCHISE, ''), 'NA')) AS GLOBAL_PROD_FRANCHISE,
        TRIM(NVL(NULLIF(product.GLOBAL_PROD_BRAND, ''), 'NA')) AS GLOBAL_PROD_BRAND,
        TRIM(NVL(NULLIF(product.GLOBAL_PROD_SUB_BRAND, ''), 'NA')) AS GLOBAL_PROD_SUB_BRAND,
        TRIM(NVL(NULLIF(product.GLOBAL_PROD_VARIANT, ''), 'NA')) AS GLOBAL_PROD_VARIANT,
        TRIM(NVL(NULLIF(product.GLOBAL_PROD_SEGMENT, ''), 'NA')) AS GLOBAL_PROD_SEGMENT,
        TRIM(NVL(NULLIF(product.GLOBAL_PROD_SUBSEGMENT, ''), 'NA')) AS GLOBAL_PROD_SUBSEGMENT,
        TRIM(NVL(NULLIF(product.GLOBAL_PROD_CATEGORY, ''), 'NA')) AS GLOBAL_PROD_CATEGORY,
        TRIM(NVL(NULLIF(product.GLOBAL_PROD_SUBCATEGORY, ''), 'NA')) AS GLOBAL_PROD_SUBCATEGORY,
        --TRIM(NVL(NULLIF(product.GLOBAL_PUT_UP_DESC,''),'NA')) AS GLOBAL_PUT_UP_DESC,
        TRIM(NVL(NULLIF(product.pka_size_desc, ''), 'NA')) AS pka_size_desc,
        TRIM(NVL(NULLIF(product.SKU, ''), 'NA')) AS SKU_CD,
        TRIM(NVL(NULLIF(product.SKU_DESC, ''), 'NA')) AS SKU_DESCRIPTION,
        --TRIM(NVL(NULLIF(product.greenlight_sku_flag,''),'NA')) AS greenlight_sku_flag,
        TRIM(NVL(NULLIF(product.pka_product_key, ''), 'NA')) AS pka_product_key,
        TRIM(NVL(NULLIF(product.pka_product_key_description, ''), 'NA')) AS pka_product_key_description,
        TRIM(NVL(NULLIF(product.product_key, ''), 'NA')) AS product_key,
        TRIM(NVL(NULLIF(product.product_key_description, ''), 'NA')) AS product_key_description,
        CAST('MYR' AS VARCHAR) AS FROM_CCY,
        'USD' AS TO_CCY,
        C.EXCH_RATE,
        TRIM(NVL(NULLIF(cust.SAP_PRNT_CUST_KEY, ''), 'NA')) AS SAP_PRNT_CUST_KEY,
        TRIM(NVL(NULLIF(cust.SAP_PRNT_CUST_DESC, ''), 'NA')) AS SAP_PRNT_CUST_DESC,
        TRIM(NVL(NULLIF(cust.SAP_CUST_CHNL_KEY, ''), 'NA')) AS SAP_CUST_CHNL_KEY,
        TRIM(NVL(NULLIF(cust.SAP_CUST_CHNL_DESC, ''), 'NA')) AS SAP_CUST_CHNL_DESC,
        TRIM(NVL(NULLIF(cust.SAP_CUST_SUB_CHNL_KEY, ''), 'NA')) AS SAP_CUST_SUB_CHNL_KEY,
        TRIM(NVL(NULLIF(cust.SAP_SUB_CHNL_DESC, ''), 'NA')) AS SAP_SUB_CHNL_DESC,
        TRIM(NVL(NULLIF(cust.SAP_GO_TO_MDL_KEY, ''), 'NA')) AS SAP_GO_TO_MDL_KEY,
        TRIM(NVL(NULLIF(cust.SAP_GO_TO_MDL_DESC, ''), 'NA')) AS SAP_GO_TO_MDL_DESC,
        TRIM(NVL(NULLIF(cust.SAP_BNR_KEY, ''), 'NA')) AS SAP_BNR_KEY,
        TRIM(NVL(NULLIF(cust.SAP_BNR_DESC, ''), 'NA')) AS SAP_BNR_DESC,
        TRIM(NVL(NULLIF(cust.SAP_BNR_FRMT_KEY, ''), 'NA')) AS SAP_BNR_FRMT_KEY,
        TRIM(NVL(NULLIF(cust.SAP_BNR_FRMT_DESC, ''), 'NA')) AS SAP_BNR_FRMT_DESC,
        TRIM(NVL(NULLIF(cust.RETAIL_ENV, ''), 'NA')) AS RETAIL_ENV,
        CASE 
            WHEN SAP_PRNT_CUST_KEY = 'PC0004'
                THEN 'Not Applicable'
            ELSE TRIM(NVL(NULLIF(cust.REGION, ''), 'NA'))
            END AS REGION,
        CASE 
            WHEN SAP_PRNT_CUST_KEY = 'PC0004'
                THEN 'Not Applicable'
            ELSE TRIM(NVL(NULLIF(cust.ZONE_OR_AREA, ''), 'NA'))
            END AS ZONE_OR_AREA,
        SUM(last_3months_so) AS last_3months_so_qty,
        SUM(last_6months_so) AS last_6months_so_qty,
        SUM(last_12months_so) AS last_12months_so_qty,
        SUM(last_3months_so_value) AS last_3months_so_val,
        SUM(last_6months_so_value) AS last_6months_so_val,
        SUM(last_12months_so_value) AS last_12months_so_val,
        SUM(last_36months_so_value) AS last_36months_so_val,
        CAST(SUM(last_3months_so_value * c.Exch_rate) AS NUMERIC(38, 5)) AS last_3months_so_val_usd,
        CAST(SUM(last_6months_so_value * c.Exch_rate) AS NUMERIC(38, 5)) AS last_6months_so_val_usd,
        CAST(SUM(last_12months_so_value * c.Exch_rate) AS NUMERIC(38, 5)) AS last_12months_so_val_usd,
        propagate_flag,
        propagate_from,
        CASE 
            WHEN propagate_flag = 'N'
                THEN 'Not propagate'
            ELSE reason
            END AS reason,
        replicated_flag,
        SUM(sell_in_qty) SI_SLS_QTY,
        SUM(sell_in_value) SI_GTS_VAL,
        SUM(sell_in_value * C.EXCH_RATE) SI_GTS_VAL_USD,
        SUM(inv_qty) INVENTORY_QUANTITY,
        SUM(inv_value) INVENTORY_VAL,
        SUM(inv_value * C.EXCH_RATE) INVENTORY_VAL_USD,
        SUM(so_qty) SO_SLS_QTY,
        SUM(so_value) SO_GRS_TRD_SLS,
        ROUND(SUM(so_value * C.EXCH_RATE)) SO_GRS_TRD_SLS_USD
    FROM (
        SELECT *
        FROM wks_my_siso_propagate_final
        WHERE sap_parent_customer_desc != ''
        ) SISO,
        (
            SELECT T1.*,
                T2.REGION,
                T2.ZONE_OR_AREA
            FROM EDW_VW_OS_CUSTOMER_DIM T1,
                (
                    SELECT cust_id,
                        (lvl1 || '-' || region) AS region,
                        substring
                        (
                            replace(replace(lvl3, '(', '- '), ')', ''),
                            case
                            when position('-', replace(replace(lvl3, '(', '- '), ')', '')) + 1 = 1
                            then 999
                            else position('-', replace(replace(lvl3, '(', '- '), ')', '')) + 1
                            end
                        ) as zone_or_area 
                    FROM itg_my_dstrbtrr_dim
                    ) T2
            WHERE T1.SAP_CNTRY_CD = 'MY'
                AND LTRIM(T1.SAP_CUST_ID, '0') = T2.CUST_ID(+)
            ) cust,
        (
            SELECT *
            FROM vw_edw_reg_exch_rate
            WHERE cntry_key = 'MY'
                AND TO_CCY = 'USD'
                AND JJ_MNTH_ID = (
                    SELECT MAX(JJ_MNTH_ID)
                    FROM vw_edw_reg_exch_rate
                    )
            ) C,
        (
            SELECT *
            FROM (
                SELECT product.*,
                    --EMD.greenlight_sku_flag as greenlight_sku_flag,
                    EMD.pka_product_key AS pka_product_key,
                    EMD.pka_product_key_description AS pka_product_key_description,
                    EMD.pka_product_key AS product_key,
                    EMD.pka_product_key_description AS product_key_description,
                    EMD.pka_size_desc AS pka_size_desc,
                    row_number() OVER (
                        PARTITION BY sku ORDER BY sku
                        ) AS rnk
                FROM (
                    SELECT *
                    FROM (
                        SELECT DISTINCT GLOBAL_PROD_FRANCHISE,
                            GLOBAL_PROD_BRAND,
                            GLOBAL_PROD_SUB_BRAND,
                            GLOBAL_PROD_VARIANT,
                            GLOBAL_PROD_SEGMENT,
                            GLOBAL_PROD_SUBSEGMENT,
                            GLOBAL_PROD_CATEGORY,
                            GLOBAL_PROD_SUBCATEGORY,
                            GLOBAL_PUT_UP_DESC,
                            nvl(NULLIF(sku, ''), 'NA') AS sku,
                            SKU_DESC
                        FROM EDW_MY_SISO_ANALYSIS
                        WHERE TO_CCY = 'MYR'
                        
                        UNION
                        
                        SELECT DISTINCT GLOBAL_PROD_FRANCHISE,
                            GLOBAL_PROD_BRAND,
                            GLOBAL_PROD_SUB_BRAND,
                            GLOBAL_PROD_VARIANT,
                            GLOBAL_PROD_SEGMENT,
                            GLOBAL_PROD_SUBSEGMENT,
                            GLOBAL_PROD_CATEGORY,
                            GLOBAL_PROD_SUBCATEGORY,
                            GLOBAL_PUT_UP_DESC,
                            nvl(NULLIF(sku, ''), 'NA') AS sku,
                            SKU_DESC
                        FROM EDW_VW_MY_SI_POS_INV_ANALYSIS
                        )
                    WHERE sku != 'NA'
                    ) product
                LEFT JOIN
                    --(Select * from rg_edw.edw_vw_greenlight_skus where sls_org='2100') EMD
                    (
                    SELECT *
                    FROM edw_material_dim
                    ) EMD ON product.sku = LTRIM(EMD.MATL_NUM, '0')
                )
            WHERE rnk = 1
            ) product,
        (
            SELECT DISTINCT cal_year AS YEAR,
                cal_qrtr_no AS qrtr_no,
                cal_MNTH_ID AS mnth_id,
                cal_MNTH_NO AS mnth_no
            FROM EDW_VW_OS_TIME_DIM
            ) TIME
    -- (SELECT * from CUST where rank=1)customer
    WHERE LTRIM(SISO.DSTRBTR_GRP_CD, '0') = LTRIM(cust.SAP_CUST_ID(+), '0')
        AND LEFT(SISO.month, 4) >= (DATE_PART(YEAR, sysdate()) - 2)
        AND SISO.month = TIME.MNTH_ID
        AND SISO.matl_num = product.sku(+)
    GROUP BY TIME.YEAR,
        TIME.QRTR_NO,
        TIME.MNTH_ID,
        TIME.MNTH_NO, --MNTH_WK_NO,
        CNTRY_NM,
        DSTRBTR_GRP_CD,
        distributor,
        GLOBAL_PROD_FRANCHISE,
        GLOBAL_PROD_BRAND,
        GLOBAL_PROD_SUB_BRAND,
        GLOBAL_PROD_VARIANT,
        GLOBAL_PROD_SEGMENT,
        GLOBAL_PROD_SUBSEGMENT,
        GLOBAL_PROD_CATEGORY,
        GLOBAL_PROD_SUBCATEGORY,
        --GLOBAL_PUT_UP_DESC,
        pka_size_desc,
        SKU,
        SKU_DESC,
        --greenlight_sku_flag,
        pka_product_key,
        pka_product_key_description,
        product_key,
        product_key_description,
        C.EXCH_RATE,
        SAP_PRNT_CUST_KEY,
        SAP_PRNT_CUST_DESC,
        SAP_CUST_CHNL_KEY,
        SAP_CUST_CHNL_DESC,
        SAP_CUST_SUB_CHNL_KEY,
        SAP_SUB_CHNL_DESC,
        SAP_GO_TO_MDL_KEY,
        SAP_GO_TO_MDL_DESC,
        SAP_BNR_KEY,
        SAP_BNR_DESC,
        SAP_BNR_FRMT_KEY,
        SAP_BNR_FRMT_DESC,
        RETAIL_ENV,
        CASE 
            WHEN SAP_PRNT_CUST_KEY = 'PC0004'
                THEN 'Not Applicable'
            ELSE TRIM(NVL(NULLIF(cust.REGION, ''), 'NA'))
            END,
        CASE 
            WHEN SAP_PRNT_CUST_KEY = 'PC0004'
                THEN 'Not Applicable'
            ELSE TRIM(NVL(NULLIF(cust.ZONE_OR_AREA, ''), 'NA'))
            END,
        propagate_flag,
        propagate_from,
        reason,
        replicated_flag
    ),
Regional
AS (
    SELECT *,
        SUM(SI_GTS_VAL) OVER (
            PARTITION BY COUNTRY_NAME,
            YEAR,
            MONTH_YEAR
            ) AS SI_INV_DB_VAL,
        SUM(SI_GTS_VAL_USD) OVER (
            PARTITION BY COUNTRY_NAME,
            YEAR,
            MONTH_YEAR
            ) AS SI_INV_DB_VAL_USD
    FROM ONSESEA
    WHERE COUNTRY_NAME || SAP_PRNT_CUST_DESC IN (
            SELECT COUNTRY_NAME || SAP_PRNT_CUST_DESC AS INCLUSION
            FROM (
                SELECT COUNTRY_NAME,
                    SAP_PRNT_CUST_DESC,
                    NVL(SUM(INVENTORY_VAL), 0) AS INV_VAL,
                    NVL(SUM(SO_GRS_TRD_SLS), 0) AS Sellout_val
                FROM ONSESEA
                WHERE SAP_PRNT_CUST_DESC IS NOT NULL
                GROUP BY COUNTRY_NAME,
                    SAP_PRNT_CUST_DESC
                HAVING INV_VAL <> 0
                )
            )
    ),
final
AS (
    SELECT cast(year AS INTEGER) AS year,
        qrtr_no AS year_quarter,
        month_year,
        cast(mnth_no AS INTEGER) AS month_number,
        country_name,
        dstrbtr_grp_cd,
        distributor_id_name,
        global_prod_franchise AS franchise,
        global_prod_brand AS brand,
        global_prod_sub_brand AS prod_sub_brand,
        global_prod_variant AS variant,
        global_prod_segment AS segment,
        global_prod_subsegment AS prod_subsegment,
        global_prod_category AS prod_category,
        global_prod_subcategory AS prod_subcategory, --global_put_up_desc as put_up_description
        pka_size_desc AS put_up_description,
        sku_cd,
        sku_description,
        --greenlight_sku_flag,
        pka_product_key,
        pka_product_key_description,
        product_key,
        product_key_description,
        from_ccy,
        to_ccy,
        exch_rate,
        sap_prnt_cust_key,
        sap_prnt_cust_desc,
        sap_cust_chnl_key,
        sap_cust_chnl_desc,
        sap_cust_sub_chnl_key,
        sap_sub_chnl_desc,
        sap_go_to_mdl_key,
        sap_go_to_mdl_desc,
        sap_bnr_key,
        sap_bnr_desc,
        sap_bnr_frmt_key,
        sap_bnr_frmt_desc,
        retail_env,
        region,
        zone_or_area,
        round(cast(si_sls_qty AS NUMERIC(38, 5)), 5) AS si_sls_qty,
        round(cast(si_gts_val AS NUMERIC(38, 5)), 5) AS si_gts_val,
        round(cast(si_gts_val_usd AS NUMERIC(38, 5)), 5) AS si_gts_val_usd,
        round(cast(inventory_quantity AS NUMERIC(38, 5)), 5) AS inventory_quantity,
        round(cast(inventory_val AS NUMERIC(38, 5)), 5) AS inventory_val,
        round(cast(inventory_val_usd AS NUMERIC(38, 5)), 5) AS inventory_val_usd,
        round(cast(so_sls_qty AS NUMERIC(38, 5)), 5) AS so_sls_qty,
        round(cast(so_grs_trd_sls AS NUMERIC(38, 5)), 5) AS so_grs_trd_sls,
        so_grs_trd_sls_usd AS so_grs_trd_sls_usd,
        last_3months_so_qty,
        last_6months_so_qty,
        last_12months_so_qty,
        last_3months_so_val,
        last_3months_so_val_usd,
        last_6months_so_val,
        last_6months_so_val_usd,
        last_12months_so_val,
        last_12months_so_val_usd,
        propagate_flag,
        propagate_from,
        reason,
        last_36months_so_val
    FROM Regional
    )
SELECT *
FROM final