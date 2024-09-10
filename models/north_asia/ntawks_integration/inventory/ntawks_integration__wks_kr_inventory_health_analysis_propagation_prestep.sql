with 
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_korea_siso_propagate_final as 
(
    select * from {{ ref('ntawks_integration__wks_korea_siso_propagate_final') }}
),
edw_customer_dim as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
vw_edw_reg_exch_rate as 
(
    select * from {{ ref('aspedw_integration__vw_edw_reg_exch_rate') }}
),
edw_vw_greenlight_skus as 
(
    select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
edw_gch_producthierarchy as 
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_material_sales_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_gch_customerhierarchy as 
(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
edw_customer_sales_dim as 
(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_customer_base_dim as 
(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_company_dim as 
(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_dstrbtn_chnl as 
(
    select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
edw_sales_org_dim as 
(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_code_descriptions as 
(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_subchnl_retail_env_mapping as 
(
    select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),
edw_copa_trans_fact as 
(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_billing_fact as 
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
edw_material_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
final as (
    Select *
    from (
            with Regional as (
                WITH ONSESEA AS (
                    --KR --
                    WITH cal AS (
                        SELECT DISTINCT "year" as CAL_YEAR,
                            QRTR_NO as cal_qrtr_no,
                            MNTH_ID as cal_mnth_id,
                            MNTH_NO as cal_mnth_no
                        FROM EDW_VW_OS_TIME_DIM
                    ),
                    CURRENCY AS (
                        Select *
                        from vw_edw_reg_exch_rate
                        where cntry_key = 'KR'
                    ),
                    PRODUCT AS (
                        Select *
                        from (
                                Select distinct EMD.matl_num AS SAP_MATL_NUM,
                                    EMD.MATL_DESC AS SAP_MAT_DESC,
                                    EMD.MATL_TYPE_CD AS SAP_MAT_TYPE_CD,
                                    EMD.MATL_TYPE_DESC AS SAP_MAT_TYPE_DESC,
                                    -- EMD.SAP_BASE_UOM_CD AS SAP_BASE_UOM_CD,
                                    -- EMD.SAP_PRCHSE_UOM_CD AS SAP_PRCHSE_UOM_CD,
                                    EMD.PRODH1 AS SAP_PROD_SGMT_CD,
                                    EMD.PRODH1_TXTMD AS SAP_PROD_SGMT_DESC,
                                    --  EMD.SAP_BASE_PROD_CD AS SAP_BASE_PROD_CD,
                                    EMD.BASE_PROD_DESC AS SAP_BASE_PROD_DESC,
                                    --  EMD.SAP_MEGA_BRND_CD AS SAP_MEGA_BRND_CD,
                                    EMD.MEGA_BRND_DESC AS SAP_MEGA_BRND_DESC,
                                    --  EMD.SAP_BRND_CD AS SAP_BRND_CD,
                                    EMD.BRND_DESC AS SAP_BRND_DESC,
                                    --  EMD.SAP_VRNT_CD AS SAP_VRNT_CD,
                                    EMD.VARNT_DESC AS SAP_VRNT_DESC,
                                    --  EMD.SAP_PUT_UP_CD AS SAP_PUT_UP_CD,
                                    EMD.PUT_UP_DESC AS SAP_PUT_UP_DESC,
                                    EMD.PRODH2 AS SAP_GRP_FRNCHSE_CD,
                                    EMD.PRODH2_TXTMD AS SAP_GRP_FRNCHSE_DESC,
                                    EMD.PRODH3 AS SAP_FRNCHSE_CD,
                                    EMD.PRODH3_TXTMD AS SAP_FRNCHSE_DESC,
                                    EMD.PRODH4 AS SAP_PROD_FRNCHSE_CD,
                                    EMD.PRODH4_TXTMD AS SAP_PROD_FRNCHSE_DESC,
                                    EMD.PRODH5 AS SAP_PROD_MJR_CD,
                                    EMD.PRODH5_TXTMD AS SAP_PROD_MJR_DESC,
                                    EMD.PRODH5 AS SAP_PROD_MNR_CD,
                                    EMD.PRODH5_TXTMD AS SAP_PROD_MNR_DESC,
                                    EMD.PRODH6 AS SAP_PROD_HIER_CD,
                                    EMD.PRODH6_TXTMD AS SAP_PROD_HIER_DESC,
                                    -- EMD.greenlight_sku_flag as greenlight_sku_flag,
                                    EMD.pka_product_key as pka_product_key,
                                    EMD.pka_size_desc as pka_size_desc,
                                    EMD.pka_product_key_description as pka_product_key_description,
                                    EMD.pka_product_key as product_key,
                                    EMD.pka_product_key_description as product_key_description,
                                    EGPH."region"  AS GPH_REGION,
                                    EGPH.regional_franchise AS GPH_REG_FRNCHSE,
                                    EGPH.regional_franchise_group AS GPH_REG_FRNCHSE_GRP,
                                    EGPH.GCPH_FRANCHISE AS GPH_PROD_FRNCHSE,
                                    EGPH.GCPH_BRAND AS GPH_PROD_BRND,
                                    EGPH.GCPH_SUBBRAND AS GPH_PROD_SUB_BRND,
                                    EGPH.GCPH_VARIANT AS GPH_PROD_VRNT,
                                    EGPH.GCPH_NEEDSTATE AS GPH_PROD_NEEDSTATE,
                                    EGPH.GCPH_CATEGORY AS GPH_PROD_CTGRY,
                                    EGPH.GCPH_SUBCATEGORY AS GPH_PROD_SUBCTGRY,
                                    EGPH.GCPH_SEGMENT AS GPH_PROD_SGMNT,
                                    EGPH.GCPH_SUBSEGMENT AS GPH_PROD_SUBSGMNT,
                                    EGPH.PUT_UP_CODE AS GPH_PROD_PUT_UP_CD,
                                    EGPH.PUT_UP_DESCRIPTION AS GPH_PROD_PUT_UP_DESC,
                                    EGPH.SIZE AS GPH_PROD_SIZE,
                                    EGPH.UNIT_OF_MEASURE AS GPH_PROD_SIZE_UOM,
                                    row_number() over(
                                        partition by sap_matl_num
                                        order by sap_matl_num
                                    ) rnk
                                FROM --(Select * from  edw_vw_greenlight_skus where  sls_org in ('3200','320A','320S','321A') ) EMD
                                    (
                                        Select *
                                        from edw_material_dim
                                    ) EMD,
                                    EDW_GCH_PRODUCTHIERARCHY EGPH
                                WHERE LTRIM(EMD.MATL_NUM, '0') = ltrim(EGPH.MATERIALNUMBER(+), 0)
                                    AND EMD.PROD_HIER_CD <> ''
                                    AND LTRIM(EMD.MATL_NUM, '0') IN (
                                        SELECT DISTINCT LTRIM(MATL_NUM, '0')
                                        FROM edw_material_sales_dim
                                        WHERE sls_org in ('3200', '320A', '320S', '321A')
                                    )
                            )
                        where rnk = 1
                    ),
                    CUSTOMER AS (
                        SELECT DISTINCT ECBD.CUST_NUM AS SAP_CUST_ID,
                            ECBD.CUST_NM AS SAP_CUST_NM,
                            ECSD.SLS_ORG AS SAP_SLS_ORG,
                            ECD.COMPANY AS SAP_CMP_ID,
                            ECD.CTRY_KEY AS SAP_CNTRY_CD,
                            ECD.CTRY_NM AS SAP_CNTRY_NM,
                            ECSD.PRNT_CUST_KEY AS SAP_PRNT_CUST_KEY,
                            CDDES_PCK.CODE_DESC AS SAP_PRNT_CUST_DESC,
                            ECSD.CHNL_KEY AS SAP_CUST_CHNL_KEY,
                            CDDES_CHNL.CODE_DESC AS SAP_CUST_CHNL_DESC,
                            ECSD.SUB_CHNL_KEY AS SAP_CUST_SUB_CHNL_KEY,
                            CDDES_SUBCHNL.CODE_DESC AS SAP_SUB_CHNL_DESC,
                            ECSD.GO_TO_MDL_KEY AS SAP_GO_TO_MDL_KEY,
                            CDDES_GTM.CODE_DESC AS SAP_GO_TO_MDL_DESC,
                            ECSD.BNR_KEY AS SAP_BNR_KEY,
                            CDDES_BNRKEY.CODE_DESC AS SAP_BNR_DESC,
                            ECSD.BNR_FRMT_KEY AS SAP_BNR_FRMT_KEY,
                            CDDES_BNRFMT.CODE_DESC AS SAP_BNR_FRMT_DESC,
                            SUBCHNL_RETAIL_ENV.RETAIL_ENV,
                            REGZONE.REGION_NAME AS REGION,
                            REGZONE.ZONE_NAME AS ZONE_OR_AREA,
                            EGCH.GCGH_REGION AS GCH_REGION,
                            EGCH.GCGH_CLUSTER AS GCH_CLUSTER,
                            EGCH.GCGH_SUBCLUSTER AS GCH_SUBCLUSTER,
                            EGCH.GCGH_MARKET AS GCH_MARKET,
                            EGCH.GCCH_RETAIL_BANNER AS GCH_RETAIL_BANNER,
                            ROW_NUMBER() OVER (
                                PARTITION BY SAP_CUST_ID
                                ORDER BY SAP_PRNT_CUST_KEY DESC
                            ) AS RANK
                        FROM EDW_GCH_CUSTOMERHIERARCHY EGCH,
                            EDW_CUSTOMER_SALES_DIM ECSD,
                            EDW_CUSTOMER_BASE_DIM ECBD,
                            EDW_COMPANY_DIM ECD,
                            EDW_DSTRBTN_CHNL EDC,
                            EDW_SALES_ORG_DIM ESOD,
                            EDW_CODE_DESCRIPTIONS CDDES_PCK,
                            EDW_CODE_DESCRIPTIONS CDDES_BNRKEY,
                            EDW_CODE_DESCRIPTIONS CDDES_BNRFMT,
                            EDW_CODE_DESCRIPTIONS CDDES_CHNL,
                            EDW_CODE_DESCRIPTIONS CDDES_GTM,
                            EDW_CODE_DESCRIPTIONS CDDES_SUBCHNL,
                            EDW_SUBCHNL_RETAIL_ENV_MAPPING SUBCHNL_RETAIL_ENV,
                            (
                                SELECT CUST_NUM,
                                    MIN(
                                        DECODE(CUST_DEL_FLAG, NULL, 'O', '', 'O', CUST_DEL_FLAG)
                                    ) AS CUST_DEL_FLAG
                                FROM EDW_CUSTOMER_SALES_DIM
                                WHERE SLS_ORG IN ('3200', '320A', '320S', '321A')
                                GROUP BY CUST_NUM
                            ) A,
                            (
                                SELECT DISTINCT CUSTOMER_CODE,
                                    REGION_NAME,
                                    ZONE_NAME
                                FROM EDW_CUSTOMER_DIM
                            ) REGZONE
                        WHERE EGCH.CUSTOMER (+) = ECBD.CUST_NUM
                            AND ECSD.CUST_NUM = ECBD.CUST_NUM
                            AND DECODE(
                                ECSD.CUST_DEL_FLAG,
                                NULL,
                                'O',
                                '',
                                'O',
                                ECSD.CUST_DEL_FLAG
                            ) = A.CUST_DEL_FLAG
                            AND A.CUST_NUM = ECSD.CUST_NUM
                            AND ECSD.DSTR_CHNL = EDC.DISTR_CHAN
                            AND ECSD.SLS_ORG = ESOD.SLS_ORG
                            AND ESOD.SLS_ORG_CO_CD = ECD.CO_CD
                            AND ECSD.SLS_ORG IN ('3200', '320A', '320S', '321A')
                            AND CDDES_PCK.CODE_TYPE(+) = 'Parent Customer Key'
                            AND CDDES_PCK.CODE(+) = ECSD.PRNT_CUST_KEY
                            AND cddes_bnrkey.code_type(+) = 'Banner Key'
                            AND CDDES_BNRKEY.CODE(+) = ECSD.BNR_KEY
                            AND cddes_bnrfmt.code_type(+) = 'Banner Format Key'
                            AND CDDES_BNRFMT.CODE(+) = ECSD.BNR_FRMT_KEY
                            AND cddes_chnl.code_type(+) = 'Channel Key'
                            AND CDDES_CHNL.CODE(+) = ECSD.CHNL_KEY
                            AND cddes_gtm.code_type(+) = 'Go To Model Key'
                            AND CDDES_GTM.CODE(+) = ECSD.GO_TO_MDL_KEY
                            AND cddes_subchnl.code_type(+) = 'Sub Channel Key'
                            AND CDDES_SUBCHNL.CODE(+) = ECSD.SUB_CHNL_KEY
                            AND UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL(+)) = UPPER(CDDES_SUBCHNL.CODE_DESC)
                            AND LTRIM(ECSD.CUST_NUM, '0') = REGZONE.CUSTOMER_CODE(+)
                    ),
                    inv_so_si as (
                        Select *
                        from wks_Korea_siso_propagate_final
                        where left(month, 4) >= (
                                date_part(
                                    year,
                                    convert_timezone('UTC', current_timestamp())
                                ) -2
                            )
                    )
                    SELECT CAL.CAL_YEAR,
                        cast(CAL.CAL_QRTR_NO as VARCHAR) as CAL_QRTR_NO,
                        cast(CAL.cal_MNTH_ID as VARCHAR) as cal_MNTH_ID,
                        CAL.CAL_MNTH_NO,
                        'Korea'::varchar AS CNTRY_NM,
                        'NA' AS DSTRBTR_GRP_CD,
                        'NA' as DSTRBTR_GRP_CD_nm,
                        TRIM(NVL (NULLIF(T3.GPH_PROD_FRNCHSE, ''), 'NA')) AS GLOBAL_PROD_FRANCHISE,
                        TRIM(NVL (NULLIF(T3.GPH_PROD_BRND, ''), 'NA')) AS GLOBAL_PROD_BRAND,
                        TRIM(NVL (NULLIF(T3.GPH_PROD_SUB_BRND, ''), 'NA')) AS GLOBAL_PROD_SUB_BRAND,
                        TRIM(NVL (NULLIF(T3.GPH_PROD_VRNT, ''), 'NA')) AS GLOBAL_PROD_VARIANT,
                        TRIM(NVL (NULLIF(T3.GPH_PROD_SGMNT, ''), 'NA')) AS GLOBAL_PROD_SEGMENT,
                        TRIM(NVL (NULLIF(T3.GPH_PROD_SUBSGMNT, ''), 'NA')) AS GLOBAL_PROD_SUBSEGMENT,
                        TRIM(NVL (NULLIF(T3.GPH_PROD_CTGRY, ''), 'NA')) AS GLOBAL_PROD_CATEGORY,
                        TRIM(NVL (NULLIF(T3.GPH_PROD_SUBCTGRY, ''), 'NA')) AS GLOBAL_PROD_SUBCATEGORY,
                        --TRIM(NVL (NULLIF(T3.GPH_PROD_PUT_UP_DESC,''),'NA')) AS GLOBAL_PUT_UP_DESC,
                        TRIM(NVL (NULLIF(t1.matl_num, ''), 'NA')) AS SKU_CD,
                        TRIM(NVL (NULLIF(T3.SAP_MAT_DESC, ''), 'NA')) AS SKU_DESCRIPTION,
                        --TRIM(NVL(NULLIF(T3.greenlight_sku_flag,''),'NA')) AS greenlight_sku_flag,
                        TRIM(NVL(NULLIF(T3.pka_product_key, ''), 'NA')) AS pka_product_key,
                        TRIM(NVL(NULLIF(T3.pka_size_desc, ''), 'NA')) AS pka_size_desc,
                        TRIM(
                            NVL(NULLIF(T3.pka_product_key_description, ''), 'NA')
                        ) AS pka_product_key_description,
                        TRIM(NVL(NULLIF(T3.product_key, ''), 'NA')) AS product_key,
                        TRIM(NVL(NULLIF(T3.product_key_description, ''), 'NA')) AS product_key_description,
                        'KRW'::VARCHAR AS FROM_CCY,
                        'USD'::VARCHAR AS TO_CCY,
                        T2.EXCH_RATE,
                        TRIM(
                            NVL (NULLIF(T4.SAP_PRNT_CUST_KEY, ''), 'Not Assigned')
                        ) AS SAP_PRNT_CUST_KEY,
                        TRIM(
                            NVL (NULLIF(T4.SAP_PRNT_CUST_DESC, ''), 'Not Assigned')
                        ) AS SAP_PRNT_CUST_DESC,
                        TRIM(NVL (NULLIF(T4.SAP_CUST_CHNL_KEY, ''), 'NA')) AS SAP_CUST_CHNL_KEY,
                        TRIM(NVL (NULLIF(T4.SAP_CUST_CHNL_DESC, ''), 'NA')) AS SAP_CUST_CHNL_DESC,
                        TRIM(NVL (NULLIF(T4.SAP_CUST_SUB_CHNL_KEY, ''), 'NA')) AS SAP_CUST_SUB_CHNL_KEY,
                        TRIM(NVL (NULLIF(T4.SAP_SUB_CHNL_DESC, ''), 'NA')) AS SAP_SUB_CHNL_DESC,
                        TRIM(NVL (NULLIF(T4.SAP_GO_TO_MDL_KEY, ''), 'NA')) AS SAP_GO_TO_MDL_KEY,
                        TRIM(NVL (NULLIF(T4.SAP_GO_TO_MDL_DESC, ''), 'NA')) AS SAP_GO_TO_MDL_DESC,
                        TRIM(NVL (NULLIF(T4.SAP_BNR_KEY, ''), 'NA')) AS SAP_BNR_KEY,
                        TRIM(NVL (NULLIF(T4.SAP_BNR_DESC, ''), 'NA')) AS SAP_BNR_DESC,
                        TRIM(NVL (NULLIF(T4.SAP_BNR_FRMT_KEY, ''), 'NA')) AS SAP_BNR_FRMT_KEY,
                        TRIM(NVL (NULLIF(T4.SAP_BNR_FRMT_DESC, ''), 'NA')) AS SAP_BNR_FRMT_DESC,
                        TRIM(NVL (NULLIF(T4.RETAIL_ENV, ''), 'NA')) AS RETAIL_ENV,
                        'Korea'::varchar as REGION,
                        'Korea'::varchar as ZONE_OR_AREA,
                        sum(last_3months_so) as last_3months_so_qty,
                        sum(last_6months_so) as last_6months_so_qty,
                        sum(last_12months_so) as last_12months_so_qty,
                        sum(last_3months_so_value) as last_3months_so_val,
                        sum(last_6months_so_value) as last_6months_so_val,
                        sum(last_12months_so_value) as last_12months_so_val,
                        sum(last_36months_so_value) as last_36months_so_val,
                        cast(
                            (sum(last_3months_so_value * T2.Exch_rate) / 1000) as numeric(38, 5)
                        ) as last_3months_so_val_usd,
                        cast(
                            (sum(last_6months_so_value * T2.Exch_rate) / 1000) as numeric(38, 5)
                        ) as last_6months_so_val_usd,
                        cast(
                            (sum(last_12months_so_value * T2.Exch_rate) / 1000) as numeric(38, 5)
                        ) as last_12months_so_val_usd,
                        propagate_flag,
                        propagate_from,
                        case
                            when propagate_flag = 'N' then 'Not propagate'
                            else reason
                        end as reason,
                        replicated_flag,
                        SUM(T1.sell_in_qty) AS SI_SLS_QTY,
                        SUM(T1.sell_in_value) AS SI_GTS_VAL,
                        SUM(T1.sell_in_value * T2.EXCH_RATE) / 1000 AS SI_GTS_VAL_USD,
                        SUM(T1.inv_qty) AS INVENTORY_QUANTITY,
                        SUM(T1.inv_value) AS INVENTORY_VAL,
                        SUM(T1.inv_value * T2.EXCH_RATE) / 1000 AS INVENTORY_VAL_USD,
                        SUM(T1.SO_QTY) AS SO_SLS_QTY,
                        SUM(T1.so_value) AS SO_TRD_SLS,
                        Round(SUM(T1.so_value * T2.EXCH_RATE) / 1000) AS SO_TRD_SLS_USD
                    FROM INV_SO_SI T1,
                        (
                            SELECT *
                            FROM CURRENCY
                            WHERE TO_CCY = 'USD'
                                AND JJ_MNTH_ID =(
                                    SELECT MAX(JJ_MNTH_ID)
                                    FROM CURRENCY
                                )
                        ) T2,
                        PRODUCT T3,
                        (
                            SELECT *
                            FROM CUSTOMER
                            WHERE RANK = 1
                        ) T4,
                        CAL
                    WHERE LTRIM(T3.SAP_MATL_NUM(+), '0') = T1.matl_num
                        AND LTRIM(T4.SAP_CUST_ID(+), '0') = T1.sap_parent_customer_key
                        AND t1.month = cal.cal_MNTH_ID
                        AND cal.CAL_YEAR >= (
                            date_part(
                                year,
                                convert_timezone('UTC', current_timestamp())
                            ) -2
                        )
                    GROUP BY CAL.CAL_YEAR,
                        CAL.CAL_QRTR_NO,
                        CAL.cal_MNTH_ID,
                        CAL.CAL_MNTH_NO,
                        CNTRY_NM,
                        T1.sap_parent_customer_key,
                        GLOBAL_PROD_FRANCHISE,
                        GLOBAL_PROD_BRAND,
                        GLOBAL_PROD_SUB_BRAND,
                        GLOBAL_PROD_VARIANT,
                        GLOBAL_PROD_SEGMENT,
                        GLOBAL_PROD_SUBSEGMENT,
                        GLOBAL_PROD_CATEGORY,
                        GLOBAL_PROD_SUBCATEGORY,
                        --GLOBAL_PUT_UP_DESC,
                        t1.matl_num,
                        SKU_DESCRIPTION,
                        --greenlight_sku_flag,
                        pka_product_key,
                        pka_size_desc,
                        pka_product_key_description,
                        product_key,
                        product_key_description,
                        FROM_CCY,
                        TO_CCY,
                        T2.EXCH_RATE,
                        T4.SAP_PRNT_CUST_KEY,
                        T4.SAP_PRNT_CUST_DESC,
                        T4.SAP_CUST_CHNL_KEY,
                        T4.SAP_CUST_CHNL_DESC,
                        T4.SAP_CUST_SUB_CHNL_KEY,
                        T4.SAP_SUB_CHNL_DESC,
                        T4.SAP_GO_TO_MDL_KEY,
                        T4.SAP_GO_TO_MDL_DESC,
                        T4.SAP_BNR_KEY,
                        T4.SAP_BNR_DESC,
                        T4.SAP_BNR_FRMT_KEY,
                        T4.SAP_BNR_FRMT_DESC,
                        T4.RETAIL_ENV,
                        T4.REGION,
                        T4.ZONE_OR_AREA,
                        propagate_flag,
                        propagate_from,
                        reason,
                        replicated_flag
                )
                SELECT *,
                    SUM(SI_GTS_VAL) OVER (PARTITION BY cntry_nm, cal_year, CAL_MNTH_ID) AS SI_INV_DB_VAL,
                    SUM(SI_GTS_VAL_USD) OVER (PARTITION BY cntry_nm, cal_year, CAL_MNTH_ID) AS SI_INV_DB_VAL_USD
                FROM ONSESEA
                WHERE cntry_nm || SAP_PRNT_CUST_DESC || cal_mnth_id IN (
                        SELECT cntry_nm || SAP_PRNT_CUST_DESC || cal_mnth_id AS INCLUSION
                        FROM (
                                SELECT cntry_nm,
                                    SAP_PRNT_CUST_DESC,
                                    cal_mnth_id,
                                    NVL(SUM(INVENTORY_VAL), 0) AS INV_VAL,
                                    NVL(SUM(so_trd_sls), 0) as Sellout_val,
                                    NVL(SUM(INVENTORY_QUANTITY), 0) as INV_QTY
                                FROM ONSESEA
                                WHERE SAP_PRNT_CUST_DESC IS NOT NULL
                                GROUP BY cntry_nm,
                                    SAP_PRNT_CUST_DESC,
                                    cal_mnth_id
                                HAVING INV_QTY <> 0 --and Sellout_val<>0
                            )
                    )
            )
            Select cal_year,
                cal_qrtr_no,
                cal_mnth_id,
                cal_mnth_no,
                cntry_nm,
                dstrbtr_grp_cd,
                dstrbtr_grp_cd_nm,
                global_prod_franchise,
                global_prod_brand,
                global_prod_sub_brand,
                global_prod_variant,
                global_prod_segment,
                global_prod_subsegment,
                global_prod_category,
                global_prod_subcategory,
                pka_size_desc as global_put_up_desc,
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
                round(cast(si_sls_qty as numeric(38, 5)), 5) as si_sls_qty,
                round(cast(si_gts_val as numeric (38, 5)), 5) as si_gts_val,
                round(cast(si_gts_val_usd as numeric(38, 5)), 5) as si_gts_val_usd,
                round(cast (inventory_quantity as numeric(38, 5)), 5) as inventory_quantity,
                round(cast(inventory_val as numeric(38, 5)), 5) as inventory_val,
                round(cast (inventory_val_usd as numeric(38, 5)), 5) as inventory_val_usd,
                round(cast (so_sls_qty as numeric(38, 5)), 5) as so_sls_qty,
                round(cast (so_trd_sls as numeric(38, 5)), 5) as so_trd_sls,
                so_trd_sls_usd as so_trd_sls_usd,
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
            from Regional
        )
)
select * from final