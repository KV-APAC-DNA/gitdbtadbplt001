with edw_gch_customerhierarchy
as (
    select *
    from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
    ),
edw_customer_sales_dim
as (
    select *
    from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
    ),
edw_customer_base_dim
as (
    select *
    from {{ ref('aspedw_integration__edw_customer_base_dim') }}
    ),
edw_company_dim
as (
    select *
    from {{ ref('aspedw_integration__edw_company_dim') }}
    ),
edw_dstrbtn_chnl
as (
    select *
    from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
    ),
edw_sales_org_dim
as (
    select *
    from {{ ref('aspedw_integration__edw_sales_org_dim') }}
    ),
edw_code_descriptions
as (
    select *
    from {{ ref('aspedw_integration__edw_code_descriptions') }}
    ),
edw_subchnl_retail_env_mapping
as (
    select *
    from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
    ),
edw_customer_dim
as (
    select *
    from indedw_integration.edw_customer_dim
    ),
v_rpt_ims_inventory_analysis
as (
    select *
    from {{ ref('ntaedw_integration__v_rpt_ims_inventory_analysis') }}
    ),
itg_parameter_reg_inventory
as (
    select *
    from {{ source('aspitg_integration', 'itg_parameter_reg_inventory') }}
    ),
edw_list_price
as (
    select *
    from {{ ref('aspedw_integration__edw_list_price') }}
    ),
edw_material_sales_dim
as (
    select *
    from {{ ref('aspedw_integration__edw_material_sales_dim') }}
    ),
edw_vw_greenlight_skus
as (
    select *
    from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
    ),
edw_gch_producthierarchy
as (
    select *
    from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
    ),
edw_material_dim
as (
    select *
    from {{ ref('aspedw_integration__edw_material_dim') }}
    ),
CUSTOMER
AS (
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
            PARTITION BY SAP_CUST_ID ORDER BY SAP_PRNT_CUST_KEY DESC
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
                MIN(DECODE(CUST_DEL_FLAG, NULL, 'O', '', 'O', CUST_DEL_FLAG)) AS CUST_DEL_FLAG
            FROM EDW_CUSTOMER_SALES_DIM
            WHERE SLS_ORG IN ('1110')
            GROUP BY CUST_NUM
            ) A,
        (
            SELECT DISTINCT CUSTOMER_CODE,
                REGION_NAME,
                ZONE_NAME
            FROM EDW_CUSTOMER_DIM
            ) REGZONE
    WHERE EGCH.CUSTOMER(+) = ECBD.CUST_NUM
        AND ECSD.CUST_NUM = ECBD.CUST_NUM
        AND DECODE(ECSD.CUST_DEL_FLAG, NULL, 'O', '', 'O', ECSD.CUST_DEL_FLAG) = A.CUST_DEL_FLAG
        AND A.CUST_NUM = ECSD.CUST_NUM
        AND ECSD.DSTR_CHNL = EDC.DISTR_CHAN
        AND ECSD.SLS_ORG = ESOD.SLS_ORG
        AND ESOD.SLS_ORG_CO_CD = ECD.CO_CD
        AND ECSD.SLS_ORG IN ('1110')
        AND UPPER(trim(CDDES_PCK.CODE_TYPE(+))) = 'PARENT CUSTOMER KEY'
        AND CDDES_PCK.CODE(+) = ECSD.PRNT_CUST_KEY
        AND UPPER(trim(cddes_bnrkey.code_type(+))) = 'BANNER KEY'
        AND CDDES_BNRKEY.CODE(+) = ECSD.BNR_KEY
        AND UPPER(TRIM(cddes_bnrfmt.code_type(+))) = 'BANNER FORMAT KEY'
        AND CDDES_BNRFMT.CODE(+) = ECSD.BNR_FRMT_KEY
        AND UPPER(TRIM(cddes_chnl.code_type(+))) = 'CHANNEL KEY'
        AND CDDES_CHNL.CODE(+) = ECSD.CHNL_KEY
        AND UPPER(TRIM(cddes_gtm.code_type(+))) = 'GO TO MODEL KEY'
        AND CDDES_GTM.CODE(+) = ECSD.GO_TO_MDL_KEY
        AND UPPER(TRIM(cddes_subchnl.code_type(+))) = 'SUB CHANNEL KEY'
        AND CDDES_SUBCHNL.CODE(+) = ECSD.SUB_CHNL_KEY
        AND UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL(+)) = UPPER(CDDES_SUBCHNL.CODE_DESC)
        AND LTRIM(ECSD.CUST_NUM, '0') = REGZONE.CUSTOMER_CODE(+)
    ),
sellout
AS (
    SELECT 'SELLOUT' AS data_type,
        dstr_cd,
        prod_cd,
        ims_txn_dt,
        sls_qty AS so_qty
    FROM (
        SELECT txn.dstr_cd,
            prod_cd,
            ean_num,
            ims_txn_dt,
            (sls_qty) - (rtrn_qty) sls_qty
        FROM v_rpt_ims_inventory_analysis txn
        WHERE (
                CASE 
                    WHEN txn.prod_cd LIKE '1U%'
                        OR txn.prod_cd LIKE 'COUNTER TOP%'
                        OR txn.prod_cd IS NULL
                        OR txn.prod_cd = ''
                        OR txn.prod_cd LIKE 'DUMPBIN%'
                        THEN 'non sellable products'
                    ELSE 'sellable products'
                    END = 'sellable products'
                )
            AND ctry_nm = 'Hong Kong'
            AND ltrim(dstr_cd, 0) IN (
                SELECT parameter_value
                FROM itg_parameter_reg_inventory
                WHERE country_name = 'HK'
                    AND parameter_name = 'inv_analysis_distributor_id'
                )
            AND from_crncy = 'HKD'
            AND to_crncy = 'HKD'
            AND left(ims_txn_dt, 4) >= (DATE_PART(YEAR, SYSDATE()) - 6)
        )
    ),
T5
AS (
    SELECT ltrim(material, 0) material,
        MAX(amount) amount
    FROM (
        SELECT T1.*,
            (
                rank() OVER (
                    PARTITION BY ltrim(T1.material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC,
                        to_date(dt_from, 'YYYYMMDD') DESC
                    )
                ) AS rn
        FROM edw_list_price T1
        WHERE sls_org = '1110'
        )
    WHERE rn = 1
    GROUP BY material
    ),
T6
AS (
    SELECT ean_num,
        amount,
        matl_num
    FROM (
        SELECT t_flp.*,
            (
                row_number() OVER (
                    PARTITION BY matl_num ORDER BY amount DESC
                    )
                ) AS rn
        FROM (
            SELECT DISTINCT T_LP.ean_num,
                T_LP.amount,
                matl_num
            FROM (
                SELECT ean_num,
                    MAX(amount) amount
                FROM (
                    SELECT T_LP1.*,
                        (
                            RANK() OVER (
                                PARTITION BY ean_num,
                                LTRIM(T_LP1.material, 0) ORDER BY TO_DATE(valid_to, 'YYYYMMDD') DESC,
                                    TO_DATE(dt_from, 'YYYYMMDD') DESC
                                )
                            ) AS rn
                    FROM (
                        SELECT DISTINCT ms.matl_num,
                            ms.ean_num,
                            LP.*
                        FROM edw_material_sales_dim ms,
                            edw_list_price LP
                        WHERE lp.sls_org = '1110'
                            AND lp.sls_org = ms.sls_org
                            AND LTRIM(ms.matl_num, '0') = LTRIM(LP.material, '0')
                        ) T_LP1
                    ) ean_lp
                WHERE rn = 1
                GROUP BY ean_num
                ) T_LP
            JOIN edw_material_sales_dim ms1 ON ms1.ean_num = T_LP.ean_num
            WHERE ms1.sls_org = '1110'
                AND ms1.ean_num != ''
            ) t_flp
        )
    WHERE rn = 1
    ),
A
AS (
    SELECT base.*
    FROM (
        SELECT dstr_cd,
            nvl(nullif(prod_cd, ''), 'NA') AS matl_num,
            ims_txn_dt AS bill_date,
            T1.SO_QTY * nvl(t5.amount, t6.amount) AS SO_TRD_SLS
        FROM sellout T1,
            T5,
            T6
        WHERE ltrim(T1.dstr_cd, 0) IN (
                SELECT parameter_value
                FROM itg_parameter_reg_inventory
                WHERE country_name = 'HK'
                    AND parameter_name = 'inv_analysis_distributor_id'
                )
            AND Ltrim(T1.prod_cd, 0) = ltrim(T5.material(+), 0)
            AND Ltrim(T1.prod_cd, 0) = ltrim(T6.matl_num(+), 0)
        ) base
    WHERE base.SO_TRD_SLS > 0
    ),
T4
AS (
    SELECT DISTINCT EMD.matl_num AS SAP_MATL_NUM,
        EMD.pka_product_key AS pka_product_key,
        EMD.pka_size_desc AS pka_size_desc,
        EGPH.GCPH_BRAND AS GPH_PROD_BRND,
        EGPH.GCPH_VARIANT AS GPH_PROD_VRNT,
        EGPH.GCPH_CATEGORY AS GPH_PROD_CTGRY,
        EGPH.GCPH_SEGMENT AS GPH_PROD_SGMNT
    FROM (
        SELECT *
        FROM edw_material_dim
        ) EMD,
        EDW_GCH_PRODUCTHIERARCHY EGPH
    WHERE LTRIM(EMD.MATL_NUM, '0') = ltrim(EGPH.MATERIALNUMBER(+), 0)
        AND EMD.PROD_HIER_CD <> ''
        AND LTRIM(EMD.MATL_NUM, '0') IN (
            SELECT DISTINCT LTRIM(MATL_NUM, '0')
            FROM edw_material_sales_dim
            WHERE sls_org = '1110'
            )
    ),
T3
AS (
    SELECT *
    FROM CUSTOMER
    WHERE RANK = 1
    ),
t_final
AS (
    SELECT min(bill_date) AS min_date,
        TRIM(NVL(NULLIF(A.dstr_cd, ''), 'NA')) AS DSTRBTR_GRP_CD,
        TRIM(NVL(NULLIF(T4.GPH_PROD_BRND, ''), 'NA')) AS BRAND,
        TRIM(NVL(NULLIF(T4.GPH_PROD_VRNT, ''), 'NA')) AS VARIANT,
        TRIM(NVL(NULLIF(T4.GPH_PROD_SGMNT, ''), 'NA')) AS SEGMENT,
        TRIM(NVL(NULLIF(T4.GPH_PROD_CTGRY, ''), 'NA')) AS PROD_CATEGORY,
        TRIM(NVL(NULLIF(T4.pka_size_desc, ''), 'NA')) AS pka_size_desc,
        TRIM(NVL(NULLIF(T4.pka_product_key, ''), 'NA')) AS pka_product_key,
        CASE 
            WHEN T3.SAP_PRNT_CUST_KEY = ''
                OR T3.SAP_PRNT_CUST_KEY IS NULL
                THEN 'Not Assigned'
            ELSE T3.SAP_PRNT_CUST_KEY
            END AS SAP_PRNT_CUST_KEY
    FROM A,
        T4,
        T3
    WHERE LTRIM(T4.SAP_MATL_NUM(+), '0') = A.matl_num
        AND LTRIM(T3.SAP_CUST_ID(+), '0') = A.dstr_cd
    GROUP BY dstr_cd,
        GPH_PROD_BRND,
        GPH_PROD_VRNT,
        GPH_PROD_SGMNT,
        GPH_PROD_CTGRY,
        pka_size_desc,
        pka_product_key,
        SAP_PRNT_CUST_KEY
    )
SELECT *
FROM t_final