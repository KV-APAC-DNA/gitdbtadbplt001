with edw_gch_customerhierarchy
as (
    select *
    from {{ref('aspedw_integration__edw_gch_customerhierarchy')}}
    ),
edw_customer_sales_dim
as (
    select *
    from {{ref('aspedw_integration__edw_customer_sales_dim')}}
    ),
edw_customer_base_dim
as (
    select *
    from {{ ref('aspedw_integration__edw_customer_base_dim') }}
    ),
edw_company_dim
as (
    select *
    from {{ref('aspedw_integration__edw_company_dim')}}
    ),
edw_dstrbtn_chnl
as (
    select *
    from {{ref('aspedw_integration__edw_dstrbtn_chnl')}}

    ),
edw_sales_org_dim
as (
    select *
    from {{ref('aspedw_integration__edw_sales_org_dim')}}

    ),
edw_code_descriptions
as (
    select *
    from {{ref('aspedw_integration__edw_code_descriptions')}}

    ),
edw_subchnl_retail_env_mapping
as (
    select *
    from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }} 

    ),
edw_customer_dim
as (
    select *
    from {{ ref('indedw_integration__edw_customer_dim') }} 

    ),
v_rpt_dly_plan_ims_enrich
as (
    select *
    from {{ref('ntaedw_integration__v_rpt_dly_plan_ims_enrich')}}

    ),
edw_pos_fact
as (
    select *
    from {{ref('ntaedw_integration__edw_pos_fact')}}

    ),
itg_pos_prom_prc_map
as (
    select *
    from {{ref('ntaitg_integration__itg_pos_prom_prc_map')}}

    ),
edw_vw_greenlight_skus
as (
    select *
    from {{ref('aspedw_integration__edw_vw_greenlight_skus')}}

    ),
itg_parameter_reg_inventory
as (
    select *
    from {{ source('aspitg_integration', 'itg_parameter_reg_inventory') }}

    ),
edw_product_attr_dim
as (
    select *
    from {{ref('aspedw_integration__edw_product_attr_dim')}}

    ),
edw_material_dim
as (
    select *
    from {{ref('aspedw_integration__edw_material_dim')}}

    ),
customer
AS (
    SELECT DISTINCT SAP_PRNT_CUST_KEY,
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
        ROW_NUMBER() OVER (
            PARTITION BY sap_bnr_key,
            sap_bnr_desc,
            sap_go_to_mdl_key ORDER BY sap_prnt_cust_key,
                sap_prnt_cust_desc,
                sap_go_to_mdl_key
            ) AS rnk
    FROM (
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
                WHERE SLS_ORG IN ('1200')
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
            AND ECSD.SLS_ORG IN ('1200')
            AND trim(Upper(CDDES_PCK.CODE_TYPE(+))) = 'PARENT CUSTOMER KEY'
            AND CDDES_PCK.CODE(+) = ECSD.PRNT_CUST_KEY
            AND trim(Upper(cddes_bnrkey.code_type(+))) = 'BANNER KEY'
            AND CDDES_BNRKEY.CODE(+) = ECSD.BNR_KEY
            AND trim(Upper(cddes_bnrfmt.code_type(+))) = 'BANNER FORMAT KEY'
            AND CDDES_BNRFMT.CODE(+) = ECSD.BNR_FRMT_KEY
            AND trim(Upper(cddes_chnl.code_type(+))) = 'CHANNEL KEY'
            AND CDDES_CHNL.CODE(+) = ECSD.CHNL_KEY
            AND trim(Upper(cddes_gtm.code_type(+))) = 'GO TO MODEL KEY'
            AND CDDES_GTM.CODE(+) = ECSD.GO_TO_MDL_KEY
            AND trim(Upper(cddes_subchnl.code_type(+))) = 'SUB CHANNEL KEY'
            AND CDDES_SUBCHNL.CODE(+) = ECSD.SUB_CHNL_KEY
            AND UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL(+)) = UPPER(CDDES_SUBCHNL.CODE_DESC)
            AND LTRIM(ECSD.CUST_NUM, '0') = REGZONE.CUSTOMER_CODE(+)
        )
    WHERE sap_prnt_cust_key <> ''
    ),
SELLOUT AS
(
    SELECT txn.dstr_cd,
                    ean_num,
                    ims_txn_dt AS bill_dt,
                    (sls_qty) - (rtrn_qty) sls_qty,
                    ((sls_qty - rtrn_qty) * sell_in_price_manual) sls_value
                FROM v_rpt_dly_plan_ims_enrich txn
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
                    AND ctry_cd = 'TW'
                    AND LEFT(ims_txn_dt, 4) >= (DATE_PART(YEAR, SYSDATE()) - 6)
                    AND to_crncy = 'TWD'
                    AND txn.dstr_cd <> '134478' ---- changed as PX data needs to be taken from POS
),
OFFTAKE AS
(
    SELECT cust_num AS dstr_cd,
                    ean_num,
                    bill_dt,
                    cast(sls_qty AS NUMERIC(38, 4)) AS sls_qty,
                    cast(sls_value AS NUMERIC(38, 4)) AS sls_value
                FROM (
                    SELECT 'TW' AS ctry_cd,
                        'OFFTAKE' AS Data_Type,
                        ean_num,
                        pos_dt AS bill_dt,
                        sls_qty AS sls_qty,
                        (sls_qty * price.prom_prc) AS sls_value
                    FROM edw_pos_fact txn,
                        (
                            SELECT CASE 
                                    WHEN cust = 'ibonMart'
                                        THEN 'ibonMart'
                                    WHEN cust = 'EC'
                                        THEN 'EC'
                                    WHEN cust = '7-11'
                                        THEN '7-11'
                                    WHEN cust = 'Carrefour'
                                        THEN 'Carrefour 家樂福'
                                    WHEN cust = 'Cosmed'
                                        THEN 'Cosmed 康是美'
                                    WHEN cust = 'PX-Civilian'
                                        THEN 'PX 全聯'
                                    WHEN cust = 'A-Mart'
                                        THEN 'A-Mart 愛買'
                                    WHEN cust = 'Poya'
                                        THEN 'Poya 寶雅'
                                    WHEN cust = 'Watsons'
                                        THEN 'Watsons 屈臣氏'
                                    WHEN cust = 'RT-Mart'
                                        THEN 'RT-Mart 大潤發'
                                    END AS cust,
                                barcd,
                                cust_prod_cd,
                                prom_prc,
                                prom_strt_dt,
                                prom_end_dt
                            FROM itg_pos_prom_prc_map
                            ) price
                    WHERE src_sys_cd IN ('Watsons 屈臣氏')
                        AND ctry_cd = 'TW'
                        AND LEFT(pos_dt, 4) >= (DATE_PART(YEAR, SYSDATE()) - 6)
                        AND crncy_cd = 'TWD'
                        AND txn.pos_dt BETWEEN price.prom_strt_dt
                            AND price.prom_end_dt
                        AND txn.ean_num = price.barcd(+)
                        AND txn.src_sys_cd = price.cust(+)
                        AND txn.vend_prod_cd = price.cust_prod_cd(+)
                    ) AS pos,
                    (
                        SELECT DISTINCT MIN(nvl(LTRIM(cust_num, '0'), '#')) AS cust_num
                        FROM EDW_CUSTOMER_SALES_DIM
                        WHERE sls_org = '1200'
                            AND prnt_cust_key = (
                                SELECT DISTINCT parameter_value
                                FROM itg_parameter_reg_inventory
                                WHERE country_name = 'TAIWAN'
                                    AND parameter_name = 'AS_WATSONS_PRNT_CUST_KEY'
                                )
                        ) AS CUST
),
ADD_PX_DATA AS
(
    SELECT cust_num AS dstr_cd,
                    ean_num,
                    pos_dt AS bill_dt,
                    cast(sls_qty AS NUMERIC(38, 4)) AS sls_qty,
                    cast(prom_sls_amt AS NUMERIC(38, 4)) AS sls_value
                FROM edw_pos_fact txn,
                    (
                        SELECT DISTINCT max(nvl(LTRIM(cust_num, '0'), '#')) AS cust_num
                        FROM EDW_CUSTOMER_SALES_DIM
                        WHERE sls_org = '1200'
                            AND prnt_cust_key = (
                                SELECT DISTINCT parameter_value
                                FROM itg_parameter_reg_inventory
                                WHERE country_name = 'TAIWAN'
                                    AND parameter_name = 'PX_PRNT_CUST_KEY'
                                )
                        ) AS CUST
                WHERE src_sys_cd = 'PX 全聯'
                    AND ctry_cd = 'TW'
                    AND LEFT(pos_dt, 4) >= (DATE_PART(YEAR, SYSDATE()) - 6)
                    AND crncy_cd = 'TWD'
                
),
UNION_OF_SELLOUT_OFFTAKE_PX_DATA AS
(
    select * from sellout
    union ALL
    select * from offtake
    union ALL
    select * from add_px_data
),
a AS
(
    SELECT dstr_cd,
                ean_num,
                bill_dt,
                sls_qty AS so_qty,
                sls_value AS sls_value
            FROM UNION_OF_SELLOUT_OFFTAKE_PX_DATA
            WHERE dstr_cd <> (
                    SELECT DISTINCT parameter_value
                    FROM itg_parameter_reg_inventory
                    WHERE country_name = 'Taiwan'
                        AND parameter_name = 'inv_analysis_distributor_id'
                    )
                AND sls_value > 0
),
b AS
(
    SELECT cust_num,
                    CASE 
                        WHEN prnt_cust_key IS NULL
                            OR prnt_cust_key = ''
                            THEN 'Not Assigned'
                        ELSE prnt_cust_key
                        END AS prnt_cust_key,
                    CASE 
                        WHEN CDDES_PCK.code_desc IS NULL
                            OR CDDES_PCK.code_desc = ''
                            THEN 'Not Assigned'
                        ELSE CDDES_PCK.code_desc
                        END AS prnt_cust_desc,
                    CASE 
                        WHEN bnr_key IS NULL
                            OR bnr_key = ''
                            THEN 'Not Assigned'
                        ELSE bnr_key
                        END AS bnr_key,
                    CASE 
                        WHEN CDDES_BNRKEY.code_desc IS NULL
                            OR CDDES_BNRKEY.code_desc = ''
                            THEN 'Not Assigned'
                        ELSE CDDES_BNRKEY.code_desc
                        END AS bnr_desc
                FROM (
                    SELECT ltrim(cust_num, 0) cust_num,
                        prnt_cust_key,
                        bnr_key
                    FROM EDW_CUSTOMER_SALES_DIM
                    WHERE sls_org = '1200'
                    ) b,
                    (
                        SELECT CODE,
                            CODE_DESC
                        FROM EDW_CODE_DESCRIPTIONS
                        WHERE trim(Upper(CODE_TYPE)) = 'PARENT CUSTOMER KEY'
                        ) CDDES_PCK,
                    (
                        SELECT CODE,
                            CODE_DESC
                        FROM EDW_CODE_DESCRIPTIONS
                        WHERE trim(Upper(CODE_TYPE)) = 'BANNER KEY'
                        ) CDDES_BNRKEY
                WHERE CDDES_PCK.CODE(+) = b.PRNT_CUST_KEY
                    AND CDDES_BNRKEY.CODE(+) = b.bnr_key
                
),
T1 AS
(
    SELECT dstr_cd,
            ean_num,
            b.bnr_key,
            bill_dt,
            so_qty,
            sls_value
        FROM a,b
        WHERE b.cust_num(+) = a.dstr_cd
),
T5 AS 
(
    SELECT *
            FROM (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY matl_num ORDER BY matl_num
                        ) rnk
                FROM (
                    SELECT m.matl_num,
                        m.matl_desc,
                        --m.greenlight_sku_flag ,
                        m.pka_product_key,
                        m.pka_size_desc,
                        m.pka_product_key_description,
                        m.pka_product_key AS product_key,
                        m.pka_product_key_description AS product_key_description,
                        pa.*
                    -- from  (Select * from rg_edw.edw_vw_greenlight_skus WHERE sls_org in ( '1200') ) m 
                    FROM (
                        SELECT *
                        FROM edw_material_dim
                        ) m,
                        edw_product_attr_dim pa
                    WHERE pa.cntry = 'TW'
                        AND ltrim(pa.sap_matl_num, '0') = ltrim(m.matl_num, '0')
                    )
                )
            WHERE rnk = 1
),
T4 AS
(
    SELECT *
            FROM CUSTOMER
            WHERE rnk = 1
),
final as
(
        SELECT min(bill_dt) AS min_date,
        TRIM(NVL(NULLIF(T5.prod_hier_l4, ''), 'NA')) AS GLOBAL_PROD_BRAND,
        TRIM(NVL(NULLIF(T5.prod_hier_l7, ''), 'NA')) AS GLOBAL_PROD_VARIANT,
        TRIM(NVL(NULLIF(T5.prod_hier_l5, ''), 'NA')) AS GLOBAL_PROD_SEGMENT,
        TRIM(NVL(NULLIF(T5.prod_hier_l6, ''), 'NA')) AS GLOBAL_PROD_CATEGORY,
        TRIM(NVL(NULLIF(t5.pka_product_key, ''), 'NA')) AS pka_product_key,
        TRIM(NVL(NULLIF(t5.pka_size_desc, ''), 'NA')) AS pka_size_desc,
        TRIM(NVL(NULLIF(T4.SAP_PRNT_CUST_KEY, ''), 'Not Assingned')) AS SAP_PRNT_CUST_KEY
    FROM T1,T4,T5
    WHERE T1.ean_num = T5.ean(+)
        AND T4.sap_bnr_key(+) = T1.bnr_key
    GROUP BY T5.prod_hier_l4,
        T5.prod_hier_l7,
        T5.pka_size_desc,
        T5.prod_hier_l5,
        T5.pka_product_key,
        T5.prod_hier_l6,
        T4.SAP_PRNT_CUST_KEY
)
SELECT *
FROM final
