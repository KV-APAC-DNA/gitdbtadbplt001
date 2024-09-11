{{ 
    config(
    sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )}}
with EDW_GCH_CUSTOMERHIERARCHY as(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),

EDW_CUSTOMER_SALES_DIM as(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),

EDW_CUSTOMER_BASE_DIM as(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),

EDW_COMPANY_DIM as(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),

EDW_DSTRBTN_CHNL as(
    select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),

EDW_SALES_ORG_DIM as(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),

EDW_CODE_DESCRIPTIONS as(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),

EDW_SUBCHNL_RETAIL_ENV_MAPPING as(
    select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),

WKS_JAPAN_REGIONAL_SELLOUT_ACTUALS as (
    select * from {{ ref('jpnwks_integration__wks_japan_regional_sellout_actuals') }}
),
EDW_VW_POP6_PRODUCTS as ( 
    select * from  {{ source('ntaedw_integration', 'edw_vw_pop6_products') }}
),
VW_JAN_CHANGE as ( 
    select * from {{ source('jpnedw_integration', 'vw_jan_change') }}
),
EDI_STORE_M as (
    select * from {{ source('jpnedw_integration', 'edi_store_m') }}
 ),
EDI_CHN_M as (
    select * from {{ source('jpnedw_integration', 'edi_chn_m') }}
 ),
MT_SGMT as (
    select * from {{ source('jpnedw_integration', 'mt_sgmt') }}
 ),
EDI_CSTM_M as (
    select * from {{ source('jpnedw_integration', 'edi_cstm_m') }}
),
EDW_LIST_PRICE as (
    select * from {{ ref('aspedw_integration__edw_list_price') }}
),
MT_PRF as (
    select * from {{ source('jpnedw_integration', 'mt_prf') }}
),
JP_INV_COVERAGE_AREA_REGION_MAPPING as (
    select * from {{ source('jpnedw_integration', 'jp_inv_coverage_area_region_mapping') }}
),
SDL_MDS_JP_C360_ENG_TRANSLATION as (
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_c360_eng_translation') }}
),
ITG_JP_RE_MSL_LIST as (
    select * from {{ ref('jpnitg_integration__itg_jp_re_msl_list') }}
),
edw_generic_product_key_attributes as (
    select * from {{ ref('aspedw_integration__edw_generic_product_key_attributes') }}
),
edw_generic_product_hierarchy as (
    select * from {{ ref('aspedw_integration__edw_generic_product_hierarchy') }}
),
transformation as( (
SELECT MDP.*,
       COM.*,
       SYSDATE() AS CRT_DTTM
FROM (SELECT TARGET.YEAR AS FISC_YR,
             TARGET.MNTH_ID AS FISC_PER,
             'Japan' AS MARKET,
			 'SELL-OUT'AS DATA_SOURCE,
             COALESCE(ACTUAL.CHANNEL, TARGET.SEGMENT) AS CHANNEL_NAME,
             COALESCE(ACTUAL.SOLD_TO_CODE, TARGET.SOLD_TO_CODE) AS SOLD_TO_CODE,
             COALESCE(ACTUAL.DISTRIBUTOR_CODE,TARGET.DISTRIBUTOR_CODE) AS DISTRIBUTOR_CODE,
             COALESCE(ACTUAL.DISTRIBUTOR_NAME, TARGET.DISTRIBUTOR_NAME) AS DISTRIBUTOR_NAME,
             COALESCE(ACTUAL.CHANNEL, TARGET.SEGMENT) AS SELL_OUT_CHANNEL,
             COALESCE(ACTUAL.RETAIL_ENVIRONMENT, TARGET.SELL_OUT_RE) AS RETAIL_ENVIRONMENT,
             COALESCE(ACTUAL.STORE_CODE,TARGET.STORE_CODE) AS STORE_CODE,
             COALESCE(ACTUAL.STORE_NAME,TARGET.STORE_NAME) AS STORE_NAME,
             COALESCE(ACTUAL.REGION,TARGET.REGION_NAME_ENG) AS REGION,
             COALESCE(ACTUAL.ZONE_OR_AREA,TARGET.ZONE_NAME_ENG) AS ZONE_NAME,
             NULL AS CITY,
             TARGET.STORE_ADDRESS,
             TARGET.POST_CODE,
             TARGET.SKU_CD AS PRODUCT_CODE,
             PROD.SKU_NAME AS PRODUCT_NAME,
             'Japan' AS PROD_HIER_L1,
             PROD.PROD_HIER_L2,
             PROD.PROD_HIER_L3,
             PROD.PROD_HIER_L4,
             PROD.PROD_HIER_L5,
             PROD.PROD_HIER_L6,
             PROD.PROD_HIER_L7,
             PROD.PROD_HIER_L8,
             PROD.PROD_HIER_L9,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_KEY,CUSTOMER.CUST_SEGMT_KEY) AS CUSTOMER_SEGMENT_KEY,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION,CUSTOMER.CUST_SEGMENT_DESC) AS CUSTOMER_SEGMENT_DESCRIPTION,
             COALESCE(ACTUAL.SAP_CUSTOMER_CHANNEL_KEY,CUSTOMER.SAP_CUST_CHNL_KEY) AS SAP_CUSTOMER_CHANNEL_KEY,
             COALESCE(ACTUAL.SAP_CUSTOMER_CHANNEL_DESCRIPTION,CUSTOMER.SAP_CUST_CHNL_DESC) AS SAP_CUSTOMER_CHANNEL_DESCRIPTION,
             COALESCE(ACTUAL.SAP_CUSTOMER_SUB_CHANNEL_KEY,CUSTOMER.SAP_CUST_SUB_CHNL_KEY) AS SAP_CUSTOMER_SUB_CHANNEL_KEY,
             COALESCE(ACTUAL.SAP_SUB_CHANNEL_DESCRIPTION,CUSTOMER.SAP_SUB_CHNL_DESC) AS SAP_SUB_CHANNEL_DESCRIPTION,
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_KEY,CUSTOMER.SAP_PRNT_CUST_KEY) AS SAP_PARENT_CUSTOMER_KEY,
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_DESCRIPTION,CUSTOMER.SAP_PRNT_CUST_DESC) AS SAP_PARENT_CUSTOMER_DESCRIPTION,
             COALESCE(ACTUAL.SAP_BANNER_KEY,CUSTOMER.SAP_BNR_KEY) AS SAP_BANNER_KEY,
             COALESCE(ACTUAL.SAP_BANNER_DESCRIPTION,CUSTOMER.SAP_BNR_DESC) AS SAP_BANNER_DESCRIPTION,
             COALESCE(ACTUAL.SAP_BANNER_FORMAT_KEY,CUSTOMER.SAP_BNR_FRMT_KEY) AS SAP_BANNER_FORMAT_KEY,
             COALESCE(ACTUAL.SAP_BANNER_FORMAT_DESCRIPTION,CUSTOMER.SAP_BNR_FRMT_DESC) AS SAP_BANNER_FORMAT_DESCRIPTION,
             TRIM(NVL (NULLIF(CUSTOMER.SAP_CUST_NM,''),'NA')) AS CUSTOMER_NAME,
             TRIM(NVL (NULLIF(CUSTOMER.SAP_CUST_ID,''),'NA')) AS CUSTOMER_CODE,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_SGMT_CD,''),'NA')) AS SAP_PROD_SGMT_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_SGMT_DESC,''),'NA')) AS SAP_PROD_SGMT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_BASE_PROD_DESC,''),'NA')) AS SAP_BASE_PROD_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MEGA_BRND_DESC,''),'NA')) AS SAP_MEGA_BRND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_BRND_DESC,''),'NA')) AS SAP_BRND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_VRNT_DESC,''),'NA')) AS SAP_VRNT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PUT_UP_DESC,''),'NA')) AS SAP_PUT_UP_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_GRP_FRNCHSE_CD,''),'NA')) AS SAP_GRP_FRNCHSE_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_GRP_FRNCHSE_DESC,''),'NA')) AS SAP_GRP_FRNCHSE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_FRNCHSE_CD,''),'NA')) AS SAP_FRNCHSE_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_FRNCHSE_DESC,''),'NA')) AS SAP_FRNCHSE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_FRNCHSE_CD,''),'NA')) AS SAP_PROD_FRNCHSE_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_FRNCHSE_DESC,''),'NA')) AS SAP_PROD_FRNCHSE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MJR_CD,''),'NA')) AS SAP_PROD_MJR_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MJR_DESC,''),'NA')) AS SAP_PROD_MJR_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MNR_CD,''),'NA')) AS SAP_PROD_MNR_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MNR_DESC,''),'NA')) AS SAP_PROD_MNR_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_HIER_CD,''),'NA')) AS SAP_PROD_HIER_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_HIER_DESC,''),'NA')) AS SAP_PROD_HIER_DESC,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,PRODUCT.GPH_PROD_FRNCHSE) AS GLOBAL_PRODUCT_FRANCHISE,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_BRAND,PRODUCT.GPH_PROD_BRND) AS GLOBAL_PRODUCT_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,PRODUCT.GPH_PROD_SUB_BRND) AS GLOBAL_PRODUCT_SUB_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_VARIANT,PRODUCT.GPH_PROD_VRNT) AS GLOBAL_PRODUCT_VARIANT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SEGMENT,PRODUCT.GPH_PROD_NEEDSTATE) AS GLOBAL_PRODUCT_SEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,PRODUCT.GPH_PROD_SUBSGMNT) AS GLOBAL_PRODUCT_SUBSEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_CATEGORY,PRODUCT.GPH_PROD_CTGRY) AS GLOBAL_PRODUCT_CATEGORY,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,PRODUCT.GPH_PROD_SUBCTGRY) AS GLOBAL_PRODUCT_SUBCATEGORY,
             COALESCE(ACTUAL.GLOBAL_PUT_UP_DESCRIPTION,PRODUCT.GPH_PROD_PUT_UP_DESC) AS GLOBAL_PUT_UP_DESCRIPTION,
             --TRIM(NVL (NULLIF(PRODUCT.EAN,''),'NA')) AS EAN,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA')) AS SKU_CODE,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA')) AS SKU_DESCRIPTION,
             TRIM(NVL (NULLIF(PRODUCT.PKA_FRANCHISE_DESC,''),'NA')) AS PKA_FRANCHISE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_BRAND_DESC,''),'NA')) AS PKA_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_BRAND_DESC,''),'NA')) AS PKA_SUB_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_VARIANT_DESC,''),'NA')) AS PKA_VARIANT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_VARIANT_DESC,''),'NA')) AS PKA_SUB_VARIANT_DESC,
             COALESCE(ACTUAL.PKA_PRODUCT_KEY,PRODUCT.PKA_PRODUCT_KEY) AS PKA_PRODUCT_KEY,
             COALESCE(ACTUAL.PKA_PRODUCT_KEY_DESCRIPTION,PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION) AS PKA_PRODUCT_KEY_DESCRIPTION,
             ACTUAL.CM_SALES AS SALES_VALUE,
             ACTUAL.CM_SALES_QTY AS SALES_QTY,
             ACTUAL.CM_AVG_SALES_QTY AS AVG_SALES_QTY,
             ACTUAL.CM_SALES_VALUE_LIST_PRICE AS SALES_VALUE_LIST_PRICE,
             ACTUAL.LM_SALES AS LM_SALES,
             ACTUAL.LM_SALES_QTY AS LM_SALES_QTY,
             ACTUAL.LM_AVG_SALES_QTY AS LM_AVG_SALES_QTY,
             ACTUAL.LM_SALES_LP AS LM_SALES_LP,
             ACTUAL.P3M_SALES AS P3M_SALES,
             ACTUAL.P3M_QTY AS P3M_QTY,
             ACTUAL.P3M_AVG_QTY AS P3M_AVG_QTY,
             ACTUAL.P3M_SALES_LP AS P3M_SALES_LP,
             ACTUAL.F3M_SALES AS F3M_SALES,
             ACTUAL.F3M_QTY AS F3M_QTY,
             ACTUAL.F3M_AVG_QTY AS F3M_AVG_QTY,
             ACTUAL.P6M_SALES AS P6M_SALES,
             ACTUAL.P6M_QTY AS P6M_QTY,
             ACTUAL.P6M_AVG_QTY AS P6M_AVG_QTY,
             ACTUAL.P6M_SALES_LP AS P6M_SALES_LP,
             ACTUAL.P12M_SALES AS P12M_SALES,
             ACTUAL.P12M_QTY AS P12M_QTY,
             ACTUAL.P12M_AVG_QTY AS P12M_AVG_QTY,
             ACTUAL.P12M_SALES_LP AS P12M_SALES_LP,
             COALESCE(ACTUAL.LM_SALES_FLAG,'N') AS LM_SALES_FLAG,
             COALESCE(ACTUAL.P3M_SALES_FLAG,'N') AS P3M_SALES_FLAG,
             COALESCE(ACTUAL.P6M_SALES_FLAG,'N') AS P6M_SALES_FLAG,
             COALESCE(ACTUAL.P12M_SALES_FLAG,'N') AS P12M_SALES_FLAG,
             'Y' AS MDP_FLAG,
             1 AS TARGET_COMPLAINCE
      FROM ITG_JP_RE_MSL_LIST TARGET
        LEFT JOIN (SELECT * FROM WKS_JAPAN_REGIONAL_SELLOUT_ACTUALS) ACTUAL
               ON TARGET.MNTH_ID = ACTUAL.MNTH_ID
              AND TARGET.DISTRIBUTOR_CODE = ACTUAL.DISTRIBUTOR_CODE
              AND TARGET.SOLD_TO_CODE = ACTUAL.SOLD_TO_CODE
              AND TARGET.STORE_CODE = ACTUAL.STORE_CODE
              AND TARGET.SKU_CD = ACTUAL.SKU_CODE
        LEFT JOIN (SELECT DISTINCT SKU.JAN_CD,
                          POP.SKU_CODE,
                          POP.SKU_ENGLISH AS SKU_NAME,
                          SKU.ITEM_CD,
                          POP.COUNTRY_L1 AS PROD_HIER_L1,
                          POP.REGIONAL_FRANCHISE_L2 AS PROD_HIER_L2,
                          POP.FRANCHISE_L3 AS PROD_HIER_L3,
                          POP.BRAND_L4 AS PROD_HIER_L4,
                          POP.SUB_CATEGORY_L5 AS PROD_HIER_L5,
                          POP.PLATFORM_L6 AS PROD_HIER_L6,
                          POP.VARIANCE_L7 AS PROD_HIER_L7,
                          POP.PACK_SIZE_L8 AS PROD_HIER_L8,
                          NULL AS PROD_HIER_L9
                   FROM EDW_VW_POP6_PRODUCTS POP
                     JOIN VW_JAN_CHANGE SKU ON SKU.JAN_CD = POP.SKU_CODE
                   WHERE POP.COUNTRY_L1 = 'Japan') PROD ON TARGET.SKU_CD = PROD.ITEM_CD
                   LEFT JOIN (SELECT *
                   FROM (SELECT DISTINCT ECBD.CUST_NUM AS SAP_CUST_ID,
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
                                REG_JAPAN.REGION AS REGION,
                                REG_JAPAN.PREFECTURE_NAME_ENG AS ZONE_OR_AREA,
                                EGCH.GCGH_REGION AS GCH_REGION,
                                EGCH.GCGH_CLUSTER AS GCH_CLUSTER,
                                EGCH.GCGH_SUBCLUSTER AS GCH_SUBCLUSTER,
                                EGCH.GCGH_MARKET AS GCH_MARKET,
                                EGCH.GCCH_RETAIL_BANNER AS GCH_RETAIL_BANNER,
                                ECSD.SEGMT_KEY AS CUST_SEGMT_KEY,
                                'NA' AS CUST_SEGMENT_DESC,
                                ROW_NUMBER() OVER (PARTITION BY SAP_CUST_ID ORDER BY SAP_PRNT_CUST_KEY DESC) AS RANK
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
                              --    RG_EDW.EDW_CODE_DESCRIPTIONS_MANUAL CODES_SEGMENT,
                         (SELECT DISTINCT CUST_NUM,
                                 REC_CRT_DT,
                                 PRNT_CUST_KEY,
                                 ROW_NUMBER() OVER (PARTITION BY CUST_NUM ORDER BY REC_CRT_DT DESC) RN
                          FROM EDW_CUSTOMER_SALES_DIM) A,
                              (SELECT DISTINCT JIS_CD,
                                      REGION,
                                      PREFECTURE_NAME_ENG
                               FROM JP_INV_COVERAGE_AREA_REGION_MAPPING) REG_JAPAN,
                              (SELECT DISTINCT CSTM_CD, JIS_PRFCT_CD FROM EDI_CSTM_M) REG_CUST_JAP_REGION
                         WHERE EGCH.CUSTOMER (+) = ECBD.CUST_NUM
                         AND   ECSD.CUST_NUM = ECBD.CUST_NUM
                         AND   LTRIM(ECBD.CUST_NUM,0) = REG_CUST_JAP_REGION.CSTM_CD (+)
                         AND   REG_CUST_JAP_REGION.JIS_PRFCT_CD = REG_JAPAN.JIS_CD
                         AND   A.CUST_NUM = ECSD.CUST_NUM
                         AND   ECSD.DSTR_CHNL = EDC.DISTR_CHAN
                         AND   ECSD.SLS_ORG = ESOD.SLS_ORG
                         AND   ESOD.SLS_ORG_CO_CD = ECD.CO_CD
                         AND   ECSD.SLS_ORG IN (SELECT DISTINCT SLS_ORG
                                                FROM EDW_SALES_ORG_DIM
                                                WHERE STATS_CRNCY IN ('JPY'))
                         AND   A.RN = 1
                         AND   TRIM(UPPER(CDDES_PCK.CODE_TYPE (+))) = 'PARENT CUSTOMER KEY'
                         AND   CDDES_PCK.CODE (+) = ECSD.PRNT_CUST_KEY
                         AND   TRIM(UPPER(CDDES_BNRKEY.CODE_TYPE (+))) = 'BANNER KEY'
                         AND   CDDES_BNRKEY.CODE (+) = ECSD.BNR_KEY
                         AND   TRIM(UPPER(CDDES_BNRFMT.CODE_TYPE (+))) = 'BANNER FORMAT KEY'
                         AND   CDDES_BNRFMT.CODE (+) = ECSD.BNR_FRMT_KEY
                         AND   TRIM(UPPER(CDDES_CHNL.CODE_TYPE (+))) = 'CHANNEL KEY'
                         AND   CDDES_CHNL.CODE (+) = ECSD.CHNL_KEY
                         AND   TRIM(UPPER(CDDES_GTM.CODE_TYPE (+))) = 'GO TO MODEL KEY'
                         AND   CDDES_GTM.CODE (+) = ECSD.GO_TO_MDL_KEY
                         AND   TRIM(UPPER(CDDES_SUBCHNL.CODE_TYPE (+))) = 'SUB CHANNEL KEY'
                         AND   CDDES_SUBCHNL.CODE (+) = ECSD.SUB_CHNL_KEY
                         AND   UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL (+)) = UPPER(CDDES_SUBCHNL.CODE_DESC))
                   WHERE RANK = 1) CUSTOMER ON nvl(ltrim(ACTUAL.SOLD_TO_CODE,0), ltrim(TARGET.SOLD_TO_CODE,0)) = LTRIM (CUSTOMER.SAP_CUST_ID,'0')
      ----------------product hierarchy------------------------------   				   
      
        LEFT JOIN (SELECT *
                   FROM (SELECT * FROM edw_generic_product_hierarchy)
                   WHERE RANK = 1) PRODUCT ON LTRIM (nvl(ACTUAL.SKU_CODE,TARGET.SKU_CD),'0') = LTRIM (PRODUCT.SAP_MATL_NUM,'0')
        -----pka----------------------------
		LEFT JOIN (SELECT * FROM edw_generic_product_key_attributes WHERE CTRY_NM = 'Japan') PROD_KEY1 ON LTRIM (nvl(ACTUAL.SKU_CODE,TARGET.SKU_CD),'0') = LTRIM (PROD_KEY1.SKU,'0')
        LEFT JOIN (SELECT LTRIM(EDW_LIST_PRICE.MATERIAL,'0') AS MATERIAL,
                          EDW_LIST_PRICE.AMOUNT AS LIST_PRICE,
                          ROW_NUMBER() OVER (PARTITION BY LTRIM(EDW_LIST_PRICE.MATERIAL,'0') ORDER BY TO_DATE(EDW_LIST_PRICE.VALID_TO,'YYYYMMDD') DESC,TO_DATE(EDW_LIST_PRICE.DT_FROM,'YYYYMMDD') DESC) AS RN
                   FROM EDW_LIST_PRICE
                   WHERE EDW_LIST_PRICE.SLS_ORG IN ('3110','3100')) LP
               ON LTRIM (nvl(ACTUAL.SKU_CODE,TARGET.SKU_CD),'0') = LP.MATERIAL   -- apply colease function
              AND RN = 1)MDP,
     (SELECT DISTINCT "cluster"
      FROM EDW_COMPANY_DIM
      WHERE CTRY_GROUP = 'Japan') COM
WHERE FISC_PER <= (SELECT MAX(mnth_id)
                   FROM WKS_JAPAN_REGIONAL_SELLOUT_ACTUALS)
)				   
UNION ALL
(
SELECT NON_MDP.*,
       COM.*,
       SYSDATE() AS CRT_DTTM
FROM (SELECT CAST(ACTUAL.YEAR AS INTEGER) AS YEAR,
             ACTUAL.MNTH_ID,
             ACTUAL.CNTRY_NM AS MARKET,
			 ACTUAL.DATA_SOURCE,
             STORE_DET.SEGMENT AS CHANNEL,
             ACTUAL.SOLD_TO_CODE,
             ACTUAL.DISTRIBUTOR_CODE,
             ACTUAL.DISTRIBUTOR_NAME,
             STORE_DET.SEGMENT AS SELL_OUT_CHANNEL,
			 COALESCE(STORE_DET.SELL_OUT_RE, 'Not defined') AS RETAIL_ENVIRONMENT,
             --STORE_DET.SELL_OUT_RE AS RETAIL_ENVIRONMENT,
             ACTUAL.STORE_CODE AS STORE_CODE,
             ACTUAL.STORE_NAME AS STORE_NAME,
             STORE_DET.REGION_NAME_ENG AS REGION,
             STORE_DET.ZONE_NAME_ENG AS ZONE_NAME,
             NULL AS CITY,
             STORE_DET.STORE_ADDRESS,
             STORE_DET.POST_CODE,
             ACTUAL.SKU_CODE AS PRODUCT_CODE,
             PROD.SKU_NAME AS PRODUCT_NAME,
             'Japan' AS PROD_HIER_L1,
             PROD.PROD_HIER_L2,
             PROD.PROD_HIER_L3,
             PROD.PROD_HIER_L4,
             PROD.PROD_HIER_L5,
             PROD.PROD_HIER_L6,
             PROD.PROD_HIER_L7,
             PROD.PROD_HIER_L8,
             PROD.PROD_HIER_L9,	
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_KEY,CUSTOMER.CUST_SEGMT_KEY) AS CUSTOMER_SEGMENT_KEY,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION,CUSTOMER.CUST_SEGMENT_DESC) AS CUSTOMER_SEGMENT_DESCRIPTION,
             COALESCE(ACTUAL.SAP_CUSTOMER_CHANNEL_KEY,CUSTOMER.SAP_CUST_CHNL_KEY) AS SAP_CUSTOMER_CHANNEL_KEY,
             COALESCE(ACTUAL.SAP_CUSTOMER_CHANNEL_DESCRIPTION,CUSTOMER.SAP_CUST_CHNL_DESC) AS SAP_CUSTOMER_CHANNEL_DESCRIPTION,
             COALESCE(ACTUAL.SAP_CUSTOMER_SUB_CHANNEL_KEY,CUSTOMER.SAP_CUST_SUB_CHNL_KEY) AS SAP_CUSTOMER_SUB_CHANNEL_KEY,
             COALESCE(ACTUAL.SAP_SUB_CHANNEL_DESCRIPTION,CUSTOMER.SAP_SUB_CHNL_DESC) AS SAP_SUB_CHANNEL_DESCRIPTION,
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_KEY,CUSTOMER.SAP_PRNT_CUST_KEY) AS SAP_PARENT_CUSTOMER_KEY,
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_DESCRIPTION,CUSTOMER.SAP_PRNT_CUST_DESC) AS SAP_PARENT_CUSTOMER_DESCRIPTION,
             COALESCE(ACTUAL.SAP_BANNER_KEY,CUSTOMER.SAP_BNR_KEY) AS SAP_BANNER_KEY,
             COALESCE(ACTUAL.SAP_BANNER_DESCRIPTION,CUSTOMER.SAP_BNR_DESC) AS SAP_BANNER_DESCRIPTION,
             COALESCE(ACTUAL.SAP_BANNER_FORMAT_KEY,CUSTOMER.SAP_BNR_FRMT_KEY) AS SAP_BANNER_FORMAT_KEY,
             COALESCE(ACTUAL.SAP_BANNER_FORMAT_DESCRIPTION,CUSTOMER.SAP_BNR_FRMT_DESC) AS SAP_BANNER_FORMAT_DESCRIPTION,
             TRIM(NVL (NULLIF(CUSTOMER.SAP_CUST_NM,''),'NA')) AS CUSTOMER_NAME,
             TRIM(NVL (NULLIF(CUSTOMER.SAP_CUST_ID,''),'NA')) AS CUSTOMER_CODE,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_SGMT_CD,''),'NA')) AS SAP_PROD_SGMT_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_SGMT_DESC,''),'NA')) AS SAP_PROD_SGMT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_BASE_PROD_DESC,''),'NA')) AS SAP_BASE_PROD_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MEGA_BRND_DESC,''),'NA')) AS SAP_MEGA_BRND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_BRND_DESC,''),'NA')) AS SAP_BRND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_VRNT_DESC,''),'NA')) AS SAP_VRNT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PUT_UP_DESC,''),'NA')) AS SAP_PUT_UP_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_GRP_FRNCHSE_CD,''),'NA')) AS SAP_GRP_FRNCHSE_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_GRP_FRNCHSE_DESC,''),'NA')) AS SAP_GRP_FRNCHSE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_FRNCHSE_CD,''),'NA')) AS SAP_FRNCHSE_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_FRNCHSE_DESC,''),'NA')) AS SAP_FRNCHSE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_FRNCHSE_CD,''),'NA')) AS SAP_PROD_FRNCHSE_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_FRNCHSE_DESC,''),'NA')) AS SAP_PROD_FRNCHSE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MJR_CD,''),'NA')) AS SAP_PROD_MJR_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MJR_DESC,''),'NA')) AS SAP_PROD_MJR_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MNR_CD,''),'NA')) AS SAP_PROD_MNR_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MNR_DESC,''),'NA')) AS SAP_PROD_MNR_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_HIER_CD,''),'NA')) AS SAP_PROD_HIER_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_HIER_DESC,''),'NA')) AS SAP_PROD_HIER_DESC,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,PRODUCT.GPH_PROD_FRNCHSE) AS GLOBAL_PRODUCT_FRANCHISE,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_BRAND,PRODUCT.GPH_PROD_BRND) AS GLOBAL_PRODUCT_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,PRODUCT.GPH_PROD_SUB_BRND) AS GLOBAL_PRODUCT_SUB_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_VARIANT,PRODUCT.GPH_PROD_VRNT) AS GLOBAL_PRODUCT_VARIANT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SEGMENT,PRODUCT.GPH_PROD_NEEDSTATE) AS GLOBAL_PRODUCT_SEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,PRODUCT.GPH_PROD_SUBSGMNT) AS GLOBAL_PRODUCT_SUBSEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_CATEGORY,PRODUCT.GPH_PROD_CTGRY) AS GLOBAL_PRODUCT_CATEGORY,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,PRODUCT.GPH_PROD_SUBCTGRY) AS GLOBAL_PRODUCT_SUBCATEGORY,
             COALESCE(ACTUAL.GLOBAL_PUT_UP_DESCRIPTION,PRODUCT.GPH_PROD_PUT_UP_DESC) AS GLOBAL_PUT_UP_DESCRIPTION,
             --TRIM(NVL (NULLIF(PRODUCT.EAN,''),'NA')) AS EAN,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA')) AS SKU_CODE,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA')) AS SKU_DESCRIPTION,
             TRIM(NVL (NULLIF(PRODUCT.PKA_FRANCHISE_DESC,''),'NA')) AS PKA_FRANCHISE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_BRAND_DESC,''),'NA')) AS PKA_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_BRAND_DESC,''),'NA')) AS PKA_SUB_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_VARIANT_DESC,''),'NA')) AS PKA_VARIANT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_VARIANT_DESC,''),'NA')) AS PKA_SUB_VARIANT_DESC,
             COALESCE(ACTUAL.PKA_PRODUCT_KEY,PRODUCT.PKA_PRODUCT_KEY) AS PKA_PRODUCT_KEY,
             COALESCE(ACTUAL.PKA_PRODUCT_KEY_DESCRIPTION,PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION) AS PKA_PRODUCT_KEY_DESCRIPTION,		 
             ACTUAL.CM_SALES AS SALES_VALUE,
             ACTUAL.CM_SALES_QTY AS SALES_QTY,
             ACTUAL.CM_AVG_SALES_QTY AS AVG_SALES_QTY,
             ACTUAL.CM_SALES_VALUE_LIST_PRICE AS SALES_VALUE_LIST_PRICE,
             ACTUAL.LM_SALES AS LM_SALES,
             ACTUAL.LM_SALES_QTY AS LM_SALES_QTY,
             ACTUAL.LM_AVG_SALES_QTY AS LM_AVG_SALES_QTY,
             ACTUAL.LM_SALES_LP AS LM_SALES_LP,
             ACTUAL.P3M_SALES AS P3M_SALES,
             ACTUAL.P3M_QTY AS P3M_QTY,
             ACTUAL.P3M_AVG_QTY AS P3M_AVG_QTY,
             ACTUAL.P3M_SALES_LP AS P3M_SALES_LP,
             ACTUAL.F3M_SALES AS F3M_SALES,
             ACTUAL.F3M_QTY AS F3M_QTY,
             ACTUAL.F3M_AVG_QTY AS F3M_AVG_QTY,
             ACTUAL.P6M_SALES AS P6M_SALES,
             ACTUAL.P6M_QTY AS P6M_QTY,
             ACTUAL.P6M_AVG_QTY AS P6M_AVG_QTY,
             ACTUAL.P6M_SALES_LP AS P6M_SALES_LP,
             ACTUAL.P12M_SALES AS P12M_SALES,
             ACTUAL.P12M_QTY AS P12M_QTY,
             ACTUAL.P12M_AVG_QTY AS P12M_AVG_QTY,
             ACTUAL.P12M_SALES_LP AS P12M_SALES_LP,
             ACTUAL.LM_SALES_FLAG,
             ACTUAL.P3M_SALES_FLAG,
             ACTUAL.P6M_SALES_FLAG,
             ACTUAL.P12M_SALES_FLAG,
             'N' AS MDP_FLAG,
              1 AS TARGET_COMPLAINCE
      FROM (SELECT *
            FROM WKS_JAPAN_REGIONAL_SELLOUT_ACTUALS A
            WHERE NOT EXISTS (SELECT 1
                              FROM ITG_JP_RE_MSL_LIST T
                              WHERE A.MNTH_ID = T.MNTH_ID
                              AND   A.DISTRIBUTOR_CODE = T.DISTRIBUTOR_CODE
                              AND   A.SOLD_TO_CODE = T.SOLD_TO_CODE
                              AND   A.STORE_CODE = T.STORE_CODE
                              AND   A.SKU_CODE = T.SKU_CD)) ACTUAL
        LEFT JOIN (SELECT DISTINCT CHN.CHN_OFFC_CD AS SELL_OUT_PARENT_CUSTOMER_L1,
                          CHN.CHN_CD AS SELL_OUT_CHILD_CUSTOMER_L2,
                          CHN.CMMN_NM AS PARENT_NAME1,
                          CHN_HQ.CMMN_NM AS CHILD_NAME2,
                          CHN.SGMT AS SEGMENT,
                          ENG.NAME_ENG AS SELL_OUT_RE,
                          CHN_HQ.LGL_NM AS DISTRIBUTOR_CODE,
                          STORE.JIS_PRFCT_C AS REGION_CODE,
                          PRF.PRF_NM_KNJ AS REGION_NAME,
                          REG_JAPAN.REGION AS REGION_NAME_ENG,
                          REG_JAPAN.PREFECTURE_NAME_KANA AS ZONE_NAME,
                          REG_JAPAN.PREFECTURE_NAME_ENG AS ZONE_NAME_ENG,
                          STORE.STR_CD AS STORE_CODE,
                          STORE.CMMN_NM_KNJ AS STORE_NAME,
                          STORE.ADRS_KNJ1 AS STORE_ADDRESS,
                          STORE.PST_CO AS POST_CODE
                   FROM EDI_STORE_M STORE
                     LEFT JOIN EDI_CHN_M CHN ON STORE.CHN_CD = CHN.CHN_CD
                     LEFT JOIN EDI_CHN_M CHN_HQ ON CHN_HQ.CHN_CD = CHN.CHN_OFFC_CD
                     LEFT JOIN MT_SGMT SGMT ON CHN.SGMT = SGMT.SGMT
                     LEFT JOIN MT_PRF PRF ON PRF.PRF_CD = STORE.JIS_PRFCT_C
                     LEFT JOIN JP_INV_COVERAGE_AREA_REGION_MAPPING REG_JAPAN ON PRF.PRF_CD = REG_JAPAN.JIS_CD
                     LEFT JOIN SDL_MDS_JP_C360_ENG_TRANSLATION ENG ON SGMT.SGMT_NM_REP = ENG.NAME_JP) STORE_DET ON STORE_DET.STORE_CODE = ACTUAL.STORE_CODE
        LEFT JOIN (SELECT DISTINCT SKU.JAN_CD,
                          POP.SKU_CODE,
                          POP.SKU_ENGLISH AS SKU_NAME,
                          SKU.ITEM_CD,
                          POP.COUNTRY_L1 AS PROD_HIER_L1,
                          POP.REGIONAL_FRANCHISE_L2 AS PROD_HIER_L2,
                          POP.FRANCHISE_L3 AS PROD_HIER_L3,
                          POP.BRAND_L4 AS PROD_HIER_L4,
                          POP.SUB_CATEGORY_L5 AS PROD_HIER_L5,
                          POP.PLATFORM_L6 AS PROD_HIER_L6,
                          POP.VARIANCE_L7 AS PROD_HIER_L7,
                          POP.PACK_SIZE_L8 AS PROD_HIER_L8,
                          NULL AS PROD_HIER_L9
                   FROM EDW_VW_POP6_PRODUCTS POP
                     JOIN VW_JAN_CHANGE SKU ON SKU.JAN_CD = POP.SKU_CODE
                   WHERE POP.COUNTRY_L1 = 'Japan') PROD ON ACTUAL.SKU_CODE = PROD.ITEM_CD
                   LEFT JOIN (SELECT *
                   FROM (SELECT DISTINCT ECBD.CUST_NUM AS SAP_CUST_ID,
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
                                REG_JAPAN.REGION AS REGION,
                                REG_JAPAN.PREFECTURE_NAME_ENG AS ZONE_OR_AREA,
                                EGCH.GCGH_REGION AS GCH_REGION,
                                EGCH.GCGH_CLUSTER AS GCH_CLUSTER,
                                EGCH.GCGH_SUBCLUSTER AS GCH_SUBCLUSTER,
                                EGCH.GCGH_MARKET AS GCH_MARKET,
                                EGCH.GCCH_RETAIL_BANNER AS GCH_RETAIL_BANNER,
                                ECSD.SEGMT_KEY AS CUST_SEGMT_KEY,
                                'NA' AS CUST_SEGMENT_DESC,
                                ROW_NUMBER() OVER (PARTITION BY SAP_CUST_ID ORDER BY SAP_PRNT_CUST_KEY DESC) AS RANK
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
                              --    EDW_CODE_DESCRIPTIONS_MANUAL CODES_SEGMENT,
                         (SELECT DISTINCT CUST_NUM,
                                 REC_CRT_DT,
                                 PRNT_CUST_KEY,
                                 ROW_NUMBER() OVER (PARTITION BY CUST_NUM ORDER BY REC_CRT_DT DESC) RN
                          FROM EDW_CUSTOMER_SALES_DIM) A,
                              (SELECT DISTINCT JIS_CD,
                                      REGION,
                                      PREFECTURE_NAME_ENG
                               FROM JP_INV_COVERAGE_AREA_REGION_MAPPING) REG_JAPAN,
                              (SELECT DISTINCT CSTM_CD, JIS_PRFCT_CD FROM EDI_CSTM_M) REG_CUST_JAP_REGION
                         WHERE EGCH.CUSTOMER (+) = ECBD.CUST_NUM
                         AND   ECSD.CUST_NUM = ECBD.CUST_NUM
                         AND   LTRIM(ECBD.CUST_NUM,0) = REG_CUST_JAP_REGION.CSTM_CD (+)
                         AND   REG_CUST_JAP_REGION.JIS_PRFCT_CD = REG_JAPAN.JIS_CD
                         AND   A.CUST_NUM = ECSD.CUST_NUM
                         AND   ECSD.DSTR_CHNL = EDC.DISTR_CHAN
                         AND   ECSD.SLS_ORG = ESOD.SLS_ORG
                         AND   ESOD.SLS_ORG_CO_CD = ECD.CO_CD
                         AND   ECSD.SLS_ORG IN (SELECT DISTINCT SLS_ORG
                                                FROM EDW_SALES_ORG_DIM
                                                WHERE STATS_CRNCY IN ('JPY'))
                         AND   A.RN = 1
                         AND   TRIM(UPPER(CDDES_PCK.CODE_TYPE (+))) = 'PARENT CUSTOMER KEY'
                         AND   CDDES_PCK.CODE (+) = ECSD.PRNT_CUST_KEY
                         AND   TRIM(UPPER(CDDES_BNRKEY.CODE_TYPE (+))) = 'BANNER KEY'
                         AND   CDDES_BNRKEY.CODE (+) = ECSD.BNR_KEY
                         AND   TRIM(UPPER(CDDES_BNRFMT.CODE_TYPE (+))) = 'BANNER FORMAT KEY'
                         AND   CDDES_BNRFMT.CODE (+) = ECSD.BNR_FRMT_KEY
                         AND   TRIM(UPPER(CDDES_CHNL.CODE_TYPE (+))) = 'CHANNEL KEY'
                         AND   CDDES_CHNL.CODE (+) = ECSD.CHNL_KEY
                         AND   TRIM(UPPER(CDDES_GTM.CODE_TYPE (+))) = 'GO TO MODEL KEY'
                         AND   CDDES_GTM.CODE (+) = ECSD.GO_TO_MDL_KEY
                         AND   TRIM(UPPER(CDDES_SUBCHNL.CODE_TYPE (+))) = 'SUB CHANNEL KEY'
                         AND   CDDES_SUBCHNL.CODE (+) = ECSD.SUB_CHNL_KEY
                         AND   UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL (+)) = UPPER(CDDES_SUBCHNL.CODE_DESC))
                   WHERE RANK = 1) CUSTOMER ON LTRIM (ACTUAL.SOLD_TO_CODE,'0') = LTRIM (CUSTOMER.SAP_CUST_ID,'0')
      ----------------product hierarchy------------------------------   				   
      
        LEFT JOIN (SELECT *
                   FROM (SELECT * FROM edw_generic_product_hierarchy)
						 ---generic
                   WHERE RANK = 1) PRODUCT ON LTRIM (ACTUAL.SKU_CODE,'0') = LTRIM (PRODUCT.SAP_MATL_NUM,'0')
        LEFT JOIN (SELECT * FROM edw_generic_product_key_attributes WHERE CTRY_NM = 'Japan') PROD_KEY1 ON LTRIM (ACTUAL.SKU_CODE,'0') = LTRIM (PROD_KEY1.SKU,'0')
        LEFT JOIN (SELECT LTRIM(EDW_LIST_PRICE.MATERIAL,'0') AS MATERIAL,
                          EDW_LIST_PRICE.AMOUNT AS LIST_PRICE,
                          ROW_NUMBER() OVER (PARTITION BY LTRIM(EDW_LIST_PRICE.MATERIAL,'0') ORDER BY TO_DATE(EDW_LIST_PRICE.VALID_TO,'YYYYMMDD') DESC,TO_DATE(EDW_LIST_PRICE.DT_FROM,'YYYYMMDD') DESC) AS RN
                   FROM EDW_LIST_PRICE
                   WHERE EDW_LIST_PRICE.SLS_ORG IN ('3110','3100')) LP
               ON LTRIM (ACTUAL.SKU_CODE,'0') = LP.MATERIAL
              AND RN = 1) NON_MDP,
     (SELECT DISTINCT "cluster"
      FROM EDW_COMPANY_DIM
      WHERE CTRY_GROUP = 'Japan') COM
)
),

final as (
select
fisc_yr::integer AS fisc_yr,
fisc_per::varchar(22) AS fisc_per,
market::varchar(5) AS market,
data_source::varchar(8) AS data_source,
channel_name::varchar(256) AS channel_name,
sold_to_code::varchar(255) AS sold_to_code,
distributor_code::varchar(100) AS distributor_code,
distributor_name::varchar(457) AS distributor_name,
sell_out_channel::varchar(256) AS sell_out_channel,
retail_environment::varchar(200) AS retail_environment,
store_code::varchar(100) AS store_code,
store_name::varchar(702) AS store_name,
region::varchar(150) AS region,
zone_name::varchar(150) AS zone_name,
city::varchar(1) AS city,
store_address::varchar(256) AS store_address,
post_code::varchar(256) AS post_code,
product_code::varchar(100) AS product_code,
product_name::varchar(200) AS product_name,
prod_hier_l1::varchar(5) AS prod_hier_l1,
prod_hier_l2::varchar(200) AS prod_hier_l2,
prod_hier_l3::varchar(200) AS prod_hier_l3,
prod_hier_l4::varchar(200) AS prod_hier_l4,
prod_hier_l5::varchar(200) AS prod_hier_l5,
prod_hier_l6::varchar(200) AS prod_hier_l6,
prod_hier_l7::varchar(200) AS prod_hier_l7,
prod_hier_l8::varchar(200) AS prod_hier_l8,
prod_hier_l9::varchar(1) AS prod_hier_l9,
customer_segment_key::varchar(12) AS customer_segment_key,
customer_segment_description::varchar(50) AS customer_segment_description,
sap_customer_channel_key::varchar(12) AS sap_customer_channel_key,
sap_customer_channel_description::varchar(75) AS sap_customer_channel_description,
sap_customer_sub_channel_key::varchar(12) AS sap_customer_sub_channel_key,
sap_sub_channel_description::varchar(75) AS sap_sub_channel_description,
sap_parent_customer_key::varchar(12) AS sap_parent_customer_key,
sap_parent_customer_description::varchar(75) AS sap_parent_customer_description,
sap_banner_key::varchar(12) AS sap_banner_key,
sap_banner_description::varchar(75) AS sap_banner_description,
sap_banner_format_key::varchar(12) AS sap_banner_format_key,
sap_banner_format_description::varchar(75) AS sap_banner_format_description,
customer_name::varchar(100) AS customer_name,
customer_code::varchar(10) AS customer_code,
sap_prod_sgmt_cd::varchar(18) AS sap_prod_sgmt_cd,
sap_prod_sgmt_desc::varchar(100) AS sap_prod_sgmt_desc,
sap_base_prod_desc::varchar(100) AS sap_base_prod_desc,
sap_mega_brnd_desc::varchar(100) AS sap_mega_brnd_desc,
sap_brnd_desc::varchar(100) AS sap_brnd_desc,
sap_vrnt_desc::varchar(100) AS sap_vrnt_desc,
sap_put_up_desc::varchar(100) AS sap_put_up_desc,
sap_grp_frnchse_cd::varchar(18) AS sap_grp_frnchse_cd,
sap_grp_frnchse_desc::varchar(100) AS sap_grp_frnchse_desc,
sap_frnchse_cd::varchar(18) AS sap_frnchse_cd,
sap_frnchse_desc::varchar(100) AS sap_frnchse_desc,
sap_prod_frnchse_cd::varchar(18) AS sap_prod_frnchse_cd,
sap_prod_frnchse_desc::varchar(100) AS sap_prod_frnchse_desc,
sap_prod_mjr_cd::varchar(18) AS sap_prod_mjr_cd,
sap_prod_mjr_desc::varchar(100) AS sap_prod_mjr_desc,
sap_prod_mnr_cd::varchar(18) AS sap_prod_mnr_cd,
sap_prod_mnr_desc::varchar(100) AS sap_prod_mnr_desc,
sap_prod_hier_cd::varchar(18) AS sap_prod_hier_cd,
sap_prod_hier_desc::varchar(100) AS sap_prod_hier_desc,
global_product_franchise::varchar(30) AS global_product_franchise,
global_product_brand::varchar(30) AS global_product_brand,
global_product_sub_brand::varchar(100) AS global_product_sub_brand,
global_product_variant::varchar(100) AS global_product_variant,
global_product_segment::varchar(50) AS global_product_segment,
global_product_subsegment::varchar(100) AS global_product_subsegment,
global_product_category::varchar(50) AS global_product_category,
global_product_subcategory::varchar(50) AS global_product_subcategory,
global_put_up_description::varchar(100) AS global_put_up_description,
sku_code::varchar(40) AS sku_code,
sku_description::varchar(100) AS sku_description,
pka_franchise_desc::varchar(30) AS pka_franchise_desc,
pka_brand_desc::varchar(30) AS pka_brand_desc,
pka_sub_brand_desc::varchar(30) AS pka_sub_brand_desc,
pka_variant_desc::varchar(30) AS pka_variant_desc,
pka_sub_variant_desc::varchar(30) AS pka_sub_variant_desc,
pka_product_key::varchar(68) AS pka_product_key,
pka_product_key_description::varchar(255) AS pka_product_key_description,
sales_value::numeric(38,6) AS sales_value,
sales_qty::numeric(38,6) AS sales_qty,
avg_sales_qty::numeric(38,6) AS avg_sales_qty,
sales_value_list_price::numeric(38,12) AS sales_value_list_price,
lm_sales::numeric(38,6) AS lm_sales,
lm_sales_qty::numeric(38,6) AS lm_sales_qty,
lm_avg_sales_qty::numeric(38,6) AS lm_avg_sales_qty,
lm_sales_lp::numeric(38,12) AS lm_sales_lp,
p3m_sales::numeric(38,6) AS p3m_sales,
p3m_qty::numeric(38,6) AS p3m_qty,
p3m_avg_qty::numeric(38,6) AS p3m_avg_qty,
p3m_sales_lp::numeric(38,12) AS p3m_sales_lp,
f3m_sales::numeric(38,6) AS f3m_sales,
f3m_qty::numeric(38,6) AS f3m_qty,
f3m_avg_qty::numeric(38,6) AS f3m_avg_qty,
p6m_sales::numeric(38,6) AS p6m_sales,
p6m_qty::numeric(38,6) AS p6m_qty,
p6m_avg_qty::numeric(38,6) AS p6m_avg_qty,
p6m_sales_lp::numeric(38,12) AS p6m_sales_lp,
p12m_sales::numeric(38,6) AS p12m_sales,
p12m_qty::numeric(38,6) AS p12m_qty,
p12m_avg_qty::numeric(38,6) AS p12m_avg_qty,
p12m_sales_lp::numeric(38,12) AS p12m_sales_lp,
lm_sales_flag::varchar(1) AS lm_sales_flag,
p3m_sales_flag::varchar(1) AS p3m_sales_flag,
p6m_sales_flag::varchar(1) AS p6m_sales_flag,
p12m_sales_flag::varchar(1) AS p12m_sales_flag,
mdp_flag::varchar(1) AS mdp_flag,
target_complaince::integer AS target_complaince,
"cluster"::varchar(100) AS cluster,
crt_dttm::timestamp AS crt_dttm
from transformation
)
select * from final