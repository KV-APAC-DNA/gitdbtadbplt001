--import cte
with itg_vn_re_msl_list as (
    select * from {{ ref('vnmitg_integration__itg_vn_re_msl_list') }}
),
wks_vn_regional_sellout_actuals as (
    select * from {{ ref('vnmwks_integration__wks_vn_regional_sellout_actuals' )}}
),
v_rpt_pop6_perfectstore as (
    select * from {{ source('ntaedw_integration','v_rpt_pop6_perfectstore') }}
),
customer_hierarchy as (
    select * from {{ ref('aspedw_integration__edw_generic_customer_hierarchy') }}
),
product_heirarchy as (
    select * from {{ ref('aspedw_integration__edw_generic_product_hierarchy') }}
),
edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_customer_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
EDW_GCH_CUSTOMERHIERARCHY as(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
EDW_CUSTOMER_BASE_DIM as(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
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
EDW_CODE_DESCRIPTIONS_MANUAL as(
    select * from {{ source('aspedw_integration', 'edw_code_descriptions_manual') }}
),
edw_product_key_attributes as (
    select * from {{ ref('aspedw_integration__edw_product_key_attributes')}}
),
itg_vn_product_mapping as (
    select * from {{ ref('vnmitg_integration__itg_vn_product_mapping')}}
),

--final cte

vn_rpt_retail_excellence_mdp as 
(
    SELECT DISTINCT MDP.*,
       COM."cluster",	
	   SYSDATE() AS CRTD_DTTM
FROM (SELECT CAST(TARGET.FISC_YR AS numeric(18,0) ) AS FISC_YR,	
             CAST(TARGET.FISC_PER AS numeric(18,0) ) AS FISC_PER,	
             COALESCE(ACTUAL.CNTRY_NM, TARGET.MARKET) AS MARKET,	
			 NVL(ACTUAL.DATA_SRC,TARGET.DATA_SRC) as data_Src,	
             UPPER(NVL(ACTUAL.CHANNEL,TARGET.SELL_OUT_CHANNEL)) as CHANNEL_NAME,
             NVL(ACTUAL.SOLDTO_CODE, TARGET.SOLDTO_CODE) AS SOLDTO_CODE,	
             UPPER(NVL(ACTUAL.DISTRIBUTOR_CODE,TARGET.DISTRIBUTOR_CODE)) as DISTRIBUTOR_CODE,
             UPPER(NVL(ACTUAL.DISTRIBUTOR_NAME,TARGET.DISTRIBUTOR_NAME)) as DISTRIBUTOR_NAME,
             UPPER(NVL(ACTUAL.CHANNEL,TARGET.SELL_OUT_CHANNEL)) as SELL_OUT_CHANNEL,
             UPPER(NVL(ACTUAL.STORE_TYPE,TARGET.STORE_TYPE)) as store_type,	
             NULL AS PRIORITIZATION_SEGMENTATION,
             NULL AS STORE_CATEGORY,
             UPPER(NVL(ACTUAL.STORE_CODE,TARGET.STORE_CODE)) AS STORE_CODE,	
             UPPER(NVL(ACTUAL.STORE_NAME,TARGET.STORE_NAME)) as STORE_NAME,	
             null as STORE_GRADE,
             'NA' as STORE_SIZE,
             NVL(ACTUAL.REGION,'NA') as REGION,	
             NVL(ACTUAL.ZONE,'NA') as ZONE_NAME,	
             'NA' as CITY,
             NULL AS RTRLATITUDE,
             NULL AS RTRLONGITUDE,
             UPPER(NVL(ACTUAL.RETAIL_ENVIRONMENT,TARGET.RETAIL_ENVIRONMENT)) AS Sell_Out_RE,
             NVL(ACTUAL.EAN,TARGET.EAN) AS PRODUCT_CODE,	
             COALESCE(ACTUAL.SKU_DESCRIPTION,TARGET.SKU_DESCRIPTION,'NA') AS PRODUCT_NAME,
             TARGET.PROD_HIER_L1 AS PROD_HIER_L1,	
             TARGET.PROD_HIER_L2 AS PROD_HIER_L2,	
             TARGET.PROD_HIER_L3 AS PROD_HIER_L3,	
             TARGET.PROD_HIER_L4 AS  PROD_HIER_L4,	
             NULL AS PROD_HIER_L5,
             NULL AS PROD_HIER_L6,
             NULL AS PROD_HIER_L7,
             NULL AS PROD_HIER_L8,
             NULL AS PROD_HIER_L9,
             NVL(ACTUAL.SKU_CODE,TARGET.SKU_CODE) AS MAPPED_SKU_CD,	
			 ACTUAL.LIST_PRICE,	
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_KEY,CUSTOMER.CUST_SEGMT_KEY,'NA') AS CUSTOMER_SEGMENT_KEY,	
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION,CUSTOMER.CUST_SEGMENT_DESC,'NA') AS CUSTOMER_SEGMENT_DESCRIPTION,	
             COALESCE(ACTUAL.RETAIL_ENVIRONMENT,TARGET.RETAIL_ENVIRONMENT,'NA') AS RETAIL_ENVIRONMENT,
             COALESCE(ACTUAL.SAP_CUSTOMER_CHANNEL_KEY,CUSTOMER.SAP_CUST_CHNL_KEY,'NA') AS SAP_CUSTOMER_CHANNEL_KEY,	
             COALESCE(ACTUAL.SAP_CUSTOMER_CHANNEL_DESCRIPTION,CUSTOMER.SAP_CUST_CHNL_DESC,'NA') AS SAP_CUSTOMER_CHANNEL_DESCRIPTION,	
             COALESCE(ACTUAL.SAP_CUSTOMER_SUB_CHANNEL_KEY,CUSTOMER.SAP_CUST_SUB_CHNL_KEY,'NA') AS SAP_CUSTOMER_SUB_CHANNEL_KEY,	
             COALESCE(ACTUAL.SAP_SUB_CHANNEL_DESCRIPTION,CUSTOMER.SAP_SUB_CHNL_DESC,'NA') AS SAP_SUB_CHANNEL_DESCRIPTION,	
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_KEY,CUSTOMER.SAP_PRNT_CUST_KEY,'NA') AS SAP_PARENT_CUSTOMER_KEY,		
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_DESCRIPTION,CUSTOMER.SAP_PRNT_CUST_DESC,'NA') AS SAP_PARENT_CUSTOMER_DESCRIPTION,	
             COALESCE(ACTUAL.SAP_BANNER_KEY,CUSTOMER.SAP_BNR_KEY,'NA') AS SAP_BANNER_KEY,
             COALESCE(ACTUAL.SAP_BANNER_DESCRIPTION,CUSTOMER.SAP_BNR_DESC,'NA') AS SAP_BANNER_DESCRIPTION,	
             COALESCE(ACTUAL.SAP_BANNER_FORMAT_KEY,CUSTOMER.SAP_BNR_FRMT_KEY,'NA') AS SAP_BANNER_FORMAT_KEY,	
             COALESCE(ACTUAL.SAP_BANNER_FORMAT_DESCRIPTION,CUSTOMER.SAP_BNR_FRMT_DESC,'NA') AS SAP_BANNER_FORMAT_DESCRIPTION,	
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
      FROM ITG_VN_RE_MSL_LIST TARGET	
        LEFT JOIN (SELECT * FROM WKS_VN_REGIONAL_SELLOUT_ACTUALS) ACTUAL	
               ON TARGET.FISC_PER = ACTUAL.MNTH_ID	
               AND UPPER(TARGET.MARKET)=UPPER(ACTUAL.CNTRY_NM)
               AND UPPER(LTRIM(TARGET.DISTRIBUTOR_CODE, '0')) = UPPER(LTRIM(ACTUAL.DISTRIBUTOR_CODE, '0'))	
               AND UPPER(LTRIM(TARGET.STORE_CODE, '0')) = UPPER(LTRIM(ACTUAL.STORE_CODE,'0'))
               AND UPPER (TRIM (TARGET.EAN)) = UPPER (TRIM (ACTUAL.EAN))	
               AND UPPER(TARGET.RETAIL_ENVIRONMENT) = UPPER(ACTUAL.RETAIL_ENVIRONMENT)
    
        ----------------customer hierarchy------------------------------
        LEFT JOIN (SELECT * FROM (SELECT DISTINCT ECBD.CUST_NUM AS SAP_CUST_ID,		
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
                    --REGZONE.REGION_NAME AS REGION,
                    --REGZONE.ZONE_NAME AS ZONE_OR_AREA,
                    EGCH.GCGH_REGION AS GCH_REGION,
                    EGCH.GCGH_CLUSTER AS GCH_CLUSTER,	
                    EGCH.GCGH_SUBCLUSTER AS GCH_SUBCLUSTER,
                    EGCH.GCGH_MARKET AS GCH_MARKET,
                    EGCH.GCCH_RETAIL_BANNER AS GCH_RETAIL_BANNER,
                    ECSD.SEGMT_KEY AS CUST_SEGMT_KEY,
                    CODES_SEGMENT.CODE_DESC AS cust_segment_desc,
                    ROW_NUMBER() OVER (PARTITION BY SAP_CUST_ID ORDER BY NULLIF(ECSD.PRNT_CUST_KEY,''),NULLIF(ECSD.BNR_KEY,''),NULLIF(ECSD.BNR_FRMT_KEY,''),NULLIF(ECSD.SEGMT_KEY,'') ASC NULLS LAST) AS RANK
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
                    EDW_CODE_DESCRIPTIONS_MANUAL CODES_SEGMENT,
                    (SELECT DISTINCT CUST_NUM,
                    REC_CRT_DT,
                    PRNT_CUST_KEY,
                    ROW_NUMBER() OVER (PARTITION BY CUST_NUM ORDER BY REC_CRT_DT DESC) RN
                    FROM EDW_CUSTOMER_SALES_DIM) A	
                    WHERE EGCH.CUSTOMER ( + ) = ECBD.CUST_NUM		
                    AND   ECSD.CUST_NUM = ECBD.CUST_NUM	
                    AND   A.CUST_NUM = ECSD.CUST_NUM	
                    AND   ECSD.DSTR_CHNL = EDC.DISTR_CHAN	
                    AND   ECSD.SLS_ORG = ESOD.SLS_ORG	
                    AND   ESOD.SLS_ORG_CO_CD = ECD.CO_CD	
                    AND   A.RN = 1
                    AND   UPPER(TRIM(CDDES_PCK.CODE_TYPE ( + ))) = 'PARENT CUSTOMER KEY'	
                    AND   CDDES_PCK.CODE ( + ) = ECSD.PRNT_CUST_KEY		
                    AND   UPPER(TRIM(CDDES_BNRKEY.CODE_TYPE ( + ))) = 'BANNER KEY'
                    AND   CDDES_BNRKEY.CODE ( + ) = ECSD.BNR_KEY		
                    AND   UPPER(TRIM(CDDES_BNRFMT.CODE_TYPE ( + ))) = 'BANNER FORMAT KEY'	
                    AND   CDDES_BNRFMT.CODE ( + ) = ECSD.BNR_FRMT_KEY	
                    AND   UPPER(TRIM(CDDES_CHNL.CODE_TYPE ( + ))) = 'CHANNEL KEY'
                    AND   CDDES_CHNL.CODE ( + ) = ECSD.CHNL_KEY	
                    AND   UPPER(TRIM(CDDES_GTM.CODE_TYPE ( + ))) = 'GO TO MODEL KEY'
                    AND   CDDES_GTM.CODE ( + ) = ECSD.GO_TO_MDL_KEY	
                    AND   UPPER(TRIM(CDDES_SUBCHNL.CODE_TYPE ( + ))) = 'SUB CHANNEL KEY'
                    AND   CDDES_SUBCHNL.CODE ( + ) = ECSD.SUB_CHNL_KEY	
                    AND   UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL ( + )) = UPPER(CDDES_SUBCHNL.CODE_DESC) 
                    AND   CODES_SEGMENT.CODE_TYPE ( + ) = 'Customer Segmentation Key'
                    AND   CODES_SEGMENT.CODE ( + ) = ECSD.SEGMT_KEY)	
                    WHERE RANK = 1) CUSTOMER ON LTRIM(COALESCE(ACTUAL.SOLDTO_CODE, TARGET.SOLDTO_CODE),'0') = LTRIM(CUSTOMER.SAP_CUST_ID ,'0')

    ----------------product hierarchy------------------------------
        LEFT JOIN (select * from product_heirarchy) PRODUCT 
        ON LTRIM (COALESCE(ACTUAL.SKU_CODE, TARGET.SKU_CODE),'0') = LTRIM (PRODUCT.SAP_MATL_NUM,'0') 
        AND PRODUCT.RANK = 1 
        ) MDP,	
        (SELECT DISTINCT "cluster", CTRY_GROUP
        FROM EDW_COMPANY_DIM
        WHERE UPPER(CTRY_GROUP) = 'VIETNAM') COM 
        --ON UPPER (MDP.MARKET) = UPPER (COM.CTRY_GROUP)
    WHERE FISC_PER <= (SELECT MAX(MNTH_ID)
                    FROM WKS_VN_REGIONAL_SELLOUT_ACTUALS) 
),

vn_rpt_retail_excellence_non_mdp as 
(
    SELECT DISTINCT NON_MDP.*,	
       COM."cluster",	
	   SYSDATE() AS CRTD_DTTM
FROM (SELECT CAST(ACTUAL.YEAR AS numeric(18,0) ) AS FISC_YR,	
             CAST(ACTUAL.MNTH_ID AS numeric(18,0) ) AS FISC_PER,	
             ACTUAL.CNTRY_NM AS MARKET,	
             ACTUAL.DATA_SRC AS DATA_SRC,	
             UPPER(NVL(ACTUAL.CHANNEL,'NA')) AS CHANNEL_NAME,	
             ACTUAL.SOLDTO_CODE,	
             UPPER(LTRIM(ACTUAL.DISTRIBUTOR_CODE,'0')) AS DISTRIBUTOR_CODE,	
             UPPER(ACTUAL.DISTRIBUTOR_NAME) AS DISTRIBUTOR_NAME,	
             UPPER(NVL(ACTUAL.CHANNEL,'NA')) AS SELL_OUT_CHANNEL,	
             UPPER(COALESCE(ACTUAL.STORE_TYPE, 'NA')) AS STORE_TYPE,	
             NULL AS PRIORITIZATION_SEGMENTATION,
             NULL AS STORE_CATEGORY,
             UPPER(COALESCE(ACTUAL.STORE_CODE,'NA')) AS STORE_CODE,		
             UPPER(COALESCE(ACTUAL.STORE_NAME,'NA')) AS STORE_NAME,		
             NULL AS STORE_GRADE,
             NULL AS STORE_SIZE,
             ACTUAL.REGION,	
             ACTUAL.ZONE AS ZONE_NAME,	
             'NA' as CITY,
             NULL AS RTRLATITUDE,
             NULL AS RTRLONGITUDE,
             UPPER(NVL(ACTUAL.RETAIL_ENVIRONMENT, 'NA')) AS sell_out_RE,	
             ACTUAL.EAN AS PRODUCT_CODE,	
             COALESCE(ACTUAL.SKU_DESCRIPTION,'NA') AS PRODUCT_NAME,	
             EPD.PROD_HIER_L1 as prod_hier_l1,	
             EPD.PROD_HIER_L2 AS prod_hier_l2,	
             EPD.PROD_HIER_L3 AS prod_hier_l3,	
             EPD.PROD_HIER_L4 AS prod_hier_l4,	
			 NULL as prod_hier_l5,
             NULL AS prod_hier_l6,
             NULL AS prod_hier_l7,
             NULL AS prod_hier_l8,
             NULL as  prod_hier_l9,
             ACTUAL.SKU_CODE AS MAPPED_SKU_CD,	
			 ACTUAL.LIST_PRICE,		
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_KEY,CUSTOMER.CUST_SEGMT_KEY) AS CUSTOMER_SEGMENT_KEY,COALESCE(ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION,CUSTOMER.CUST_SEGMENT_DESC) AS CUSTOMER_SEGMENT_DESCRIPTION,	
             COALESCE(ACTUAL.RETAIL_ENVIRONMENT,'NA') AS RETAIL_ENVIRONMENT,	
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
            FROM WKS_VN_REGIONAL_SELLOUT_ACTUALS A	
            WHERE NOT EXISTS (SELECT 1
                              FROM ITG_VN_RE_MSL_LIST T
                              WHERE A.MNTH_ID = T.FISC_PER
                              AND UPPER(A.CNTRY_NM)=UPPER(T.market)
                              AND UPPER(LTRIM(A.DISTRIBUTOR_CODE, '0')) = UPPER(LTRIM(T.DISTRIBUTOR_CODE, '0'))	
                              AND UPPER(LTRIM(A.STORE_CODE, '0')) = UPPER(LTRIM(T.STORE_CODE, '0'))
                              AND UPPER(TRIM(A.EAN)) = UPPER(TRIM(T.EAN))
                              AND UPPER(A.RETAIL_ENVIRONMENT) = UPPER(T.RETAIL_ENVIRONMENT))) ACTUAL
			LEFT JOIN (select matl_num,
						ean_code,
						prod_hier_l1,
						prod_hier_l2,
						prod_hier_l3,
						prod_hier_l4
					from (select LTRIM(matl_num,'0') as matl_num,
							ean_upc as ean_code,
							gcph_franchise AS prod_hier_l1,
							gcph_needstate AS prod_hier_l2,
							gcph_category as prod_hier_l3,
							gcph_subcategory as prod_hier_l4,
							row_number() over (partition by ean_upc order by lst_nts desc) as rn
						from edw_product_key_attributes
						where ctry_nm = 'Vietnam' and matl_type_cd = 'FERT'
						and (lst_nts is not null)
						) PROD_KEY 
            JOIN ITG_VN_PRODUCT_MAPPING PROD_MAP ON LTRIM(PROD_KEY.EAN_CODE, '0') = LTRIM(PROD_MAP.BARCODE, '0') where rn = 1) epd 
                ON LTRIM(ACTUAL.ean,'0') = LTRIM(epd.ean_code,'0')
----------------customer hierarchy------------------------------
        LEFT JOIN (SELECT * FROM (SELECT DISTINCT ECBD.CUST_NUM AS SAP_CUST_ID,		
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
                    --REGZONE.REGION_NAME AS REGION,
                    --REGZONE.ZONE_NAME AS ZONE_OR_AREA,
                    EGCH.GCGH_REGION AS GCH_REGION,
                    EGCH.GCGH_CLUSTER AS GCH_CLUSTER,	
                    EGCH.GCGH_SUBCLUSTER AS GCH_SUBCLUSTER,
                    EGCH.GCGH_MARKET AS GCH_MARKET,
                    EGCH.GCCH_RETAIL_BANNER AS GCH_RETAIL_BANNER,
                    ECSD.SEGMT_KEY AS CUST_SEGMT_KEY,
                    CODES_SEGMENT.CODE_DESC AS cust_segment_desc,
                    ROW_NUMBER() OVER (PARTITION BY SAP_CUST_ID ORDER BY NULLIF(ECSD.PRNT_CUST_KEY,''),NULLIF(ECSD.BNR_KEY,''),NULLIF(ECSD.BNR_FRMT_KEY,''),NULLIF(ECSD.SEGMT_KEY,'') ASC NULLS LAST) AS RANK
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
                    EDW_CODE_DESCRIPTIONS_MANUAL CODES_SEGMENT,
                    (SELECT DISTINCT CUST_NUM,
                    REC_CRT_DT,
                    PRNT_CUST_KEY,
                    ROW_NUMBER() OVER (PARTITION BY CUST_NUM ORDER BY REC_CRT_DT DESC) RN
                    FROM EDW_CUSTOMER_SALES_DIM) A	
                    WHERE EGCH.CUSTOMER ( + ) = ECBD.CUST_NUM		
                    AND   ECSD.CUST_NUM = ECBD.CUST_NUM	
                    AND   A.CUST_NUM = ECSD.CUST_NUM	
                    AND   ECSD.DSTR_CHNL = EDC.DISTR_CHAN	
                    AND   ECSD.SLS_ORG = ESOD.SLS_ORG	
                    AND   ESOD.SLS_ORG_CO_CD = ECD.CO_CD	
                    AND   A.RN = 1
                    AND   UPPER(TRIM(CDDES_PCK.CODE_TYPE ( + ))) = 'PARENT CUSTOMER KEY'	
                    AND   CDDES_PCK.CODE ( + ) = ECSD.PRNT_CUST_KEY		
                    AND   UPPER(TRIM(CDDES_BNRKEY.CODE_TYPE ( + ))) = 'BANNER KEY'
                    AND   CDDES_BNRKEY.CODE ( + ) = ECSD.BNR_KEY		
                    AND   UPPER(TRIM(CDDES_BNRFMT.CODE_TYPE ( + ))) = 'BANNER FORMAT KEY'	
                    AND   CDDES_BNRFMT.CODE ( + ) = ECSD.BNR_FRMT_KEY	
                    AND   UPPER(TRIM(CDDES_CHNL.CODE_TYPE ( + ))) = 'CHANNEL KEY'
                    AND   CDDES_CHNL.CODE ( + ) = ECSD.CHNL_KEY	
                    AND   UPPER(TRIM(CDDES_GTM.CODE_TYPE ( + ))) = 'GO TO MODEL KEY'
                    AND   CDDES_GTM.CODE ( + ) = ECSD.GO_TO_MDL_KEY	
                    AND   UPPER(TRIM(CDDES_SUBCHNL.CODE_TYPE ( + ))) = 'SUB CHANNEL KEY'
                    AND   CDDES_SUBCHNL.CODE ( + ) = ECSD.SUB_CHNL_KEY	
                    AND   UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL ( + )) = UPPER(CDDES_SUBCHNL.CODE_DESC) 
                    AND   CODES_SEGMENT.CODE_TYPE ( + ) = 'Customer Segmentation Key'
                    AND   CODES_SEGMENT.CODE ( + ) = ECSD.SEGMT_KEY)	
                    WHERE RANK = 1) CUSTOMER ON LTRIM(ACTUAL.SOLDTO_CODE,'0') = LTRIM(CUSTOMER.SAP_CUST_ID ,'0')
 
----------------product hierarchy------------------------------
        LEFT JOIN (select * from product_heirarchy) PRODUCT 
        ON LTRIM(ACTUAL.SKU_CODE,'0') = LTRIM (PRODUCT.SAP_MATL_NUM,'0') 
        AND PRODUCT.RANK = 1 
        ) NON_MDP, 

        (SELECT DISTINCT "cluster", CTRY_GROUP
        FROM EDW_COMPANY_DIM
        WHERE UPPER(CTRY_GROUP) = 'VIETNAM') COM 
        --ON UPPER (NON_MDP.MARKET) = UPPER (COM.CTRY_GROUP)
),

vn_rpt_retail_excellence as 
(
    select * from vn_rpt_retail_excellence_mdp
    union
    select * from vn_rpt_retail_excellence_non_mdp
),

final as 
(
    select fisc_yr :: numeric(18,0) as fisc_yr,
    fisc_per :: numeric(18,0) as fisc_per,
    market :: varchar(50) as market,
    data_src :: varchar(14) as data_src,
    channel_name :: varchar(337) as channel_name,
    soldto_code :: varchar(255) as soldto_code,
    distributor_code :: varchar(225) as distributor_code,
    distributor_name :: varchar(534) as distributor_name,
    sell_out_channel :: varchar(337) as sell_out_channel,
    store_type :: varchar(573) as store_type,
    prioritization_segmentation :: varchar(1) as prioritization_segmentation,
    store_category :: varchar(1) as store_category,
    store_code :: varchar(150) as store_code,
    store_name :: varchar(901) as store_name,
    store_grade :: varchar(1) as store_grade,
    store_size :: varchar(2) as store_size,
    region :: varchar(150) as region,
    zone_name :: varchar(150) as zone_name,
    city :: varchar(2) as city,
    rtrlatitude :: varchar(1) as rtrlatitude,
    rtrlongitude :: varchar(1) as rtrlongitude,
    sell_out_re :: varchar(225) as sell_out_re,
    product_code :: varchar(150) as product_code,
    product_name :: varchar(300) as product_name,
    prod_hier_l1 :: varchar(30) as prod_hier_l1,
    prod_hier_l2 :: varchar(50) as prod_hier_l2,
    prod_hier_l3 :: varchar(50) as prod_hier_l3,
    prod_hier_l4 :: varchar(50) as prod_hier_l4,
    prod_hier_l5 :: varchar(1) as prod_hier_l5,
    prod_hier_l6 :: varchar(1) as prod_hier_l6,
    prod_hier_l7 :: varchar(1) as prod_hier_l7,
    prod_hier_l8 :: varchar(1) as prod_hier_l8,
    prod_hier_l9 :: varchar(1) as prod_hier_l9,
    mapped_sku_cd :: varchar(40) as mapped_sku_cd,
    list_price :: numeric(38,6) as list_price,
    customer_segment_key :: varchar(12) as customer_segment_key,
    customer_segment_description :: varchar(50) as customer_segment_description,
    retail_environment :: varchar(150) as retail_environment,
    sap_customer_channel_key :: varchar(12) as sap_customer_channel_key,
    sap_customer_channel_description :: varchar(75) as sap_customer_channel_description,
    sap_customer_sub_channel_key :: varchar(12) as sap_customer_sub_channel_key,
    sap_sub_channel_description :: varchar(75) as sap_sub_channel_description,
    sap_parent_customer_key :: varchar(12) as sap_parent_customer_key,
    sap_parent_customer_description :: varchar(75) as sap_parent_customer_description,
    sap_banner_key :: varchar(12) as sap_banner_key,
    sap_banner_description :: varchar(75) as sap_banner_description,
    sap_banner_format_key :: varchar(12) as sap_banner_format_key,
    sap_banner_format_description :: varchar(75) as sap_banner_format_description,
    customer_name :: varchar(100) as customer_name,
    customer_code :: varchar(10) as customer_code,
    sap_prod_sgmt_cd :: varchar(18) as sap_prod_sgmt_cd,
    sap_prod_sgmt_desc :: varchar(100) as sap_prod_sgmt_desc,
    sap_base_prod_desc :: varchar(100) as sap_base_prod_desc,
    sap_mega_brnd_desc :: varchar(100) as sap_mega_brnd_desc,
    sap_brnd_desc :: varchar(100) as sap_brnd_desc,
    sap_vrnt_desc :: varchar(100) as sap_vrnt_desc,
    sap_put_up_desc :: varchar(100) as sap_put_up_desc,
    sap_grp_frnchse_cd :: varchar(18) as sap_grp_frnchse_cd,
    sap_grp_frnchse_desc :: varchar(100) as sap_grp_frnchse_desc,
    sap_frnchse_cd :: varchar(18) as sap_frnchse_cd,
    sap_frnchse_desc :: varchar(100) as sap_frnchse_desc,
    sap_prod_frnchse_cd :: varchar(18) as sap_prod_frnchse_cd,
    sap_prod_frnchse_desc :: varchar(100) as sap_prod_frnchse_desc,
    sap_prod_mjr_cd :: varchar(18) as sap_prod_mjr_cd,
    sap_prod_mjr_desc :: varchar(100) as sap_prod_mjr_desc,
    sap_prod_mnr_cd :: varchar(18) as sap_prod_mnr_cd,
    sap_prod_mnr_desc :: varchar(100) as sap_prod_mnr_desc,
    sap_prod_hier_cd :: varchar(18) as sap_prod_hier_cd,
    sap_prod_hier_desc :: varchar(100) as sap_prod_hier_desc,
    global_product_franchise :: varchar(30) as global_product_franchise,
    global_product_brand :: varchar(30) as global_product_brand,
    global_product_sub_brand :: varchar(100) as global_product_sub_brand,
    global_product_variant :: varchar(100) as global_product_variant,
    global_product_segment :: varchar(50) as global_product_segment,
    global_product_subsegment :: varchar(100) as global_product_subsegment,
    global_product_category :: varchar(50) as global_product_category,
    global_product_subcategory :: varchar(50) as global_product_subcategory,
    global_put_up_description :: varchar(100) as global_put_up_description,
    pka_franchise_desc :: varchar(30) as pka_franchise_desc,
    pka_brand_desc :: varchar(30) as pka_brand_desc,
    pka_sub_brand_desc :: varchar(30) as pka_sub_brand_desc,
    pka_variant_desc :: varchar(30) as pka_variant_desc,
    pka_sub_variant_desc :: varchar(30) as pka_sub_variant_desc,
    pka_product_key :: varchar(68) as pka_product_key,
    pka_product_key_description :: varchar(255) as pka_product_key_description,
    sales_value :: numeric(38,6) as sales_value,
    sales_qty :: numeric(38,6) as sales_qty,
    avg_sales_qty :: numeric(38,6) as avg_sales_qty,
    sales_value_list_price :: numeric(38,12) as sales_value_list_price,
    lm_sales :: numeric(38,6) as lm_sales,
    lm_sales_qty :: numeric(38,6) as lm_sales_qty,
    lm_avg_sales_qty :: numeric(38,6) as lm_avg_sales_qty,
    lm_sales_lp :: numeric(38,12) as lm_sales_lp,
    p3m_sales :: numeric(38,6) as p3m_sales,
    p3m_qty :: numeric(38,6) as p3m_qty,
    p3m_avg_qty :: numeric(38,6) as p3m_avg_qty,
    p3m_sales_lp :: numeric(38,12) as p3m_sales_lp,
    f3m_sales :: numeric(38,6) as f3m_sales,
    f3m_qty :: numeric(38,6) as f3m_qty,
    f3m_avg_qty :: numeric(38,6) as f3m_avg_qty,
    p6m_sales :: numeric(38,6) as p6m_sales,
    p6m_qty :: numeric(38,6) as p6m_qty,
    p6m_avg_qty :: numeric(38,6) as p6m_avg_qty,
    p6m_sales_lp :: numeric(38,12) as p6m_sales_lp,
    p12m_sales :: numeric(38,6) as p12m_sales,
    p12m_qty :: numeric(38,6) as p12m_qty,
    p12m_avg_qty :: numeric(38,6) as p12m_avg_qty,
    p12m_sales_lp :: numeric(38,12) as p12m_sales_lp,
    lm_sales_flag :: varchar(1) as lm_sales_flag,
    p3m_sales_flag :: varchar(1) as p3m_sales_flag,
    p6m_sales_flag :: varchar(1) as p6m_sales_flag,
    p12m_sales_flag :: varchar(1) as p12m_sales_flag,
    mdp_flag :: varchar(1) as mdp_flag,
    target_complaince :: numeric(18,0) as target_complaince,
    "cluster" :: varchar(100) as "cluster",
    crtd_dttm :: timestamp without time zone as crtd_dttm
    from vn_rpt_retail_excellence
)

--final select

select * from final 