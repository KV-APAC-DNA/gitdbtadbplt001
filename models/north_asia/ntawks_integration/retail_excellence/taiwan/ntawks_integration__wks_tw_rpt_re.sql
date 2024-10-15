--import cte     
with wks_tw_re_msl_list as (
    select * from {{ ref('ntawks_integration__wks_tw_re_msl_list') }}
),
wks_tw_re_actuals as (
    select * from {{ ref('ntawks_integration__wks_tw_re_actuals') }}
),
customer_heirarchy as (
    select * from {{ ref('aspedw_integration__edw_generic_customer_hierarchy') }}
),
product_heirarchy as (
    select * from {{ ref('aspedw_integration__edw_generic_product_hierarchy') }}
),
edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_vw_pop6_products as (
    --select * from {{ source('ntaedw_integration', 'edw_vw_pop6_products') }}
    select * from {{ ref('aspedw_integration__edw_vw_pop6_products') }}
),

TW_RPT_RE_mdp AS (
SELECT distinct Q.*,
       COM.*
FROM (SELECT TARGET.jj_year,
             TARGET.jj_mnth_id,		 
             --TARGET.market AS MARKET,
             COALESCE(ACTUAL.CNTRY_NM, TARGET.MARKET) AS MARKET,
             COALESCE(ACTUAL.CHANNEL_NAME,TARGET.Sell_Out_Channel,'NA') as CHANNEL_NAME,
             COALESCE(ACTUAL.SOLDTO_CODE, TARGET.SOLDTO_CODE) AS SOLDTO_CODE,
             COALESCE(actual.DISTRIBUTOR_CODE,TARGET.DISTRIBUTOR_CODE,'NA') as DISTRIBUTOR_CODE,
             COALESCE(actual.DISTRIBUTOR_NAME,TARGET.DISTRIBUTOR_NAME,'NA') as DISTRIBUTOR_NAME,
             COALESCE(actual.CHANNEL_NAME,TARGET.SELL_OUT_CHANNEL,'NA') as SELL_OUT_CHANNEL,
             nvl(actual.store_type,TARGET.STORE_TYPE) as store_type,
             NULL AS PRIORITIZATION_SEGMENTATION,
             NULL AS STORE_CATEGORY,
             COALESCE(actual.store_code,TARGET.STORE_CODE,'NA') AS STORE_CODE,
             COALESCE(actual.store_name,TARGET.STORE_NAME,'NA') as STORE_NAME,
             COALESCE(TARGET.STORE_GRADE, 'NA') AS STORE_GRADE,
             'NA' as STORE_SIZE,
             nvl(actual.REGION,TARGET.REGION) as REGION,
             nvl(actual.ZONE_OR_AREA,TARGET.zone_or_area) as ZONE_NAME,
             'NA' as CITY,
             NULL AS RTRLATITUDE,
             NULL AS RTRLONGITUDE,
             nvl(actual.RETAIL_ENVIRONMENT,TARGET.retail_environment) AS Sell_Out_RE,
             nvl(actual.EAN,TARGET.EAN) AS PRODUCT_CODE,
             COALESCE(actual.msl_product_desc, target.msl_product_desc,'NA') AS PRODUCT_NAME,
             TARGET.PROD_HIER_L1 AS PROD_HIER_L1,
             TARGET.PROD_HIER_L2 AS PROD_HIER_L2,
             TARGET.PROD_HIER_L3 AS PROD_HIER_L3,
             target.PROD_HIER_L4 AS  PROD_HIER_L4,
             TARGET.PROD_HIER_L5 AS PROD_HIER_L5,
             TARGET.PROD_HIER_L6 AS PROD_HIER_L6,
             TARGET.PROD_HIER_L7 AS PROD_HIER_L7,
             TARGET.PROD_HIER_L8 AS PROD_HIER_L8,
			 --NULL AS PROD_HIER_L9,
             TARGET.PROD_HIER_L9 AS PROD_HIER_L9,
             target.sku_code AS MAPPED_SKU_CD,
			 actual.list_price,
             --'POS' as data_src,
             COALESCE(ACTUAL.DATA_SRC, TARGET.DATA_SRC) AS DATA_SRC,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_KEY,CUSTOMER.CUST_SEGMT_KEY,'NA') AS CUSTOMER_SEGMENT_KEY,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION,CUSTOMER.CUST_SEGMENT_DESC,'NA') AS CUSTOMER_SEGMENT_DESCRIPTION,
             COALESCE(ACTUAL.RETAIL_ENVIRONMENT,target.retail_environment,'NA') AS RETAIL_ENVIRONMENT,
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
             --REGION,
             --ZONE_OR_AREA,
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
             --TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA'))AS SKU_CODE,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA'))AS SKU_DESCRIPTION,
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
             ACTUAL.SALES_VALUE_LIST_PRICE AS SALES_VALUE_LIST_PRICE,
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
      FROM wks_tw_re_msl_list TARGET
        LEFT JOIN (SELECT * FROM WKS_TW_RE_ACTUALS) ACTUAL
               ON TARGET.jj_mnth_id = ACTUAL.MNTH_ID
              --AND TARGET.DISTRIBUTOR_NAME = ACTUAL.DISTRIBUTOR_NAME
              AND UPPER(LTRIM(TARGET.DISTRIBUTOR_CODE,'0')) = UPPER(LTRIM(ACTUAL.DISTRIBUTOR_CODE,'0'))
              AND ltrim(TARGET.STORE_CODE,0) = ltrim(ACTUAL.STORE_CODE,0)
              AND UPPER (TRIM (TARGET.EAN)) = UPPER (TRIM (ACTUAL.EAN))
			  and UPPER (target.retail_environment) = UPPER (actual.retail_environment) 
              and UPPER (target.Sell_Out_Channel) = UPPER (actual.CHANNEL_NAME) 
			  and UPPER(target.market) = UPPER(actual.CNTRY_NM)
     
      ----------------customer hierarchy------------------------------          
      
        LEFT JOIN (SELECT *
                   FROM customer_heirarchy 
				   WHERE RANK = 1) CUSTOMER ON  nvl(ltrim(ACTUAL.SOLDTO_CODE,0), ltrim(TARGET.SOLDTO_CODE,0)) = LTRIM (CUSTOMER.SAP_CUST_ID,'0')
      ----------------product hierarchy------------------------------   				   
      
        LEFT JOIN product_heirarchy PRODUCT
			   ON LTRIM(COALESCE(ACTUAL.SKU_CODE,TARGET.SKU_CODE),'0') = LTRIM(PRODUCT.sap_matl_num,'0')
              AND product.rank = 1
	) Q
  JOIN (SELECT DISTINCT "cluster",
               CTRY_GROUP
        FROM EDW_COMPANY_DIM
        WHERE CTRY_GROUP in ('Taiwan')) COM ON UPPER (TRIM (Q.market)) = UPPER (TRIM (com.CTRY_GROUP))
WHERE jj_mnth_id::NUMERIC<= (SELECT MAX(mnth_id)::NUMERIC FROM WKS_TW_RE_ACTUALS)
),

------------Non mdp--------------------------------
TW_RPT_RE_non_mdp  as (
SELECT distinct Q.*,
       COM.*
FROM (SELECT LEFT (ACTUAL.MNTH_ID,4) AS YEAR,
             ACTUAL.MNTH_ID AS MNTH_ID,			 			
             ACTUAL.CNTRY_NM AS MARKET,
             nvl(actual.channel_name,'NA') AS CHANNEL,
             ACTUAL.soldto_code,
             ACTUAL.DISTRIBUTOR_CODE,
             ACTUAL.DISTRIBUTOR_NAME,
             nvl(actual.channel_name,'NA') AS SELL_OUT_CHANNEL,
             COALESCE(actual.STORE_TYPE, 'NA') AS STORE_TYPE,
             NULL AS PRIORITIZATION_SEGMENTATION,
             NULL AS STORE_CATEGORY,
             COALESCE(ACTUAL.STORE_CODE,'NA') AS STORE_CODE,
             COALESCE(ACTUAL.STORE_NAME,'NA') AS STORE_NAME,
             NULL AS STORE_GRADE,
             NULL AS STORE_SIZE,
             actual.region,
             actual.zone_or_area AS ZONE_NAME,
             'NA' as CITY,
             NULL AS RTRLATITUDE,
             NULL AS RTRLONGITUDE,
             ACTUAL.retail_environment AS sell_out_RE,
             ACTUAL.EAN AS PRODUCT_CODE,
             COALESCE(actual.msl_product_desc,'NA') AS PRODUCT_NAME,
             epd.prod_hier_l1  AS prod_hier_l1,
             epd.prod_hier_l2 AS prod_hier_l2,
             epd.prod_hier_l3 AS prod_hier_l3,
             epd.prod_hier_l4 AS prod_hier_l4,
             epd.prod_hier_l5 as prod_hier_l5,
             -- varient is sku_name from msl table NA_ITG.itg_mcs_gt
             epd.prod_hier_l6 AS prod_hier_l6,
             epd.prod_hier_l7 AS prod_hier_l7,
             epd.prod_hier_l8 AS prod_hier_l8,
			 --NULL as  prod_hier_l9,
             epd.prod_hier_l9 as  prod_hier_l9,
             actual.sku_code AS MAPPED_SKU_CD,
			 actual.list_price,
             --actual.data_src,
             ACTUAL.DATA_SRC AS DATA_SRC
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_KEY,CUSTOMER.CUST_SEGMT_KEY) AS CUSTOMER_SEGMENT_KEY,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION,CUSTOMER.CUST_SEGMENT_DESC) AS CUSTOMER_SEGMENT_DESCRIPTION,
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
             --REGION,
             --ZONE_OR_AREA,
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
             --TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA'))AS SKU_CODE,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA'))AS SKU_DESCRIPTION,
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
             ACTUAL.SALES_VALUE_LIST_PRICE AS SALES_VALUE_LIST_PRICE,
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
      FROM (SELECT * FROM WKS_TW_RE_ACTUALS A
            WHERE NOT EXISTS (SELECT 1
                              FROM wks_tw_re_msl_list T
                              WHERE T.jj_mnth_id = A.MNTH_ID
              --AND T.DISTRIBUTOR_NAME = A.DISTRIBUTOR_NAME
              AND UPPER(LTRIM(TARGET.DISTRIBUTOR_CODE,'0')) = UPPER(LTRIM(ACTUAL.DISTRIBUTOR_CODE,'0'))
              AND ltrim(T.STORE_CODE,0) = ltrim(A.STORE_CODE,0)
              AND UPPER (TRIM (T.EAN)) = UPPER (TRIM (A.EAN))
              and UPPER (T.retail_environment) = UPPER (A.retail_environment) 
              and UPPER (T.Sell_Out_Channel) = UPPER (A.CHANNEL_NAME) 
			  and UPPER(T.market) = UPPER(A.CNTRY_NM)
			)) ACTUAL
        left join (SELECT DISTINCT prd.country_l1 AS prod_hier_l1,
            prd.regional_franchise_l2 AS prod_hier_l2,
            prd.franchise_l3 AS prod_hier_l3,
            prd.brand_l4 AS prod_hier_l4,
            prd.sub_category_l5 AS prod_hier_l5,
            prd.platform_l6 AS prod_hier_l6,
            prd.variance_l7 AS prod_hier_l7,
            prd.pack_size_l8 AS prod_hier_l8,
            NULL AS prod_hier_l9,
            prd.barcode
        FROM edw_vw_pop6_products prd
        WHERE cntry_cd = 'TW') epd
        ON UPPER (TRIM (actual.ean)) = UPPER (TRIM (epd.barcode))

      ----------------customer hierarchy------------------------------          
      
        LEFT JOIN (SELECT *
                   FROM customer_heirarchy
                   WHERE RANK = 1) CUSTOMER ON LTRIM (ACTUAL.soldto_code,'0') = LTRIM (CUSTOMER.SAP_CUST_ID,'0')
      ----------------product hierarchy------------------------------   				   
      
        LEFT JOIN product_heirarchy PRODUCT
               ON LTRIM (actual.sku_code,'0') = LTRIM (PRODUCT.SAP_MATL_NUM,'0')
              AND product.rank = 1) Q
			  JOIN (SELECT DISTINCT "cluster",
               CTRY_GROUP
        FROM EDW_COMPANY_DIM
        WHERE CTRY_GROUP in ('Taiwan')) COM ON UPPER (TRIM (Q.market)) = UPPER (TRIM (com.CTRY_GROUP))

),

TW_RPT_RE as
(
select * from TW_RPT_RE_mdp
union 
select * from TW_RPT_RE_non_mdp
),

final as (
select
jj_year::numeric(18,0) as jj_year ,
jj_mnth_id::numeric(18,0) as jj_mnth_id ,
market::varchar(50) as market ,
channel_name::varchar(150) as channel_name ,
soldto_code::varchar(255) as soldto_code ,
distributor_code::varchar(32) as distributor_code ,
distributor_name::varchar(255) as distributor_name ,
sell_out_channel::varchar(150) as sell_out_channel ,
store_type::varchar(255) as store_type ,
prioritization_segmentation::varchar(1) as prioritization_segmentation ,
store_category::varchar(1) as store_category ,
store_code::varchar(100) as store_code ,
store_name::varchar(601) as store_name ,
store_grade::varchar(20) as store_grade ,
store_size::varchar(2) as store_size ,
region::varchar(150) as region ,
zone_name::varchar(150) as zone_name ,
city::varchar(2) as city ,
rtrlatitude::varchar(1) as rtrlatitude ,
rtrlongitude::varchar(1) as rtrlongitude ,
sell_out_re::varchar(225) as sell_out_re ,
product_code::varchar(500) as product_code ,
product_name::varchar(300) as product_name ,
prod_hier_l1::varchar(200) as prod_hier_l1 ,
prod_hier_l2::varchar(200) as prod_hier_l2 ,
prod_hier_l3::varchar(200) as prod_hier_l3 ,
prod_hier_l4::varchar(200) as prod_hier_l4 ,
prod_hier_l5::varchar(200) as prod_hier_l5 ,
prod_hier_l6::varchar(200) as prod_hier_l6 ,
prod_hier_l7::varchar(200) as prod_hier_l7 ,
prod_hier_l8::varchar(200) as prod_hier_l8 ,
prod_hier_l9::varchar(1) as prod_hier_l9 ,
mapped_sku_cd::varchar(40) as mapped_sku_cd ,
list_price::numeric(38,6) as list_price ,
data_src::varchar(14) as data_src ,
customer_segment_key::varchar(12) as customer_segment_key ,
customer_segment_description::varchar(50) as customer_segment_description ,
retail_environment::varchar(225) as retail_environment ,
sap_customer_channel_key::varchar(12) as sap_customer_channel_key ,
sap_customer_channel_description::varchar(75) as sap_customer_channel_description ,
sap_customer_sub_channel_key::varchar(12) as sap_customer_sub_channel_key ,
sap_sub_channel_description::varchar(75) as sap_sub_channel_description ,
sap_parent_customer_key::varchar(12) as sap_parent_customer_key ,
sap_parent_customer_description::varchar(75) as sap_parent_customer_description ,
sap_banner_key::varchar(12) as sap_banner_key ,
sap_banner_description::varchar(75) as sap_banner_description ,
sap_banner_format_key::varchar(12) as sap_banner_format_key ,
sap_banner_format_description::varchar(75) as sap_banner_format_description ,
customer_name::varchar(100) as customer_name ,
customer_code::varchar(10) as customer_code ,
sap_prod_sgmt_cd::varchar(18) as sap_prod_sgmt_cd ,
sap_prod_sgmt_desc::varchar(100) as sap_prod_sgmt_desc ,
sap_base_prod_desc::varchar(100) as sap_base_prod_desc ,
sap_mega_brnd_desc::varchar(100) as sap_mega_brnd_desc ,
sap_brnd_desc::varchar(100) as sap_brnd_desc ,
sap_vrnt_desc::varchar(100) as sap_vrnt_desc ,
sap_put_up_desc::varchar(100) as sap_put_up_desc ,
sap_grp_frnchse_cd::varchar(18) as sap_grp_frnchse_cd ,
sap_grp_frnchse_desc::varchar(100) as sap_grp_frnchse_desc ,
sap_frnchse_cd::varchar(18) as sap_frnchse_cd ,
sap_frnchse_desc::varchar(100) as sap_frnchse_desc ,
sap_prod_frnchse_cd::varchar(18) as sap_prod_frnchse_cd ,
sap_prod_frnchse_desc::varchar(100) as sap_prod_frnchse_desc ,
sap_prod_mjr_cd::varchar(18) as sap_prod_mjr_cd ,
sap_prod_mjr_desc::varchar(100) as sap_prod_mjr_desc ,
sap_prod_mnr_cd::varchar(18) as sap_prod_mnr_cd ,
sap_prod_mnr_desc::varchar(100) as sap_prod_mnr_desc ,
sap_prod_hier_cd::varchar(18) as sap_prod_hier_cd ,
sap_prod_hier_desc::varchar(100) as sap_prod_hier_desc ,
global_product_franchise::varchar(30) as global_product_franchise ,
global_product_brand::varchar(30) as global_product_brand ,
global_product_sub_brand::varchar(100) as global_product_sub_brand ,
global_product_variant::varchar(100) as global_product_variant ,
global_product_segment::varchar(50) as global_product_segment ,
global_product_subsegment::varchar(100) as global_product_subsegment ,
global_product_category::varchar(50) as global_product_category ,
global_product_subcategory::varchar(50) as global_product_subcategory ,
global_put_up_description::varchar(100) as global_put_up_description ,
sku_description :: varchar(150) as sku_description,
pka_franchise_desc::varchar(30) as pka_franchise_desc ,
pka_brand_desc::varchar(30) as pka_brand_desc ,
pka_sub_brand_desc::varchar(30) as pka_sub_brand_desc ,
pka_variant_desc::varchar(30) as pka_variant_desc ,
pka_sub_variant_desc::varchar(30) as pka_sub_variant_desc ,
pka_product_key::varchar(68) as pka_product_key ,
pka_product_key_description::varchar(255) as pka_product_key_description ,
sales_value::numeric(38,6) as sales_value ,
sales_qty::numeric(38,6) as sales_qty ,
avg_sales_qty::numeric(10,2) as avg_sales_qty ,
sales_value_list_price::numeric(38,12) as sales_value_list_price ,
lm_sales::numeric(38,6) as lm_sales ,
lm_sales_qty::numeric(38,6) as lm_sales_qty ,
lm_avg_sales_qty::numeric(10,2) as lm_avg_sales_qty ,
lm_sales_lp::numeric(38,12) as lm_sales_lp ,
p3m_sales::numeric(38,6) as p3m_sales ,
p3m_qty::numeric(38,6) as p3m_qty ,
p3m_avg_qty::numeric(38,6) as p3m_avg_qty ,
p3m_sales_lp::numeric(38,12) as p3m_sales_lp ,
f3m_sales::numeric(38,6) as f3m_sales ,
f3m_qty::numeric(38,6) as f3m_qty ,
f3m_avg_qty::numeric(38,6) as f3m_avg_qty ,
p6m_sales::numeric(38,6) as p6m_sales ,
p6m_qty::numeric(38,6) as p6m_qty ,
p6m_avg_qty::numeric(38,6) as p6m_avg_qty ,
p6m_sales_lp::numeric(38,12) as p6m_sales_lp ,
p12m_sales::numeric(38,6) as p12m_sales ,
p12m_qty::numeric(38,6) as p12m_qty ,
p12m_avg_qty::numeric(38,6) as p12m_avg_qty ,
p12m_sales_lp::numeric(38,12) as p12m_sales_lp ,
lm_sales_flag::varchar(1) as lm_sales_flag ,
p3m_sales_flag::varchar(1) as p3m_sales_flag ,
p6m_sales_flag::varchar(1) as p6m_sales_flag ,
p12m_sales_flag::varchar(1) as p12m_sales_flag ,
mdp_flag::varchar(1) as mdp_flag ,
target_complaince:: numeric(18,0) as target_complaince ,
"cluster"::varchar(100) as "cluster" 

from TW_RPT_RE
)
select * from final