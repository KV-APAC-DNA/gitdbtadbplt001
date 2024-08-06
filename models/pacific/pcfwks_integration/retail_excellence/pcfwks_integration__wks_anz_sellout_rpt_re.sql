with itg_anz_sellout_re_msl_list as (
    select * from {{ ref('pcfitg_integration__itg_anz_sellout_re_msl_list') }}
),
wks_anz_sellout_re_actuals as (
    select * from {{ ref('pcfwks_integration__wks_anz_sellout_re_actuals') }}
),
customer_heirarchy as (
    select * from {{ ref('aspedw_integration__edw_generic_customer_hierarchy') }}
),
EDW_COMPANY_DIM as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
EDW_SALES_ORG_DIM as(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
product_heirarchy as (
    select * from {{ ref('aspedw_integration__edw_generic_product_hierarchy') }}
),
wks_anz_sellout_c360_mapped_sku_cd as(
    select * from {{ ref('pcfwks_integration__wks_anz_sellout_c360_mapped_sku_cd') }}
),
EDW_MATERIAL_SALES_DIM as(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
itg_trax_md_product as (
    select * from {{ ref('pcfitg_integration__itg_trax_md_product') }}
),
product_key_attribute as (
    select * from {{ ref('aspedw_integration__edw_generic_product_key_attributes') }}
),

anz_rpt_retail_excellence_mdp  as (
SELECT distinct Q.*,
       COM."cluster",
       SYSDATE() as crt_dttm
FROM (SELECT TARGET.jj_year,
             TARGET.jj_mnth_id,
             --COM.CLUSTER,			 
             TARGET.market AS MARKET,
             COALESCE(ACTUAL.CHANNEL_NAME,TARGET.Sell_Out_Channel,'NA') as CHANNEL_NAME,
             nvl(ACTUAL.SOLDTO_CODE, TARGET.SOLDTO_CODE) AS SOLDTO_CODE,
             nvl(actual.DISTRIBUTOR_CODE,TARGET.DISTRIBUTOR_CODE) as DISTRIBUTOR_CODE,
             nvl(actual.DISTRIBUTOR_NAME,TARGET.DISTRIBUTOR_NAME) as DISTRIBUTOR_NAME,
             COALESCE(actual.CHANNEL_NAME,TARGET.SELL_OUT_CHANNEL,'NA') as SELL_OUT_CHANNEL,
             nvl(actual.store_type,TARGET.STORE_TYPE) as store_type,
             NULL AS PRIORITIZATION_SEGMENTATION,
             NULL AS STORE_CATEGORY,
             nvl(actual.store_code,TARGET.STORE_CODE) AS STORE_CODE,
             nvl(actual.store_name,TARGET.STORE_NAME) as STORE_NAME,
             TARGET.STORE_GRADE,
             'NA' as STORE_SIZE,
             nvl(actual.REGION,TARGET.REGION) as REGION,
             nvl(actual.ZONE_OR_AREA,TARGET.zone_or_area) as ZONE_NAME,
             'NA' as CITY,
             NULL AS RTRLATITUDE,
             NULL AS RTRLONGITUDE,
             nvl(actual.RETAIL_ENVIRONMENT,TARGET.retail_environment) AS Sell_Out_RE,
             nvl(actual.EAN,TARGET.EAN) AS PRODUCT_CODE,
             COALESCE(target.msl_product_desc,PRODUCT.SAP_MAT_DESC,'NA') AS PRODUCT_NAME,
             NULL AS PROD_HIER_L1,
             NULL AS PROD_HIER_L2,
             TARGET.PROD_HIER_L3 AS PROD_HIER_L3,
             target.PROD_HIER_L4 AS  PROD_HIER_L4,
             TARGET.PROD_HIER_L5 AS PROD_HIER_L5,
             NULL AS PROD_HIER_L6,
             NULL AS PROD_HIER_L7,
             NULL AS PROD_HIER_L8,
			 --NULL AS PROD_HIER_L9,
             TARGET.PROD_HIER_L9 AS PROD_HIER_L9,
             target.MAPPED_SKU_CD AS MAPPED_SKU_CD,
			 target.list_price,
             'SELL-OUT' AS data_src,
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
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,'NA'),PRODUCT.GPH_PROD_FRNCHSE,'NA') AS GLOBAL_PRODUCT_FRANCHISE,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_BRAND,'NA'),PRODUCT.GPH_PROD_BRND,'NA') AS GLOBAL_PRODUCT_BRAND,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,'NA'),PRODUCT.GPH_PROD_SUB_BRND,'NA') AS GLOBAL_PRODUCT_SUB_BRAND,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_VARIANT,'NA'),PRODUCT.GPH_PROD_VRNT,'NA') AS GLOBAL_PRODUCT_VARIANT,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SEGMENT,'NA'),PRODUCT.GPH_PROD_NEEDSTATE,'NA') AS GLOBAL_PRODUCT_SEGMENT,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,'NA'),PRODUCT.GPH_PROD_SUBSGMNT,'NA') AS GLOBAL_PRODUCT_SUBSEGMENT,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_CATEGORY,'NA'),PRODUCT.GPH_PROD_CTGRY,'NA') AS GLOBAL_PRODUCT_CATEGORY,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,'NA'),PRODUCT.GPH_PROD_SUBCTGRY,'NA') AS GLOBAL_PRODUCT_SUBCATEGORY,
             COALESCE(nullif(ACTUAL.GLOBAL_PUT_UP_DESCRIPTION,'NA'),PRODUCT.GPH_PROD_PUT_UP_DESC,'NA') AS GLOBAL_PUT_UP_DESCRIPTION,
             --TRIM(NVL (NULLIF(PRODUCT.EAN,''),'NA')) AS EAN,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA'))AS SKU_CODE,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA'))AS SKU_DESCRIPTION,
             TRIM(NVL (NULLIF(PRODUCT.PKA_FRANCHISE_DESC,''),'NA')) AS PKA_FRANCHISE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_BRAND_DESC,''),'NA')) AS PKA_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_BRAND_DESC,''),'NA')) AS PKA_SUB_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_VARIANT_DESC,''),'NA')) AS PKA_VARIANT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_VARIANT_DESC,''),'NA')) AS PKA_SUB_VARIANT_DESC,
             COALESCE(nullif(ACTUAL.PKA_PRODUCT_KEY,'NA'),PRODUCT.PKA_PRODUCT_KEY) AS PKA_PRODUCT_KEY,
             COALESCE(nullif(ACTUAL.PKA_PRODUCT_KEY_DESCRIPTION,'NA'),PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION) AS PKA_PRODUCT_KEY_DESCRIPTION,
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
      FROM itg_anz_sellout_re_msl_list TARGET
        LEFT JOIN (SELECT * FROM wks_anz_sellout_re_actuals) ACTUAL
               ON TARGET.jj_mnth_id = ACTUAL.MNTH_ID
              AND TARGET.DISTRIBUTOR_NAME = ACTUAL.DISTRIBUTOR_NAME
              AND TARGET.STORE_CODE = ACTUAL.STORE_CODE
              AND UPPER (TRIM (TARGET.EAN)) = UPPER (TRIM (ACTUAL.EAN))
			  and target.store_grade=actual.store_grade 
			  and target.market=actual.CNTRY_NM
     
      ----------------customer hierarchy------------------------------          
      
        LEFT JOIN (SELECT * from customer_heirarchy
                   WHERE RANK = 1) CUSTOMER ON  nvl(ltrim(ACTUAL.SOLDTO_CODE,0), ltrim(TARGET.SOLDTO_CODE,0)) = LTRIM (CUSTOMER.SAP_CUST_ID,'0')
      ----------------product hierarchy------------------------------   				   
      
        LEFT JOIN product_heirarchy PRODUCT
                ON LTRIM (target.MAPPED_SKU_CD,'0') = LTRIM (PRODUCT.SAP_MATL_NUM,'0')
              AND PRODUCT.RANK = 1) Q
            

  JOIN (SELECT DISTINCT "cluster",
               CTRY_GROUP
        FROM EDW_COMPANY_DIM
        WHERE CTRY_GROUP in ('Australia','New Zealand')) COM ON UPPER (TRIM (Q.market)) = UPPER (TRIM (com.CTRY_GROUP))
WHERE jj_mnth_id::NUMERIC<= (SELECT MAX(mnth_id)::NUMERIC FROM wks_anz_sellout_re_actuals)
),
anz_rpt_retail_excellence_non_mdp as
(SELECT distinct Q.*,
       COM."cluster",
	   SYSDATE() as crt_dttm
FROM (SELECT LEFT (ACTUAL.MNTH_ID,4) AS YEAR,
             CAST(ACTUAL.MNTH_ID AS INTEGER) AS MNTH_ID,
             -- COM.CLUSTER,			 			
             ACTUAL.CNTRY_NM AS MARKET,
             nvl(actual.channel_name,'NA') AS CHANNEL,
             ACTUAL.soldto_code,
             ACTUAL.DISTRIBUTOR_CODE,
             ACTUAL.DISTRIBUTOR_NAME,
             nvl(actual.channel_name,'NA') AS SELL_OUT_CHANNEL,
             actual.STORE_TYPE,
             NULL AS PRIORITIZATION_SEGMENTATION,
             NULL AS STORE_CATEGORY,
             COALESCE(ACTUAL.STORE_CODE,'NA') AS STORE_CODE,
             COALESCE(ACTUAL.STORE_NAME,'NA') AS STORE_NAME,
             ACTUAL.STORE_GRADE,
             NULL AS STORE_SIZE,
             actual.region,
             actual.zone_or_area AS ZONE_NAME,
             'NA' as CITY,
             NULL AS RTRLATITUDE,
             NULL AS RTRLONGITUDE,
             ACTUAL.retail_environment AS sell_out_RE,
             ACTUAL.EAN AS PRODUCT_CODE,
             COALESCE(actual.msl_product_desc,product.SAP_MAT_DESC,'NA') AS PRODUCT_NAME,
             NULL  AS prod_hier_l1,
             NULL AS prod_hier_l2,
             epd.prod_hier_l3 AS prod_hier_l3,
             epd.prod_hier_l4 AS prod_hier_l4,
             epd.prod_hier_l5 as prod_hier_l5,
             -- varient is sku_name from msl table au_itg.itg_mcs_gt
             NULL AS prod_hier_l6,
             NULL AS prod_hier_l7,
             NULL AS prod_hier_l8,
			 --NULL as  prod_hier_l9,
             epd.prod_hier_l9 as  prod_hier_l9,
             coalesce(nullif(LTRIM (sku.sku_code,'0'),'NA'),ltrim(MAT.MAPPED_SKU_CD,0)) AS MAPPED_SKU_CD,
			 actual.list_price,
             actual.data_src,
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
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,'NA'),PRODUCT.GPH_PROD_FRNCHSE,'NA') AS GLOBAL_PRODUCT_FRANCHISE,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_BRAND,'NA'),PRODUCT.GPH_PROD_BRND,'NA') AS GLOBAL_PRODUCT_BRAND,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,'NA'),PRODUCT.GPH_PROD_SUB_BRND,'NA') AS GLOBAL_PRODUCT_SUB_BRAND,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_VARIANT,'NA'),PRODUCT.GPH_PROD_VRNT,'NA') AS GLOBAL_PRODUCT_VARIANT,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SEGMENT,'NA'),PRODUCT.GPH_PROD_NEEDSTATE,'NA') AS GLOBAL_PRODUCT_SEGMENT,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,'NA'),PRODUCT.GPH_PROD_SUBSGMNT,'NA') AS GLOBAL_PRODUCT_SUBSEGMENT,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_CATEGORY,'NA'),PRODUCT.GPH_PROD_CTGRY,'NA') AS GLOBAL_PRODUCT_CATEGORY,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,'NA'),PRODUCT.GPH_PROD_SUBCTGRY,'NA') AS GLOBAL_PRODUCT_SUBCATEGORY,
             COALESCE(nullif(ACTUAL.GLOBAL_PUT_UP_DESCRIPTION,'NA'),PRODUCT.GPH_PROD_PUT_UP_DESC,'NA') AS GLOBAL_PUT_UP_DESCRIPTION,
             --TRIM(NVL (NULLIF(PRODUCT.EAN,''),'NA')) AS EAN,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA'))AS SKU_CODE,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA'))AS SKU_DESCRIPTION,
             TRIM(NVL (NULLIF(PRODUCT.PKA_FRANCHISE_DESC,''),'NA')) AS PKA_FRANCHISE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_BRAND_DESC,''),'NA')) AS PKA_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_BRAND_DESC,''),'NA')) AS PKA_SUB_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_VARIANT_DESC,''),'NA')) AS PKA_VARIANT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_VARIANT_DESC,''),'NA')) AS PKA_SUB_VARIANT_DESC,
             COALESCE(nullif(ACTUAL.PKA_PRODUCT_KEY,'NA'),PRODUCT.PKA_PRODUCT_KEY) AS PKA_PRODUCT_KEY,
             COALESCE(nullif(ACTUAL.PKA_PRODUCT_KEY_DESCRIPTION,'NA'),PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION) AS PKA_PRODUCT_KEY_DESCRIPTION,
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
      FROM (SELECT *
            FROM wks_anz_sellout_re_actuals A
            WHERE NOT EXISTS (SELECT 1
                              FROM itg_anz_sellout_re_msl_list T
                              WHERE A.MNTH_ID = T.jj_mnth_id
                              AND   A.DISTRIBUTOR_NAME = T.DISTRIBUTOR_NAME
                              and A.CNTRY_NM=T.market 
                              AND   A.STORE_CODE = T.STORE_CODE
                              AND   UPPER(TRIM(A.EAN)) = UPPER(TRIM(T.EAN))
							  and T.store_grade = A.store_grade 
							  )) ACTUAL
left join (select distinct EAN,sku_code,COUNTRY_CODE from wks_anz_sellout_c360_mapped_sku_cd) sku on UPPER(LTRIM(actual.EAN,0)) = UPPER(LTRIM(sku.EAN,0)) and  sku.COUNTRY_CODE = actual.CNTRY_CD
LEFT JOIN (SELECT EAN_NUM,MAPPED_SKU_CD,ctry_key FROM (SELECT DISTINCT EAN_NUM, LTRIM(MATL_NUM,'0') AS MAPPED_SKU_CD,ctry_key,
                                ROW_NUMBER() OVER (PARTITION BY EAN_NUM,ctry_key ORDER BY CRT_DTTM DESC) AS RN
                         FROM EDW_MATERIAL_SALES_DIM A join (select distinct sls_org,ctry_key from EDW_SALES_ORG_DIM 
where ctry_key in ('AU','NZ')) B on A.sls_org=B.sls_org )                       
                   WHERE RN = 1) MAT ON UPPER(LTRIM(actual.EAN,0)) = UPPER(LTRIM(MAT.EAN_NUM,0)) and MAT.ctry_key = actual.CNTRY_CD
left join (
select distinct LTRIM(product_client_code,0) AS ean_number,upper(category_name)  AS prod_hier_l3,
      upper(subcategory_local_name) AS prod_hier_l4,upper(brand_name) AS prod_hier_l5
      ,LTRIM(product_client_code,0)|| ' - '  || product_local_name AS prod_hier_l9,		--//       ,LTRIM(product_client_code,0)+' - ' + product_local_name AS prod_hier_l9,
	  ROW_NUMBER() OVER (PARTITION BY ltrim(product_client_code,0) ORDER BY (LTRIM(product_client_code,0)|| ' - '  || product_local_name) DESC) AS rno		--// 	  ROW_NUMBER() OVER (PARTITION BY ltrim(product_client_code,0) ORDER BY (LTRIM(product_client_code,0)+' - ' + product_local_name) DESC) AS rno
	  from  itg_trax_md_product
      WHERE ITG_TRAX_MD_PRODUCT.BUSINESSUNITID::text = 'PC'::text		--//       WHERE itg_trax_md_product.businessunitid::text = 'PC'::text
AND ITG_TRAX_MD_PRODUCT.MANUFACTURER_NAME::text = 'JOHNSON & JOHNSON'::text)epd		--// AND itg_trax_md_product.manufacturer_name::text = 'JOHNSON & JOHNSON'::text)epd
 ON UPPER (TRIM (ACTUAL.EAN)) = UPPER (TRIM (EPD.EAN_NUMBER)) and rno=1		--//  on UPPER (TRIM (actual.EAN)) = UPPER (TRIM (epd.ean_number)) and rno=1
 

      ----------------customer hierarchy------------------------------          
      
        LEFT JOIN (SELECT *
                   FROM customer_heirarchy
                   WHERE RANK = 1) CUSTOMER ON LTRIM (ACTUAL.soldto_code,'0') = LTRIM (CUSTOMER.SAP_CUST_ID,'0')
      ----------------product hierarchy------------------------------   				   
      
        LEFT JOIN  product_heirarchy product
        
               ON  coalesce(nullif(LTRIM (sku.sku_code,'0'),'NA'),ltrim(MAT.MAPPED_SKU_CD,0)) = LTRIM (PRODUCT.SAP_MATL_NUM,'0')
			   --LTRIM (actual.sku_code,'0') = LTRIM (PRODUCT.SAP_MATL_NUM,'0')
              AND PRODUCT.RANK = 1) Q
              
              			  JOIN (SELECT DISTINCT "cluster",
               CTRY_GROUP
        FROM EDW_COMPANY_DIM
        WHERE CTRY_GROUP in ('Australia','New Zealand')) COM ON UPPER (TRIM (Q.market)) = UPPER (TRIM (com.CTRY_GROUP))
),
anz_rpt_retail_excellence as
(
select * from anz_rpt_retail_excellence_mdp
union 
select * from anz_rpt_retail_excellence_non_mdp
),

final as 
(
select 
jj_year::VARCHAR(16) AS jj_year,
jj_mnth_id::numeric(18,0) AS jj_mnth_id,
market::VARCHAR(50) AS market,
channel_name::VARCHAR(150) AS channel_name,
soldto_code::VARCHAR(255) AS soldto_code,
distributor_code::VARCHAR(32) AS distributor_code,
distributor_name::VARCHAR(255) AS distributor_name,
sell_out_channel::VARCHAR(150) AS sell_out_channel,
store_type::VARCHAR(255) AS store_type,
prioritization_segmentation::VARCHAR(1) AS prioritization_segmentation,
store_category::VARCHAR(1) AS store_category,
store_code::VARCHAR(100) AS store_code,
store_name::VARCHAR(601) AS store_name,
store_grade::VARCHAR(20) AS store_grade,
store_size::VARCHAR(2) AS store_size,
region::VARCHAR(150) AS region,
zone_name::VARCHAR(150) AS zone_name,
city::VARCHAR(2) AS city,
rtrlatitude::VARCHAR(1) AS rtrlatitude,
rtrlongitude::VARCHAR(1) AS rtrlongitude,
sell_out_re::VARCHAR(225) AS sell_out_re,
product_code::VARCHAR(150) AS product_code,
product_name::VARCHAR(300) AS product_name,
prod_hier_l1::VARCHAR(1) AS prod_hier_l1,
prod_hier_l2::VARCHAR(1) AS prod_hier_l2,
prod_hier_l3::VARCHAR(384) AS prod_hier_l3,
prod_hier_l4::VARCHAR(384) AS prod_hier_l4,
prod_hier_l5::VARCHAR(384) AS prod_hier_l5,
prod_hier_l6::VARCHAR(1) AS prod_hier_l6,
prod_hier_l7::VARCHAR(1) AS prod_hier_l7,
prod_hier_l8::VARCHAR(1) AS prod_hier_l8,
prod_hier_l9::VARCHAR(2307) AS prod_hier_l9,
mapped_sku_cd::VARCHAR(40) AS mapped_sku_cd,
list_price::NUMERIC(38,6) AS list_price,
data_src::VARCHAR(8) AS data_src,
customer_segment_key::VARCHAR(12) AS customer_segment_key,
customer_segment_description::VARCHAR(50) AS customer_segment_description,
retail_environment::VARCHAR(225) AS retail_environment,
sap_customer_channel_key::VARCHAR(12) AS sap_customer_channel_key,
sap_customer_channel_description::VARCHAR(75) AS sap_customer_channel_description,
sap_customer_sub_channel_key::VARCHAR(12) AS sap_customer_sub_channel_key,
sap_sub_channel_description::VARCHAR(75) AS sap_sub_channel_description,
sap_parent_customer_key::VARCHAR(12) AS sap_parent_customer_key,
sap_parent_customer_description::VARCHAR(75) AS sap_parent_customer_description,
sap_banner_key::VARCHAR(12) AS sap_banner_key,
sap_banner_description::VARCHAR(75) AS sap_banner_description,
sap_banner_format_key::VARCHAR(12) AS sap_banner_format_key,
sap_banner_format_description::VARCHAR(75) AS sap_banner_format_description,
customer_name::VARCHAR(100) AS customer_name,
customer_code::VARCHAR(10) AS customer_code,
sap_prod_sgmt_cd::VARCHAR(18) AS sap_prod_sgmt_cd,
sap_prod_sgmt_desc::VARCHAR(100) AS sap_prod_sgmt_desc,
sap_base_prod_desc::VARCHAR(100) AS sap_base_prod_desc,
sap_mega_brnd_desc::VARCHAR(100) AS sap_mega_brnd_desc,
sap_brnd_desc::VARCHAR(100) AS sap_brnd_desc,
sap_vrnt_desc::VARCHAR(100) AS sap_vrnt_desc,
sap_put_up_desc::VARCHAR(100) AS sap_put_up_desc,
sap_grp_frnchse_cd::VARCHAR(18) AS sap_grp_frnchse_cd,
sap_grp_frnchse_desc::VARCHAR(100) AS sap_grp_frnchse_desc,
sap_frnchse_cd::VARCHAR(18) AS sap_frnchse_cd,
sap_frnchse_desc::VARCHAR(100) AS sap_frnchse_desc,
sap_prod_frnchse_cd::VARCHAR(18) AS sap_prod_frnchse_cd,
sap_prod_frnchse_desc::VARCHAR(100) AS sap_prod_frnchse_desc,
sap_prod_mjr_cd::VARCHAR(18) AS sap_prod_mjr_cd,
sap_prod_mjr_desc::VARCHAR(100) AS sap_prod_mjr_desc,
sap_prod_mnr_cd::VARCHAR(18) AS sap_prod_mnr_cd,
sap_prod_mnr_desc::VARCHAR(100) AS sap_prod_mnr_desc,
sap_prod_hier_cd::VARCHAR(18) AS sap_prod_hier_cd,
sap_prod_hier_desc::VARCHAR(100) AS sap_prod_hier_desc,
global_product_franchise::VARCHAR(30) AS global_product_franchise,
global_product_brand::VARCHAR(30) AS global_product_brand,
global_product_sub_brand::VARCHAR(100) AS global_product_sub_brand,
global_product_variant::VARCHAR(100) AS global_product_variant,
global_product_segment::VARCHAR(50) AS global_product_segment,
global_product_subsegment::VARCHAR(100) AS global_product_subsegment,
global_product_category::VARCHAR(50) AS global_product_category,
global_product_subcategory::VARCHAR(50) AS global_product_subcategory,
global_put_up_description::VARCHAR(100) AS global_put_up_description,
pka_franchise_desc::VARCHAR(30) AS pka_franchise_desc,
pka_brand_desc::VARCHAR(30) AS pka_brand_desc,
pka_sub_brand_desc::VARCHAR(30) AS pka_sub_brand_desc,
pka_variant_desc::VARCHAR(30) AS pka_variant_desc,
pka_sub_variant_desc::VARCHAR(30) AS pka_sub_variant_desc,
pka_product_key::VARCHAR(68) AS pka_product_key,
pka_product_key_description::VARCHAR(255) AS pka_product_key_description,
sales_value::NUMERIC(38,6) AS sales_value,
sales_qty::NUMERIC(38,6) AS sales_qty,
avg_sales_qty::NUMERIC(10,2) AS avg_sales_qty,
sales_value_list_price::NUMERIC(38,12) AS sales_value_list_price,
lm_sales::NUMERIC(38,6) AS lm_sales,
lm_sales_qty::NUMERIC(38,6) AS lm_sales_qty,
lm_avg_sales_qty::NUMERIC(10,2) AS lm_avg_sales_qty,
lm_sales_lp::NUMERIC(38,12) AS lm_sales_lp,
p3m_sales::NUMERIC(38,6) AS p3m_sales,
p3m_qty::NUMERIC(38,6) AS p3m_qty,
p3m_avg_qty::NUMERIC(38,6) AS p3m_avg_qty,
p3m_sales_lp::NUMERIC(38,12) AS p3m_sales_lp,
f3m_sales::NUMERIC(38,6) AS f3m_sales,
f3m_qty::NUMERIC(38,6) AS f3m_qty,
f3m_avg_qty::NUMERIC(38,6) AS f3m_avg_qty,
p6m_sales::NUMERIC(38,6) AS p6m_sales,
p6m_qty::NUMERIC(38,6) AS p6m_qty,
p6m_avg_qty::NUMERIC(38,6) AS p6m_avg_qty,
p6m_sales_lp::NUMERIC(38,12) AS p6m_sales_lp,
p12m_sales::NUMERIC(38,6) AS p12m_sales,
p12m_qty::NUMERIC(38,6) AS p12m_qty,
p12m_avg_qty::NUMERIC(38,6) AS p12m_avg_qty,
p12m_sales_lp::NUMERIC(38,12) AS p12m_sales_lp,
lm_sales_flag::VARCHAR(1) AS lm_sales_flag,
p3m_sales_flag::VARCHAR(1) AS p3m_sales_flag,
p6m_sales_flag::VARCHAR(1) AS p6m_sales_flag,
p12m_sales_flag::VARCHAR(1) AS p12m_sales_flag,
mdp_flag::VARCHAR(1) AS mdp_flag,
target_complaince::numeric(18,0) AS target_complaince,
"cluster"::VARCHAR(100) AS cluster,
crt_dttm::timestamp without time zone AS crt_dttm
from anz_rpt_retail_excellence
)
--Final select
select * from final