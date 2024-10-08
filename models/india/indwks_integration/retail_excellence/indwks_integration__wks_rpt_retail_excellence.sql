
{{ 
    config(
    sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )}}
--import CTE
with WKS_SKU_RECOM_MSL_TARGETS as 
(
select * from {{ ref('indwks_integration__wks_sku_recom_msl_targets') }}
),
wks_india_regional_sellout_actuals as (
 select * from {{ ref('indwks_integration__wks_india_regional_sellout_actuals') }}
),
edw_product_dim as 
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
    --select * from {{ source('indedw_integration', 'edw_product_dim_sync') }}
),
edw_retailer_dim as (
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
    --select * from {{ source('indedw_integration', 'edw_retailer_dim_sync') }}
),
edw_material_dim as (

select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_gch_producthierarchy as (

 select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
 ),
edw_customer_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_company_dim as (
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
edw_subchnl_retail_env_mapping as(
    select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),

edw_code_descriptions_manual as(
    select * from {{ source('aspedw_integration', 'edw_code_descriptions_manual') }}
),
edw_customer_dim as (
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
    --select * from {{ source('indedw_integration', 'edw_customer_dim_sync') }}
),
EDW_GCH_CUSTOMERHIERARCHY as(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
EDW_CUSTOMER_BASE_DIM as(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
itg_in_mds_channel_mapping as 
(
    select * from {{ ref('inditg_integration__itg_in_mds_channel_mapping') }}
    --select * from {{ source('inditg_integration', 'itg_in_mds_channel_mapping_sync') }}
),
wks_rpt_retail_excellence_mdp as(

SELECT MDP.*,COM.*,SYSDATE() AS CRT_DTTM FROM (SELECT CAST(TARGET.FISC_YR AS INTEGER) AS FISC_YR,
             TARGET.FISC_PER,	 
             TARGET.PROD_HIER_L1 AS MARKET,
             RETAILER.CHANNEL_NAME,
             TARGET.DISTRIBUTOR_CODE,
             COALESCE(ACTUAL.DISTRIBUTOR_NAME,RETAILER.CUSTOMER_NAME) as DISTRIBUTOR_NAME,
			       COALESCE(ACTUAL.BUSINESS_CHANNEL,RETAILER.BUSINESS_CHANNEL) AS SELL_OUT_CHANNEL,
             COALESCE(ACTUAL.RETAILER_CATEGORY_NAME,RETAILER.RETAILER_CATEGORY_NAME) AS STORE_TYPE,
             COALESCE(ACTUAL.CLASS_DESC,RETAILER.CLASS_DESC) AS PRIORITIZATION_SEGMENTATION,
             NVL(TARGET.STORE_CATEGORY,'NA') AS STORE_CATEGORY,
             TARGET.STORE_CODE AS STORE_CODE,
             upper(COALESCE(RETAILER.RETAILER_NAME,ACTUAL.STORE_NAME)) AS STORE_NAME,
             COALESCE(ACTUAL.BUSINESS_CHANNEL,RETAILER.BUSINESS_CHANNEL) AS STORE_GRADE,
             COALESCE(ACTUAL.CLASS_DESC,RETAILER.CLASS_DESC)  AS STORE_SIZE,
             COALESCE(ACTUAL.REGION_NAME,RETAILER.REGION_NAME) AS REGION,
             COALESCE(ACTUAL.ZONE_NAME,RETAILER.ZONE_NAME) AS ZONE_NAME,
             COALESCE(ACTUAL.TOWN_NAME,RETAILER.TOWN_NAME) AS CITY,
             --COALESCE(RETAILER.RTRLATITUDE) AS RTRLATITUDE,
			 RETAILER.RTRLATITUDE AS RTRLATITUDE,
             --COALESCE(RETAILER.RTRLONGITUDE) AS RTRLONGITUDE,
			 RETAILER.RTRLONGITUDE AS RTRLONGITUDE,
             TARGET.MOTHER_SKU_CD AS PRODUCT_CODE,
             LOCAL_PROD.MOTHERSKU_NAME AS PRODUCT_NAME,
             'India' as PROD_HIER_L1,
             NULL AS PROD_HIER_L2,
             LOCAL_PROD.FRANCHISE_NAME as PROD_HIER_L3,
             LOCAL_PROD.BRAND_NAME as PROD_HIER_L4,
             LOCAL_PROD.VARIANT_NAME as PROD_HIER_L5,
             NULL AS PROD_HIER_L6,
             NULL AS PROD_HIER_L7,
             NULL AS PROD_HIER_L8,
             LOCAL_PROD.MOTHERSKU_NAME as PROD_HIER_L9,
             --LOCAL_PROD.MAPPED_SKU_CD,
            COALESCE(ACTUAL.CUSTOMER_SEGMENT_KEY,CUSTOMER.CUST_SEGMT_KEY)AS CUSTOMER_SEGMENT_KEY,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION,CUSTOMER.CUST_SEGMENT_DESC)AS CUSTOMER_SEGMENT_DESCRIPTION,
             COALESCE(cmap.retailer_channel_level_3,'NA')AS RETAIL_ENVIRONMENT,
             COALESCE(ACTUAL.SAP_CUSTOMER_CHANNEL_KEY,CUSTOMER.SAP_CUST_CHNL_KEY)AS SAP_CUSTOMER_CHANNEL_KEY,
             COALESCE(ACTUAL.SAP_CUSTOMER_CHANNEL_DESCRIPTION,CUSTOMER.SAP_CUST_CHNL_DESC)AS SAP_CUSTOMER_CHANNEL_DESCRIPTION,
             COALESCE(ACTUAL.SAP_CUSTOMER_SUB_CHANNEL_KEY,CUSTOMER.SAP_CUST_SUB_CHNL_KEY)AS SAP_CUSTOMER_SUB_CHANNEL_KEY,
             COALESCE(ACTUAL.SAP_SUB_CHANNEL_DESCRIPTION,CUSTOMER.SAP_SUB_CHNL_DESC)AS SAP_SUB_CHANNEL_DESCRIPTION,
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_KEY,CUSTOMER.SAP_PRNT_CUST_KEY)AS SAP_PARENT_CUSTOMER_KEY,
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_DESCRIPTION,CUSTOMER.SAP_PRNT_CUST_DESC)AS SAP_PARENT_CUSTOMER_DESCRIPTION,
             COALESCE(ACTUAL.SAP_BANNER_KEY,CUSTOMER.SAP_BNR_KEY)AS SAP_BANNER_KEY,
             COALESCE(ACTUAL.SAP_BANNER_DESCRIPTION,CUSTOMER.SAP_BNR_DESC)AS SAP_BANNER_DESCRIPTION,
             COALESCE(ACTUAL.SAP_BANNER_FORMAT_KEY,CUSTOMER.SAP_BNR_FRMT_KEY)AS SAP_BANNER_FORMAT_KEY,
             COALESCE(ACTUAL.SAP_BANNER_FORMAT_DESCRIPTION,CUSTOMER.SAP_BNR_FRMT_DESC)AS SAP_BANNER_FORMAT_DESCRIPTION, 
            --REGION,
            --ZONE_OR_AREA,
             TRIM(NVL (NULLIF(CUSTOMER.SAP_CUST_NM,''),'NA')) AS CUSTOMER_NAME,
             TRIM(NVL (NULLIF(CUSTOMER.SAP_CUST_ID,''),'NA')) AS CUSTOMER_CODE,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_SGMT_CD,''),'NA'))AS SAP_PROD_SGMT_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_SGMT_DESC,''),'NA'))AS SAP_PROD_SGMT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_BASE_PROD_DESC,''),'NA'))AS SAP_BASE_PROD_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MEGA_BRND_DESC,''),'NA'))AS SAP_MEGA_BRND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_BRND_DESC,''),'NA'))AS SAP_BRND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_VRNT_DESC,''),'NA'))AS SAP_VRNT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PUT_UP_DESC,''),'NA'))AS SAP_PUT_UP_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_GRP_FRNCHSE_CD,''),'NA'))AS SAP_GRP_FRNCHSE_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_GRP_FRNCHSE_DESC,''),'NA'))AS SAP_GRP_FRNCHSE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_FRNCHSE_CD,''),'NA'))AS SAP_FRNCHSE_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_FRNCHSE_DESC,''),'NA'))AS SAP_FRNCHSE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_FRNCHSE_CD,''),'NA'))AS SAP_PROD_FRNCHSE_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_FRNCHSE_DESC,''),'NA'))AS SAP_PROD_FRNCHSE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MJR_CD,''),'NA'))AS SAP_PROD_MJR_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MJR_DESC,''),'NA'))AS SAP_PROD_MJR_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MNR_CD,''),'NA'))AS SAP_PROD_MNR_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MNR_DESC,''),'NA'))AS SAP_PROD_MNR_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_HIER_CD,''),'NA'))AS SAP_PROD_HIER_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_HIER_DESC,''),'NA'))AS SAP_PROD_HIER_DESC,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,PRODUCT.GPH_PROD_FRNCHSE)AS GLOBAL_PRODUCT_FRANCHISE,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_BRAND,PRODUCT.GPH_PROD_BRND)AS GLOBAL_PRODUCT_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,PRODUCT.GPH_PROD_SUB_BRND)AS GLOBAL_PRODUCT_SUB_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_VARIANT,PRODUCT.GPH_PROD_VRNT)AS GLOBAL_PRODUCT_VARIANT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SEGMENT,PRODUCT.GPH_PROD_NEEDSTATE)AS GLOBAL_PRODUCT_SEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,PRODUCT.GPH_PROD_SUBSGMNT)AS GLOBAL_PRODUCT_SUBSEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_CATEGORY,PRODUCT.GPH_PROD_CTGRY)AS GLOBAL_PRODUCT_CATEGORY,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,PRODUCT.GPH_PROD_SUBCTGRY)AS GLOBAL_PRODUCT_SUBCATEGORY,
             COALESCE(ACTUAL.GLOBAL_PUT_UP_DESCRIPTION,PRODUCT.GPH_PROD_PUT_UP_DESC)AS GLOBAL_PUT_UP_DESCRIPTION,
             TRIM(NVL (NULLIF(PRODUCT.EAN,''),'NA')) AS EAN,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA'))AS SKU_CODE,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA'))AS SKU_DESCRIPTION,
             TRIM(NVL (NULLIF(PRODUCT.PKA_FRANCHISE_DESC,''),'NA'))AS PKA_FRANCHISE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_BRAND_DESC,''),'NA'))AS PKA_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_BRAND_DESC,''),'NA'))AS PKA_SUB_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_VARIANT_DESC,''),'NA'))AS PKA_VARIANT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_VARIANT_DESC,''),'NA'))AS PKA_SUB_VARIANT_DESC,
             COALESCE(ACTUAL.PKA_PRODUCT_KEY,PRODUCT.PKA_PRODUCT_KEY)AS PKA_PRODUCT_KEY,
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
             COALESCE(ACTUAL.P3M_SALES_FLAG,'N')AS P3M_SALES_FLAG,
             COALESCE(ACTUAL.P6M_SALES_FLAG,'N') AS P6M_SALES_FLAG,
             COALESCE(ACTUAL.P12M_SALES_FLAG,'N')AS P12M_SALES_FLAG,
             'Y' AS MDP_FLAG,
      		 1 AS TARGET_COMPLAINCE
FROM WKS_SKU_RECOM_MSL_TARGETS TARGET
        LEFT JOIN (SELECT * FROM WKS_INDIA_REGIONAL_SELLOUT_ACTUALS) ACTUAL
               ON TARGET.FISC_PER = ACTUAL.MNTH_ID
              AND TARGET.DISTRIBUTOR_CODE = ACTUAL.DISTRIBUTOR_CODE
              AND TARGET.STORE_CODE = ACTUAL.STORE_CODE
              AND TARGET.MOTHER_SKU_CD = ACTUAL.MOTHERSKU_CODE 
        LEFT JOIN (SELECT * FROM (SELECT DISTINCT FRANCHISE_NAME,BRAND_NAME,PRODUCT_CATEGORY_NAME,
						       VARIANT_NAME,MOTHERSKU_NAME,MOTHERSKU_CODE,PRODUCT_CODE,row_number () over (partition by mothersku_code order by crt_dttm desc) as rn
               FROM edw_product_dim  WHERE PRODUCT_CODE NOT IN ('233test1')) WHERE RN=1)LOCAL_PROD
    ON TARGET.MOTHER_SKU_CD=LOCAL_PROD.MOTHERSKU_CODE 
   
    	  
LEFT JOIN (SELECT * FROM(SELECT *, ROW_NUMBER() OVER (PARTITION BY CUSTOMER_CODE,RTRUNIQUECODE ORDER BY END_DATE DESC) AS RN
                         FROM EDW_RETAILER_DIM) WHERE RN=1) RETAILER
                     ON  TARGET.STORE_CODE =  RETAILER.RTRUNIQUECODE
                    AND TARGET.DISTRIBUTOR_CODE = RETAILER.CUSTOMER_CODE
left join 	(select distinct retailer_channel_level_3,channel_name,retailer_category_name,retailer_class,territory_classification from itg_in_mds_channel_mapping ) cmap on cmap.channel_name= CASE WHEN CASE WHEN RETAILER.channel_name= '' THEN NULL
 ELSE RETAILER.channel_name END IS NULL THEN 'Unknown' ELSE RETAILER.channel_name END	
AND cmap.retailer_category_name = CASE WHEN CASE WHEN RETAILER.retailer_category_name = '' THEN NULL ELSE RETAILER.retailer_category_name END IS NULL THEN 'Unknown' ELSE RETAILER.retailer_category_name END
AND cmap.retailer_class = CASE WHEN CASE WHEN RETAILER.class_desc = '' THEN NULL  ELSE RETAILER.class_desc END IS NULL THEN 'Unknown' ELSE RETAILER.class_desc END											   
AND cmap.territory_classification = CASE WHEN CASE WHEN RETAILER.territory_classification = '' THEN NULL
ELSE RETAILER.territory_classification END IS NULL THEN 'Unknown' ELSE RETAILER.territory_classification END					
                    
LEFT JOIN (SELECT * FROM(SELECT DISTINCT 
                          INDIA_PROD.MOTHERSKU_CODE AS MOTHERSKU_CODE,
                          EMD.MATL_NUM AS SAP_MATL_NUM,
                          EMD.MATL_DESC AS SAP_MAT_DESC,
                          EMD.MATL_TYPE_CD AS SAP_MAT_TYPE_CD,
                          EMD.MATL_TYPE_DESC AS SAP_MAT_TYPE_DESC,
                          EMD.PRODH1 AS SAP_PROD_SGMT_CD,
                          EMD.PRODH1_TXTMD AS SAP_PROD_SGMT_DESC,
                          EMD.BASE_PROD_DESC AS SAP_BASE_PROD_DESC,
                          EMD.MEGA_BRND_DESC AS SAP_MEGA_BRND_DESC,
                          EMD.BRND_DESC AS SAP_BRND_DESC,
                          EMD.VARNT_DESC AS SAP_VRNT_DESC,
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
                          EMD.PKA_PRODUCT_KEY AS PKA_PRODUCT_KEY,
                          EMD.PKA_PRODUCT_KEY_DESCRIPTION AS PKA_PRODUCT_KEY_DESCRIPTION,
                          EMD.PKA_FRANCHISE_DESC,
                          EMD.PKA_BRAND_DESC,
                          EMD.PKA_SUB_BRAND_DESC,
                          EMD.PKA_VARIANT_DESC,
                          EMD.PKA_SUB_VARIANT_DESC,
						              EMD.PRMRY_UPC_CD as EAN,					  
                          EGPH."region" AS GPH_REGION,
                          EGPH.REGIONAL_FRANCHISE AS GPH_REG_FRNCHSE,
                          EGPH.REGIONAL_FRANCHISE_GROUP AS GPH_REG_FRNCHSE_GRP,
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
                          ROW_NUMBER() OVER (PARTITION BY sap_matl_num ORDER BY sap_matl_num) RANK
                   FROM  EDW_MATERIAL_DIM EMD,
                        --EDW_GCH_PRODUCTHIERARCHY EGPH,
                        (SELECT * FROM (SELECT a.*, ROW_NUMBER() OVER (PARTITION BY LTRIM(MATERIALNUMBER,0) ORDER BY LTRIM(MATERIALNUMBER,0)) rnum FROM EDW_GCH_PRODUCTHIERARCHY a) where rnum=1) EGPH,
                  (SELECT * FROM (SELECT DISTINCT FRANCHISE_NAME,BRAND_NAME,PRODUCT_CATEGORY_NAME,
						       VARIANT_NAME,MOTHERSKU_NAME,MOTHERSKU_CODE,PRODUCT_CODE,row_number () over (partition by mothersku_code order by crt_dttm desc) as rn
                   FROM EDW_PRODUCT_DIM  WHERE PRODUCT_CODE NOT IN ('233test1')) WHERE RN=1)INDIA_PROD 
                   WHERE LTRIM(EMD.MATL_NUM,0) = LTRIM(EGPH.MATERIALNUMBER (+),0) 
                   AND LTRIM(EMD.MATL_NUM,0)= INDIA_PROD.PRODUCT_CODE(+)      
                   AND   EMD.PROD_HIER_CD <> ''
                   ))PRODUCT ON TARGET.MOTHER_SKU_CD=PRODUCT.MOTHERSKU_CODE
                   
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
                                REGZONE.REGION_NAME AS REGION,
                                REGZONE.ZONE_NAME AS ZONE_OR_AREA,
                                EGCH.GCGH_REGION AS GCH_REGION,
                                EGCH.GCGH_CLUSTER AS GCH_CLUSTER,
                                EGCH.GCGH_SUBCLUSTER AS GCH_SUBCLUSTER,
                                EGCH.GCGH_MARKET AS GCH_MARKET,
                                EGCH.GCCH_RETAIL_BANNER AS GCH_RETAIL_BANNER,
                                ECSD.SEGMT_KEY AS CUST_SEGMT_KEY,
                                CODES_SEGMENT.CODE_DESC AS CUST_SEGMENT_DESC,
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
                              EDW_CODE_DESCRIPTIONS_MANUAL CODES_SEGMENT,
                            (SELECT DISTINCT CUST_NUM,
                                      REC_CRT_DT,
                                      PRNT_CUST_KEY,
                                      ROW_NUMBER() OVER (PARTITION BY CUST_NUM ORDER BY REC_CRT_DT DESC) RN
                               FROM EDW_CUSTOMER_SALES_DIM
                               WHERE SLS_ORG IN
                              (SELECT DISTINCT SLS_ORG
                                FROM EDW_SALES_ORG_DIM
                                WHERE STATS_CRNCY IN ('INR','LKR','BDT'))) A,
                              (SELECT DISTINCT CUSTOMER_CODE,
                                      REGION_NAME,
                                      ZONE_NAME
                               FROM EDW_CUSTOMER_DIM) REGZONE
                         WHERE EGCH.CUSTOMER (+) = ECBD.CUST_NUM
                         AND   ECSD.CUST_NUM = ECBD.CUST_NUM
                           AND   A.CUST_NUM = ECSD.CUST_NUM
                         AND   ECSD.DSTR_CHNL = EDC.DISTR_CHAN
                         AND   ECSD.SLS_ORG = ESOD.SLS_ORG
                         AND   ESOD.SLS_ORG_CO_CD = ECD.CO_CD
                         AND   ECSD.SLS_ORG IN (SELECT DISTINCT SLS_ORG
                                                FROM EDW_SALES_ORG_DIM
                                                WHERE STATS_CRNCY IN ('INR','LKR','BDT'))
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
                         AND   UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL (+)) = UPPER(CDDES_SUBCHNL.CODE_DESC)
                         AND   LTRIM(ECSD.CUST_NUM,'0') = REGZONE.CUSTOMER_CODE (+)
                         AND   CODES_SEGMENT.CODE_TYPE (+) = 'CUSTOMER SEGMENTATION KEY'
                         AND   CODES_SEGMENT.CODE (+) = ECSD.SEGMT_KEY)
                   WHERE RANK = 1) CUSTOMER ON LTRIM (TARGET.DISTRIBUTOR_CODE,'0') = LTRIM (CUSTOMER.SAP_CUST_ID,'0')
                   
)MDP,
					
 (SELECT DISTINCT "cluster" FROM EDW_COMPANY_DIM WHERE CTRY_GROUP='India')COM 
 WHERE FISC_PER <= (select max(mnth_id) from wks_india_regional_sellout_actuals)
 ),
wks_rpt_retail_excellence_nonmdp as 
 (
 SELECT NON_MDP.*,COM.*,SYSDATE() AS CRT_DTTM FROM
(SELECT      CAST(ACTUAL.YEAR AS INTEGER) AS YEAR,
             CAST(ACTUAL.MNTH_ID AS INTEGER) AS MNTH_ID,	 			
            ACTUAL.CNTRY_NM AS MARKET,
            RETAILER.CHANNEL_NAME AS CHANNEL,
            ACTUAL.DISTRIBUTOR_CODE,
            COALESCE(ACTUAL.DISTRIBUTOR_NAME,RETAILER.CUSTOMER_NAME) as DISTRIBUTOR_NAME,
			       COALESCE(ACTUAL.BUSINESS_CHANNEL,RETAILER.BUSINESS_CHANNEL) AS SELL_OUT_CHANNEL,
             COALESCE(ACTUAL.RETAILER_CATEGORY_NAME,RETAILER.RETAILER_CATEGORY_NAME) AS STORE_TYPE,
             COALESCE(ACTUAL.CLASS_DESC,RETAILER.CLASS_DESC) AS PRIORITIZATION_SEGMENTATION,
             'NA' AS STORE_CATEGORY,
             ACTUAL.STORE_CODE AS STORE_CODE,
             upper(COALESCE(RETAILER.RETAILER_NAME,ACTUAL.STORE_NAME)) AS STORE_NAME,
             COALESCE(ACTUAL.BUSINESS_CHANNEL,RETAILER.BUSINESS_CHANNEL) AS STORE_GRADE,
             COALESCE(ACTUAL.CLASS_DESC,RETAILER.CLASS_DESC)  AS STORE_SIZE,
             COALESCE(ACTUAL.REGION_NAME,RETAILER.REGION_NAME) AS REGION,
             COALESCE(ACTUAL.ZONE_NAME,RETAILER.ZONE_NAME) AS ZONE_NAME,
             COALESCE(ACTUAL.TOWN_NAME,RETAILER.TOWN_NAME) AS CITY,
             --COALESCE(RETAILER.RTRLATITUDE) AS RTRLATITUDE,
			 RETAILER.RTRLATITUDE AS RTRLATITUDE,
             --COALESCE(RETAILER.RTRLONGITUDE) AS RTRLONGITUDE,
			 RETAILER.RTRLONGITUDE AS RTRLONGITUDE,
             ACTUAL.MOTHERSKU_CODE AS PRODUCT_CODE,
             LOCAL_PROD.MOTHERSKU_NAME AS PRODUCT_NAME,
             'India' AS PROD_HIER_L1,
             NULL AS PROD_HIER_L2,
             LOCAL_PROD.FRANCHISE_NAME AS PROD_HIER_L3,
             LOCAL_PROD.BRAND_NAME AS PROD_HIER_L4,
             LOCAL_PROD.VARIANT_NAME AS PROD_HIER_L5,
             NULL AS PROD_HIER_L6,
             NULL AS PROD_HIER_L7,
             NULL AS PROD_HIER_L8,
             LOCAL_PROD.MOTHERSKU_NAME AS PROD_HIER_L9,
           --  LOCAL_PROD.MAPPED_SKU_CD AS MAPPED_SKU_CD,		
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_KEY,CUSTOMER.CUST_SEGMT_KEY)AS CUSTOMER_SEGMENT_KEY,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION,CUSTOMER.CUST_SEGMENT_DESC)AS CUSTOMER_SEGMENT_DESCRIPTION,
             COALESCE(cmap.retailer_channel_level_3,'NA')AS RETAIL_ENVIRONMENT,
             COALESCE(ACTUAL.SAP_CUSTOMER_CHANNEL_KEY,CUSTOMER.SAP_CUST_CHNL_KEY)AS SAP_CUSTOMER_CHANNEL_KEY,
             COALESCE(ACTUAL.SAP_CUSTOMER_CHANNEL_DESCRIPTION,CUSTOMER.SAP_CUST_CHNL_DESC)AS SAP_CUSTOMER_CHANNEL_DESCRIPTION,
             COALESCE(ACTUAL.SAP_CUSTOMER_SUB_CHANNEL_KEY,CUSTOMER.SAP_CUST_SUB_CHNL_KEY)AS SAP_CUSTOMER_SUB_CHANNEL_KEY,
             COALESCE(ACTUAL.SAP_SUB_CHANNEL_DESCRIPTION,CUSTOMER.SAP_SUB_CHNL_DESC)AS SAP_SUB_CHANNEL_DESCRIPTION,
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_KEY,CUSTOMER.SAP_PRNT_CUST_KEY)AS SAP_PARENT_CUSTOMER_KEY,
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_DESCRIPTION,CUSTOMER.SAP_PRNT_CUST_DESC)AS SAP_PARENT_CUSTOMER_DESCRIPTION,
             COALESCE(ACTUAL.SAP_BANNER_KEY,CUSTOMER.SAP_BNR_KEY)AS SAP_BANNER_KEY,
             COALESCE(ACTUAL.SAP_BANNER_DESCRIPTION,CUSTOMER.SAP_BNR_DESC)AS SAP_BANNER_DESCRIPTION,
             COALESCE(ACTUAL.SAP_BANNER_FORMAT_KEY,CUSTOMER.SAP_BNR_FRMT_KEY)AS SAP_BANNER_FORMAT_KEY,
             COALESCE(ACTUAL.SAP_BANNER_FORMAT_DESCRIPTION,CUSTOMER.SAP_BNR_FRMT_DESC)AS SAP_BANNER_FORMAT_DESCRIPTION, 
             --REGION,
            --ZONE_OR_AREA,
             TRIM(NVL (NULLIF(CUSTOMER.SAP_CUST_NM,''),'NA')) AS CUSTOMER_NAME,
             TRIM(NVL (NULLIF(CUSTOMER.SAP_CUST_ID,''),'NA')) AS CUSTOMER_CODE,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_SGMT_CD,''),'NA'))AS SAP_PROD_SGMT_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_SGMT_DESC,''),'NA'))AS SAP_PROD_SGMT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_BASE_PROD_DESC,''),'NA'))AS SAP_BASE_PROD_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MEGA_BRND_DESC,''),'NA'))AS SAP_MEGA_BRND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_BRND_DESC,''),'NA'))AS SAP_BRND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_VRNT_DESC,''),'NA'))AS SAP_VRNT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PUT_UP_DESC,''),'NA'))AS SAP_PUT_UP_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_GRP_FRNCHSE_CD,''),'NA'))AS SAP_GRP_FRNCHSE_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_GRP_FRNCHSE_DESC,''),'NA'))AS SAP_GRP_FRNCHSE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_FRNCHSE_CD,''),'NA'))AS SAP_FRNCHSE_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_FRNCHSE_DESC,''),'NA'))AS SAP_FRNCHSE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_FRNCHSE_CD,''),'NA'))AS SAP_PROD_FRNCHSE_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_FRNCHSE_DESC,''),'NA'))AS SAP_PROD_FRNCHSE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MJR_CD,''),'NA'))AS SAP_PROD_MJR_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MJR_DESC,''),'NA'))AS SAP_PROD_MJR_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MNR_CD,''),'NA'))AS SAP_PROD_MNR_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MNR_DESC,''),'NA'))AS SAP_PROD_MNR_DESC,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_HIER_CD,''),'NA'))AS SAP_PROD_HIER_CD,
             TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_HIER_DESC,''),'NA'))AS SAP_PROD_HIER_DESC,     
             COALESCE(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,PRODUCT.GPH_PROD_FRNCHSE)AS GLOBAL_PRODUCT_FRANCHISE,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_BRAND,PRODUCT.GPH_PROD_BRND)AS GLOBAL_PRODUCT_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,PRODUCT.GPH_PROD_SUB_BRND)AS GLOBAL_PRODUCT_SUB_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_VARIANT,PRODUCT.GPH_PROD_VRNT)AS GLOBAL_PRODUCT_VARIANT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SEGMENT,PRODUCT.GPH_PROD_NEEDSTATE)AS GLOBAL_PRODUCT_SEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,PRODUCT.GPH_PROD_SUBSGMNT)AS GLOBAL_PRODUCT_SUBSEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_CATEGORY,PRODUCT.GPH_PROD_CTGRY)AS GLOBAL_PRODUCT_CATEGORY,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,PRODUCT.GPH_PROD_SUBCTGRY)AS GLOBAL_PRODUCT_SUBCATEGORY,
             COALESCE(ACTUAL.GLOBAL_PUT_UP_DESCRIPTION,PRODUCT.GPH_PROD_PUT_UP_DESC)AS GLOBAL_PUT_UP_DESCRIPTION,
             TRIM(NVL (NULLIF(PRODUCT.EAN,''),'NA')) AS EAN,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA'))AS SKU_CODE,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA'))AS SKU_DESCRIPTION,
             TRIM(NVL (NULLIF(PRODUCT.PKA_FRANCHISE_DESC,''),'NA'))AS PKA_FRANCHISE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_BRAND_DESC,''),'NA'))AS PKA_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_BRAND_DESC,''),'NA'))AS PKA_SUB_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_VARIANT_DESC,''),'NA'))AS PKA_VARIANT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_VARIANT_DESC,''),'NA'))AS PKA_SUB_VARIANT_DESC,
             COALESCE(ACTUAL.PKA_PRODUCT_KEY,PRODUCT.PKA_PRODUCT_KEY)AS PKA_PRODUCT_KEY,
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
FROM ( SELECT *
		FROM WKS_INDIA_REGIONAL_SELLOUT_ACTUALS A
		WHERE NOT EXISTS (SELECT 1
						  FROM WKS_SKU_RECOM_MSL_TARGETS T
						  WHERE A.MNTH_ID = T.FISC_PER 
						   AND A.DISTRIBUTOR_CODE = T.DISTRIBUTOR_CODE 
						   AND A.STORE_CODE = T.STORE_CODE 
						   AND A.MOTHERSKU_CODE = T.MOTHER_SKU_CD)
 
			   )ACTUAL
 
    LEFT JOIN /*(SELECT DISTINCT 
						FRANCHISE_NAME,BRAND_NAME,PRODUCT_CATEGORY_NAME,
						VARIANT_NAME,MOTHERSKU_NAME,MOTHERSKU_CODE,MAX(PRODUCT_CODE) OVER (PARTITION BY MOTHERSKU_CODE) AS MAPPED_SKU_CD
				FROM EDW_PRODUCT_DIM)*/
				(SELECT * FROM (SELECT DISTINCT FRANCHISE_NAME,BRAND_NAME,PRODUCT_CATEGORY_NAME,
						VARIANT_NAME,MOTHERSKU_NAME,MOTHERSKU_CODE,PRODUCT_CODE,ROW_NUMBER() OVER(PARTITION BY MOTHERSKU_CODE ORDER BY  CRT_DTTM DESC) AS RN 
                   FROM EDW_PRODUCT_DIM WHERE PRODUCT_CODE NOT IN ('233test1'))WHERE RN=1) LOCAL_PROD
    ON ACTUAL.MOTHERSKU_CODE=LOCAL_PROD.MOTHERSKU_CODE
    	
LEFT JOIN (SELECT * FROM(SELECT *, ROW_NUMBER() OVER (PARTITION BY CUSTOMER_CODE,RTRUNIQUECODE ORDER BY END_DATE DESC) AS RN
                         FROM EDW_RETAILER_DIM) WHERE RN=1) RETAILER
                     ON  ACTUAL.STORE_CODE =  RETAILER.RTRUNIQUECODE
                    AND ACTUAL.DISTRIBUTOR_CODE = RETAILER.CUSTOMER_CODE
left join 	(select distinct retailer_channel_level_3,channel_name,retailer_category_name,retailer_class,territory_classification from itg_in_mds_channel_mapping ) cmap on cmap.channel_name= CASE WHEN CASE WHEN RETAILER.channel_name= '' THEN NULL
 ELSE RETAILER.channel_name END IS NULL THEN 'Unknown' ELSE RETAILER.channel_name END	
AND cmap.retailer_category_name = CASE WHEN CASE WHEN RETAILER.retailer_category_name = '' THEN NULL ELSE RETAILER.retailer_category_name END IS NULL THEN 'Unknown' ELSE RETAILER.retailer_category_name END
AND cmap.retailer_class = CASE WHEN CASE WHEN RETAILER.class_desc = '' THEN NULL  ELSE RETAILER.class_desc END IS NULL THEN 'Unknown' ELSE RETAILER.class_desc END											   
AND cmap.territory_classification = CASE WHEN CASE WHEN RETAILER.territory_classification = '' THEN NULL
ELSE RETAILER.territory_classification END IS NULL THEN 'Unknown' ELSE RETAILER.territory_classification END					
					
                    
LEFT JOIN (SELECT * FROM(SELECT DISTINCT 
                          INDIA_PROD.MOTHERSKU_CODE AS MOTHERSKU_CODE,
                          EMD.MATL_NUM AS SAP_MATL_NUM,
                          EMD.MATL_DESC AS SAP_MAT_DESC,
                          EMD.MATL_TYPE_CD AS SAP_MAT_TYPE_CD,
                          EMD.MATL_TYPE_DESC AS SAP_MAT_TYPE_DESC,
                          EMD.PRODH1 AS SAP_PROD_SGMT_CD,
                          EMD.PRODH1_TXTMD AS SAP_PROD_SGMT_DESC,
                          EMD.BASE_PROD_DESC AS SAP_BASE_PROD_DESC,
                          EMD.MEGA_BRND_DESC AS SAP_MEGA_BRND_DESC,
                          EMD.BRND_DESC AS SAP_BRND_DESC,
                          EMD.VARNT_DESC AS SAP_VRNT_DESC,
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
                          EMD.PKA_PRODUCT_KEY AS PKA_PRODUCT_KEY,
                          EMD.PKA_PRODUCT_KEY_DESCRIPTION AS PKA_PRODUCT_KEY_DESCRIPTION,
                          EMD.PKA_FRANCHISE_DESC,
                          EMD.PKA_BRAND_DESC,
                          EMD.PKA_SUB_BRAND_DESC,
                          EMD.PKA_VARIANT_DESC,
                          EMD.PKA_SUB_VARIANT_DESC,
						              EMD.PRMRY_UPC_CD as EAN,					  
                          EGPH."region" AS GPH_REGION,
                          EGPH.REGIONAL_FRANCHISE AS GPH_REG_FRNCHSE,
                          EGPH.REGIONAL_FRANCHISE_GROUP AS GPH_REG_FRNCHSE_GRP,
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
                          ROW_NUMBER() OVER (PARTITION BY sap_matl_num ORDER BY sap_matl_num) RANK
                   FROM  EDW_MATERIAL_DIM EMD,
                        --EDW_GCH_PRODUCTHIERARCHY EGPH,
                        (SELECT * FROM (SELECT a.*, ROW_NUMBER() OVER (PARTITION BY LTRIM(MATERIALNUMBER,0) ORDER BY LTRIM(MATERIALNUMBER,0)) rnum FROM EDW_GCH_PRODUCTHIERARCHY a) where rnum=1) EGPH,
                  (SELECT * FROM (SELECT DISTINCT FRANCHISE_NAME,BRAND_NAME,PRODUCT_CATEGORY_NAME,
						       VARIANT_NAME,MOTHERSKU_NAME,MOTHERSKU_CODE,PRODUCT_CODE,row_number () over (partition by mothersku_code order by crt_dttm desc) as rn
                   FROM EDW_PRODUCT_DIM  WHERE PRODUCT_CODE NOT IN ('233test1')) WHERE RN=1)INDIA_PROD 
                   WHERE LTRIM(EMD.MATL_NUM,0) = LTRIM(EGPH.MATERIALNUMBER (+),0) 
                   AND LTRIM(EMD.MATL_NUM,0)= INDIA_PROD.PRODUCT_CODE(+)      
                   AND   EMD.PROD_HIER_CD <> ''
                   ))PRODUCT ON ACTUAL.MOTHERSKU_CODE=PRODUCT.MOTHERSKU_CODE
                   
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
                                REGZONE.REGION_NAME AS REGION,
                                REGZONE.ZONE_NAME AS ZONE_OR_AREA,
                                EGCH.GCGH_REGION AS GCH_REGION,
                                EGCH.GCGH_CLUSTER AS GCH_CLUSTER,
                                EGCH.GCGH_SUBCLUSTER AS GCH_SUBCLUSTER,
                                EGCH.GCGH_MARKET AS GCH_MARKET,
                                EGCH.GCCH_RETAIL_BANNER AS GCH_RETAIL_BANNER,
                                ECSD.SEGMT_KEY AS CUST_SEGMT_KEY,
                                CODES_SEGMENT.CODE_DESC AS CUST_SEGMENT_DESC,
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
                              EDW_CODE_DESCRIPTIONS_MANUAL CODES_SEGMENT,
                            (SELECT DISTINCT CUST_NUM,
                                      REC_CRT_DT,
                                      PRNT_CUST_KEY,
                                      ROW_NUMBER() OVER (PARTITION BY CUST_NUM ORDER BY REC_CRT_DT DESC) RN
                               FROM EDW_CUSTOMER_SALES_DIM
                               WHERE SLS_ORG IN
                              (SELECT DISTINCT SLS_ORG
                                FROM EDW_SALES_ORG_DIM
                                WHERE STATS_CRNCY IN ('INR','LKR','BDT'))) A,
                              (SELECT DISTINCT CUSTOMER_CODE,
                                      REGION_NAME,
                                      ZONE_NAME
                               FROM EDW_CUSTOMER_DIM) REGZONE
                         WHERE EGCH.CUSTOMER (+) = ECBD.CUST_NUM
                         AND   ECSD.CUST_NUM = ECBD.CUST_NUM
                           AND   A.CUST_NUM = ECSD.CUST_NUM
                         AND   ECSD.DSTR_CHNL = EDC.DISTR_CHAN
                         AND   ECSD.SLS_ORG = ESOD.SLS_ORG
                         AND   ESOD.SLS_ORG_CO_CD = ECD.CO_CD
                         AND   ECSD.SLS_ORG IN (SELECT DISTINCT SLS_ORG
                                                FROM EDW_SALES_ORG_DIM
                                                WHERE STATS_CRNCY IN ('INR','LKR','BDT'))
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
                         AND   UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL (+)) = UPPER(CDDES_SUBCHNL.CODE_DESC)
                         AND   LTRIM(ECSD.CUST_NUM,'0') = REGZONE.CUSTOMER_CODE (+)
                         AND   CODES_SEGMENT.CODE_TYPE (+) = 'CUSTOMER SEGMENTATION KEY'
                         AND   CODES_SEGMENT.CODE (+) = ECSD.SEGMT_KEY)
                   WHERE RANK = 1) CUSTOMER ON LTRIM (ACTUAL.DISTRIBUTOR_CODE,'0') = LTRIM (CUSTOMER.SAP_CUST_ID,'0'))NON_MDP,
                     (SELECT DISTINCT "cluster" FROM EDW_COMPANY_DIM WHERE CTRY_GROUP='India')COM
 ),
wks_rpt_retail_excellence as 
(

select * from wks_rpt_retail_excellence_mdp 

union all 

select * from wks_rpt_retail_excellence_nonmdp 

),
final as(

select 

	fisc_per::numeric(18,0) AS fisc_per,
	fisc_yr::numeric(18,0) AS fisc_yr,
	market::varchar(5) AS market,
	channel_name::varchar(150) AS channel_name,
	distributor_code::varchar(100) AS distributor_code,
	distributor_name::varchar(255) AS distributor_name,
	sell_out_channel::varchar(150) AS sell_out_channel,
	store_type::varchar(150) AS store_type,
	prioritization_segmentation::varchar(150) AS prioritization_segmentation,
	store_category::varchar(203) AS store_category,
	store_code::varchar(100) AS store_code,
	store_name::varchar(1125) AS store_name,
	store_grade::varchar(150) AS store_grade,
	store_size::varchar(150) AS store_size,
	region::varchar(150) AS region,
	zone_name::varchar(150) AS zone_name,
	city::varchar(50) AS city,
	rtrlatitude::varchar(40) AS rtrlatitude,
	rtrlongitude::varchar(40) AS rtrlongitude,
	product_code::varchar(50) AS product_code,
	product_name::varchar(150) AS product_name,
	prod_hier_l1::varchar(5) AS prod_hier_l1,
	prod_hier_l2::varchar(1) AS prod_hier_l2,
	prod_hier_l3::varchar(50) AS prod_hier_l3,
	prod_hier_l4::varchar(50) AS prod_hier_l4,
	prod_hier_l5::varchar(150) AS prod_hier_l5,
	prod_hier_l6::varchar(1) AS prod_hier_l6,
	prod_hier_l7::varchar(1) AS prod_hier_l7,
	prod_hier_l8::varchar(1) AS prod_hier_l8,
	prod_hier_l9::varchar(150) AS prod_hier_l9,
	customer_segment_key::varchar(12) AS customer_segment_key,
	customer_segment_description::varchar(50) AS customer_segment_description,
	retail_environment::varchar(200) AS retail_environment,
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
	ean::varchar(40) AS ean,
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
	target_complaince::numeric(18,0) AS target_complaince,
	"cluster"::varchar(100) AS cluster,
	crt_dttm::timestamp AS crt_dttm
from wks_rpt_retail_excellence
)
select * from final 