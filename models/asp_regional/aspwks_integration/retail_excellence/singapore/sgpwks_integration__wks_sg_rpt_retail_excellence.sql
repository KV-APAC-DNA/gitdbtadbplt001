with itg_sg_re_msl_list as (
    select * from {{ ref('sgpwks_integration__wks_itg_sg_re_msl_list') }}
),
wks_singapore_regional_sellout_actuals as (
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_actuals') }}
),

customer_heirarchy as (
    select * from {{ ref('aspedw_integration__edw_generic_customer_hierarchy') }}
),

product_heirarchy as (
    select * from {{ ref('aspedw_integration__edw_generic_product_hierarchy') }}
),

product_key_attribute as (
    select * from {{ ref('aspedw_integration__edw_generic_product_key_attributes') }}
),
edw_company_dim as (
    select * from {{ source('aspedw_integration','edw_company_dim') }}
),
itg_mds_sg_customer_hierarchy as (
    select * from {{ source('sgpitg_integration','itg_mds_sg_customer_hierarchy') }}
),
edw_vw_pop6_products as (
    select * from {{ source('ntaedw_integration','edw_vw_pop6_products') }}
),

sg_rpt_retail_excellence_mdp  as (
SELECT DISTINCT MDP.*,
       COM.*,
       SYSDATE AS CRT_DTTM
FROM (SELECT CAST(TARGET.YEAR AS INTEGER) AS FISC_YR,
             TARGET.MNTH_ID AS FISC_PER,
             COALESCE(ACTUAL.CNTRY_NM,'Singapore') AS MARKET,
             TARGET.CHANNEL AS CHANNEL_NAME,
             COALESCE(ACTUAL.SOLDTO_CODE,TARGET.SOLD_TO_CODE) AS SOLDTO_CODE,
             COALESCE(ACTUAL.DISTRIBUTOR_CODE,TARGET.DISTRIBUTOR_CODE) AS DISTRIBUTOR_CODE,
             COALESCE(ACTUAL.DISTRIBUTOR_NAME,TARGET.DISTRIBUTOR_NAME) AS DISTRIBUTOR_NAME,
             TARGET.SELLOUT_CHANNEL AS SELL_OUT_CHANNEL,
             --RETAILER.RETAILER_CATEGORY_NAME AS STORE_TYPE,
             --RETAILER.CLASS_DESC AS PRIORITIZATION_SEGMENTATION,
             --NVL(TARGET.STORE_CATEGORY,'NA') as STORE_CATEGORY,
             COALESCE(ACTUAL.STORE_CODE,TARGET.STORE_CODE) AS STORE_CODE,
             COALESCE(ACTUAL.STORE_NAME,TARGET.STORE_NAME) AS STORE_NAME,
             COALESCE(ACTUAL.STORE_TYPE,TARGET.STORE_TYPE) AS STORE_TYPE,
             --RETAILER.BUSINESS_CHANNEL AS STORE_GRADE,
             --RETAILER.CLASS_DESC AS STORE_SIZE,
             COALESCE(ACTUAL.REGION,TARGET.REGION) AS REGION,
             COALESCE(ACTUAL.ZONE_OR_AREA,TARGET.ZONE_NAME) AS ZONE_NAME,
             'Not Defined' AS CITY,
             --TARGET.STORE_ADDRESS,
             --TARGET.POST_CODE,
             --RETAILER.ZONE_NAME AS ZONE_NAME,
             --RETAILER.TOWN_NAME AS CITY,
             --RETAILER.RTRLATITUDE AS RTRLATITUDE,
             --RETAILER.RTRLONGITUDE AS RTRLONGITUDE,
             COALESCE(ACTUAL.MASTER_CODE,TARGET.MASTER_CODE) AS PRODUCT_CODE,
             COALESCE(ACTUAL.MASTER_CODE_DESC,TARGET.CUSTOMER_PRODUCT_DESC) AS PRODUCT_NAME,
             'Singapore' AS PROD_HIER_L1,
             PROD.PROD_HIER_L2,
             PROD.PROD_HIER_L3,
             PROD.PROD_HIER_L4,
             PROD.PROD_HIER_L5,
             PROD.PROD_HIER_L6,
             PROD.PROD_HIER_L7,
             PROD.PROD_HIER_L8,
             PROD.PROD_HIER_L9,
             COALESCE(ACTUAL.MAPPED_SKU_CD,TARGET.MAPPED_SKU_CD) AS MAPPED_SKU_CD,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_KEY,CUSTOMER.CUST_SEGMT_KEY) AS CUSTOMER_SEGMENT_KEY,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION,CUSTOMER.CUST_SEGMENT_DESC) AS CUSTOMER_SEGMENT_DESCRIPTION,
             COALESCE(ACTUAL.STORE_TYPE,TARGET.STORE_TYPE) AS RETAIL_ENVIRONMENT,
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
             coalesce(actual.global_product_franchise,product.gph_prod_frnchse) as global_product_franchise,
             coalesce(actual.global_product_brand,product.gph_prod_brnd) as global_product_brand,
             coalesce(actual.global_product_sub_brand,product.gph_prod_sub_brnd) as global_product_sub_brand,
             coalesce(actual.global_product_variant,product.gph_prod_vrnt) as global_product_variant,
             coalesce(actual.global_product_segment,product.gph_prod_needstate) as global_product_segment,
             coalesce(actual.global_product_subsegment,product.gph_prod_subsgmnt) as global_product_subsegment,
             coalesce(actual.global_product_category,product.gph_prod_ctgry) as global_product_category,
             coalesce(actual.global_product_subcategory,product.gph_prod_subctgry) as global_product_subcategory,
             coalesce(actual.global_put_up_description,product.gph_prod_put_up_desc) as global_put_up_description,
             TRIM(NVL (NULLIF(PRODUCT.EAN,''),'NA')) AS EAN,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA')) AS SKU_CODE,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA')) AS SKU_DESCRIPTION,
             TRIM(NVL (NULLIF(PRODUCT.PKA_FRANCHISE_DESC,''),'NA')) AS PKA_FRANCHISE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_BRAND_DESC,''),'NA')) AS PKA_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_BRAND_DESC,''),'NA')) AS PKA_SUB_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_VARIANT_DESC,''),'NA')) AS PKA_VARIANT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_VARIANT_DESC,''),'NA')) AS PKA_SUB_VARIANT_DESC,
             CASE
               WHEN TRIM(NVL (NULLIF(ACTUAL.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(ACTUAL.PKA_PRODUCT_KEY,''),'NA'))
               WHEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA'))
               WHEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA'))
               ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA'))
             END AS PKA_PRODUCT_KEY,
             CASE
               WHEN TRIM(NVL (NULLIF(ACTUAL.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(ACTUAL.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA'))
               WHEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA'))
               WHEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productdesc,''),'NA'))
               ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA'))
             END AS PKA_PRODUCT_KEY_DESCRIPTION,
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
             100 AS TARGET_COMPLAINCE
      FROM itg_sg_re_msl_list TARGET
        LEFT JOIN (SELECT * FROM wks_singapore_regional_sellout_actuals) ACTUAL
               ON TARGET.MNTH_ID = ACTUAL.MNTH_ID
              AND UPPER (TARGET.DISTRIBUTOR_CODE) = UPPER (ACTUAL.DISTRIBUTOR_CODE)
              AND UPPER (TARGET.STORE_CODE) = UPPER (ACTUAL.STORE_CODE)
              AND UPPER (TARGET.MASTER_CODE) = UPPER (ACTUAL.MASTER_CODE)
        LEFT JOIN (SELECT *
                   FROM (SELECT DISTINCT POP.SKU_CODE,
                                POP.SKU_ENGLISH AS SKU_NAME,
                                POP.COUNTRY_L1 AS PROD_HIER_L1,
                                POP.REGIONAL_FRANCHISE_L2 AS PROD_HIER_L2,
                                POP.FRANCHISE_L3 AS PROD_HIER_L3,
                                POP.BRAND_L4 AS PROD_HIER_L4,
                                POP.SUB_CATEGORY_L5 AS PROD_HIER_L5,
                                POP.PLATFORM_L6 AS PROD_HIER_L6,
                                POP.VARIANCE_L7 AS PROD_HIER_L7,
                                POP.PACK_SIZE_L8 AS PROD_HIER_L8,
                                NULL AS PROD_HIER_L9,
                                ROW_NUMBER() OVER (PARTITION BY SKU_CODE ORDER BY src_file_date DESC) AS RN
                         FROM edw_vw_pop6_products POP
                         WHERE POP.COUNTRY_L1 = 'Singapore')
                   WHERE RN = 1) PROD ON LTRIM (COALESCE (ACTUAL.MASTER_CODE,TARGET.MASTER_CODE),'0') = PROD.SKU_CODE
      ----------------customer hierarchy------------------------------
      
        LEFT JOIN (SELECT *
                   FROM customer_heirarchy
                   WHERE RANK = 1) CUSTOMER ON LTRIM (COALESCE (ACTUAL.DISTRIBUTOR_CODE,TARGET.DISTRIBUTOR_CODE),'0') = LTRIM (CUSTOMER.SAP_CUST_ID,'0')
      ----------------product hierarchy------------------------------
      
        LEFT JOIN  product_heirarchy PRODUCT
               ON LTRIM (COALESCE (ACTUAL.MAPPED_SKU_CD,TARGET.MAPPED_SKU_CD),'0') = LTRIM (PRODUCT.SAP_MATL_NUM,'0')
              AND PRODUCT.RANK = 1
      --product key attribute selection
      
        LEFT JOIN (SELECT * from product_key_attribute
                      where ctry_nm = 'Singapore') PROD_KEY1 ON LTRIM (COALESCE (ACTUAL.MAPPED_SKU_CD,TARGET.MAPPED_SKU_CD),'0') = LTRIM (PROD_KEY1.SKU,'0')) MDP,
     (SELECT DISTINCT CLUSTER
      FROM edw_company_dim
      WHERE CTRY_GROUP = 'Singapore') COM
WHERE FISC_PER <= (SELECT MAX(mnth_id)
                   FROM wks_singapore_regional_sellout_actuals)
),
sg_rpt_retail_excellence_non_mdp as
(
SELECT DISTINCT NON_MDP.*,
       COM.*,
       SYSDATE AS CRT_DTTM
FROM (SELECT CAST(ACTUAL.YEAR AS INTEGER) AS YEAR,
             CAST(ACTUAL.MNTH_ID AS INTEGER) AS MNTH_ID,
             ACTUAL.CNTRY_NM AS MARKET,
             CUST.CHANNEL,
             ACTUAL.SOLDTO_CODE,
             ACTUAL.DISTRIBUTOR_CODE,
             ACTUAL.DISTRIBUTOR_NAME,
             CUST.CHANNEL AS SELL_OUT_CHANNEL,
             --RETAILER.CLASS_DESC AS PRIORITIZATION_SEGMENTATION,
             --'NA' AS STORE_CATEGORY,
             ACTUAL.STORE_CODE,
             ACTUAL.STORE_NAME,
             ACTUAL.STORE_TYPE,
             --RETAILER.BUSINESS_CHANNEL AS STORE_GRADE,
             --RETAILER.CLASS_DESC AS STORE_SIZE,
             ACTUAL.REGION AS REGION,
             ACTUAL.ZONE_OR_AREA AS ZONE_NAME,
             'Not Defined' AS CITY,
             --STORE_DET.STORE_ADDRESS,
             --STORE_DET.POST_CODE,
             --RETAILER.RTRLATITUDE AS RTRLATITUDE,
             --RETAILER.RTRLONGITUDE AS RTRLONGITUDE,
             ACTUAL.MASTER_CODE AS PRODUCT_CODE,
             ACTUAL.MASTER_CODE_DESC AS PRODUCT_NAME,
             'Singapore' AS PROD_HIER_L1,
             PROD.PROD_HIER_L2,
             PROD.PROD_HIER_L3,
             PROD.PROD_HIER_L4,
             PROD.PROD_HIER_L5,
             PROD.PROD_HIER_L6,
             PROD.PROD_HIER_L7,
             PROD.PROD_HIER_L8,
             PROD.PROD_HIER_L9,
             ACTUAL.MAPPED_SKU_CD AS MAPPED_SKU_CD,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_KEY,CUSTOMER.CUST_SEGMT_KEY) AS CUSTOMER_SEGMENT_KEY,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION,CUSTOMER.CUST_SEGMENT_DESC) AS CUSTOMER_SEGMENT_DESCRIPTION,
             COALESCE(ACTUAL.STORE_TYPE,'NA') AS RETAIL_ENVIRONMENT,
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
             TRIM(NVL (NULLIF(PRODUCT.EAN,''),'NA')) AS EAN,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA')) AS SKU_CODE,
             TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA')) AS SKU_DESCRIPTION,
             TRIM(NVL (NULLIF(PRODUCT.PKA_FRANCHISE_DESC,''),'NA')) AS PKA_FRANCHISE_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_BRAND_DESC,''),'NA')) AS PKA_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_BRAND_DESC,''),'NA')) AS PKA_SUB_BRAND_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_VARIANT_DESC,''),'NA')) AS PKA_VARIANT_DESC,
             TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_VARIANT_DESC,''),'NA')) AS PKA_SUB_VARIANT_DESC,
             CASE
               WHEN TRIM(NVL (NULLIF(ACTUAL.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(ACTUAL.PKA_PRODUCT_KEY,''),'NA'))
               WHEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA'))
               WHEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA'))
               ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA'))
             END AS PKA_PRODUCT_KEY,
             CASE
               WHEN TRIM(NVL (NULLIF(ACTUAL.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(ACTUAL.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA'))
               WHEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA'))
               WHEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productdesc,''),'NA'))
               ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA'))
             END AS PKA_PRODUCT_KEY_DESCRIPTION,
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
             100 AS TARGET_COMPLAINCE
      FROM (SELECT *
            FROM wks_singapore_regional_sellout_actuals A
            WHERE NOT EXISTS (SELECT 1
                              FROM itg_sg_re_msl_list T
                              WHERE A.MNTH_ID = T.MNTH_ID
                              AND   UPPER(A.DISTRIBUTOR_CODE) = UPPER(T.DISTRIBUTOR_CODE)
                              AND   UPPER(A.STORE_CODE) = UPPER(T.STORE_CODE)
                              AND   UPPER(A.MASTER_CODE) = UPPER(T.master_code))) ACTUAL
        LEFT JOIN (SELECT DISTINCT CHANNEL,
                          CUSTOMER_NUMBER
                   FROM itg_mds_sg_customer_hierarchy) CUST ON ACTUAL.DISTRIBUTOR_CODE = CUST.CUSTOMER_NUMBER
        LEFT JOIN (SELECT *
                   FROM (SELECT DISTINCT POP.SKU_CODE,
                                POP.SKU_ENGLISH AS SKU_NAME,
                                POP.COUNTRY_L1 AS PROD_HIER_L1,
                                POP.REGIONAL_FRANCHISE_L2 AS PROD_HIER_L2,
                                POP.FRANCHISE_L3 AS PROD_HIER_L3,
                                POP.BRAND_L4 AS PROD_HIER_L4,
                                POP.SUB_CATEGORY_L5 AS PROD_HIER_L5,
                                POP.PLATFORM_L6 AS PROD_HIER_L6,
                                POP.VARIANCE_L7 AS PROD_HIER_L7,
                                POP.PACK_SIZE_L8 AS PROD_HIER_L8,
                                NULL AS PROD_HIER_L9,
                                ROW_NUMBER() OVER (PARTITION BY SKU_CODE ORDER BY src_file_date DESC) AS RN
                         FROM edw_vw_pop6_products POP
                         WHERE POP.COUNTRY_L1 = 'Singapore')
                   WHERE RN = 1) PROD ON ACTUAL.MASTER_CODE = PROD.SKU_CODE
      ----------------customer hierarchy------------------------------
      
        LEFT JOIN (SELECT *
                   FROM customer_heirarchy
                   WHERE RANK = 1) CUSTOMER ON LTRIM (ACTUAL.DISTRIBUTOR_CODE,'0') = LTRIM (CUSTOMER.SAP_CUST_ID,'0')
      ----------------product hierarchy------------------------------   	
      
        LEFT JOIN product_heirarchy PRODUCT
               ON LTRIM (ACTUAL.MAPPED_SKU_CD,'0') = LTRIM (PRODUCT.SAP_MATL_NUM,'0')
              AND PRODUCT.RANK = 1
      ---------product key attribute selection
      
        LEFT JOIN (SELECT * from product_key_attribute
                      where ctry_nm = 'Singapore') PROD_KEY1 ON LTRIM (ACTUAL.MAPPED_SKU_CD,'0') = LTRIM (PROD_KEY1.sku,'0')) NON_MDP,
     (SELECT DISTINCT CLUSTER
      FROM edw_company_dim
      WHERE CTRY_GROUP = 'Singapore') COM
),
sg_rpt_retail_excellence as
(
select * from sg_rpt_retail_excellence_mdp
union 
select * from sg_rpt_retail_excellence_non_mdp
)

--Final select
select * from sg_rpt_retail_excellence