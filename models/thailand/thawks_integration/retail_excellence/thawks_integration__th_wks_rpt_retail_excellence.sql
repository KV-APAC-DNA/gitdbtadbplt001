with itg_jnj_mer_share_of_shelf as (
select * from {{ ref('thaitg_integration__itg_jnj_mer_share_of_shelf') }}
),
ITG_TH_RE_MSL_LIST as (
select * from {{ ref('thaitg_integration__itg_th_re_msl_list') }}
),
WKS_TH_REGIONAL_SELLOUT_ACTUALS as (
select * from {{ ref('thawks_integration__wks_th_regional_sellout_actuals') }}
),
EDW_COMPANY_DIM as (
select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_material_dim as (
select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
EDW_VW_OS_MATERIAL_DIM as (
select * from {{ ref('thaedw_integration__edw_vw_th_material_dim') }}
),

EDW_VW_OS_CUSTOMER_DIM as (
select * from {{ ref('thaedw_integration__edw_vw_th_customer_dim') }}
),

transformation as (SELECT
FISC_YR,
  FISC_PER,
  MARKET,
  DATA_SRC,
  DISTRIBUTOR_CODE,
  DISTRIBUTOR_NAME,
  SELL_OUT_CHANNEL,
  SELL_OUT_RE,
  STORE_TYPE,
  STORE_CODE,
  STORE_NAME,
  REGION,
  ZONE_NAME,
   SOLDTO_CODE,
  PRODUCT_CODE,
  --SKU_CODE,
  PRODUCT_NAME,
  PROD_HIER_L1,
  PROD_HIER_L3,
  PROD_HIER_L5,
  MAPPED_SKU_CD,
  CUSTOMER_SEGMENT_KEY,
  CUSTOMER_SEGMENT_DESCRIPTION,
  RETAIL_ENVIRONMENT,
  SAP_CUSTOMER_CHANNEL_KEY,
  SAP_CUSTOMER_CHANNEL_DESCRIPTION,
  SAP_CUSTOMER_SUB_CHANNEL_KEY,
  SAP_SUB_CHANNEL_DESCRIPTION,
  SAP_PARENT_CUSTOMER_KEY,
  SAP_PARENT_CUSTOMER_DESCRIPTION,
  SAP_BANNER_KEY,
  SAP_BANNER_DESCRIPTION,
  SAP_BANNER_FORMAT_KEY,
  SAP_BANNER_FORMAT_DESCRIPTION,
  CUSTOMER_NAME,
  CUSTOMER_CODE,
  SAP_PROD_SGMT_CD,
  SAP_PROD_SGMT_DESC,
  SAP_BASE_PROD_DESC,
  SAP_MEGA_BRND_DESC,
  SAP_BRND_DESC,
  SAP_VRNT_DESC,
  SAP_PUT_UP_DESC,
  SAP_GRP_FRNCHSE_CD,
  SAP_GRP_FRNCHSE_DESC,
  SAP_FRNCHSE_CD,
  SAP_FRNCHSE_DESC,
  SAP_PROD_FRNCHSE_CD,
  SAP_PROD_FRNCHSE_DESC,
  SAP_PROD_MJR_CD,
  SAP_PROD_MJR_DESC,
  SAP_PROD_MNR_CD,
  SAP_PROD_MNR_DESC,
  SAP_PROD_HIER_CD,
  SAP_PROD_HIER_DESC,
   SAP_GO_TO_MDL_KEY,
   SAP_GO_TO_MDL_DESCRIPTION,
  GLOBAL_PRODUCT_FRANCHISE,
  GLOBAL_PRODUCT_BRAND,
  GLOBAL_PRODUCT_SUB_BRAND,
  GLOBAL_PRODUCT_VARIANT,
  GLOBAL_PRODUCT_SEGMENT,
  GLOBAL_PRODUCT_SUBSEGMENT,
  GLOBAL_PRODUCT_CATEGORY,
  GLOBAL_PRODUCT_SUBCATEGORY,
  GLOBAL_PUT_UP_DESCRIPTION,
 -- EAN,
  SAP_SKU_CODE,
  SAP_SKU_DESCRIPTION,
  PKA_FRANCHISE_DESC,
  PKA_BRAND_DESC,
  PKA_SUB_BRAND_DESC,
  PKA_VARIANT_DESC,
  PKA_SUB_VARIANT_DESC,
  PKA_PRODUCT_KEY,
  PKA_PRODUCT_KEY_DESCRIPTION,
  SALES_VALUE,
  SALES_QTY,
  AVG_SALES_QTY,
  SALES_VALUE_LIST_PRICE,
  LM_SALES,
  LM_SALES_QTY,
  LM_AVG_SALES_QTY,
  LM_SALES_LP,
  P3M_SALES,
  P3M_QTY,
  P3M_AVG_QTY,
  P3M_SALES_LP,
  F3M_SALES,
  F3M_QTY,
  F3M_AVG_QTY,
  P6M_SALES,
  P6M_QTY,
  P6M_AVG_QTY,
  P6M_SALES_LP,
  P12M_SALES,
  P12M_QTY,
  P12M_AVG_QTY,
  P12M_SALES_LP,
  LM_SALES_FLAG,
  P3M_SALES_FLAG,
  P6M_SALES_FLAG,
  P12M_SALES_FLAG,
  MDP_FLAG,
  TARGET_COMPLAINCE,
  CLUSTER
  FROM
((SELECT distinct Q.*,
       COM."cluster" as cluster
FROM (SELECT CAST(TARGET.FISC_YR AS INTEGER) AS FISC_YR,
             CAST(TARGET.fisc_per AS INTEGER) AS FISC_PER,
             COALESCE(ACTUAL.CNTRY_NM,TARGET.PROD_HIER_L1) AS MARKET,
             TARGET.Data_Src AS DATA_SRC,
             COALESCE(ACTUAL.DISTRIBUTOR_CODE,TARGET.DISTRIBUTOR_CODE) AS DISTRIBUTOR_CODE,
             COALESCE(ACTUAL.DISTRIBUTOR_NAME,TARGET.DISTRIBUTOR_NAME) AS DISTRIBUTOR_NAME,
             COALESCE(ACTUAL.Channel,TARGET.channel) AS SELL_OUT_CHANNEL,-----------------------------check
             COALESCE(ACTUAL.RETAIL_ENVIRONMENT,TARGET.RETAIL_ENVIRONMENT) AS Sell_Out_RE,-----------------------check
             COALESCE(ACTUAL.STORE_TYPE,TARGET.STORE_TYPE) AS STORE_TYPE,
             COALESCE(ACTUAL.STORE_CODE,TARGET.STORE_CODE) AS STORE_CODE,
             COALESCE(ACTUAL.STORE_NAME,TARGET.STORE_NAME) AS STORE_NAME,
             COALESCE(ACTUAL.REGION,TARGET.REGION) AS REGION,
             COALESCE(ACTUAL.zone,TARGET.Zone) AS ZONE_NAME,
             COALESCE(ACTUAL.SOLDTO_CODE,TARGET.SOLDTO_CODE) AS SOLDTO_CODE,
             --COALESCE(ACTUAL.CITY,TARGET.City)AS CITY,
             COALESCE(ACTUAL.EAN_CODE,TARGET.PRODUCT_CODE) AS PRODUCT_CODE,------------------------------------------------------KEEP AS EAN
             COALESCE(ACTUAL.MAPPED_SKU_DESCRIPTION, TARGET.product_description)AS PRODUCT_NAME,----------------------------- check
             TARGET.PROD_HIER_L1,
             UPPER(b.category::TEXT)::CHARACTER VARYING AS PROD_HIER_L3,
             Null AS PROD_HIER_L5,
             ACTUAL.MAPPED_SKU_CODE AS MAPPED_SKU_CD,
             --removed COALESCE since it had only 1 arg
             ACTUAL.CUSTOMER_SEGMENT_KEY AS CUSTOMER_SEGMENT_KEY,
             --removed COALESCE since it had only 1 arg
             ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION AS CUSTOMER_SEGMENT_DESCRIPTION,
             COALESCE(ACTUAL.RETAIL_ENVIRONMENT, TARGET.RETAIL_ENVIRONMENT) AS RETAIL_ENVIRONMENT,
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
			 COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_DESCRIPTION,CUSTOMER.SAP_PRNT_CUST_DESC) AS CUSTOMER_NAME,
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_KEY,CUSTOMER.SAP_PRNT_CUST_KEY) AS CUSTOMER_CODE,
             /*COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_KEY) AS CUSTOMER_NAME,
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_DESCRIPTION)AS CUSTOMER_CODE,*/
             Null AS SAP_PROD_SGMT_CD,
             Null AS SAP_PROD_SGMT_DESC,
             NULL AS SAP_BASE_PROD_DESC,
             NULL AS SAP_MEGA_BRND_DESC,
             NULL AS SAP_BRND_DESC,
             NULL AS SAP_VRNT_DESC,
             NULL AS SAP_PUT_UP_DESC,
             NULL AS SAP_GRP_FRNCHSE_CD,
             NULL AS SAP_GRP_FRNCHSE_DESC,
             NULL AS SAP_FRNCHSE_CD,
             NULL AS SAP_FRNCHSE_DESC,
             NULL AS SAP_PROD_FRNCHSE_CD,
             NULL AS SAP_PROD_FRNCHSE_DESC,
             NULL AS SAP_PROD_MJR_CD,
             NULL AS SAP_PROD_MJR_DESC,
             NULL AS SAP_PROD_MNR_CD,
             NULL AS SAP_PROD_MNR_DESC,
             NULL AS SAP_PROD_HIER_CD,
             NULL AS SAP_PROD_HIER_DESC,
             COALESCE(ACTUAL.SAP_GO_TO_MDL_KEY,CUSTOMER.SAP_BNR_FRMT_DESC) AS SAP_GO_TO_MDL_KEY,
             COALESCE(ACTUAL.SAP_GO_TO_MDL_DESCRIPTION,CUSTOMER.SAP_BNR_FRMT_DESC) AS SAP_GO_TO_MDL_DESCRIPTION,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,PRODUCT.GPH_PROD_FRNCHSE) AS GLOBAL_PRODUCT_FRANCHISE,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_BRAND,PRODUCT.GPH_PROD_BRND) AS GLOBAL_PRODUCT_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,PRODUCT.GPH_PROD_SUB_BRND) AS GLOBAL_PRODUCT_SUB_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_VARIANT,PRODUCT.GPH_PROD_VRNT) AS GLOBAL_PRODUCT_VARIANT,
             --removed COALESCE since it had only 1 arg
             ACTUAL.GLOBAL_PRODUCT_SEGMENT AS GLOBAL_PRODUCT_SEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,PRODUCT.GPH_PROD_SUBSGMNT) AS GLOBAL_PRODUCT_SUBSEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_CATEGORY,PRODUCT.GPH_PROD_CTGRY) AS GLOBAL_PRODUCT_CATEGORY,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,PRODUCT.GPH_PROD_SUBCTGRY) AS GLOBAL_PRODUCT_SUBCATEGORY,
             COALESCE(ACTUAL.GLOBAL_PUT_UP_DESCRIPTION,PRODUCT.GPH_PROD_PUT_UP_DESC) AS GLOBAL_PUT_UP_DESCRIPTION,
             NULL AS SAP_SKU_CODE,
             NULL AS SAP_SKU_DESCRIPTION,
             NULL AS PKA_FRANCHISE_DESC,
             NULL AS PKA_BRAND_DESC,
             NULL AS PKA_SUB_BRAND_DESC,
             NULL AS PKA_VARIANT_DESC,
             NULL AS PKA_SUB_VARIANT_DESC,
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
             'Y' AS MDP_FLAG,
             1 AS TARGET_COMPLAINCE
      FROM ITG_TH_RE_MSL_LIST TARGET
        LEFT JOIN (SELECT * FROM WKS_TH_REGIONAL_SELLOUT_ACTUALS) ACTUAL
               ON TARGET.FISC_PER = ACTUAL.MNTH_ID
              AND TARGET.DISTRIBUTOR_CODE = ACTUAL.DISTRIBUTOR_CODE
              AND TARGET.STORE_CODE = ACTUAL.STORE_CODE
              --AND TARGET.STORE_TYPE = ACTUAL.STORE_TYPE
              AND TARGET.PRODUCT_CODE = ACTUAL.EAN_CODE----------------------------------------------------- updated
      
        LEFT JOIN (SELECT itg_jnj_mer_share_of_shelf.sos_date,
             itg_jnj_mer_share_of_shelf.merchandiser_name,
             itg_jnj_mer_share_of_shelf.supervisor_name,
             itg_jnj_mer_share_of_shelf.area,
             itg_jnj_mer_share_of_shelf.channel,
             itg_jnj_mer_share_of_shelf.account,
             itg_jnj_mer_share_of_shelf.store_id,
             itg_jnj_mer_share_of_shelf.store_name,
             itg_jnj_mer_share_of_shelf.category,
             itg_jnj_mer_share_of_shelf.agency,
             itg_jnj_mer_share_of_shelf.brand,
             itg_jnj_mer_share_of_shelf.size,
             itg_jnj_mer_share_of_shelf.yearmo
      FROM itg_jnj_mer_share_of_shelf
      WHERE UPPER(itg_jnj_mer_share_of_shelf.agency::TEXT) = 'JOHNSON'::CHARACTER VARYING::TEXT
      GROUP BY itg_jnj_mer_share_of_shelf.sos_date,
               itg_jnj_mer_share_of_shelf.merchandiser_name,
               itg_jnj_mer_share_of_shelf.supervisor_name,
               itg_jnj_mer_share_of_shelf.area,
               itg_jnj_mer_share_of_shelf.channel,
               itg_jnj_mer_share_of_shelf.account,
               itg_jnj_mer_share_of_shelf.store_id,
               itg_jnj_mer_share_of_shelf.store_name,
               itg_jnj_mer_share_of_shelf.category,
               itg_jnj_mer_share_of_shelf.agency,
               itg_jnj_mer_share_of_shelf.brand,
               itg_jnj_mer_share_of_shelf.size,
               itg_jnj_mer_share_of_shelf.yearmo)B ON upper(COALESCE(ACTUAL.STORE_NAME,TARGET.STORE_NAME))= upper(B.STORE_NAME) 
               --LTRIM (ACTUAL.MAPPED_SKU_CODE::TEXT,'0'::CHARACTER VARYING::TEXT) = 
                  -- LTRIM (epspm.product_code::TEXT,'0'::CHARACTER VARYING::TEXT)
        /*LEFT JOIN (SELECT *
                   FROM (SELECT DISTINCT EAN_NUM,
                                LTRIM(MATL_NUM,'0') AS MAPPED_SKU_CD,
                                ROW_NUMBER() OVER (PARTITION BY EAN_NUM ORDER BY CRT_DTTM DESC) AS RN
                         FROM EDW_MATERIAL_SALES_DIM)
                   WHERE RN = 1) MAT ON MAT.EAN_NUM = TARGET.EAN*/
                   ----------------customer hierarchy------------------------------          
      left join (SELECT *
FROM (SELECT DISTINCT T3.sap_cust_id,
             T3.SAP_PRNT_CUST_KEY,
             T3.SAP_PRNT_CUST_DESC,
             T3.SAP_CUST_CHNL_KEY,
             T3.SAP_CUST_CHNL_DESC,
             T3.SAP_CUST_SUB_CHNL_KEY,
             T3.SAP_SUB_CHNL_DESC,
             T3.SAP_GO_TO_MDL_KEY,
             T3.SAP_GO_TO_MDL_DESC,
             T3.SAP_BNR_KEY,
             T3.SAP_BNR_DESC,
             T3.SAP_BNR_FRMT_KEY,
             T3.SAP_BNR_FRMT_DESC,
             --T3.segmt_key,
             --T3.segment_desc,
             T3.RETAIL_ENV,
             t3.sap_region,
             ROW_NUMBER() OVER (PARTITION BY sap_prnt_cust_key ORDER BY sap_cust_id ) AS RANK
      FROM (SELECT *
            FROM EDW_VW_OS_CUSTOMER_DIM
            WHERE SAP_CNTRY_CD = 'TH'
            AND   sap_prnt_cust_key != '') AS T3)
WHERE RANK = 1)CUSTOMER on LTRIM(ACTUAL.SAP_PARENT_CUSTOMER_KEY, '0')= LTRIM(CUSTOMER.sap_prnt_cust_key, '0')
        
      ----------------product hierarchy------------------------------   				   
      left join (SELECT *
                FROM (SELECT *,
             ROW_NUMBER() OVER (PARTITION BY sku_cd ORDER BY sku_cd DESC) AS RANK
                  FROM (SELECT --EMD.greenlight_sku_flag AS greenlight_sku_flag,
                   EMD.pka_product_key AS pka_product_key,
                   EMD.pka_product_key_description AS pka_product_key_description,
				           --EMD.sls_org as SLS_ORG,
                   T4.gph_prod_frnchse,
                   T4.GPH_PROD_BRND,
                   T4.GPH_PROD_SUB_BRND,
                   T4.GPH_PROD_VRNT,
                   T4.GPH_PROD_SGMNT,
                   T4.GPH_PROD_SUBSGMNT,
                   T4.GPH_PROD_CTGRY,
                   T4.GPH_PROD_SUBCTGRY,
                   T4.gph_prod_put_up_desc,
                   LTRIM(T4.SAP_MATL_NUM) AS sku_cd,
                   SAP_MAT_DESC
            FROM (SELECT *
                  FROM EDW_VW_OS_MATERIAL_DIM
                  WHERE CNTRY_KEY = 'TH') t4
              LEFT JOIN 
			  edw_material_dim	EMD 
              ON LTRIM (t4.sap_matl_num,0) = LTRIM (emd.matl_num,0)))
WHERE RANK = 1)product on ltrim(ACTUAL.MAPPED_SKU_CODE,0)=ltrim(product.sku_cd,0)) Q,
     (SELECT DISTINCT "cluster",
             CTRY_GROUP
      FROM EDW_COMPANY_DIM
      WHERE CTRY_GROUP = 'Thailand') COM
WHERE FISC_PER <= (SELECT MAX(mnth_id)
                   FROM WKS_TH_REGIONAL_SELLOUT_ACTUALS))

UNION ALL

(
  SELECT distinct Q.*,
       COM."cluster"
FROM (SELECT CAST(ACTUAL.YEAR AS INTEGER) AS YEAR,
             CAST(ACTUAL.MNTH_ID AS INTEGER) AS MNTH_ID,
             ACTUAL.CNTRY_NM AS MARKET,
             ACTUAL.DATA_SRC AS DATA_SRC,
             ACTUAL.DISTRIBUTOR_CODE AS DISTRIBUTOR_CODE,
             ACTUAL.DISTRIBUTOR_NAME AS DISTRIBUTOR_NAME,
             ACTUAL.CHANNEL AS SELL_OUT_CHANNEL,
             ACTUAL.RETAIL_ENVIRONMENT AS Sell_Out_RE,
             ACTUAL.STORE_TYPE AS STORE_TYPE,
             ACTUAL.STORE_CODE AS STORE_CODE,
             ACTUAL.STORE_NAME AS STORE_NAME,
             ACTUAL.SOLDTO_CODE AS SOLDTO_CODE,
             ACTUAL.REGION AS REGION,
             ACTUAL.ZONE AS ZONE_NAME,
            -- ACTUAL.CITY AS CITY,
             ------------------------- check 
             ACTUAL.EAN_CODE AS PRODUCT_CODE,
             ---ACTUAL.SKU_CODE AS SKU_CODE,
             ACTUAL.mapped_sku_description AS PRODUCT_NAME,
             'Thailand' AS PROD_HIER_L1,
             UPPER(b.category::TEXT)::CHARACTER VARYING AS PROD_HIER_L3,
             Null AS PROD_HIER_L5,
             ACTUAL.MAPPED_SKU_CODE AS MAPPED_SKU_CD,
             --removed COALESCE since it had only 1 arg
			 ACTUAL.CUSTOMER_SEGMENT_KEY AS CUSTOMER_SEGMENT_KEY,
             --removed COALESCE since it had only 1 arg
             ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION AS CUSTOMER_SEGMENT_DESCRIPTION,
             --removed COALESCE since it had only 1 arg
             ACTUAL.RETAIL_ENVIRONMENT AS RETAIL_ENVIRONMENT,
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
			 COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_DESCRIPTION,CUSTOMER.SAP_PRNT_CUST_DESC) AS CUSTOMER_NAME,
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_KEY,CUSTOMER.SAP_PRNT_CUST_KEY) AS CUSTOMER_CODE,
             /*COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_KEY) AS CUSTOMER_NAME,
             COALESCE(ACTUAL.SAP_PARENT_CUSTOMER_DESCRIPTION)AS CUSTOMER_CODE,*/
             Null AS SAP_PROD_SGMT_CD,
             Null AS SAP_PROD_SGMT_DESC,
             NULL AS SAP_BASE_PROD_DESC,
             NULL AS SAP_MEGA_BRND_DESC,
             NULL AS SAP_BRND_DESC,
             NULL AS SAP_VRNT_DESC,
             NULL AS SAP_PUT_UP_DESC,
             NULL AS SAP_GRP_FRNCHSE_CD,
             NULL AS SAP_GRP_FRNCHSE_DESC,
             NULL AS SAP_FRNCHSE_CD,
            NULL AS SAP_FRNCHSE_DESC,
             NULL AS SAP_PROD_FRNCHSE_CD,
             NULL AS SAP_PROD_FRNCHSE_DESC,
             NULL AS SAP_PROD_MJR_CD,
             NULL AS SAP_PROD_MJR_DESC,
             NULL AS SAP_PROD_MNR_CD,
            NULL AS SAP_PROD_MNR_DESC,
             NULL AS SAP_PROD_HIER_CD,
             NULL AS SAP_PROD_HIER_DESC,
             TRIM(NVL (NULLIF(ACTUAL.SAP_GO_TO_MDL_KEY,''),'NA')) AS SAP_GO_TO_MDL_KEY,
             TRIM(NVL (NULLIF(ACTUAL.SAP_GO_TO_MDL_DESCRIPTION,''),'NA')) AS SAP_GO_TO_MDL_DESCRIPTION,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,PRODUCT.GPH_PROD_FRNCHSE) AS GLOBAL_PRODUCT_FRANCHISE,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_BRAND,PRODUCT.GPH_PROD_BRND) AS GLOBAL_PRODUCT_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,PRODUCT.GPH_PROD_SUB_BRND) AS GLOBAL_PRODUCT_SUB_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_VARIANT,PRODUCT.GPH_PROD_VRNT) AS GLOBAL_PRODUCT_VARIANT,
             --removed COALESCE since it had only 1 arg
             ACTUAL.GLOBAL_PRODUCT_SEGMENT AS GLOBAL_PRODUCT_SEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,PRODUCT.GPH_PROD_SUBSGMNT) AS GLOBAL_PRODUCT_SUBSEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_CATEGORY,PRODUCT.GPH_PROD_CTGRY) AS GLOBAL_PRODUCT_CATEGORY,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,PRODUCT.GPH_PROD_SUBCTGRY) AS GLOBAL_PRODUCT_SUBCATEGORY,
             COALESCE(ACTUAL.GLOBAL_PUT_UP_DESCRIPTION,PRODUCT.GPH_PROD_PUT_UP_DESC) AS GLOBAL_PUT_UP_DESCRIPTION,
             --TRIM(NVL (NULLIF(PRODUCT.EAN,''),'NA')) AS EAN,
             NULL  AS SAP_SKU_CODE,
             NULL AS SAP_SKU_DESCRIPTION,
             NULL AS PKA_FRANCHISE_DESC,
            NULL AS PKA_BRAND_DESC,
             NULL AS PKA_SUB_BRAND_DESC,
            NULL AS PKA_VARIANT_DESC,
             NULL AS PKA_SUB_VARIANT_DESC,
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
            FROM WKS_TH_REGIONAL_SELLOUT_ACTUALS A
            WHERE NOT EXISTS (SELECT 1
                              FROM ITG_TH_RE_MSL_LIST T
                              WHERE A.MNTH_ID = T.FISC_PER
                              AND   A.DISTRIBUTOR_CODE = T.DISTRIBUTOR_CODE
                              AND   A.STORE_CODE = T.STORE_CODE
                              --AND   A.STORE_TYPE = T.STORE_TYPE
                              AND   A.EAN_CODE = T.PRODUCT_CODE
                              --AND   A.ZONE = T.ZONE
                              --AND   A.REGION = T.REGION
                              )
            ) ACTUAL
        LEFT JOIN (SELECT itg_jnj_mer_share_of_shelf.sos_date,
             itg_jnj_mer_share_of_shelf.merchandiser_name,
             itg_jnj_mer_share_of_shelf.supervisor_name,
             itg_jnj_mer_share_of_shelf.area,
             itg_jnj_mer_share_of_shelf.channel,
             itg_jnj_mer_share_of_shelf.account,
             itg_jnj_mer_share_of_shelf.store_id,
             itg_jnj_mer_share_of_shelf.store_name,
             itg_jnj_mer_share_of_shelf.category,
             itg_jnj_mer_share_of_shelf.agency,
             itg_jnj_mer_share_of_shelf.brand,
             itg_jnj_mer_share_of_shelf.size,
             itg_jnj_mer_share_of_shelf.yearmo
      FROM itg_jnj_mer_share_of_shelf
      WHERE UPPER(itg_jnj_mer_share_of_shelf.agency::TEXT) = 'JOHNSON'::CHARACTER VARYING::TEXT
      GROUP BY itg_jnj_mer_share_of_shelf.sos_date,
               itg_jnj_mer_share_of_shelf.merchandiser_name,
               itg_jnj_mer_share_of_shelf.supervisor_name,
               itg_jnj_mer_share_of_shelf.area,
               itg_jnj_mer_share_of_shelf.channel,
               itg_jnj_mer_share_of_shelf.account,
               itg_jnj_mer_share_of_shelf.store_id,
               itg_jnj_mer_share_of_shelf.store_name,
               itg_jnj_mer_share_of_shelf.category,
               itg_jnj_mer_share_of_shelf.agency,
               itg_jnj_mer_share_of_shelf.brand,
               itg_jnj_mer_share_of_shelf.size,
               itg_jnj_mer_share_of_shelf.yearmo)B ON upper(ACTUAL.STORE_NAME)= upper(B.STORE_NAME)
               --LTRIM (ACTUAL.MAPPED_SKU_CODE::TEXT,'0'::CHARACTER VARYING::TEXT) = 
                  -- LTRIM (epspm.product_code::TEXT,'0'::CHARACTER VARYING::TEXT)
        /*LEFT JOIN (SELECT *
                   FROM (SELECT DISTINCT EAN_NUM,
                                LTRIM(MATL_NUM,'0') AS MAPPED_SKU_CD,
                                ROW_NUMBER() OVER (PARTITION BY EAN_NUM ORDER BY CRT_DTTM DESC) AS RN
                         FROM EDW_MATERIAL_SALES_DIM)
                   WHERE RN = 1) MAT ON MAT.EAN_NUM = TARGET.EAN*/
                   ----------------customer hierarchy------------------------------          
      left join (SELECT *
FROM (SELECT DISTINCT T3.sap_cust_id,
             T3.SAP_PRNT_CUST_KEY,
             T3.SAP_PRNT_CUST_DESC,
             T3.SAP_CUST_CHNL_KEY,
             T3.SAP_CUST_CHNL_DESC,
             T3.SAP_CUST_SUB_CHNL_KEY,
             T3.SAP_SUB_CHNL_DESC,
             T3.SAP_GO_TO_MDL_KEY,
             T3.SAP_GO_TO_MDL_DESC,
             T3.SAP_BNR_KEY,
             T3.SAP_BNR_DESC,
             T3.SAP_BNR_FRMT_KEY,
             T3.SAP_BNR_FRMT_DESC,
             --T3.segmt_key,
             --T3.segment_desc,
             T3.RETAIL_ENV,
             t3.sap_region,
             ROW_NUMBER() OVER (PARTITION BY sap_prnt_cust_key ORDER BY sap_cust_id ) AS RANK
      FROM (SELECT *
            FROM EDW_VW_OS_CUSTOMER_DIM
            WHERE SAP_CNTRY_CD = 'TH'
            AND   sap_prnt_cust_key != '') AS T3)
WHERE RANK = 1)CUSTOMER on LTRIM(ACTUAL.SAP_PARENT_CUSTOMER_KEY,'0') = LTRIM(CUSTOMER.sap_prnt_cust_key,'0')
        
      ----------------product hierarchy------------------------------   				   
      left join (SELECT *
                FROM (SELECT *,
             ROW_NUMBER() OVER (PARTITION BY sku_cd ORDER BY sku_cd DESC) AS RANK
                  FROM (SELECT --EMD.greenlight_sku_flag AS greenlight_sku_flag,
                   EMD.pka_product_key AS pka_product_key,
                   EMD.pka_product_key_description AS pka_product_key_description,
				           --EMD.sls_org as SLS_ORG,
                   T4.gph_prod_frnchse,
                   T4.GPH_PROD_BRND,
                   T4.GPH_PROD_SUB_BRND,
                   T4.GPH_PROD_VRNT,
                   T4.GPH_PROD_SGMNT,
                   T4.GPH_PROD_SUBSGMNT,
                   T4.GPH_PROD_CTGRY,
                   T4.GPH_PROD_SUBCTGRY,
                   T4.gph_prod_put_up_desc,
                   LTRIM(T4.SAP_MATL_NUM) AS sku_cd,
                   SAP_MAT_DESC
            FROM (SELECT *
                  FROM EDW_VW_OS_MATERIAL_DIM
                  WHERE CNTRY_KEY = 'TH') t4
              LEFT JOIN 
			  edw_material_dim
			  --(SELECT *
				 --FROM edw_vw_greenlight_skus
				 --WHERE sls_org IN ('2400','2500')) 
						 EMD ON LTRIM (t4.sap_matl_num,0) = LTRIM (emd.matl_num ,0)))
WHERE RANK = 1)product on ltrim(ACTUAL.MAPPED_SKU_CODE,0)=ltrim(product.sku_cd,0)) Q,
                   
        (SELECT DISTINCT "cluster",CTRY_GROUP FROM EDW_COMPANY_DIM WHERE CTRY_GROUP = 'Thailand')COM  
))),

final as (
    select
    fisc_yr::integer AS fisc_yr,
fisc_per::integer AS fisc_per,
market::varchar(50) AS market,
data_src::varchar(14) AS data_src,
distributor_code::varchar(100) AS distributor_code,
distributor_name::varchar(356) AS distributor_name,
sell_out_channel::varchar(150) AS sell_out_channel,
sell_out_re::varchar(150) AS sell_out_re,
store_type::varchar(255) AS store_type,
store_code::varchar(100) AS store_code,
store_name::varchar(601) AS store_name,
region::varchar(150) AS region,
zone_name::varchar(150) AS zone_name,
soldto_code::varchar(255) AS soldto_code,
product_code::varchar(150) AS product_code,
product_name::varchar(225) AS product_name,
prod_hier_l1::varchar(8) AS prod_hier_l1,
prod_hier_l3::varchar(382) AS prod_hier_l3,
prod_hier_l5::varchar(1) AS prod_hier_l5,
mapped_sku_cd::varchar(40) AS mapped_sku_cd,
customer_segment_key::varchar(12) AS customer_segment_key,
customer_segment_description::varchar(50) AS customer_segment_description,
retail_environment::varchar(150) AS retail_environment,
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
customer_name::varchar(75) AS customer_name,
customer_code::varchar(12) AS customer_code,
sap_prod_sgmt_cd::varchar(1) AS sap_prod_sgmt_cd,
sap_prod_sgmt_desc::varchar(1) AS sap_prod_sgmt_desc,
sap_base_prod_desc::varchar(1) AS sap_base_prod_desc,
sap_mega_brnd_desc::varchar(1) AS sap_mega_brnd_desc,
sap_brnd_desc::varchar(1) AS sap_brnd_desc,
sap_vrnt_desc::varchar(1) AS sap_vrnt_desc,
sap_put_up_desc::varchar(1) AS sap_put_up_desc,
sap_grp_frnchse_cd::varchar(1) AS sap_grp_frnchse_cd,
sap_grp_frnchse_desc::varchar(1) AS sap_grp_frnchse_desc,
sap_frnchse_cd::varchar(1) AS sap_frnchse_cd,
sap_frnchse_desc::varchar(1) AS sap_frnchse_desc,
sap_prod_frnchse_cd::varchar(1) AS sap_prod_frnchse_cd,
sap_prod_frnchse_desc::varchar(1) AS sap_prod_frnchse_desc,
sap_prod_mjr_cd::varchar(1) AS sap_prod_mjr_cd,
sap_prod_mjr_desc::varchar(1) AS sap_prod_mjr_desc,
sap_prod_mnr_cd::varchar(1) AS sap_prod_mnr_cd,
sap_prod_mnr_desc::varchar(1) AS sap_prod_mnr_desc,
sap_prod_hier_cd::varchar(1) AS sap_prod_hier_cd,
sap_prod_hier_desc::varchar(1) AS sap_prod_hier_desc,
sap_go_to_mdl_key::varchar(50) AS sap_go_to_mdl_key,
sap_go_to_mdl_description::varchar(75) AS sap_go_to_mdl_description,
global_product_franchise::varchar(30) AS global_product_franchise,
global_product_brand::varchar(30) AS global_product_brand,
global_product_sub_brand::varchar(100) AS global_product_sub_brand,
global_product_variant::varchar(100) AS global_product_variant,
global_product_segment::varchar(50) AS global_product_segment,
global_product_subsegment::varchar(100) AS global_product_subsegment,
global_product_category::varchar(50) AS global_product_category,
global_product_subcategory::varchar(50) AS global_product_subcategory,
global_put_up_description::varchar(100) AS global_put_up_description,
sap_sku_code::varchar(1) AS sap_sku_code,
sap_sku_description::varchar(1) AS sap_sku_description,
pka_franchise_desc::varchar(1) AS pka_franchise_desc,
pka_brand_desc::varchar(1) AS pka_brand_desc,
pka_sub_brand_desc::varchar(1) AS pka_sub_brand_desc,
pka_variant_desc::varchar(1) AS pka_variant_desc,
pka_sub_variant_desc::varchar(1) AS pka_sub_variant_desc,
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
cluster::varchar(100) AS cluster
from transformation
)


select * from final