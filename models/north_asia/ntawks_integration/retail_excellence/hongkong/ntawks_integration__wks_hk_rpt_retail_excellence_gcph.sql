--import cte

with wks_hk_rpt_retail_excellence as (
    select * from {{ ref('ntawks_integration__wks_hk_rpt_retail_excellence') }}
),
--final cte

wks_hk_rpt_retail_excellence_gcph as 
(
    SELECT FISC_YR,
       FISC_PER,
       "cluster",
       MARKET,
       DATA_SRC,
       CHANNEL_NAME,
       SOLD_TO_CODE,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       SELL_OUT_CHANNEL,
       STORE_TYPE,
       PRIORITIZATION_SEGMENTATION,
       STORE_CATEGORY,
       STORE_CODE,
       STORE_NAME,
       STORE_GRADE,
       STORE_SIZE,
       REGION,
       STORE_ADDRESS,
       POST_CODE,
       ZONE_NAME,
       CITY,
       RTRLATITUDE,
       RTRLONGITUDE,
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
       PRODUCT_CODE,
       PRODUCT_NAME,
       PROD_HIER_L1,
       PROD_HIER_L2,
       PROD_HIER_L3,
       PROD_HIER_L4,
       PROD_HIER_L5,
       PROD_HIER_L6,
       PROD_HIER_L7,
       PROD_HIER_L8,
       PROD_HIER_L9,
       MAPPED_SKU_CD,
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
       PKA_FRANCHISE_DESC,
       PKA_BRAND_DESC,
       PKA_SUB_BRAND_DESC,
       PKA_VARIANT_DESC,
       PKA_SUB_VARIANT_DESC,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_VARIANT,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       GLOBAL_PUT_UP_DESCRIPTION,
       --EAN,
       --SKU_CODE,
       --SKU_DESCRIPTION,
       --GREENLIGHT_SKU_FLAG,
       CASE
         WHEN PKA_PRODUCT_KEY IN ('N/A','NA') THEN 'NA'
         ELSE PKA_PRODUCT_KEY
       END AS PKA_PRODUCT_KEY,
       CASE
         WHEN PKA_PRODUCT_KEY_DESCRIPTION IN ('N/A','NA') THEN 'NA'
         ELSE PKA_PRODUCT_KEY_DESCRIPTION
       END AS PKA_PRODUCT_KEY_DESCRIPTION,
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
       P6M_SALES,
       P6M_QTY,
       P6M_AVG_QTY,
	   P6M_SALES_LP,
       P12M_SALES,
       P12M_QTY,
       P12M_AVG_QTY,
	   P12M_SALES_LP,
       F3M_SALES,
       F3M_QTY,
       F3M_AVG_QTY,
       LM_SALES_FLAG,
       P3M_SALES_FLAG,
       P6M_SALES_FLAG,
       P12M_SALES_FLAG,
       MDP_FLAG,
       TARGET_COMPLAINCE,
       LIST_PRICE,
       TOTAL_SALES_LM,
       TOTAL_SALES_P3M,
       TOTAL_SALES_P6M,
       TOTAL_SALES_P12M,
       TOTAL_SALES_BY_STORE_LM,
       TOTAL_SALES_BY_STORE_P3M,
       TOTAL_SALES_BY_STORE_P6M,
       TOTAL_SALES_BY_STORE_P12M,
       TOTAL_SALES_BY_SKU_LM,
       TOTAL_SALES_BY_SKU_P3M,
       TOTAL_SALES_BY_SKU_P6M,
       TOTAL_SALES_BY_SKU_P12M,
	   TOTAL_SALES_LM_LP,
       TOTAL_SALES_P3M_LP,
       TOTAL_SALES_P6M_LP,
       TOTAL_SALES_P12M_LP,
       TOTAL_SALES_BY_STORE_LM_LP,
       TOTAL_SALES_BY_STORE_P3M_LP,
       TOTAL_SALES_BY_STORE_P6M_LP,
       TOTAL_SALES_BY_STORE_P12M_LP,
       TOTAL_SALES_BY_SKU_LM_LP,
       TOTAL_SALES_BY_SKU_P3M_LP,
       TOTAL_SALES_BY_SKU_P6M_LP,
       TOTAL_SALES_BY_SKU_P12M_LP,
       (TOTAL_SALES_BY_STORE_LM / NULLIF(TOTAL_SALES_LM,0)) AS STORE_CONTRIBUTION_LM,
       (TOTAL_SALES_BY_SKU_LM / NULLIF(TOTAL_SALES_LM,0)) AS SKU_CONTRIBUTION_LM,
       (STORE_CONTRIBUTION_LM*SKU_CONTRIBUTION_LM*TOTAL_SALES_LM) AS SIZE_OF_PRICE_LM,
       (TOTAL_SALES_BY_STORE_P3M / NULLIF(TOTAL_SALES_P3M,0)) AS STORE_CONTRIBUTION_P3M,
       (TOTAL_SALES_BY_SKU_P3M / NULLIF(TOTAL_SALES_P3M,0)) AS SKU_CONTRIBUTION_P3M,
       (STORE_CONTRIBUTION_P3M*SKU_CONTRIBUTION_P3M*TOTAL_SALES_P3M) AS SIZE_OF_PRICE_P3M,
       (TOTAL_SALES_BY_STORE_P6M / NULLIF(TOTAL_SALES_P6M,0)) AS STORE_CONTRIBUTION_P6M,
       (TOTAL_SALES_BY_SKU_P6M / NULLIF(TOTAL_SALES_P6M,0)) AS SKU_CONTRIBUTION_P6M,
       (STORE_CONTRIBUTION_P6M*SKU_CONTRIBUTION_P6M*TOTAL_SALES_P6M) AS SIZE_OF_PRICE_P6M,
       (TOTAL_SALES_BY_STORE_P12M / NULLIF(TOTAL_SALES_P12M,0)) AS STORE_CONTRIBUTION_P12M,
       (TOTAL_SALES_BY_SKU_P12M / NULLIF(TOTAL_SALES_P12M,0)) AS SKU_CONTRIBUTION_P12M,
       (STORE_CONTRIBUTION_P12M*SKU_CONTRIBUTION_P12M*TOTAL_SALES_P12M) AS SIZE_OF_PRICE_P12M,
	   (TOTAL_SALES_BY_STORE_LM_LP / NULLIF(TOTAL_SALES_LM_LP,0)) AS STORE_CONTRIBUTION_LM_LP,
       (TOTAL_SALES_BY_SKU_LM_LP / NULLIF(TOTAL_SALES_LM_LP,0)) AS SKU_CONTRIBUTION_LM_LP,
       (STORE_CONTRIBUTION_LM_LP*SKU_CONTRIBUTION_LM_LP*TOTAL_SALES_LM_LP) AS SIZE_OF_PRICE_LM_LP,
       (TOTAL_SALES_BY_STORE_P3M_LP / NULLIF(TOTAL_SALES_P3M_LP,0)) AS STORE_CONTRIBUTION_P3M_LP,
       (TOTAL_SALES_BY_SKU_P3M_LP / NULLIF(TOTAL_SALES_P3M_LP,0)) AS SKU_CONTRIBUTION_P3M_LP,
       (STORE_CONTRIBUTION_P3M_LP*SKU_CONTRIBUTION_P3M_LP*TOTAL_SALES_P3M_LP) AS SIZE_OF_PRICE_P3M_LP,
       (TOTAL_SALES_BY_STORE_P6M_LP / NULLIF(TOTAL_SALES_P6M_LP,0)) AS STORE_CONTRIBUTION_P6M_LP,
       (TOTAL_SALES_BY_SKU_P6M_LP / NULLIF(TOTAL_SALES_P6M_LP,0)) AS SKU_CONTRIBUTION_P6M_LP,
       (STORE_CONTRIBUTION_P6M_LP*SKU_CONTRIBUTION_P6M_LP*TOTAL_SALES_P6M_LP) AS SIZE_OF_PRICE_P6M_LP,
       (TOTAL_SALES_BY_STORE_P12M_LP / NULLIF(TOTAL_SALES_P12M_LP,0)) AS STORE_CONTRIBUTION_P12M_LP,
       (TOTAL_SALES_BY_SKU_P12M_LP / NULLIF(TOTAL_SALES_P12M_LP,0)) AS SKU_CONTRIBUTION_P12M_LP,
       (STORE_CONTRIBUTION_P12M_LP*SKU_CONTRIBUTION_P12M_LP*TOTAL_SALES_P12M_LP) AS SIZE_OF_PRICE_P12M_LP,
       MD5(DISTRIBUTOR_CODE||SELL_OUT_CHANNEL||RETAIL_ENVIRONMENT||nvl (CUSTOMER_SEGMENT_DESCRIPTION,'csd') || nvl (SAP_CUSTOMER_CHANNEL_DESCRIPTION,'sccd') ||nvl (SAP_SUB_CHANNEL_DESCRIPTION,'sscd') ||nvl (SAP_PARENT_CUSTOMER_DESCRIPTION,'spscd') || nvl (SAP_BANNER_DESCRIPTION,'sbd') || nvl (SAP_BANNER_FORMAT_DESCRIPTION,'sbfd')) AS CUSTOMER_AGG_DIM_KEY,
       MD5(nvl (SAP_BASE_PROD_DESC,'sbpd') || nvl (SAP_MEGA_BRND_DESC,'smbd') ||nvl (SAP_BRND_DESC,'sb') || nvl (SAP_FRNCHSE_DESC,'sfd') || nvl (SAP_PROD_MJR_DESC,'spmd') ||nvl (SAP_PROD_MNR_DESC,'spm') || nvl (PKA_FRANCHISE_DESC,'pfd') || nvl (PKA_BRAND_DESC,'pbd') ||nvl (PKA_SUB_BRAND_DESC,'pbsd') ||nvl (GLOBAL_PRODUCT_FRANCHISE,'gpf') || nvl (GLOBAL_PRODUCT_BRAND,'gpb') ||nvl (GLOBAL_PRODUCT_SUB_BRAND,'gpsb') ||nvl (GLOBAL_PRODUCT_SEGMENT,'gps') || nvl (GLOBAL_PRODUCT_SUBSEGMENT,'gpss') ||nvl (GLOBAL_PRODUCT_CATEGORY,'gpc') ||nvl (GLOBAL_PRODUCT_SUBCATEGORY,'gpsc')) AS PRODUCT_AGG_DIM_KEY,
       SYSDATE() AS CRTD_DTTM
FROM (SELECT MAIN.FISC_YR,
             MAIN.FISC_PER,	
             MAIN."cluster",	
             MAIN.MARKET AS MARKET,	
             MAIN.DATA_SRC,	
             COALESCE(MAIN.CHANNEL_NAME,'Not defined') AS CHANNEL_NAME,
             MAIN.SOLD_TO_CODE,	
             MAIN.DISTRIBUTOR_CODE,	
             MAIN.DISTRIBUTOR_NAME,	
             COALESCE(MAIN.SELL_OUT_CHANNEL,'Not defined') AS SELL_OUT_CHANNEL,	
			 UPPER(MAIN.STORE_TYPE) AS STORE_TYPE,
		     'Not available' as PRIORITIZATION_SEGMENTATION,
		     'Not available' as STORE_CATEGORY,
		     MAIN.STORE_CODE,
		     MAIN.STORE_NAME,
		     COALESCE(MAIN.STORE_GRADE, 'Not available') as STORE_GRADE,
		     'Not available' as STORE_SIZE,
		     MAIN.REGION,
		     'Not available' AS STORE_ADDRESS,
			 'Not available' AS POST_CODE,
		     MAIN.ZONE_NAME,	
		     'Not available' AS CITY,
		     'Not available' as RTRLATITUDE,
		     'Not available' as RTRLONGITUDE,
             TRIM(NVL (NULLIF(MAIN.CUSTOMER_SEGMENT_KEY,''),'NA')) AS CUSTOMER_SEGMENT_KEY,	
             TRIM(NVL (NULLIF(MAIN.CUSTOMER_SEGMENT_DESCRIPTION,''),'NA')) AS CUSTOMER_SEGMENT_DESCRIPTION,
			 TRIM(NVL (NULLIF(MAIN.RETAIL_ENVIRONMENT,''),'NA')) AS RETAIL_ENVIRONMENT,	
             TRIM(NVL (NULLIF(MAIN.SAP_CUSTOMER_CHANNEL_KEY,''),'NA')) AS SAP_CUSTOMER_CHANNEL_KEY,	
             UPPER(TRIM(NVL (NULLIF(MAIN.SAP_CUSTOMER_CHANNEL_DESCRIPTION,''),'NA'))) AS SAP_CUSTOMER_CHANNEL_DESCRIPTION,
             TRIM(NVL (NULLIF(MAIN.SAP_CUSTOMER_SUB_CHANNEL_KEY,''),'NA')) AS SAP_CUSTOMER_SUB_CHANNEL_KEY,	
             UPPER(TRIM(NVL (NULLIF(MAIN.SAP_SUB_CHANNEL_DESCRIPTION,''),'NA'))) AS SAP_SUB_CHANNEL_DESCRIPTION,
             TRIM(NVL (NULLIF(MAIN.SAP_PARENT_CUSTOMER_KEY,''),'NA')) AS SAP_PARENT_CUSTOMER_KEY,	
             UPPER(TRIM(NVL (NULLIF(MAIN.SAP_PARENT_CUSTOMER_DESCRIPTION,''),'NA'))) AS SAP_PARENT_CUSTOMER_DESCRIPTION,
             TRIM(NVL (NULLIF(MAIN.SAP_BANNER_KEY,''),'NA')) AS SAP_BANNER_KEY,	
             UPPER(TRIM(NVL (NULLIF(MAIN.SAP_BANNER_DESCRIPTION,''),'NA'))) AS SAP_BANNER_DESCRIPTION,	
             TRIM(NVL (NULLIF(MAIN.SAP_BANNER_FORMAT_KEY,''),'NA')) AS SAP_BANNER_FORMAT_KEY,	
             UPPER(TRIM(NVL (NULLIF(MAIN.SAP_BANNER_FORMAT_DESCRIPTION,''),'NA'))) AS SAP_BANNER_FORMAT_DESCRIPTION,
             TRIM(NVL (NULLIF(MAIN.CUSTOMER_NAME,''),'NA')) AS CUSTOMER_NAME,
             TRIM(NVL (NULLIF(MAIN.CUSTOMER_CODE,''),'NA')) AS CUSTOMER_CODE,
             MAIN.PRODUCT_CODE AS PRODUCT_CODE,	
             MAIN.PRODUCT_NAME AS PRODUCT_NAME,	
             MAIN.PROD_HIER_L1,	
             MAIN.PROD_HIER_L2,		
             MAIN.PROD_HIER_L3,	
             MAIN.PROD_HIER_L4,	
             MAIN.PROD_HIER_L5,	
             MAIN.PROD_HIER_L6,	
             MAIN.PROD_HIER_L7,	
             MAIN.PROD_HIER_L8,	
             MAIN.PROD_HIER_L9,	
             MAIN.MAPPED_SKU_CD,
             MAIN.SAP_PROD_SGMT_CD,	
             MAIN.SAP_PROD_SGMT_DESC,
             MAIN.SAP_BASE_PROD_DESC,	
             MAIN.SAP_MEGA_BRND_DESC,
             MAIN.SAP_BRND_DESC,
             MAIN.SAP_VRNT_DESC,
             MAIN.SAP_PUT_UP_DESC,	
             MAIN.SAP_GRP_FRNCHSE_CD,
             MAIN.SAP_GRP_FRNCHSE_DESC,	
             MAIN.SAP_FRNCHSE_CD,	
             MAIN.SAP_FRNCHSE_DESC,	
             MAIN.SAP_PROD_FRNCHSE_CD,	
             MAIN.SAP_PROD_FRNCHSE_DESC,	
             MAIN.SAP_PROD_MJR_CD,	
             MAIN.SAP_PROD_MJR_DESC,	
             MAIN.SAP_PROD_MNR_CD,	
             MAIN.SAP_PROD_MNR_DESC,	
             MAIN.SAP_PROD_HIER_CD,	
             MAIN.SAP_PROD_HIER_DESC,	
             TRIM(NVL (NULLIF(MAIN.GLOBAL_PRODUCT_FRANCHISE,''),'NA')) AS GLOBAL_PRODUCT_FRANCHISE,	
             TRIM(NVL (NULLIF(MAIN.GLOBAL_PRODUCT_BRAND,''),'NA')) AS GLOBAL_PRODUCT_BRAND,	
             TRIM(NVL (NULLIF(MAIN.GLOBAL_PRODUCT_SUB_BRAND,''),'NA')) AS GLOBAL_PRODUCT_SUB_BRAND,	
             TRIM(NVL (NULLIF(MAIN.GLOBAL_PRODUCT_VARIANT,''),'NA')) AS GLOBAL_PRODUCT_VARIANT,	
             TRIM(NVL (NULLIF(MAIN.GLOBAL_PRODUCT_SEGMENT,''),'NA')) AS GLOBAL_PRODUCT_SEGMENT,	
             TRIM(NVL (NULLIF(MAIN.GLOBAL_PRODUCT_SUBSEGMENT,''),'NA')) AS GLOBAL_PRODUCT_SUBSEGMENT,	
             TRIM(NVL (NULLIF(MAIN.GLOBAL_PRODUCT_CATEGORY,''),'NA')) AS GLOBAL_PRODUCT_CATEGORY,	
             TRIM(NVL (NULLIF(MAIN.GLOBAL_PRODUCT_SUBCATEGORY,''),'NA')) AS GLOBAL_PRODUCT_SUBCATEGORY,	
             TRIM(NVL (NULLIF(MAIN.GLOBAL_PUT_UP_DESCRIPTION,''),'NA')) AS GLOBAL_PUT_UP_DESCRIPTION,	
             --TRIM(NVL (NULLIF(MAIN.EAN,''),'NA')) AS EAN,
             --LTRIM(MAIN.SKU_CODE,0) AS SKU_CODE,
             --UPPER(MAIN.SKU_DESCRIPTION) AS SKU_DESCRIPTION,
             CASE
               WHEN TRIM(NVL (NULLIF(MAIN.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(MAIN.PKA_PRODUCT_KEY,''),'NA'))	
               ELSE TRIM(NVL (NULLIF(MAIN.PKA_PRODUCT_KEY,''),'NA'))
             END AS PKA_PRODUCT_KEY,
             CASE
               WHEN TRIM(NVL (NULLIF(MAIN.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(MAIN.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA'))		
               ELSE TRIM(NVL (NULLIF(MAIN.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA'))	
             END AS PKA_PRODUCT_KEY_DESCRIPTION,
             MAIN.PKA_FRANCHISE_DESC,	
             MAIN.PKA_BRAND_DESC,	
             MAIN.PKA_SUB_BRAND_DESC,	
             MAIN.PKA_VARIANT_DESC,	
             MAIN.PKA_SUB_VARIANT_DESC,	
             MAIN.SALES_VALUE AS SALES_VALUE,	
             MAIN.SALES_QTY AS SALES_QTY,	
             MAIN.AVG_SALES_QTY AS AVG_SALES_QTY,	
			 MAIN.SALES_VALUE_LIST_PRICE AS SALES_VALUE_LIST_PRICE,		
             MAIN.LM_SALES AS LM_SALES,	
             MAIN.LM_SALES_QTY AS LM_SALES_QTY,	
             MAIN.LM_AVG_SALES_QTY AS LM_AVG_SALES_QTY,
			 MAIN.LM_SALES_LP AS LM_SALES_LP,	
             MAIN.P3M_SALES AS P3M_SALES,		
             MAIN.P3M_QTY AS P3M_QTY,	
             MAIN.P3M_AVG_QTY AS P3M_AVG_QTY,	
			 MAIN.P3M_SALES_LP AS P3M_SALES_LP,	
             MAIN.F3M_SALES AS F3M_SALES,	
             MAIN.F3M_QTY AS F3M_QTY,	
             MAIN.F3M_AVG_QTY AS F3M_AVG_QTY,	
             MAIN.P6M_SALES AS P6M_SALES,	
             MAIN.P6M_QTY AS P6M_QTY,	
             MAIN.P6M_AVG_QTY AS P6M_AVG_QTY,	
			 MAIN.P6M_SALES_LP AS P6M_SALES_LP,	
             MAIN.P12M_SALES AS P12M_SALES,	
             MAIN.P12M_QTY AS P12M_QTY,	
             MAIN.P12M_AVG_QTY AS P12M_AVG_QTY,	
			 MAIN.P12M_SALES_LP AS P12M_SALES_LP,	
             MAIN.LM_SALES_FLAG,	
             MAIN.P3M_SALES_FLAG,	
             MAIN.P6M_SALES_FLAG,	
             MAIN.P12M_SALES_FLAG,	
             MAIN.MDP_FLAG,	
             1 AS TARGET_COMPLAINCE,
             MAIN.LIST_PRICE,	
             SUM(MAIN.LM_SALES) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND) AS TOTAL_SALES_LM,		
             SUM(MAIN.P3M_SALES) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND) AS TOTAL_SALES_P3M,		
             SUM(MAIN.P6M_SALES) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND) AS TOTAL_SALES_P6M,		
             SUM(MAIN.P12M_SALES) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND) AS TOTAL_SALES_P12M,
             SUM(MAIN.LM_SALES) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,STORE_CODE) AS TOTAL_SALES_BY_STORE_LM,	
             SUM(MAIN.P3M_SALES) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,STORE_CODE) AS TOTAL_SALES_BY_STORE_P3M,	
             SUM(MAIN.P6M_SALES) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,STORE_CODE) AS TOTAL_SALES_BY_STORE_P6M,
             SUM(MAIN.P12M_SALES) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,STORE_CODE) AS TOTAL_SALES_BY_STORE_P12M,	
             SUM(MAIN.LM_SALES) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,PRODUCT_CODE) AS TOTAL_SALES_BY_SKU_LM,
             SUM(MAIN.P3M_SALES) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,PRODUCT_CODE) AS TOTAL_SALES_BY_SKU_P3M,
             SUM(MAIN.P6M_SALES) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,PRODUCT_CODE) AS TOTAL_SALES_BY_SKU_P6M,	
             SUM(MAIN.P12M_SALES) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,PRODUCT_CODE) AS TOTAL_SALES_BY_SKU_P12M,	
			 SUM(MAIN.LM_SALES_LP) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND) AS TOTAL_SALES_LM_LP,	
             SUM(MAIN.P3M_SALES_LP) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND) AS TOTAL_SALES_P3M_LP,	
             SUM(MAIN.P6M_SALES_LP) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND) AS TOTAL_SALES_P6M_LP,	
             SUM(MAIN.P12M_SALES_LP) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND) AS TOTAL_SALES_P12M_LP,	
             SUM(MAIN.LM_SALES_LP) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,STORE_CODE) AS TOTAL_SALES_BY_STORE_LM_LP,		
             SUM(MAIN.P3M_SALES_LP) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,STORE_CODE) AS TOTAL_SALES_BY_STORE_P3M_LP,		
             SUM(MAIN.P6M_SALES_LP) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,STORE_CODE) AS TOTAL_SALES_BY_STORE_P6M_LP,		
             SUM(MAIN.P12M_SALES_LP) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,STORE_CODE) AS TOTAL_SALES_BY_STORE_P12M_LP,		
             SUM(MAIN.LM_SALES_LP) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,PRODUCT_CODE) AS TOTAL_SALES_BY_SKU_LM_LP,		
             SUM(MAIN.P3M_SALES_LP) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,PRODUCT_CODE) AS TOTAL_SALES_BY_SKU_P3M_LP,		
             SUM(MAIN.P6M_SALES_LP) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,PRODUCT_CODE) AS TOTAL_SALES_BY_SKU_P6M_LP,		
             SUM(MAIN.P12M_SALES_LP) OVER (PARTITION BY FISC_PER,DISTRIBUTOR_CODE,GLOBAL_PRODUCT_BRAND,PRODUCT_CODE) AS TOTAL_SALES_BY_SKU_P12M_LP		
      FROM WKS_HK_RPT_RETAIL_EXCELLENCE MAIN)
),

final as 
(
    select fisc_yr :: numeric(18,0) as fisc_yr,
	fisc_per :: numeric(18,0) as fisc_per,
	"cluster" :: varchar(100) as "cluster",
	market :: varchar(30) as market,
	data_src :: varchar(14) as data_src,
	channel_name :: varchar(337) as channel_name,
	sold_to_code :: varchar(382) as sold_to_code,
	distributor_code :: varchar(225) as distributor_code,
	distributor_name :: varchar(801) as distributor_name,
	sell_out_channel :: varchar(337) as sell_out_channel,
	store_type :: varchar(859) as store_type,
	prioritization_segmentation :: varchar(13) as prioritization_segmentation,
	store_category :: varchar(13) as store_category,
	store_code :: varchar(225) as store_code,
	store_name :: varchar(1351) as store_name,
	store_grade :: varchar(225) as store_grade,
	store_size :: varchar(13) as store_size,
	region :: varchar(150) as region,
	store_address :: varchar(13) as store_address,
	post_code :: varchar(13) as post_code,
	zone_name :: varchar(150) as zone_name,
	city :: varchar(13) as city,
	rtrlatitude :: varchar(13) as rtrlatitude,
	rtrlongitude :: varchar(13) as rtrlongitude,
	customer_segment_key :: varchar(12) as customer_segment_key,
	customer_segment_description :: varchar(50) as customer_segment_description,
	retail_environment :: varchar(337) as retail_environment,
	sap_customer_channel_key :: varchar(12) as sap_customer_channel_key,
	sap_customer_channel_description :: varchar(112) as sap_customer_channel_description,
	sap_customer_sub_channel_key :: varchar(12) as sap_customer_sub_channel_key,
	sap_sub_channel_description :: varchar(112) as sap_sub_channel_description,
	sap_parent_customer_key :: varchar(12) as sap_parent_customer_key,
	sap_parent_customer_description :: varchar(112) as sap_parent_customer_description,
	sap_banner_key :: varchar(12) as sap_banner_key,
	sap_banner_description :: varchar(112) as sap_banner_description,
	sap_banner_format_key :: varchar(12) as sap_banner_format_key,
	sap_banner_format_description :: varchar(112) as sap_banner_format_description,
	customer_name :: varchar(100) as customer_name,
	customer_code :: varchar(10) as customer_code,
	product_code :: varchar(200) as product_code,
	product_name :: varchar(300) as product_name,
	prod_hier_l1 :: varchar(200) as prod_hier_l1,
	prod_hier_l2 :: varchar(200) as prod_hier_l2,
	prod_hier_l3 :: varchar(200) as prod_hier_l3,
	prod_hier_l4 :: varchar(200) as prod_hier_l4,
	prod_hier_l5 :: varchar(200) as prod_hier_l5,
	prod_hier_l6 :: varchar(200) as prod_hier_l6,
	prod_hier_l7 :: varchar(200) as prod_hier_l7,
	prod_hier_l8 :: varchar(200) as prod_hier_l8,
	prod_hier_l9 :: varchar(1) as prod_hier_l9,
	mapped_sku_cd :: varchar(40) as mapped_sku_cd,
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
	pka_franchise_desc :: varchar(30) as pka_franchise_desc,
	pka_brand_desc :: varchar(30) as pka_brand_desc,
	pka_sub_brand_desc :: varchar(30) as pka_sub_brand_desc,
	pka_variant_desc :: varchar(30) as pka_variant_desc,
	pka_sub_variant_desc :: varchar(30) as pka_sub_variant_desc,
	global_product_franchise :: varchar(30) as global_product_franchise,
	global_product_brand :: varchar(30) as global_product_brand,
	global_product_sub_brand :: varchar(100) as global_product_sub_brand,
	global_product_variant :: varchar(100) as global_product_variant,
	global_product_segment :: varchar(50) as global_product_segment,
	global_product_subsegment :: varchar(100) as global_product_subsegment,
	global_product_category :: varchar(50) as global_product_category,
	global_product_subcategory :: varchar(50) as global_product_subcategory,
	global_put_up_description :: varchar(100) as global_put_up_description,
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
	p6m_sales :: numeric(38,6) as p6m_sales,
	p6m_qty :: numeric(38,6) as p6m_qty,
	p6m_avg_qty :: numeric(38,6) as p6m_avg_qty,
	p6m_sales_lp :: numeric(38,12) as p6m_sales_lp,
	p12m_sales :: numeric(38,6) as p12m_sales,
	p12m_qty :: numeric(38,6) as p12m_qty,
	p12m_avg_qty :: numeric(38,6) as p12m_avg_qty,
	p12m_sales_lp :: numeric(38,12) as p12m_sales_lp,
	f3m_sales :: numeric(38,6) as f3m_sales,
	f3m_qty :: numeric(38,6) as f3m_qty,
	f3m_avg_qty :: numeric(38,6) as f3m_avg_qty,
	lm_sales_flag :: varchar(1) as lm_sales_flag,
	p3m_sales_flag :: varchar(1) as p3m_sales_flag,
	p6m_sales_flag :: varchar(1) as p6m_sales_flag,
	p12m_sales_flag :: varchar(1) as p12m_sales_flag,
	mdp_flag :: varchar(1) as mdp_flag,
	target_complaince :: numeric(18,0) as target_complaince,
	list_price :: numeric(38,6) as list_price,
	total_sales_lm :: numeric(38,6) as total_sales_lm,
	total_sales_p3m :: numeric(38,6) as total_sales_p3m,
	total_sales_p6m :: numeric(38,6) as total_sales_p6m,
	total_sales_p12m :: numeric(38,6) as total_sales_p12m,
	total_sales_by_store_lm :: numeric(38,6) as total_sales_by_store_lm,
	total_sales_by_store_p3m :: numeric(38,6) as total_sales_by_store_p3m,
	total_sales_by_store_p6m :: numeric(38,6) as total_sales_by_store_p6m,
	total_sales_by_store_p12m :: numeric(38,6) as total_sales_by_store_p12m,
	total_sales_by_sku_lm :: numeric(38,6) as total_sales_by_sku_lm,
	total_sales_by_sku_p3m :: numeric(38,6) as total_sales_by_sku_p3m,
	total_sales_by_sku_p6m :: numeric(38,6) as total_sales_by_sku_p6m,
	total_sales_by_sku_p12m :: numeric(38,6) as total_sales_by_sku_p12m,
	total_sales_lm_lp :: numeric(38,12) as total_sales_lm_lp,
	total_sales_p3m_lp :: numeric(38,12) as total_sales_p3m_lp,
	total_sales_p6m_lp :: numeric(38,12) as total_sales_p6m_lp,
	total_sales_p12m_lp :: numeric(38,12) as total_sales_p12m_lp,
	total_sales_by_store_lm_lp :: numeric(38,12) as total_sales_by_store_lm_lp,
	total_sales_by_store_p3m_lp :: numeric(38,12) as total_sales_by_store_p3m_lp,
	total_sales_by_store_p6m_lp :: numeric(38,12) as total_sales_by_store_p6m_lp,
	total_sales_by_store_p12m_lp :: numeric(38,12) as total_sales_by_store_p12m_lp,
	total_sales_by_sku_lm_lp :: numeric(38,12) as total_sales_by_sku_lm_lp,
	total_sales_by_sku_p3m_lp :: numeric(38,12) as total_sales_by_sku_p3m_lp,
	total_sales_by_sku_p6m_lp :: numeric(38,12) as total_sales_by_sku_p6m_lp,
	total_sales_by_sku_p12m_lp :: numeric(38,12) as total_sales_by_sku_p12m_lp,
	store_contribution_lm :: numeric(38,4) as store_contribution_lm,
	sku_contribution_lm :: numeric(38,4) as sku_contribution_lm,
	size_of_price_lm :: numeric(38,14) as size_of_price_lm,
	store_contribution_p3m :: numeric(38,4) as store_contribution_p3m,
	sku_contribution_p3m :: numeric(38,4) as sku_contribution_p3m,
	size_of_price_p3m :: numeric(38,14) as size_of_price_p3m,
	store_contribution_p6m :: numeric(38,4) as store_contribution_p6m,
	sku_contribution_p6m :: numeric(38,4) as sku_contribution_p6m,
	size_of_price_p6m :: numeric(38,14) as size_of_price_p6m,
	store_contribution_p12m :: numeric(38,4) as store_contribution_p12m,
	sku_contribution_p12m :: numeric(38,4) as sku_contribution_p12m,
	size_of_price_p12m :: numeric(38,14) as size_of_price_p12m,
	store_contribution_lm_lp :: numeric(38,4) as store_contribution_lm_lp,
	sku_contribution_lm_lp :: numeric(38,4) as sku_contribution_lm_lp,
	size_of_price_lm_lp :: numeric(38,20) as size_of_price_lm_lp,
	store_contribution_p3m_lp :: numeric(38,4) as store_contribution_p3m_lp,
	sku_contribution_p3m_lp :: numeric(38,4) as sku_contribution_p3m_lp,
	size_of_price_p3m_lp :: numeric(38,20) as size_of_price_p3m_lp,
	store_contribution_p6m_lp :: numeric(38,4) as store_contribution_p6m_lp,
	sku_contribution_p6m_lp :: numeric(38,4) as sku_contribution_p6m_lp,
	size_of_price_p6m_lp :: numeric(38,20) as size_of_price_p6m_lp,
	store_contribution_p12m_lp :: numeric(38,4) as store_contribution_p12m_lp,
	sku_contribution_p12m_lp :: numeric(38,4) as sku_contribution_p12m_lp,
	size_of_price_p12m_lp :: numeric(38,20) as size_of_price_p12m_lp,
	customer_agg_dim_key :: varchar(32) as customer_agg_dim_key,
	product_agg_dim_key :: varchar(32) as product_agg_dim_key,
	crtd_dttm :: timestamp without time zone as crtd_dttm
    from wks_hk_rpt_retail_excellence_gcph
)

--final select

select * from final 