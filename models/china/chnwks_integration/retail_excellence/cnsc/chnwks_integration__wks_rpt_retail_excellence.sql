with itg_cn_sc_re_msl_list as (
    select * from {{ ref('chnitg_integration__itg_cn_sc_re_msl_list') }}
),
wks_cnsc_regional_sellout_actuals as (
    select * from {{ ref('chnwks_integration__wks_cnsc_regional_sellout_actuals') }}
),
edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),

transformation as (
    (SELECT distinct Q.*,COM."cluster" FROM (SELECT distinct TARGET.FISC_YR,
             TARGET.FISC_PER,			 
             TARGET.PROD_HIER_L1 AS MARKET,
             COALESCE(ACTUAL.DISTRIBUTOR_CODE,TARGET.DISTRIBUTOR_CODE) AS DISTRIBUTOR_CODE,
             COALESCE(ACTUAL.DISTRIBUTOR_NAME,TARGET.DISTRIBUTOR_NAME) AS DISTRIBUTOR_NAME,
			 COALESCE(ACTUAL.CHANNEL,TARGET.SELL_OUT_CHANNEL) AS SELL_OUT_CHANNEL,
             COALESCE(ACTUAL.RETAIL_ENV,TARGET.Sell_Out_RE) AS Sell_Out_RE,
             COALESCE(ACTUAL.STORE_TYPE,TARGET.STORE_TYPE) AS STORE_TYPE,
             COALESCE(ACTUAL.STORE_CODE,TARGET.STORE_CODE) AS STORE_CODE,
             COALESCE(ACTUAL.STORE_NAME,TARGET.STORE_NAME)AS STORE_NAME,
             COALESCE(ACTUAL.REGION,TARGET.REGION)AS REGION,
             COALESCE(ACTUAL.ZONE_OR_AREA,TARGET.ZONE_NAME)AS ZONE_NAME,
             COALESCE(ACTUAL.msl_PRODUCT_CODE,TARGET.PRODUCT_CODE)AS PRODUCT_CODE,
             COALESCE(ACTUAL.MSL_PRODUCT_DESC,TARGET.PRODUCT_NAME)AS PRODUCT_NAME,
             --ACTUAL.MSL_PRODUCT_DESC AS PRODUCT_NAME,-------------- CHANGE TO SKU_DESC FROM TARGET
             TARGET.PROD_HIER_L1,
             TARGET.product_code AS MAPPED_SKU_CD,-----------------------ADD AND MAP AS MOTHER_SKU_CODE
             TRIM(NVL (NULLIF(ACTUAL.CUSTOMER_SEGMENT_KEY,''),'NA')) AS CUSTOMER_SEGMENT_KEY,
			 TRIM(NVL (NULLIF(ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION,''),'NA')) AS CUSTOMER_SEGMENT_DESCRIPTION,
			 TRIM(NVL (NULLIF(ACTUAL.RETAIL_ENV,''),'NA')) AS RETAIL_ENVIRONMENT,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_CUSTOMER_CHANNEL_KEY,''),'NA')) AS SAP_CUSTOMER_CHANNEL_KEY,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_CUSTOMER_CHANNEL_DESCRIPTION,''),'NA')) AS SAP_CUSTOMER_CHANNEL_DESCRIPTION,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_CUSTOMER_SUB_CHANNEL_KEY,''),'NA')) AS SAP_CUSTOMER_SUB_CHANNEL_KEY,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_SUB_CHANNEL_DESCRIPTION,''),'NA')) AS SAP_SUB_CHANNEL_DESCRIPTION,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_PARENT_CUSTOMER_KEY,''),'NA')) AS SAP_PARENT_CUSTOMER_KEY,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_PARENT_CUSTOMER_DESCRIPTION,''),'NA')) AS SAP_PARENT_CUSTOMER_DESCRIPTION,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_BANNER_KEY,''),'NA')) AS SAP_BANNER_KEY,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_BANNER_FORMAT_DESCRIPTION,''),'NA')) AS SAP_BANNER_FORMAT_DESCRIPTION,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_GO_TO_MDL_KEY,''),'NA')) AS SAP_GO_TO_MDL_KEY,
             TRIM(NVL (NULLIF(ACTUAL.SAP_GO_TO_MDL_DESCRIPTION,''),'NA')) AS SAP_GO_TO_MDL_DESCRIPTION,
			 TRIM(NVL (NULLIF(ACTUAL.PRODUCT_BRAND,''),'NA')) AS GLOBAL_PRODUCT_BRAND,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_SGMT_CD,''),'NA')) AS SAP_PROD_SGMT_CD,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_SGMT_DESC,''),'NA')) AS SAP_PROD_SGMT_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_BASE_PROD_DESC,''),'NA')) AS SAP_BASE_PROD_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_MEGA_BRND_DESC,''),'NA')) AS SAP_MEGA_BRND_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_BRND_DESC,''),'NA')) AS SAP_BRND_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_VRNT_DESC,''),'NA')) AS SAP_VRNT_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PUT_UP_DESC,''),'NA')) AS SAP_PUT_UP_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_GRP_FRNCHSE_CD,''),'NA')) AS SAP_GRP_FRNCHSE_CD,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_GRP_FRNCHSE_DESC,''),'NA')) AS SAP_GRP_FRNCHSE_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_FRNCHSE_CD,''),'NA')) AS SAP_FRNCHSE_CD,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_FRNCHSE_DESC,''),'NA')) AS SAP_FRNCHSE_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_FRNCHSE_CD,''),'NA')) AS SAP_PROD_FRNCHSE_CD,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_FRNCHSE_DESC,''),'NA')) AS SAP_PROD_FRNCHSE_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MJR_CD,''),'NA')) AS SAP_PROD_MJR_CD,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MJR_DESC,''),'NA')) AS SAP_PROD_MJR_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MNR_CD,''),'NA')) AS SAP_PROD_MNR_CD,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MNR_DESC,''),'NA')) AS SAP_PROD_MNR_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_HIER_CD,''),'NA')) AS SAP_PROD_HIER_CD,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_HIER_DESC,''),'NA')) AS SAP_PROD_HIER_DESC,
			 --COALESCE(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,PRODUCT.GPH_PROD_FRNCHSE) AS GLOBAL_PRODUCT_FRANCHISE,
             --COALESCE(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,PRODUCT.GPH_PROD_SUB_BRND) AS GLOBAL_PRODUCT_SUB_BRAND,
             --COALESCE(ACTUAL.GLOBAL_PRODUCT_VARIANT,PRODUCT.GPH_PROD_VRNT) AS GLOBAL_PRODUCT_VARIANT,
             --COALESCE(ACTUAL.GLOBAL_PRODUCT_SEGMENT,PRODUCT.GPH_PROD_NEEDSTATE) AS GLOBAL_PRODUCT_SEGMENT,
            --- COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,PRODUCT.GPH_PROD_SUBSGMNT) AS GLOBAL_PRODUCT_SUBSEGMENT,
            -- COALESCE(ACTUAL.GLOBAL_PRODUCT_CATEGORY,PRODUCT.GPH_PROD_CTGRY) AS GLOBAL_PRODUCT_CATEGORY,
            --- COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,PRODUCT.GPH_PROD_SUBCTGRY) AS GLOBAL_PRODUCT_SUBCATEGORY,
            -- COALESCE(ACTUAL.GLOBAL_PUT_UP_DESCRIPTION,PRODUCT.GPH_PROD_PUT_UP_DESC) AS GLOBAL_PUT_UP_DESCRIPTION,
             --TRIM(NVL (NULLIF(PRODUCT.EAN,''),'NA')) AS EAN,
             TRIM(NVL (NULLIF(ACTUAL.SKU_CODE,''),'NA')) AS SKU_CODE,----------------------------------------------------add 
             TRIM(NVL (NULLIF(ACTUAL.SKU_DESCRIPTION,''),'NA')) AS SKU_DESCRIPTION,--------------------------------------add
             --TRIM(NVL (NULLIF(PRODUCT.PKA_FRANCHISE_DESC,''),'NA')) AS PKA_FRANCHISE_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.PKA_BRAND_DESC,''),'NA')) AS PKA_BRAND_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_BRAND_DESC,''),'NA')) AS PKA_SUB_BRAND_DESC,
             --(NVL (NULLIF(PRODUCT.PKA_VARIANT_DESC,''),'NA')) AS PKA_VARIANT_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_VARIANT_DESC,''),'NA')) AS PKA_SUB_VARIANT_DESC,

             --Removing COALESCE as it has one argument
             ACTUAL.PKA_PRODUCT_KEY AS PKA_PRODUCT_KEY,
             --Removing COALESCE as it has one argument
             ACTUAL.PKA_PRODUCT_KEY_DESCRIPTION AS PKA_PRODUCT_KEY_DESCRIPTION,
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
   FROM ITG_CN_SC_RE_MSL_LIST TARGET
        LEFT JOIN (SELECT * FROM WKS_CNSC_REGIONAL_SELLOUT_ACTUALS) ACTUAL
               ON TARGET.FISC_PER = ACTUAL.MNTH_ID
              AND TARGET.DISTRIBUTOR_CODE = ACTUAL.DISTRIBUTOR_CODE
              --AND TARGET.DISTRIBUTOR_NAME = ACTUAL.DISTRIBUTOR_NAME
              AND TARGET.STORE_CODE = ACTUAL.STORE_CODE
              --AND TARGET.STORE_NAME = ACTUAL.STORE_NAME
              AND TARGET.STORE_type = ACTUAL.STORE_type
              AND TARGET.PRODUCT_CODE = ACTUAL.MSL_PRODUCT_CODE
			  
	--LEFT JOIN (SELECT DISTINCT xjp_code, brand_en FROM cn_itg.itg_mds_cn_otc_product_mapping)MDS ON MDS.xjp_code=ACTUAL.MSL_PRODUCT_CODE
        
		)Q
					
JOIN (SELECT DISTINCT "cluster",CTRY_GROUP FROM EDW_COMPANY_DIM WHERE CTRY_GROUP='China Selfcare')COM
ON TRIM(Q.PROD_HIER_L1)= TRIM(COM.CTRY_GROUP)			
    
where FISC_PER <= (select max(mnth_id) from WKS_CNSC_REGIONAL_SELLOUT_ACTUALS))


UNION ALL



(SELECT distinct Q.*,COM."cluster" FROM
(SELECT distinct cast(LEFT(ACTUAL.MNTH_ID,4)as integer) AS YEAR,
             CAST(ACTUAL.MNTH_ID AS INTEGER) AS MNTH_ID,		 			
            ACTUAL.CNTRY_NM AS MARKET,
            ACTUAL.DISTRIBUTOR_CODE AS DISTRIBUTOR_CODE,
            ACTUAL.DISTRIBUTOR_NAME as DISTRIBUTOR_NAME,
             ACTUAL.CHANNEL AS SELL_OUT_CHANNEL,
             ACTUAL.retail_env as Sell_Out_RE,
             ACTUAL.STORE_TYPE AS STORE_TYPE,
             ACTUAL.STORE_CODE AS STORE_CODE,
             ACTUAL.STORE_NAME AS STORE_NAME,
             ACTUAL.REGION AS REGION,
             ACTUAL.zone_or_area AS ZONE_NAME,
             ---FACT.CITY AS CITY,
             ACTUAL.msl_product_CODE AS PRODUCT_CODE,
             actual.MSL_PRODUCT_DESC AS PRODUCT_NAME,
              'China Selfcare' as PROD_HIER_L1,
             actual.MSL_PRODUCT_CODE AS MAPPED_SKU_CD,		
             TRIM(NVL (NULLIF(ACTUAL.CUSTOMER_SEGMENT_KEY,''),'NA')) AS CUSTOMER_SEGMENT_KEY,
			 TRIM(NVL (NULLIF(ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION,''),'NA')) AS CUSTOMER_SEGMENT_DESCRIPTION,
			 TRIM(NVL (NULLIF(ACTUAL.RETAIL_ENV,''),'NA')) AS RETAIL_ENVIRONMENT,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_CUSTOMER_CHANNEL_KEY,''),'NA')) AS SAP_CUSTOMER_CHANNEL_KEY,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_CUSTOMER_CHANNEL_DESCRIPTION,''),'NA')) AS SAP_CUSTOMER_CHANNEL_DESCRIPTION,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_CUSTOMER_SUB_CHANNEL_KEY,''),'NA')) AS SAP_CUSTOMER_SUB_CHANNEL_KEY,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_SUB_CHANNEL_DESCRIPTION,''),'NA')) AS SAP_SUB_CHANNEL_DESCRIPTION,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_PARENT_CUSTOMER_KEY,''),'NA')) AS SAP_PARENT_CUSTOMER_KEY,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_PARENT_CUSTOMER_DESCRIPTION,''),'NA')) AS SAP_PARENT_CUSTOMER_DESCRIPTION,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_BANNER_KEY,''),'NA')) AS SAP_BANNER_KEY,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_BANNER_FORMAT_DESCRIPTION,''),'NA')) AS SAP_BANNER_FORMAT_DESCRIPTION,
			 TRIM(NVL (NULLIF(ACTUAL.SAP_GO_TO_MDL_KEY,''),'NA')) AS SAP_GO_TO_MDL_KEY,
             TRIM(NVL (NULLIF(ACTUAL.SAP_GO_TO_MDL_DESCRIPTION,''),'NA')) AS SAP_GO_TO_MDL_DESCRIPTION,
			 TRIM(NVL (NULLIF(ACTUAL.PRODUCT_BRAND,''),'NA')) AS GLOBAL_PRODUCT_BRAND,
            
             --TRIM(NVL (NULLIF(CUSTOMER.SAP_CUST_NM,''),'NA')) AS CUSTOMER_NAME,
             --TRIM(NVL (NULLIF(CUSTOMER.SAP_CUST_ID,''),'NA')) AS CUSTOMER_CODE,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_SGMT_CD,''),'NA')) AS SAP_PROD_SGMT_CD,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_SGMT_DESC,''),'NA')) AS SAP_PROD_SGMT_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_BASE_PROD_DESC,''),'NA')) AS SAP_BASE_PROD_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_MEGA_BRND_DESC,''),'NA')) AS SAP_MEGA_BRND_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_BRND_DESC,''),'NA')) AS SAP_BRND_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_VRNT_DESC,''),'NA')) AS SAP_VRNT_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PUT_UP_DESC,''),'NA')) AS SAP_PUT_UP_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_GRP_FRNCHSE_CD,''),'NA')) AS SAP_GRP_FRNCHSE_CD,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_GRP_FRNCHSE_DESC,''),'NA')) AS SAP_GRP_FRNCHSE_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_FRNCHSE_CD,''),'NA')) AS SAP_FRNCHSE_CD,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_FRNCHSE_DESC,''),'NA')) AS SAP_FRNCHSE_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_FRNCHSE_CD,''),'NA')) AS SAP_PROD_FRNCHSE_CD,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_FRNCHSE_DESC,''),'NA')) AS SAP_PROD_FRNCHSE_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MJR_CD,''),'NA')) AS SAP_PROD_MJR_CD,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MJR_DESC,''),'NA')) AS SAP_PROD_MJR_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MNR_CD,''),'NA')) AS SAP_PROD_MNR_CD,
             ---TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_MNR_DESC,''),'NA')) AS SAP_PROD_MNR_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_HIER_CD,''),'NA')) AS SAP_PROD_HIER_CD,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_PROD_HIER_DESC,''),'NA')) AS SAP_PROD_HIER_DESC,
             --TRIM(NVL (NULLIF(CUSTOMER.SAP_GO_TO_MDL_KEY,''),'NA')) AS SAP_GO_TO_MDL_KEY,
       --UPPER(TRIM(NVL (NULLIF(CUSTOMER.SAP_GO_TO_MDL_DESC,''),'NA'))) AS SAP_GO_TO_MDL_DESCRIPTION,
             --COALESCE(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,PRODUCT.GPH_PROD_FRNCHSE) AS GLOBAL_PRODUCT_FRANCHISE,
             --COALESCE(ACTUAL.GLOBAL_PRODUCT_BRAND,PRODUCT.GPH_PROD_BRND) AS GLOBAL_PRODUCT_BRAND,
             --COALESCE(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,PRODUCT.GPH_PROD_SUB_BRND) AS GLOBAL_PRODUCT_SUB_BRAND,
             --COALESCE(ACTUAL.GLOBAL_PRODUCT_VARIANT,PRODUCT.GPH_PROD_VRNT) AS GLOBAL_PRODUCT_VARIANT,
             --COALESCE(ACTUAL.GLOBAL_PRODUCT_SEGMENT,PRODUCT.GPH_PROD_NEEDSTATE) AS GLOBAL_PRODUCT_SEGMENT,
             --COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,PRODUCT.GPH_PROD_SUBSGMNT) AS GLOBAL_PRODUCT_SUBSEGMENT,
             ---COALESCE(ACTUAL.GLOBAL_PRODUCT_CATEGORY,PRODUCT.GPH_PROD_CTGRY) AS GLOBAL_PRODUCT_CATEGORY,
             --COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,PRODUCT.GPH_PROD_SUBCTGRY) AS GLOBAL_PRODUCT_SUBCATEGORY,
             --COALESCE(ACTUAL.GLOBAL_PUT_UP_DESCRIPTION,PRODUCT.GPH_PROD_PUT_UP_DESC) AS GLOBAL_PUT_UP_DESCRIPTION,
             --TRIM(NVL (NULLIF(PRODUCT.EAN,''),'NA')) AS EAN,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA')) AS SKU_CODE,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA')) AS SKU_DESCRIPTION,
             --TRIM(NVL (NULLIF(PRODUCT.PKA_FRANCHISE_DESC,''),'NA')) AS PKA_FRANCHISE_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.PKA_BRAND_DESC,''),'NA')) AS PKA_BRAND_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_BRAND_DESC,''),'NA')) AS PKA_SUB_BRAND_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.PKA_VARIANT_DESC,''),'NA')) AS PKA_VARIANT_DESC,
             --TRIM(NVL (NULLIF(PRODUCT.PKA_SUB_VARIANT_DESC,''),'NA')) AS PKA_SUB_VARIANT_DESC,
			 TRIM(NVL (NULLIF(ACTUAL.SKU_CODE,''),'NA')) AS SKU_CODE,----------------------------------------------------add 
             TRIM(NVL (NULLIF(ACTUAL.SKU_DESCRIPTION,''),'NA')) AS SKU_DESCRIPTION,--------------------------------------add
             --Removing COALESCE as it has one argument
             ACTUAL.PKA_PRODUCT_KEY AS PKA_PRODUCT_KEY,
             --Removing COALESCE as it has one argument
             ACTUAL.PKA_PRODUCT_KEY_DESCRIPTION AS PKA_PRODUCT_KEY_DESCRIPTION,	 
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
                --sys_date as Created_date
       FROM ( SELECT  distinct *
		FROM WKS_CNSC_REGIONAL_SELLOUT_ACTUALS A
		WHERE NOT EXISTS (SELECT distinct 1
						  FROM ITG_CN_SC_RE_MSL_LIST T
						  WHERE A.MNTH_ID = T.FISC_PER 
						   AND A.DISTRIBUTOR_CODE = T.DISTRIBUTOR_CODE 
						   ---AND A.DISTRIBUTOR_NAME = T.DISTRIBUTOR_NAME
						   AND A.STORE_CODE = T.STORE_CODE 
						   AND A.STORE_TYPE = T.STORE_TYPE
						   --AND A.store_Name=T.store_Name
						   AND A.msl_product_CODE = T.PRODUCT_CODE
						   ))ACTUAL
						   

		 
   --LEFT JOIN (SELECT DISTINCT xjp_code, brand_en FROM cn_itg.itg_mds_cn_otc_product_mapping)MDS ON MDS.xjp_code=ACTUAL.MSL_PRODUCT_CODE 
   )Q,
                    
(SELECT DISTINCT "cluster",CTRY_GROUP FROM EDW_COMPANY_DIM WHERE CTRY_GROUP='China Selfcare')COM
					
			--WHERE NOT(SALES_VALUE = 0 AND SALES_QTY = 0 AND  AVG_SALES_QTY=0 AND LM_SALES=0 AND LM_SALES_QTY= 0 AND LM_AVG_SALES_QTY=0 AND
---P3M_SALES=0 AND P3M_QTY=0 AND P3M_AVG_QTY=0 AND F3M_SALES=0 AND F3M_QTY=0 AND F3M_AVG_QTY=0 AND
---P6M_SALES=0 AND P6M_QTY=0 AND P6M_AVG_QTY=0 AND P12M_SALES=0 AND P12M_QTY=0 AND P12M_AVG_QTY=0 AND P12M_SALES_LP=0 AND P6M_SALES_LP=0 AND 
---P3M_SALES_LP=0 AND LM_SALES_LP=0 AND SALES_VALUE_LIST_PRICE=0)
)
),
final as (
select 
fisc_yr::integer AS fisc_yr,
fisc_per::varchar(22) AS fisc_per,
market::varchar(14) AS market,
distributor_code::varchar(100) AS distributor_code,
distributor_name::varchar(356) AS distributor_name,
sell_out_channel::varchar(50) AS sell_out_channel,
sell_out_re::varchar(255) AS sell_out_re,
store_type::varchar(255) AS store_type,
store_code::varchar(100) AS store_code,
store_name::varchar(601) AS store_name,
region::varchar(150) AS region,
zone_name::varchar(150) AS zone_name,
product_code::varchar(150) AS product_code,
product_name::varchar(300) AS product_name,
prod_hier_l1::varchar(14) AS prod_hier_l1,
mapped_sku_cd::varchar(150) AS mapped_sku_cd,
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
sap_banner_format_description::varchar(75) AS sap_banner_format_description,
sap_go_to_mdl_key::varchar(12) AS sap_go_to_mdl_key,
sap_go_to_mdl_description::varchar(75) AS sap_go_to_mdl_description,
global_product_brand::varchar(200) AS global_product_brand,
sku_code::varchar(40) AS sku_code,
sku_description::varchar(150) AS sku_description,
pka_product_key::varchar(68) AS pka_product_key,
pka_product_key_description::varchar(255) AS pka_product_key_description,
sales_value::numeric(38,6) AS sales_value,
sales_qty::numeric(38,6) AS sales_qty,
avg_sales_qty::numeric(38,6) AS avg_sales_qty,
sales_value_list_price::numeric(38,12) AS sales_value_list_price,
lm_sales::numeric(38,6) AS lm_sales,
lm_sales_qty::numeric(38,6) AS lm_sales_qty,
lm_avg_sales_qty::numeric(10,2) AS lm_avg_sales_qty,
lm_sales_lp::numeric(38,12) AS lm_sales_lp,
p3m_sales::numeric(38,6) AS p3m_sales,
p3m_qty::numeric(38,6) AS p3m_qty,
p3m_avg_qty::numeric(10,2) AS p3m_avg_qty,
p3m_sales_lp::numeric(38,12) AS p3m_sales_lp,
f3m_sales::numeric(38,6) AS f3m_sales,
f3m_qty::numeric(38,6) AS f3m_qty,
f3m_avg_qty::numeric(10,2) AS f3m_avg_qty,
p6m_sales::numeric(38,6) AS p6m_sales,
p6m_qty::numeric(38,6) AS p6m_qty,
p6m_avg_qty::numeric(10,2) AS p6m_avg_qty,
p6m_sales_lp::numeric(38,12) AS p6m_sales_lp,
p12m_sales::numeric(38,6) AS p12m_sales,
p12m_qty::numeric(38,6) AS p12m_qty,
p12m_avg_qty::numeric(10,2) AS p12m_avg_qty,
p12m_sales_lp::numeric(38,12) AS p12m_sales_lp,
lm_sales_flag::varchar(1) AS lm_sales_flag,
p3m_sales_flag::varchar(1) AS p3m_sales_flag,
p6m_sales_flag::varchar(1) AS p6m_sales_flag,
p12m_sales_flag::varchar(1) AS p12m_sales_flag,
mdp_flag::varchar(1) AS mdp_flag,
target_complaince::integer AS target_complaince,
"cluster"::varchar(100) AS cluster
from transformation
)



select * from final