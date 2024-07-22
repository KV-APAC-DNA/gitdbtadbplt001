--import cte     
with itg_MY_re_msl_list as (
    select * from {{ ref('myswks_integration__wks_my_re_msl_list') }}
),
wks_my_re_actuals as (
    select * from {{ ref('myswks_integration__wks_my_re_actuals') }}
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
itg_my_material_dim as (
    select * from {{ ref('mysitg_integration__itg_my_material_dim') }}
),
edw_list_price as (
    select * from {{ ref('aspedw_integration__edw_list_price') }}
),


MY_RPT_RE_mdp  as (
SELECT Q.*,
       COM.*
FROM (SELECT DISTINCT TARGET.jj_year,
             TARGET.jj_mnth_id,
             --COM.CLUSTER,			 
             TARGET.market AS MARKET,
             COALESCE(ACTUAL.CHANNEL_NAME,TARGET.Sell_Out_Channel,'NA') as CHANNEL_NAME,
             COALESCE(ACTUAL.SOLDTO_CODE, TARGET.SOLDTO_CODE,'NA') AS SOLDTO_CODE,
             COALESCE(actual.DISTRIBUTOR_CODE,TARGET.DISTRIBUTOR_CODE,'NA') as DISTRIBUTOR_CODE,
             COALESCE(actual.DISTRIBUTOR_NAME,TARGET.DISTRIBUTOR_NAME,'NA') as DISTRIBUTOR_NAME,
             COALESCE(actual.CHANNEL_NAME,TARGET.SELL_OUT_CHANNEL,'NA') as SELL_OUT_CHANNEL,
             COALESCE(actual.store_type,TARGET.STORE_TYPE,'NA') as store_type,
             NULL AS PRIORITIZATION_SEGMENTATION,
             NULL AS STORE_CATEGORY,
             COALESCE(actual.store_code,TARGET.STORE_CODE,'NA') AS STORE_CODE,
             COALESCE(actual.store_name,TARGET.STORE_NAME,'NA') as STORE_NAME,
             TARGET.STORE_GRADE,
             'NA' as STORE_SIZE,
             nvl(actual.REGION,'NA') as REGION,
             nvl(actual.ZONE_OR_AREA,'NA') as ZONE_NAME,
             'NA' as CITY,
             NULL AS RTRLATITUDE,
             NULL AS RTRLONGITUDE,
             COALESCE(actual.RETAIL_ENVIRONMENT,TARGET.retail_environment,'NA') AS Sell_Out_RE,
             nvl(actual.EAN,TARGET.EAN) AS PRODUCT_CODE,
             COALESCE(ACTUAL.MSL_PRODUCT_DESC,target.product_name,'NA') AS PRODUCT_NAME,
             COALESCE(Target.prod_hier_l1,TARGET.PROD_HIER_L1,prodhier.prod_hier_l1,'NA') AS PROD_HIER_L1,
             COALESCE(target.prod_hier_l2,TARGET.PROD_HIER_L2,prodhier.prod_hier_l1,'NA') AS PROD_HIER_L2,
             COALESCE(target.prod_hier_l3,TARGET.PROD_HIER_L3,prodhier.prod_hier_l1,'NA') AS PROD_HIER_L3,
             COALESCE(target.prod_hier_l4,target.PROD_HIER_L4,prodhier.prod_hier_l1,'NA') AS  PROD_HIER_L4,
             NULL AS PROD_HIER_L5,
             NULL AS PROD_HIER_L6,
             NULL AS PROD_HIER_L7,
             NULL AS PROD_HIER_L8,
			 --NULL AS PROD_HIER_L9,
             NULL AS PROD_HIER_L9,
             COALESCE(actual.sku_code,target.sku_code,'NA') AS MAPPED_SKU_CD,
              lp.list_price,
			       case when target.retail_environment = 'GT' then 'SELL-OUT'
                  when target.retail_environment = 'MT' then 'POS' end AS data_src,
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
             COALESCE(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,PRODUCT.GPH_PROD_FRNCHSE,'NA') AS GLOBAL_PRODUCT_FRANCHISE,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_BRAND,PRODUCT.GPH_PROD_BRND,'NA') AS GLOBAL_PRODUCT_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,PRODUCT.GPH_PROD_SUB_BRND,'NA') AS GLOBAL_PRODUCT_SUB_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_VARIANT,PRODUCT.GPH_PROD_VRNT,'NA') AS GLOBAL_PRODUCT_VARIANT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SEGMENT,PRODUCT.GPH_PROD_NEEDSTATE,'NA') AS GLOBAL_PRODUCT_SEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,PRODUCT.GPH_PROD_SUBSGMNT,'NA') AS GLOBAL_PRODUCT_SUBSEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_CATEGORY,PRODUCT.GPH_PROD_CTGRY,'NA') AS GLOBAL_PRODUCT_CATEGORY,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,PRODUCT.GPH_PROD_SUBCTGRY,'NA') AS GLOBAL_PRODUCT_SUBCATEGORY,
             COALESCE(ACTUAL.GLOBAL_PUT_UP_DESCRIPTION,PRODUCT.GPH_PROD_PUT_UP_DESC,'NA') AS GLOBAL_PUT_UP_DESCRIPTION,
             --TRIM(NVL (NULLIF(PRODUCT.EAN,''),'NA')) AS EAN,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA'))AS SKU_CODE,
             --TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA'))AS SKU_DESCRIPTION,
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
             100 AS TARGET_COMPLAINCE
      FROM itg_MY_re_msl_list TARGET
        LEFT JOIN (SELECT * FROM WKS_MY_RE_ACTUALS) ACTUAL
               ON TARGET.jj_mnth_id = ACTUAL.MNTH_ID
              AND TARGET.DISTRIBUTOR_CODE = ACTUAL.DISTRIBUTOR_CODE
              AND TARGET.STORE_CODE = ACTUAL.STORE_CODE
              AND UPPER (TRIM (TARGET.EAN)) = UPPER (TRIM (ACTUAL.EAN))
			  and target.market=actual.CNTRY_NM
			  
	 ----------------customer hierarchy------------------------------
      
        LEFT JOIN (SELECT *
                   FROM customer_heirarchy
                   WHERE RANK = 1) CUSTOMER ON nvl(ltrim(actual.soldto_code),LTRIM (target.soldto_code,'0')) = LTRIM (CUSTOMER.SAP_CUST_ID,'0')
      ----------------product hierarchy------------------------------   	
      
        LEFT JOIN product_heirarchy PRODUCT
              ON nvl(LTRIM (actual.sku_code,'0'),LTRIM (target.sku_code,'0')) = LTRIM (PRODUCT.SAP_MATL_NUM,'0')
              AND PRODUCT.RANK = 1
			  ---------------------------------------------------------------
left join (select ean,prod_hier_l1,prod_hier_l2,prod_hier_l3,prod_hier_l4
			from
				(	select ltrim(item_bar_cd,'0') as ean, 'Malaysia' as prod_hier_l1, null::character varying as prod_hier_l2,
							frnchse_desc AS prod_hier_l3, 
							brnd_desc AS prod_hier_l4,
							row_number() over (partition by ltrim(item_bar_cd,'0') order by frnchse_desc,brnd_desc desc) as rno
					from itg_my_material_dim) where rno = 1 )prodhier on nvl(ltrim(actual.ean,'0'),ltrim(target.ean,'0')) = ltrim(prodhier.ean,'0')	  
           LEFT JOIN (SELECT LTRIM(edw_list_price.material,'0') AS material,
                          edw_list_price.amount AS list_price,
                          row_number() OVER (PARTITION BY LTRIM(edw_list_price.material,'0') ORDER BY TO_DATE(edw_list_price.valid_to,'YYYYMMDD') DESC,TO_DATE(edw_list_price.dt_from,'YYYYMMDD') DESC) AS rn
                   FROM EDW_LIST_PRICE
                   WHERE edw_list_price.sls_org  = '2100') lp
      ---- NEED TO CHECK ON
      
              ON nvl(LTRIM (actual.sku_code,'0'),LTRIM (target.sku_code,'0')) = lp.material
              AND rn = 1 
           ) Q
  JOIN (SELECT DISTINCT "cluster",
               CTRY_GROUP
        FROM EDW_COMPANY_DIM
        WHERE CTRY_GROUP = 'Malaysia') COM ON UPPER (TRIM (Q.market)) = UPPER (TRIM (com.CTRY_GROUP))
WHERE jj_mnth_id::NUMERIC<= (SELECT MAX(mnth_id)::NUMERIC FROM WKS_MY_RE_ACTUALS)
			  
),

MY_RPT_RE_non_mdp  as (
SELECT Q.*,
       COM.*
FROM (SELECT DISTINCT LEFT (ACTUAL.MNTH_ID,4) AS YEAR,
             ACTUAL.MNTH_ID  AS MNTH_ID,
             -- COM.CLUSTER,			 			
             ACTUAL.CNTRY_NM AS MARKET,
             nvl(actual.channel_name,'NA') AS CHANNEL,
             nvl(ACTUAL.soldto_code,'NA') as soldto_code,
             nvl(ACTUAL.DISTRIBUTOR_CODE,'NA') as DISTRIBUTOR_CODE,
             nvl(ACTUAL.DISTRIBUTOR_NAME,'NA') as DISTRIBUTOR_NAME,
             nvl(actual.channel_name,'NA') AS SELL_OUT_CHANNEL,
             nvl(actual.STORE_TYPE,'NA') as STORE_TYPE,
             NULL AS PRIORITIZATION_SEGMENTATION,
             NULL AS STORE_CATEGORY,
             COALESCE(ACTUAL.STORE_CODE,'NA') AS STORE_CODE,
             COALESCE(ACTUAL.STORE_NAME,'NA') AS STORE_NAME,
             NULL AS STORE_GRADE,
             NULL AS STORE_SIZE,
             nvl(actual.region,'NA') as region,
             nvl(actual.zone_or_area,'NA') AS ZONE_NAME,
             'NA' as CITY,
             NULL AS RTRLATITUDE,
             NULL AS RTRLONGITUDE,
             nvl(ACTUAL.retail_environment,'NA') AS sell_out_RE,
             nvl(ACTUAL.EAN,'NA') AS PRODUCT_CODE,
             COALESCE(ACTUAL.MSL_PRODUCT_DESC,'NA') AS PRODUCT_NAME,
             nvl(epd.prod_hier_l1,'NA') as prod_hier_l1,
             nvl(epd.prod_hier_l2,'NA') AS prod_hier_l2,
             nvl(epd.prod_hier_l3,'NA') AS prod_hier_l3,
             nvl(epd.prod_hier_l4,'NA') AS prod_hier_l4,
             NULL as prod_hier_l5,
             NULL AS prod_hier_l6,
             NULL AS prod_hier_l7,
             NULL AS prod_hier_l8,
			 --NULL as  prod_hier_l9,
             NULL as  prod_hier_l9,
             actual.sku_code AS MAPPED_SKU_CD,
			 lp.list_price,
             actual.data_src,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_KEY,CUSTOMER.CUST_SEGMT_KEY,'NA') AS CUSTOMER_SEGMENT_KEY,
             COALESCE(ACTUAL.CUSTOMER_SEGMENT_DESCRIPTION,CUSTOMER.CUST_SEGMENT_DESC,'NA') AS CUSTOMER_SEGMENT_DESCRIPTION,
             COALESCE(ACTUAL.RETAIL_ENVIRONMENT,'NA') AS RETAIL_ENVIRONMENT,
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
             COALESCE(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,PRODUCT.GPH_PROD_FRNCHSE,'NA') AS GLOBAL_PRODUCT_FRANCHISE,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_BRAND,PRODUCT.GPH_PROD_BRND,'NA') AS GLOBAL_PRODUCT_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,PRODUCT.GPH_PROD_SUB_BRND,'NA') AS GLOBAL_PRODUCT_SUB_BRAND,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_VARIANT,PRODUCT.GPH_PROD_VRNT,'NA') AS GLOBAL_PRODUCT_VARIANT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SEGMENT,PRODUCT.GPH_PROD_NEEDSTATE,'NA') AS GLOBAL_PRODUCT_SEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,PRODUCT.GPH_PROD_SUBSGMNT,'NA') AS GLOBAL_PRODUCT_SUBSEGMENT,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_CATEGORY,PRODUCT.GPH_PROD_CTGRY,'NA') AS GLOBAL_PRODUCT_CATEGORY,
             COALESCE(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,PRODUCT.GPH_PROD_SUBCTGRY,'NA') AS GLOBAL_PRODUCT_SUBCATEGORY,
             COALESCE(ACTUAL.GLOBAL_PUT_UP_DESCRIPTION,PRODUCT.GPH_PROD_PUT_UP_DESC,'NA') AS GLOBAL_PUT_UP_DESCRIPTION,
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
             NVL(ACTUAL.LM_SALES_FLAG,'N') as LM_SALES_FLAG ,
             NVL(ACTUAL.P3M_SALES_FLAG,'N') as P3M_SALES_FLAG,
             NVL(ACTUAL.P6M_SALES_FLAG,'N') as P6M_SALES_FLAG,
             NVL(ACTUAL.P12M_SALES_FLAG,'N') as P12M_SALES_FLAG,
             'N' AS MDP_FLAG,
             100 AS TARGET_COMPLAINCE
      FROM (SELECT *
            FROM WKS_MY_RE_ACTUALS A
            WHERE NOT EXISTS (SELECT 1
                              FROM itg_my_re_msl_list T
                              WHERE A.MNTH_ID = T.jj_mnth_id
                               AND A.DISTRIBUTOR_CODE = T.DISTRIBUTOR_CODE
                              and A.CNTRY_NM=T.market 
                              AND   A.STORE_CODE = T.STORE_CODE
                              AND   UPPER(TRIM(A.EAN)) = UPPER(TRIM(T.EAN)))) ACTUAL
left join (select ean,prod_hier_l1,prod_hier_l2,prod_hier_l3,prod_hier_l4
			from
				(	select ltrim(item_bar_cd,'0') as ean, 'Malaysia' as prod_hier_l1, null::character varying as prod_hier_l2,
							frnchse_desc AS prod_hier_l3, 
							brnd_desc AS prod_hier_l4,
							row_number() over (partition by ltrim(item_bar_cd,'0') order by frnchse_desc,brnd_desc desc) as rno
					from itg_my_material_dim) where rno = 1 )epd on ltrim(ACTUAL.ean,'0') = ltrim(epd.ean,'0')
			  
	 ----------------customer hierarchy------------------------------
      
        LEFT JOIN (SELECT *
                   FROM customer_heirarchy
                   WHERE RANK = 1) CUSTOMER ON LTRIM (ACTUAL.soldto_code,'0') = LTRIM (CUSTOMER.SAP_CUST_ID,'0')
      ----------------product hierarchy------------------------------   	
      
        LEFT JOIN product_heirarchy PRODUCT
              ON LTRIM (actual.sku_code,'0') = LTRIM (PRODUCT.SAP_MATL_NUM,'0')
              AND PRODUCT.RANK = 1
			  ---------------------------------------------------------------
   LEFT JOIN (SELECT LTRIM(edw_list_price.material,'0') AS material,
                          edw_list_price.amount AS list_price,
                          row_number() OVER (PARTITION BY LTRIM(edw_list_price.material,'0') ORDER BY TO_DATE(edw_list_price.valid_to,'YYYYMMDD') DESC,TO_DATE(edw_list_price.dt_from,'YYYYMMDD') DESC) AS rn
                   FROM EDW_LIST_PRICE
                   WHERE edw_list_price.sls_org  = '2100') lp
      ---- NEED TO CHECK ON
      
              ON LTRIM (actual.sku_code,'0') = lp.material
              AND rn = 1 ) Q
			  JOIN (SELECT DISTINCT "cluster",
               CTRY_GROUP
        FROM EDW_COMPANY_DIM
        WHERE CTRY_GROUP in ('Malaysia')) COM ON UPPER (TRIM (Q.market)) = UPPER (TRIM (com.CTRY_GROUP))			  
),

MY_RPT_RE as
(
select * from MY_RPT_RE_mdp
union 
select * from MY_RPT_RE_non_mdp
),

final as
(
   select 

jj_year::numeric(18,0) AS jj_year,
jj_mnth_id::varchar(22) AS jj_mnth_id,
market::varchar(50) AS market,
channel_name::varchar(50) AS channel_name,
soldto_code::varchar(255) AS soldto_code,
distributor_code::varchar(100) AS distributor_code,
distributor_name::varchar(356) AS distributor_name,
sell_out_channel::varchar(50) AS sell_out_channel,
store_type::varchar(255) AS store_type,
prioritization_segmentation::varchar(2) as prioritization_segmentation,
store_category::varchar(2) as store_category, 
store_code::varchar(100) AS store_code,
store_name::varchar(601) AS store_name,
store_grade::varchar(20) AS store_grade,
store_size::varchar(2) AS store_size,
region::varchar(150) AS region,
zone_name::varchar(150) AS zone_name,
city::varchar(11) AS city,
rtrlatitude::varchar(2) AS rtrlatitude,
rtrlongitude::varchar(2) AS rtrlongitude,
sell_out_re::varchar(75) AS sell_out_re,
product_code::varchar(150) AS product_code,
product_name::varchar(150) AS product_name,
prod_hier_l1::varchar(9) AS prod_hier_l1,
prod_hier_l2::varchar(200) AS prod_hier_l2,
prod_hier_l3::varchar(200) AS prod_hier_l3,
prod_hier_l4::varchar(200) AS prod_hier_l4,
prod_hier_l5::varchar(200) AS prod_hier_l5,
prod_hier_l6::varchar(200) AS prod_hier_l6,
prod_hier_l7::varchar(200) AS prod_hier_l7,
prod_hier_l8::varchar(200) AS prod_hier_l8,
prod_hier_l9::varchar(1) AS prod_hier_l9,
mapped_sku_cd::varchar(40) AS mapped_sku_cd,
list_price::numeric(20,4) as list_price,
data_src::varchar(9) AS data_src,
customer_segment_key::varchar(12) AS customer_segment_key,
customer_segment_description::varchar(50) AS customer_segment_description,
retail_environment::varchar(255) AS retail_environment,
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
"cluster"::varchar(100) AS "cluster" 

from MY_RPT_RE
)
--Final select
select * from final