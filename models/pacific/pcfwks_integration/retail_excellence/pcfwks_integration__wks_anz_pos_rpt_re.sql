with itg_anz_pos_re_msl_list as (
    select * from {{ ref('pcfitg_integration__itg_anz_pos_re_msl_list') }}
),
wks_anz_pos_re_actuals as (
    select * from {{ ref('pcfwks_integration__wks_anz_pos_re_actuals') }}
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
wks_anz_pos_c360_mapped_sku_cd as(
    select * from {{ ref('pcfwks_integration__wks_anz_pos_c360_mapped_sku_cd') }}
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

anz_rpt_pos_retail_excellence_mdp  as (
SELECT distinct Q.*,
       COM."cluster",
       SYSDATE() as crt_dttm
FROM (SELECT TARGET.jj_year,
             TARGET.jj_mnth_id,
             --COM.CLUSTER,			 
             TARGET.market AS MARKET,
             COALESCE(ACTUAL.CHANNEL_NAME,TARGET.Sell_Out_Channel,'NA') as CHANNEL_NAME,
             COALESCE(actual.DISTRIBUTOR_CODE,TARGET.DISTRIBUTOR_CODE,'NA') as DISTRIBUTOR_CODE,
             COALESCE(actual.DISTRIBUTOR_NAME,TARGET.DISTRIBUTOR_NAME,'NA') as DISTRIBUTOR_NAME,
             COALESCE(actual.CHANNEL_NAME,TARGET.SELL_OUT_CHANNEL,'NA') as SELL_OUT_CHANNEL,
             TARGET.STORE_GRADE,
             nvl(actual.REGION,TARGET.REGION) as REGION,
             nvl(actual.ZONE_OR_AREA,TARGET.zone_or_area) as ZONE_NAME,
             'NA' as CITY,
             COALESCE(actual.RETAIL_ENVIRONMENT,TARGET.retail_environment,'NA') AS Sell_Out_RE,
             COALESCE(actual.EAN,TARGET.EAN,'NA') AS PRODUCT_CODE,
            
             NULL AS PROD_HIER_L1,
             NULL AS PROD_HIER_L2,
             TARGET.PROD_HIER_L3 AS PROD_HIER_L3,
             target.PROD_HIER_L4 AS  PROD_HIER_L4,
             TARGET.PROD_HIER_L5 AS PROD_HIER_L5,
             NULL AS PROD_HIER_L6,
             NULL AS PROD_HIER_L7,
             NULL AS PROD_HIER_L8,
			 TARGET.PROD_HIER_L9 AS PROD_HIER_L9,
             target.MAPPED_SKU_CD AS MAPPED_SKU_CD,
			 target.list_price,
             data_src,
			 COALESCE(ACTUAL.RETAIL_ENVIRONMENT,target.retail_environment,'NA') AS RETAIL_ENVIRONMENT,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,'NA'),PRODUCT.GPH_PROD_FRNCHSE,'NA') AS GLOBAL_PRODUCT_FRANCHISE,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_BRAND,'NA'),PRODUCT.GPH_PROD_BRND,'NA') AS GLOBAL_PRODUCT_BRAND,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,'NA'),PRODUCT.GPH_PROD_SUB_BRND,'NA') AS GLOBAL_PRODUCT_SUB_BRAND,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SEGMENT,'NA'),PRODUCT.GPH_PROD_NEEDSTATE,'NA') AS GLOBAL_PRODUCT_SEGMENT,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,'NA'),PRODUCT.GPH_PROD_SUBSGMNT,'NA') AS GLOBAL_PRODUCT_SUBSEGMENT,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_CATEGORY,'NA'),PRODUCT.GPH_PROD_CTGRY,'NA') AS GLOBAL_PRODUCT_CATEGORY,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,'NA'),PRODUCT.GPH_PROD_SUBCTGRY,'NA') AS GLOBAL_PRODUCT_SUBCATEGORY,
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
             1 AS TARGET_COMPLAINCE,
			 CM_actual_stores,CM_universe_stores,CM_numeric_distribution,
			 LM_actual_stores,LM_universe_stores,LM_numeric_distribution,
             L3M_actual_stores,L3M_universe_stores,L3M_numeric_distribution,
             L6M_actual_stores,L6M_universe_stores,L6M_numeric_distribution,
             L12M_actual_stores,L12M_universe_stores,L12M_numeric_distribution 
      FROM itg_anz_pos_re_msl_list TARGET
        LEFT JOIN (SELECT * FROM wks_anz_pos_re_actuals) ACTUAL
               ON TARGET.jj_mnth_id = ACTUAL.MNTH_ID
              AND TARGET.DISTRIBUTOR_NAME = ACTUAL.DISTRIBUTOR_NAME
              --AND TARGET.STORE_CODE = ACTUAL.STORE_CODE
              AND UPPER (TRIM (TARGET.EAN)) = UPPER (TRIM (ACTUAL.EAN))
			  and target.store_grade=actual.store_grade 
			  and target.market=actual.CNTRY_NM
     
            ----------------product hierarchy------------------------------   				   
      
        LEFT JOIN product_heirarchy PRODUCT
                ON LTRIM (target.MAPPED_SKU_CD,'0') = LTRIM (PRODUCT.SAP_MATL_NUM,'0')
              AND PRODUCT.RANK = 1) Q
  JOIN (SELECT DISTINCT "cluster",
               CTRY_GROUP
        FROM EDW_COMPANY_DIM
        WHERE CTRY_GROUP in ('Australia','New Zealand')) COM ON UPPER (TRIM (Q.market)) = UPPER (TRIM (com.CTRY_GROUP))
WHERE jj_mnth_id::NUMERIC<= (SELECT MAX(mnth_id)::NUMERIC FROM wks_anz_pos_re_actuals)
),
anz_rpt_pos_retail_excellence_non_mdp as
(SELECT distinct Q.*,
       COM."cluster",
       SYSDATE() as crt_dttm
FROM (SELECT LEFT (ACTUAL.MNTH_ID,4) AS YEAR,
             CAST(ACTUAL.MNTH_ID AS INTEGER) AS MNTH_ID,
             -- COM.CLUSTER,			 			
             ACTUAL.CNTRY_NM AS MARKET,
             nvl(actual.channel_name,'NA') AS CHANNEL,
             nvl(ACTUAL.DISTRIBUTOR_CODE,'NA') as DISTRIBUTOR_CODE,
             nvl(ACTUAL.DISTRIBUTOR_NAME,'NA') as DISTRIBUTOR_NAME,
             nvl(actual.channel_name,'NA') AS SELL_OUT_CHANNEL,
             nvl(STORE_GRADE,'NA') as STORE_GRADE,
             nvl(actual.region,'NA') as region,
             nvl(actual.zone_or_area,'NA') AS ZONE_NAME,
             'NA' as CITY,
             nvl(ACTUAL.retail_environment,'NA') AS sell_out_RE,
             ACTUAL.EAN AS PRODUCT_CODE,
            
             NULL  AS prod_hier_l1,
             NULL AS prod_hier_l2,
             epd.prod_hier_l3 AS prod_hier_l3,
             epd.prod_hier_l4 AS prod_hier_l4,
             epd.prod_hier_l5 as prod_hier_l5,
             NULL AS prod_hier_l6,
             NULL AS prod_hier_l7,
             NULL AS prod_hier_l8,
			 epd.prod_hier_l9 as  prod_hier_l9,
             coalesce(nullif(LTRIM (sku.sku_code,'0'),'NA'),ltrim(MAT.MAPPED_SKU_CD,0)) AS MAPPED_SKU_CD,
			 actual.list_price,
             actual.data_src,
             COALESCE(ACTUAL.RETAIL_ENVIRONMENT,'NA') AS RETAIL_ENVIRONMENT,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_FRANCHISE,'NA'),PRODUCT.GPH_PROD_FRNCHSE,'NA') AS GLOBAL_PRODUCT_FRANCHISE,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_BRAND,'NA'),PRODUCT.GPH_PROD_BRND,'NA') AS GLOBAL_PRODUCT_BRAND,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SUB_BRAND,'NA'),PRODUCT.GPH_PROD_SUB_BRND,'NA') AS GLOBAL_PRODUCT_SUB_BRAND,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SEGMENT,'NA'),PRODUCT.GPH_PROD_NEEDSTATE,'NA') AS GLOBAL_PRODUCT_SEGMENT,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SUBSEGMENT,'NA'),PRODUCT.GPH_PROD_SUBSGMNT,'NA') AS GLOBAL_PRODUCT_SUBSEGMENT,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_CATEGORY,'NA'),PRODUCT.GPH_PROD_CTGRY,'NA') AS GLOBAL_PRODUCT_CATEGORY,
             COALESCE(nullif(ACTUAL.GLOBAL_PRODUCT_SUBCATEGORY,'NA'),PRODUCT.GPH_PROD_SUBCTGRY,'NA') AS GLOBAL_PRODUCT_SUBCATEGORY,
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
             1 AS TARGET_COMPLAINCE,
			  CM_actual_stores,CM_universe_stores,CM_numeric_distribution,
			  LM_actual_stores,LM_universe_stores,LM_numeric_distribution,
              L3M_actual_stores,L3M_universe_stores,L3M_numeric_distribution,
              L6M_actual_stores,L6M_universe_stores,L6M_numeric_distribution,
              L12M_actual_stores,L12M_universe_stores,L12M_numeric_distribution 
      FROM (SELECT *
            FROM WKS_AnZ_POS_RE_ACTUALS A
            WHERE NOT EXISTS (SELECT 1
                              FROM itg_anz_pos_re_msl_list T
                              WHERE A.MNTH_ID = T.jj_mnth_id
                              AND   A.DISTRIBUTOR_NAME = T.DISTRIBUTOR_NAME
                              and A.CNTRY_NM=T.market 
                              --AND   A.STORE_CODE = T.STORE_CODE
                              AND   UPPER(TRIM(A.EAN)) = UPPER(TRIM(T.EAN))
							  and T.store_grade = A.store_grade 
							  )) ACTUAL
left join (select distinct EAN,sku_code,COUNTRY_CODE from wks_anz_pos_c360_mapped_sku_cd) sku on UPPER(LTRIM(actual.EAN,0)) = UPPER(LTRIM(sku.EAN,0)) and  sku.COUNTRY_CODE = actual.CNTRY_CD
LEFT JOIN (SELECT EAN_NUM,MAPPED_SKU_CD,ctry_key FROM (SELECT DISTINCT EAN_NUM, LTRIM(MATL_NUM,'0') AS MAPPED_SKU_CD,ctry_key,
                                ROW_NUMBER() OVER (PARTITION BY EAN_NUM,ctry_key ORDER BY CRT_DTTM DESC) AS RN
                         FROM EDW_MATERIAL_SALES_DIM A join (select distinct sls_org,ctry_key from edw_sales_org_dim 
where ctry_key in ('AU','NZ')) B on A.sls_org=B.sls_org )                       
                   WHERE RN = 1) MAT ON UPPER(LTRIM(actual.EAN,0)) = UPPER(LTRIM(MAT.EAN_NUM,0)) and MAT.ctry_key = actual.CNTRY_CD
left join (
select distinct LTRIM(product_client_code,0) AS ean_number,upper(category_name)  AS prod_hier_l3,
      upper(subcategory_local_name) AS prod_hier_l4,upper(brand_name) AS prod_hier_l5
      ,LTRIM(product_client_code,0)||' - ' || product_local_name AS prod_hier_l9,ROW_NUMBER() OVER (PARTITION BY ltrim(product_client_code,0) ORDER BY (LTRIM(product_client_code,0)||' - ' || product_local_name) DESC) AS rno
	  from  itg_trax_md_product 
      WHERE itg_trax_md_product.businessunitid::text = 'PC'::text 
AND itg_trax_md_product.manufacturer_name::text = 'JOHNSON & JOHNSON'::text)epd
 on UPPER (TRIM (actual.EAN)) = UPPER (TRIM (epd.ean_number)) and rno=1 

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
anz_rpt_pos_retail_excellence as
(
select * from anz_rpt_pos_retail_excellence_mdp
union 
select * from anz_rpt_pos_retail_excellence_non_mdp
),

final as 
(
select 
jj_year::VARCHAR(16) AS jj_year,
jj_mnth_id::VARCHAR(22) AS jj_mnth_id,
market::VARCHAR(50) AS market,
channel_name::VARCHAR(150) AS channel_name,
distributor_code::VARCHAR(100) AS distributor_code,
distributor_name::VARCHAR(255) AS distributor_name,
sell_out_channel::VARCHAR(150) AS sell_out_channel,
store_grade::VARCHAR(20) AS store_grade,
region::VARCHAR(150) AS region,
zone_name::VARCHAR(150) AS zone_name,
city::VARCHAR(2) AS city,
sell_out_re::VARCHAR(225) AS sell_out_re,
product_code::VARCHAR(150) AS product_code,
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
data_src::VARCHAR(14) AS data_src,
retail_environment::VARCHAR(225) AS retail_environment,
global_product_franchise::VARCHAR(30) AS global_product_franchise,
global_product_brand::VARCHAR(30) AS global_product_brand,
global_product_sub_brand::VARCHAR(100) AS global_product_sub_brand,
global_product_segment::VARCHAR(50) AS global_product_segment,
global_product_subsegment::VARCHAR(100) AS global_product_subsegment,
global_product_category::VARCHAR(50) AS global_product_category,
global_product_subcategory::VARCHAR(50) AS global_product_subcategory,
sales_value::NUMERIC(38,6) AS sales_value,
sales_qty::NUMERIC(38,6) AS sales_qty,
avg_sales_qty::NUMERIC(38,6) AS avg_sales_qty,
sales_value_list_price::NUMERIC(38,12) AS sales_value_list_price,
lm_sales::NUMERIC(38,6) AS lm_sales,
lm_sales_qty::NUMERIC(38,6) AS lm_sales_qty,
lm_avg_sales_qty::NUMERIC(10,2) AS lm_avg_sales_qty,
lm_sales_lp::NUMERIC(38,12) AS lm_sales_lp,
p3m_sales::NUMERIC(38,6) AS p3m_sales,
p3m_qty::NUMERIC(38,6) AS p3m_qty,
p3m_avg_qty::NUMERIC(10,2) AS p3m_avg_qty,
p3m_sales_lp::NUMERIC(38,12) AS p3m_sales_lp,
f3m_sales::VARCHAR(1) AS f3m_sales,
f3m_qty::VARCHAR(1) AS f3m_qty,
f3m_avg_qty::VARCHAR(1) AS f3m_avg_qty,
p6m_sales::NUMERIC(38,6) AS p6m_sales,
p6m_qty::NUMERIC(38,6) AS p6m_qty,
p6m_avg_qty::NUMERIC(10,2) AS p6m_avg_qty,
p6m_sales_lp::NUMERIC(38,12) AS p6m_sales_lp,
p12m_sales::NUMERIC(38,6) AS p12m_sales,
p12m_qty::NUMERIC(38,6) AS p12m_qty,
p12m_avg_qty::NUMERIC(10,2) AS p12m_avg_qty,
p12m_sales_lp::NUMERIC(38,12) AS p12m_sales_lp,
lm_sales_flag::VARCHAR(1) AS lm_sales_flag,
p3m_sales_flag::VARCHAR(1) AS p3m_sales_flag,
p6m_sales_flag::VARCHAR(1) AS p6m_sales_flag,
p12m_sales_flag::VARCHAR(1) AS p12m_sales_flag,
mdp_flag::VARCHAR(1) AS mdp_flag,
target_complaince::numeric(18,0) AS target_complaince,
cm_actual_stores::NUMERIC(38,6) AS cm_actual_stores,
cm_universe_stores::NUMERIC(38,6) AS cm_universe_stores,
cm_numeric_distribution::NUMERIC(38,6) AS cm_numeric_distribution,
lm_actual_stores::NUMERIC(38,6) AS lm_actual_stores,
lm_universe_stores::NUMERIC(38,14)lm_universe_stores,
lm_numeric_distribution::NUMERIC(20,4) AS lm_numeric_distribution,
l3m_actual_stores::NUMERIC(38,6) AS l3m_actual_stores,
l3m_universe_stores::NUMERIC(38,14) AS l3m_universe_stores,
l3m_numeric_distribution::NUMERIC(20,4) AS l3m_numeric_distribution,
l6m_actual_stores::NUMERIC(38,6) AS l6m_actual_stores,
l6m_universe_stores::NUMERIC(38,14) AS l6m_universe_stores,
l6m_numeric_distribution::NUMERIC(20,4) AS l6m_numeric_distribution,
l12m_actual_stores::NUMERIC(38,6) AS l12m_actual_stores,
l12m_universe_stores::NUMERIC(38,14) AS l12m_universe_stores,
l12m_numeric_distribution::NUMERIC(20,4) AS l12m_numeric_distribution,
"cluster"::VARCHAR(100) AS "cluster",
crt_dttm::timestamp without time zone AS crt_dttm
from anz_rpt_pos_retail_excellence
)
--Final select
select * from final