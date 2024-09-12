--import cte
with edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
wks_anz_pos_re_act_lm as (
    select * from {{ ref('pcfwks_integration__wks_anz_pos_re_act_lm') }}
),
wks_anz_pos_re_act_l3m as (
    select * from {{ ref('pcfwks_integration__wks_anz_pos_re_act_l3m') }}
),

wks_anz_pos_re_act_l6m as (
    select * from {{ ref('pcfwks_integration__wks_anz_pos_re_act_l6m') }}
),

wks_anz_pos_re_act_l12m as (
    select * from {{ ref('pcfwks_integration__wks_anz_pos_re_act_l12m') }}
),

wks_anz_pos_base_retail_excellence as (
    select * from {{ ref('pcfwks_integration__wks_anz_pos_base_retail_excellence') }}
),

--final cte
anz_pos_re_actuals  as (
SELECT  RE_BASE_DIM.CNTRY_CD,RE_BASE_DIM.sellout_dim_key,
       RE_BASE_DIM.CNTRY_NM,
       RE_BASE_DIM.data_src,
       SUBSTRING(BASE_DIM.MONTH,1,4) AS YEAR,
       BASE_DIM.MONTH AS MNTH_ID,
       RE_BASE_DIM.DISTRIBUTOR_CODE,
       RE_BASE_DIM.DISTRIBUTOR_NAME,
       RE_BASE_DIM.EAN,
       RETAIL_ENVIRONMENT,
	   store_grade,list_price,sku_code,
	   Region,zone_or_area,
       CHANNEL_NAME,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       CM.so_sls_qty AS CM_SALES_QTY,
       CM.so_sls_value AS CM_SALES,
       CM.so_avg_qty AS CM_AVG_SALES_QTY,
       CM.SALES_VALUE_LIST_PRICE AS SALES_VALUE_LIST_PRICE,
       LM_SALES AS LM_SALES,
       LM_SALES_QTY AS LM_SALES_QTY,
       LM_AVG_SALES_QTY AS LM_AVG_SALES_QTY,
       LM_SALES_LP AS LM_SALES_LP,
       L3M_SALES AS P3M_SALES,
       L3M_SALES_QTY AS P3M_QTY,
       L3M_AVG_SALES_QTY AS P3M_AVG_QTY,
       L3M_SALES_LP AS P3M_SALES_LP,
       null AS F3M_SALES,
       null AS F3M_QTY,
       null AS F3M_AVG_QTY,
       L6M_SALES AS P6M_SALES,
       L6M_SALES_QTY AS P6M_QTY,
       L6M_AVG_SALES_QTY AS P6M_AVG_QTY,
       L6M_SALES_LP AS P6M_SALES_LP,
       L12M_SALES AS P12M_SALES,
       L12M_SALES_QTY AS P12M_QTY,
       L12M_AVG_SALES_QTY AS P12M_AVG_QTY,
       L12M_SALES_LP AS P12M_SALES_LP,
       CASE
         WHEN LM_actual_stores > 0 THEN 'Y'
         ELSE 'N'
       END AS LM_SALES_FLAG,
       CASE
         WHEN L3M_actual_stores > 0 THEN 'Y'
         ELSE 'N'
       END AS P3M_SALES_FLAG,
       CASE
         WHEN L6M_actual_stores > 0 THEN 'Y'
         ELSE 'N'
       END AS P6M_SALES_FLAG,
       CASE
         WHEN L12M_actual_stores > 0 THEN 'Y'
         ELSE 'N'
       END AS P12M_SALES_FLAG,
       SYSDATE() AS CRT_DTTM ,
	   CM.CM_actual_stores,CM.CM_universe_stores,CM.CM_numeric_distribution,
	   LM_actual_stores,LM_universe_stores,LM_numeric_distribution,
	   L3M_actual_stores,L3M_universe_stores,L3M_numeric_distribution,
	   L6M_actual_stores,L6M_universe_stores,L6M_numeric_distribution,
	   L12M_actual_stores,L12M_universe_stores,L12M_numeric_distribution 
FROM (SELECT DISTINCT CNTRY_CD,
             sellout_dim_key,
             MONTH
      FROM (SELECT CNTRY_CD,
                   sellout_dim_key,
                   MONTH
            FROM wks_anz_pos_re_act_lm
            WHERE LM_actual_stores IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   MONTH
            FROM wks_anz_pos_re_act_l3m
            WHERE L3M_actual_stores IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   MONTH
            FROM wks_anz_pos_re_act_l6m
            WHERE L6M_actual_stores IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   MONTH
            FROM wks_anz_pos_re_act_l12m
            WHERE L12M_actual_stores IS NOT NULL)) BASE_DIM
  LEFT JOIN (SELECT DISTINCT CNTRY_CD,
                    cntry_nm,
                    data_src,
                    sellout_dim_key,
                    DISTRIBUTOR_CODE,
                    DISTRIBUTOR_NAME,
                    EAN,
                    RETAIL_ENVIRONMENT,
					store_grade,list_price,sku_code,
					Region,zone_or_area,
                    CHANNEL_NAME,
                    GLOBAL_PRODUCT_FRANCHISE,
                    GLOBAL_PRODUCT_BRAND,
                    GLOBAL_PRODUCT_SUB_BRAND,
                    GLOBAL_PRODUCT_SEGMENT,
                    GLOBAL_PRODUCT_SUBSEGMENT,
                    GLOBAL_PRODUCT_CATEGORY,
                    GLOBAL_PRODUCT_SUBCATEGORY
					--,row_number() over (partition by sellout_dim_key order by mnth_id desc,length(msl_product_desc) desc) as rno
                                           
             FROM wks_anz_pos_base_retail_excellence where  MNTH_ID >= (select last_28mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
	  and mnth_id <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)::numeric) RE_BASE_DIM
         ON RE_BASE_DIM.cntry_cd = BASE_DIM.cntry_cd
        AND RE_BASE_DIM.sellout_dim_key = BASE_DIM.sellout_dim_key --and rno=1 
  LEFT OUTER JOIN (SELECT DISTINCT CNTRY_CD,
                          sellout_dim_key,
                          mnth_id,
                          store_count_where_scanned::NUMERIC(38,6) as CM_actual_stores,
                          universe_stores::NUMERIC(38,6) as CM_universe_stores,
						  numeric_distribution::NUMERIC(38,6) as CM_numeric_distribution,
						  so_sls_qty,
                          so_sls_value,
                          so_avg_qty,
                          SALES_VALUE_LIST_PRICE
                   FROM wks_anz_pos_base_retail_excellence where  MNTH_ID >= (select last_28mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
	  and mnth_id <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)::numeric) CM
               ON BASE_DIM.CNTRY_CD = CM.CNTRY_CD
              AND BASE_DIM.Month = CM.mnth_id
              AND BASE_DIM.sellout_dim_key = CM.sellout_dim_key
  LEFT OUTER JOIN
--Last Month
wks_anz_pos_re_act_lm LM
               ON BASE_DIM.CNTRY_CD = LM.CNTRY_CD
              AND BASE_DIM.month = LM.MONTH
              AND BASE_DIM.sellout_dim_key = LM.sellout_dim_key
  LEFT OUTER JOIN
--L3M
wks_anz_pos_re_act_l3m L3M
               ON BASE_DIM.CNTRY_CD = L3M.CNTRY_CD
              AND BASE_DIM.month = L3M.MONTH
              AND BASE_DIM.sellout_dim_key = L3M.sellout_dim_key
  LEFT OUTER JOIN
--L6M
wks_anz_pos_re_act_l6m L6M
               ON BASE_DIM.CNTRY_CD = L6M.CNTRY_CD
              AND BASE_DIM.month = L6M.MONTH
              AND BASE_DIM.sellout_dim_key = L6M.sellout_dim_key
  LEFT OUTER JOIN
--L12M
wks_anz_pos_re_act_l12m L12M
               ON BASE_DIM.CNTRY_CD = L12M.CNTRY_CD
              AND BASE_DIM.month = L12M.MONTH
              AND BASE_DIM.sellout_dim_key = L12M.sellout_dim_key
			  
where BASE_DIM.month >= (select last_17mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
  and BASE_DIM.month <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)::numeric		
),
final as(
select 
cntry_cd::VARCHAR(2) AS cntry_cd,
sellout_dim_key::VARCHAR(32) AS sellout_dim_key,
cntry_nm::VARCHAR(50) AS cntry_nm,
data_src::VARCHAR(14) AS data_src,
year::VARCHAR(16) AS year,
mnth_id::VARCHAR(23) AS mnth_id,
distributor_code::VARCHAR(100) AS distributor_code,
distributor_name::VARCHAR(255) AS distributor_name,
ean::VARCHAR(150) AS ean,
retail_environment::VARCHAR(225) AS retail_environment,
store_grade::VARCHAR(150) AS store_grade,
list_price::NUMERIC(38,6) AS list_price,
sku_code::VARCHAR(40) AS sku_code,
region::VARCHAR(150) AS region,
zone_or_area::VARCHAR(150) AS zone_or_area,
channel_name::VARCHAR(150) AS channel_name,
global_product_franchise::VARCHAR(30) AS global_product_franchise,
global_product_brand::VARCHAR(30) AS global_product_brand,
global_product_sub_brand::VARCHAR(100) AS global_product_sub_brand,
global_product_segment::VARCHAR(50) AS global_product_segment,
global_product_subsegment::VARCHAR(100) AS global_product_subsegment,
global_product_category::VARCHAR(50) AS global_product_category,
global_product_subcategory::VARCHAR(50) AS global_product_subcategory,
cm_sales_qty::NUMERIC(38,6) AS cm_sales_qty,
cm_sales::NUMERIC(38,6) AS cm_sales,
cm_avg_sales_qty::NUMERIC(38,6) AS cm_avg_sales_qty,
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
crt_dttm::TIMESTAMP AS crt_dttm,
cm_actual_stores::NUMERIC(38,6) AS cm_actual_stores,
cm_universe_stores::NUMERIC(38,6) AS cm_universe_stores,
cm_numeric_distribution::NUMERIC(38,6) AS cm_numeric_distribution,
lm_actual_stores::NUMERIC(38,6) AS lm_actual_stores,
lm_universe_stores::NUMERIC(38,14) AS lm_universe_stores,
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
  from anz_pos_re_actuals
)
--final select
select * from final