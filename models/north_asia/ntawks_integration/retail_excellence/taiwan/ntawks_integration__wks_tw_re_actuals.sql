{{ 
    config(
    sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )}}
--import cte     
with wks_tw_base_re as (
    select * from {{ ref('ntawks_integration__wks_tw_base_re') }}
),
wks_tw_re_act_lm as (
    select * from {{ ref('ntawks_integration__wks_tw_re_act_lm') }}
),
wks_tw_re_act_l3m as (
    select * from {{ ref('ntawks_integration__wks_tw_re_act_l3m') }}
),
wks_tw_re_act_l6m as (
    select * from {{ ref('ntawks_integration__wks_tw_re_act_l6m') }}
),
wks_tw_re_act_l12m as (
    select * from {{ ref('ntawks_integration__wks_tw_re_act_l12m') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
TW_RE_ACTUALS  as (	

SELECT RE_BASE_DIM.CNTRY_CD,
       RE_BASE_DIM.CNTRY_NM,
       RE_BASE_DIM.data_src,
       SUBSTRING(BASE_DIM.MONTH,1,4) AS YEAR,
       BASE_DIM.MONTH AS MNTH_ID,
       RE_BASE_DIM.SOLDTO_CODE,
       RE_BASE_DIM.DISTRIBUTOR_CODE,
       RE_BASE_DIM.DISTRIBUTOR_NAME,
       RE_BASE_DIM.STORE_CODE,
       RE_BASE_DIM.EAN,
       RE_BASE_DIM.STORE_NAME,
       RE_BASE_DIM.RETAIL_ENVIRONMENT,
	   RE_BASE_DIM.store_type,
       RE_BASE_DIM.store_grade,
       RE_BASE_DIM.list_price,
       RE_BASE_DIM.msl_product_desc,
       RE_BASE_DIM.sku_code,
	   RE_BASE_DIM.Region,
       RE_BASE_DIM.zone_or_area,
       CHANNEL_NAME,
       SAP_PARENT_CUSTOMER_KEY,
       SAP_PARENT_CUSTOMER_DESCRIPTION,
       SAP_CUSTOMER_CHANNEL_KEY,
       SAP_CUSTOMER_CHANNEL_DESCRIPTION,
       SAP_CUSTOMER_SUB_CHANNEL_KEY,
       SAP_SUB_CHANNEL_DESCRIPTION,
       SAP_GO_TO_MDL_KEY,
       SAP_GO_TO_MDL_DESCRIPTION,
       SAP_BANNER_KEY,
       SAP_BANNER_DESCRIPTION,
       SAP_BANNER_FORMAT_KEY,
       SAP_BANNER_FORMAT_DESCRIPTION,
       CUSTOMER_SEGMENT_KEY,
       CUSTOMER_SEGMENT_DESCRIPTION,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_VARIANT,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       GLOBAL_PUT_UP_DESCRIPTION,
       PKA_PRODUCT_KEY,
       PKA_PRODUCT_KEY_DESCRIPTION,
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
       F3M_SALES AS F3M_SALES,
       F3M_SALES_QTY AS F3M_QTY,
       F3M_AVG_SALES_QTY AS F3M_AVG_QTY,
       L6M_SALES AS P6M_SALES,
       L6M_SALES_QTY AS P6M_QTY,
       L6M_AVG_SALES_QTY AS P6M_AVG_QTY,
       L6M_SALES_LP AS P6M_SALES_LP,
       L12M_SALES AS P12M_SALES,
       L12M_SALES_QTY AS P12M_QTY,
       L12M_AVG_SALES_QTY AS P12M_AVG_QTY,
       L12M_SALES_LP AS P12M_SALES_LP,
       CASE
         WHEN LM_SALES > 0 THEN 'Y'
         ELSE 'N'
       END AS LM_SALES_FLAG,
       CASE
         WHEN P3M_SALES > 0 THEN 'Y'
         ELSE 'N'
       END AS P3M_SALES_FLAG,
       CASE
         WHEN P6M_SALES > 0 THEN 'Y'
         ELSE 'N'
       END AS P6M_SALES_FLAG,
       CASE
         WHEN P12M_SALES > 0 THEN 'Y'
         ELSE 'N'
       END AS P12M_SALES_FLAG,
       SYSDATE() AS CRT_DTTM
FROM (SELECT DISTINCT CNTRY_CD,
             sellout_dim_key,
             data_src,
             MONTH
      FROM (SELECT CNTRY_CD,
                   sellout_dim_key,
                   data_src,
                   MONTH
            FROM WKS_TW_RE_ACT_LM
            WHERE LM_SALES IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   data_src,
                   MONTH
            FROM WKS_TW_RE_ACT_L3M
            WHERE L3M_SALES IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   data_src,
                   MONTH
            FROM WKS_TW_RE_ACT_L6M
            WHERE L6M_SALES IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   data_src,
                   MONTH
            FROM WKS_TW_RE_ACT_L12M
            WHERE L12M_SALES IS NOT NULL)) BASE_DIM
  LEFT JOIN (SELECT DISTINCT CNTRY_CD,
                    cntry_nm,
                    data_src,
                    sellout_dim_key,
                    DISTRIBUTOR_CODE,
                    DISTRIBUTOR_NAME,
                    SOLDTO_CODE,
                    store_code,
                    EAN,
                    STORE_NAME,
                    RETAIL_ENVIRONMENT,
					store_type,
                    store_grade,
                    list_price,
                    msl_product_desc,
                    sku_code,
					Region,
                    zone_or_area,
                    CHANNEL_NAME,
                    SAP_PARENT_CUSTOMER_KEY,
                    SAP_PARENT_CUSTOMER_DESCRIPTION,
                    SAP_CUSTOMER_CHANNEL_KEY,
                    SAP_CUSTOMER_CHANNEL_DESCRIPTION,
                    SAP_CUSTOMER_SUB_CHANNEL_KEY,
                    SAP_SUB_CHANNEL_DESCRIPTION,
                    SAP_GO_TO_MDL_KEY,
                    SAP_GO_TO_MDL_DESCRIPTION,
                    SAP_BANNER_KEY,
                    SAP_BANNER_DESCRIPTION,
                    SAP_BANNER_FORMAT_KEY,
                    SAP_BANNER_FORMAT_DESCRIPTION,
                    CUSTOMER_SEGMENT_KEY,
                    CUSTOMER_SEGMENT_DESCRIPTION,
                    GLOBAL_PRODUCT_FRANCHISE,
                    GLOBAL_PRODUCT_BRAND,
                    GLOBAL_PRODUCT_SUB_BRAND,
                    GLOBAL_PRODUCT_VARIANT,
                    GLOBAL_PRODUCT_SEGMENT,
                    GLOBAL_PRODUCT_SUBSEGMENT,
                    GLOBAL_PRODUCT_CATEGORY,
                    GLOBAL_PRODUCT_SUBCATEGORY,
                    GLOBAL_PUT_UP_DESCRIPTION,
                    PKA_PRODUCT_KEY,
                    PKA_PRODUCT_KEY_DESCRIPTION
             FROM WKS_TW_BASE_RE) RE_BASE_DIM
         ON RE_BASE_DIM.cntry_cd = BASE_DIM.cntry_cd
        AND RE_BASE_DIM.sellout_dim_key = BASE_DIM.sellout_dim_key
        AND RE_BASE_DIM.DATA_SRC = BASE_DIM.DATA_SRC
  LEFT OUTER JOIN (SELECT DISTINCT CNTRY_CD,
                          sellout_dim_key,
                          data_src,
                          mnth_id,
                          so_sls_qty,
                          so_sls_value,
                          so_avg_qty,
                          SALES_VALUE_LIST_PRICE
                   FROM WKS_TW_BASE_RE) CM
               ON BASE_DIM.CNTRY_CD = CM.CNTRY_CD
              AND BASE_DIM.Month = CM.mnth_id
              AND BASE_DIM.sellout_dim_key = CM.sellout_dim_key
              AND BASE_DIM.DATA_SRC = CM.DATA_SRC
  LEFT OUTER JOIN
--Last Month
WKS_TW_RE_ACT_LM LM
               ON BASE_DIM.CNTRY_CD = LM.CNTRY_CD
              AND BASE_DIM.month = LM.MONTH
              AND BASE_DIM.sellout_dim_key = LM.sellout_dim_key
              AND BASE_DIM.DATA_SRC = LM.DATA_SRC
  LEFT OUTER JOIN
--L3M
WKS_TW_RE_ACT_L3M L3M
               ON BASE_DIM.CNTRY_CD = L3M.CNTRY_CD
              AND BASE_DIM.month = L3M.MONTH
              AND BASE_DIM.sellout_dim_key = L3M.sellout_dim_key
              AND BASE_DIM.DATA_SRC = L3M.DATA_SRC
  LEFT OUTER JOIN
--L6M
WKS_TW_RE_ACT_L6M L6M
               ON BASE_DIM.CNTRY_CD = L6M.CNTRY_CD
              AND BASE_DIM.month = L6M.MONTH
              AND BASE_DIM.sellout_dim_key = L6M.sellout_dim_key
              AND BASE_DIM.DATA_SRC = L6M.DATA_SRC
  LEFT OUTER JOIN
--L12M
WKS_TW_RE_ACT_L12M L12M
               ON BASE_DIM.CNTRY_CD = L12M.CNTRY_CD
              AND BASE_DIM.month = L12M.MONTH
              AND BASE_DIM.sellout_dim_key = L12M.sellout_dim_key
              AND BASE_DIM.DATA_SRC = L12M.DATA_SRC
where BASE_DIM.month >= (select last_17mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
  and BASE_DIM.month <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)::numeric 
),
final as
(
select 
cntry_cd::varchar(2) as cntry_cd ,
cntry_nm::varchar(30) as cntry_nm ,
data_src::varchar(14) as data_src ,
year::varchar(16) as year ,
mnth_id::varchar(27) as mnth_id ,
soldto_code::varchar(255) as soldto_code ,
distributor_code::varchar(32) as distributor_code ,
distributor_name::varchar(255) as distributor_name ,
store_code::varchar(100) as store_code ,
ean::varchar(150) as ean ,
store_name::varchar(601) as store_name ,
retail_environment::varchar(225) as retail_environment ,
store_type::varchar(255) as store_type ,
store_grade::varchar(150) as store_grade ,
list_price::numeric(38,6) as list_price ,
msl_product_desc::varchar(300) as msl_product_desc ,
sku_code::varchar(40) as sku_code ,
region::varchar(150) as region ,
zone_or_area::varchar(150) as zone_or_area ,
channel_name::varchar(150) as channel_name ,
sap_parent_customer_key::varchar(12) as sap_parent_customer_key ,
sap_parent_customer_description::varchar(75) as sap_parent_customer_description ,
sap_customer_channel_key::varchar(12) as sap_customer_channel_key ,
sap_customer_channel_description::varchar(75) as sap_customer_channel_description ,
sap_customer_sub_channel_key::varchar(12) as sap_customer_sub_channel_key ,
sap_sub_channel_description::varchar(75) as sap_sub_channel_description ,
sap_go_to_mdl_key::varchar(12) as sap_go_to_mdl_key ,
sap_go_to_mdl_description::varchar(75) as sap_go_to_mdl_description ,
sap_banner_key::varchar(12) as sap_banner_key ,
sap_banner_description::varchar(75) as sap_banner_description ,
sap_banner_format_key::varchar(12) as sap_banner_format_key ,
sap_banner_format_description::varchar(75) as sap_banner_format_description ,
customer_segment_key::varchar(12) as customer_segment_key ,
customer_segment_description::varchar(50) as customer_segment_description ,
global_product_franchise::varchar(30) as global_product_franchise ,
global_product_brand::varchar(30) as global_product_brand ,
global_product_sub_brand::varchar(100) as global_product_sub_brand ,
global_product_variant::varchar(100) as global_product_variant ,
global_product_segment::varchar(50) as global_product_segment ,
global_product_subsegment::varchar(100) as global_product_subsegment ,
global_product_category::varchar(50) as global_product_category ,
global_product_subcategory::varchar(50) as global_product_subcategory ,
global_put_up_description::varchar(100) as global_put_up_description ,
pka_product_key::varchar(68) as pka_product_key ,
pka_product_key_description::varchar(255) as pka_product_key_description ,
cm_sales_qty::numeric(38,6) as cm_sales_qty ,
cm_sales::numeric(38,6) as cm_sales ,
cm_avg_sales_qty::numeric(10,2) as cm_avg_sales_qty ,
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
crt_dttm::timestamp without time zone AS crt_dttm 
from tw_re_actuals 
)

select * from final