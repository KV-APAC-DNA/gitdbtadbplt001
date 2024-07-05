with WKS_CNSC_BASE_RETAIL_EXCELLENCE as (
    select * from {{ ref('chnwks_integration__wks_cnsc_base_retail_excellence') }}
),
edw_vw_cal_Retail_excellence_Dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
WKS_CNSC_REGIONAL_SELLOUT_ACT_LM as (
    select * from {{ ref('chnwks_integration__wks_cnsc_regional_sellout_act_lm') }}
),
WKS_CNSC_REGIONAL_SELLOUT_ACT_L3M as (
    select * from {{ ref('chnwks_integration__wks_cnsc_regional_sellout_act_l3m') }}
),
WKS_CNSC_REGIONAL_SELLOUT_ACT_L6M as (
    select * from {{ ref('chnwks_integration__wks_cnsc_regional_sellout_act_l6m') }}
),
WKS_CNSC_REGIONAL_SELLOUT_ACT_L12M as (
    select * from {{ ref('chnwks_integration__wks_cnsc_regional_sellout_act_l12m') }}
),

transformation as (SELECT RE_BASE_DIM.CNTRY_CD,
       RE_BASE_DIM.CNTRY_NM,
       SUBSTRING(BASE_DIM.MONTH,1,4) AS YEAR,
       BASE_DIM.MONTH AS MNTH_ID,
       ---MD5(nvl(RE_BASE_DIM.DISTRIBUTOR_CODE,'dc')||nvl(RE_BASE_DIM.STORE_CODE,'sc')||nvl(RE_BASE_DIM.SKU_CODE,'sku')) AS dist_store_sku_key,
       RE_BASE_DIM.DISTRIBUTOR_CODE,
       RE_BASE_DIM.DISTRIBUTOR_Name,
       RE_BASE_DIM.STORE_CODE,
       RE_BASE_DIM.STORE_NAME,
       RE_BASE_DIM.STORE_TYPE,
       RE_BASE_DIM.msl_product_CODE,
       RE_BASE_DIM.msl_product_desc,
       RE_BASE_DIM.CUSTOMER_PRODUCT_DESC ,
       RE_BASE_DIM.SAP_PARENT_CUSTOMER_KEY,
       RE_BASE_DIM.SAP_PARENT_CUSTOMER_DESCRIPTION,
       RE_BASE_DIM.SAP_CUSTOMER_CHANNEL_KEY,
       RE_BASE_DIM.SAP_CUSTOMER_CHANNEL_DESCRIPTION,
       RE_BASE_DIM.SAP_CUSTOMER_SUB_CHANNEL_KEY,
       RE_BASE_DIM.SAP_SUB_CHANNEL_DESCRIPTION,
       RE_BASE_DIM.SAP_GO_TO_MDL_KEY,
       RE_BASE_DIM.SAP_GO_TO_MDL_DESCRIPTION,
       RE_BASE_DIM.SAP_BANNER_KEY,
       RE_BASE_DIM.SAP_BANNER_DESCRIPTION,
       RE_BASE_DIM.SAP_BANNER_FORMAT_KEY,
       RE_BASE_DIM.SAP_BANNER_FORMAT_DESCRIPTION,
       RE_BASE_DIM.RETAIL_ENV,
       RE_BASE_DIM.REGION,
       RE_BASE_DIM.ZONE_OR_AREA,
       RE_BASE_DIM.CUSTOMER_SEGMENT_KEY,
       RE_BASE_DIM.CUSTOMER_SEGMENT_DESCRIPTION,
       RE_BASE_DIM.PKA_PRODUCT_KEY,
       RE_BASE_DIM.PKA_PRODUCT_KEY_DESCRIPTION,
	   RE_BASE_DIM.SKU_DESCRIPTION,
	   RE_BASE_DIM.SKU_CODE,
	   RE_BASE_DIM.PRODUCT_BRAND,
	   RE_BASE_DIM.CHANNEL,
       CM.sls_qty AS CM_SALES_QTY,
       CM.sls_value AS CM_SALES,
       CM.avg_qty AS CM_AVG_SALES_QTY,
       CM.SALES_VALUE_LIST_PRICE AS CM_SALES_VALUE_LIST_PRICE,
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
       SYSDATE() AS Created_date
FROM (SELECT DISTINCT CNTRY_CD,
             sellout_dim_key,
             MONTH
      FROM (SELECT CNTRY_CD,
                   sellout_dim_key,
                   MONTH
            FROM WKS_CNSC_REGIONAL_SELLOUT_ACT_LM
            WHERE LM_SALES IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   MONTH
            FROM WKS_CNSC_REGIONAL_SELLOUT_ACT_L3M
            WHERE L3M_SALES IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   MONTH
            FROM WKS_CNSC_REGIONAL_SELLOUT_ACT_L6M
            WHERE L6M_SALES IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   MONTH
            FROM WKS_CNSC_REGIONAL_SELLOUT_ACT_L12M
            WHERE L12M_SALES IS NOT NULL)) BASE_DIM
  LEFT JOIN (SELECT DISTINCT CNTRY_CD,
                    cntry_nm,
                    sellout_dim_key,
                    distributor_code,
                    Distributor_Name,
                    store_code,
                    store_NAME,
                    STORE_type,
                    CUSTOMER_PRODUCT_DESC,
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
       RETAIL_ENV,
       REGION,
       ZONE_OR_AREA,
       CUSTOMER_SEGMENT_KEY,
       CUSTOMER_SEGMENT_DESCRIPTION,
       msl_product_CODE,
      msl_product_desc,
       PKA_PRODUCT_KEY,
       PKA_PRODUCT_KEY_DESCRIPTION,
	   SKU_CODE,
	   SKU_DESCRIPTION,
	   PRODUCT_BRAND,
	   CHANNEL
             FROM WKS_CNSC_BASE_RETAIL_EXCELLENCE) RE_BASE_DIM
         ON RE_BASE_DIM.cntry_cd = BASE_DIM.cntry_cd
        AND RE_BASE_DIM.sellout_dim_key = BASE_DIM.sellout_dim_key
  LEFT OUTER JOIN (SELECT DISTINCT CNTRY_CD,
                          sellout_dim_key,
                          mnth_id,
                          sls_qty,
                          sls_value,
                          avg_qty,
                          SALES_VALUE_LIST_PRICE
                   FROM WKS_CNSC_BASE_RETAIL_EXCELLENCE) CM
               ON BASE_DIM.CNTRY_CD = CM.CNTRY_CD
              AND BASE_DIM.Month = CM.mnth_id
              AND BASE_DIM.sellout_dim_key = CM.sellout_dim_key
  LEFT OUTER JOIN
--Last Month
WKS_CNSC_REGIONAL_SELLOUT_ACT_LM LM
               ON BASE_DIM.CNTRY_CD = LM.CNTRY_CD
              AND BASE_DIM.month = LM.MONTH
              AND BASE_DIM.sellout_dim_key = LM.sellout_dim_key
  LEFT OUTER JOIN
--L3M
WKS_CNSC_REGIONAL_SELLOUT_ACT_L3M L3M
               ON BASE_DIM.CNTRY_CD = L3M.CNTRY_CD
              AND BASE_DIM.month = L3M.MONTH
              AND BASE_DIM.sellout_dim_key = L3M.sellout_dim_key
  LEFT OUTER JOIN
--L6M
WKS_CNSC_REGIONAL_SELLOUT_ACT_L6M L6M
               ON BASE_DIM.CNTRY_CD = L6M.CNTRY_CD
              AND BASE_DIM.month = L6M.MONTH
              AND BASE_DIM.sellout_dim_key = L6M.sellout_dim_key
  LEFT OUTER JOIN
--L12M
WKS_CNSC_REGIONAL_SELLOUT_ACT_L12M L12M
               ON BASE_DIM.CNTRY_CD = L12M.CNTRY_CD
              AND BASE_DIM.month = L12M.MONTH
              AND BASE_DIM.sellout_dim_key = L12M.sellout_dim_key
WHERE BASE_DIM.month >= (select last_19mnths from edw_vw_cal_Retail_excellence_Dim)
  and BASE_DIM.month <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)),

final as (
select 
cntry_cd::varchar(2) AS cntry_cd,
cntry_nm::varchar(50) AS cntry_nm,
year::varchar(16) AS year,
mnth_id::varchar(23) AS mnth_id,
distributor_code::varchar(100) AS distributor_code,
distributor_name::varchar(356) AS distributor_name,
store_code::varchar(100) AS store_code,
store_name::varchar(601) AS store_name,
store_type::varchar(255) AS store_type,
msl_product_code::varchar(150) AS msl_product_code,
msl_product_desc::varchar(300) AS msl_product_desc,
customer_product_desc::varchar(300) AS customer_product_desc,
sap_parent_customer_key::varchar(12) AS sap_parent_customer_key,
sap_parent_customer_description::varchar(75) AS sap_parent_customer_description,
sap_customer_channel_key::varchar(12) AS sap_customer_channel_key,
sap_customer_channel_description::varchar(75) AS sap_customer_channel_description,
sap_customer_sub_channel_key::varchar(12) AS sap_customer_sub_channel_key,
sap_sub_channel_description::varchar(75) AS sap_sub_channel_description,
sap_go_to_mdl_key::varchar(12) AS sap_go_to_mdl_key,
sap_go_to_mdl_description::varchar(75) AS sap_go_to_mdl_description,
sap_banner_key::varchar(12) AS sap_banner_key,
sap_banner_description::varchar(75) AS sap_banner_description,
sap_banner_format_key::varchar(12) AS sap_banner_format_key,
sap_banner_format_description::varchar(75) AS sap_banner_format_description,
retail_env::varchar(150) AS retail_env,
region::varchar(150) AS region,
zone_or_area::varchar(150) AS zone_or_area,
customer_segment_key::varchar(12) AS customer_segment_key,
customer_segment_description::varchar(50) AS customer_segment_description,
pka_product_key::varchar(68) AS pka_product_key,
pka_product_key_description::varchar(255) AS pka_product_key_description,
sku_description::varchar(150) AS sku_description,
sku_code::varchar(40) AS sku_code,
product_brand::varchar(200) AS product_brand,
channel::varchar(50) AS channel,
cm_sales_qty::numeric(38,6) AS cm_sales_qty,
cm_sales::numeric(38,6) AS cm_sales,
cm_avg_sales_qty::numeric(38,6) AS cm_avg_sales_qty,
cm_sales_value_list_price::numeric(38,12) AS cm_sales_value_list_price,
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
created_date::timestamp AS created_date
from transformation    
)


select * from final        