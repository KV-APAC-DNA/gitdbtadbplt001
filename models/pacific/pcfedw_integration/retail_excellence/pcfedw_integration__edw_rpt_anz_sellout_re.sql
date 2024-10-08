--Import CTE

with wks_anz_sellout_rpt_re_gcph as (
    select * from {{ ref('pcfwks_integration__wks_anz_sellout_rpt_re_gcph') }}
),
wks_anz_sellout_re_target_compliance as 
(
    select * from {{ ref('pcfwks_integration__wks_anz_sellout_re_target_compliance' )}}
),
edw_anz_sellout_rpt_re  as 
(
SELECT jj_year AS FISC_YR,
       gcph.jj_mnth_id AS FISC_PER,
       "cluster",
       MARKET,
       data_src,
       CHANNEL_NAME,
       soldto_code,
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
       --STORE_ADDRESS,
       --POST_CODE,
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
       --null as  SAP_BASE_PROD_CD,
       SAP_BASE_PROD_DESC,
       --null as SAP_MEGA_BRND_CD,
       SAP_MEGA_BRND_DESC,
       --null as SAP_BRND_CD,
       SAP_BRND_DESC,
       --null as SAP_VRNT_CD,
       SAP_VRNT_DESC,
       --null as SAP_PUT_UP_CD,
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
       --null as PKA_FRANCHISE_CD, 
       PKA_FRANCHISE_DESC,
       -- null as PKA_BRAND_CD, 
       PKA_BRAND_DESC,
       --null as PKA_SUB_BRAND_CD, 
       PKA_SUB_BRAND_DESC,
       --null as PKA_VARIANT_CD, 
       PKA_VARIANT_DESC,
       --null as PKA_SUB_VARIANT_CD, 
       PKA_SUB_VARIANT_DESC,
       GLOBAL_PRODUCT_FRANCHISE,
       gcph.GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_VARIANT,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       GLOBAL_PUT_UP_DESCRIPTION,
       product_code AS EAN,
       MAPPED_SKU_CD AS SKU_CODE,
       product_name AS SKU_DESCRIPTION,
       --null as GREENLIGHT_SKU_FLAG,
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
      -- TARGET_COMPLAINCE,
       CASE 
            WHEN (MDP_FLAG = 'Y' AND UPPER(gcph.global_product_brand) = UPPER(TRGT_CMP.global_product_brand)) THEN TRGT_CMP.TARGET_COMPLAINCE
            ELSE 1
            END AS TARGET_COMPLAINCE,
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
       STORE_CONTRIBUTION_LM,
       SKU_CONTRIBUTION_LM,
       SIZE_OF_PRICE_LM,
       STORE_CONTRIBUTION_P3M,
       SKU_CONTRIBUTION_P3M,
       SIZE_OF_PRICE_P3M,
       STORE_CONTRIBUTION_P6M,
       SKU_CONTRIBUTION_P6M,
       SIZE_OF_PRICE_P6M,
       STORE_CONTRIBUTION_P12M,
       SKU_CONTRIBUTION_P12M,
       SIZE_OF_PRICE_P12M,
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
       STORE_CONTRIBUTION_LM_LP,
       SKU_CONTRIBUTION_LM_LP,
       SIZE_OF_PRICE_LM_LP,
       STORE_CONTRIBUTION_P3M_LP,
       SKU_CONTRIBUTION_P3M_LP,
       SIZE_OF_PRICE_P3M_LP,
       STORE_CONTRIBUTION_P6M_LP,
       SKU_CONTRIBUTION_P6M_LP,
       SIZE_OF_PRICE_P6M_LP,
       STORE_CONTRIBUTION_P12M_LP,
       SKU_CONTRIBUTION_P12M_LP,
       SIZE_OF_PRICE_P12M_LP,
       COUNT(LM_SALES_FLAG) OVER (PARTITION BY CUSTOMER_AGG_DIM_KEY,PRODUCT_AGG_DIM_KEY,LM_SALES_FLAG,MDP_FLAG) AS lM_SALES_FLAG_COUNT,
       COUNT(P3M_SALES_FLAG) OVER (PARTITION BY CUSTOMER_AGG_DIM_KEY,PRODUCT_AGG_DIM_KEY,P3M_SALES_FLAG,MDP_FLAG) AS P3M_SALES_FLAG_COUNT,
       COUNT(P6M_SALES_FLAG) OVER (PARTITION BY CUSTOMER_AGG_DIM_KEY,PRODUCT_AGG_DIM_KEY,P6M_SALES_FLAG,MDP_FLAG) AS P6M_SALES_FLAG_COUNT,
       COUNT(P12M_SALES_FLAG) OVER (PARTITION BY CUSTOMER_AGG_DIM_KEY,PRODUCT_AGG_DIM_KEY,P12M_SALES_FLAG,MDP_FLAG) AS P12M_SALES_FLAG_COUNT,
       COUNT(MDP_FLAG) OVER (PARTITION BY CUSTOMER_AGG_DIM_KEY,PRODUCT_AGG_DIM_KEY,MDP_FLAG) AS MDP_FLAG_COUNT,
	   SYSDATE() AS CRT_DTTM
	   FROM wks_anz_sellout_rpt_re_gcph gcph
       LEFT JOIN wks_anz_sellout_re_target_compliance TRGT_CMP on (gcph.jj_mnth_id=TRGT_CMP.jj_mnth_id and UPPER(gcph.global_product_brand)=UPPER(TRGT_CMP.global_product_brand))
),
final as
(
select 
fisc_yr::VARCHAR(16) AS fisc_yr,
fisc_per::numeric(18,0) AS fisc_per,
"cluster"::VARCHAR(100) AS "cluster",
market::VARCHAR(50) AS market,
data_src::VARCHAR(8) AS data_src,
channel_name::VARCHAR(150) AS channel_name,
soldto_code::VARCHAR(255) AS soldto_code,
distributor_code::VARCHAR(32) AS distributor_code,
distributor_name::VARCHAR(255) AS distributor_name,
sell_out_channel::VARCHAR(150) AS sell_out_channel,
store_type::VARCHAR(255) AS store_type,
prioritization_segmentation::VARCHAR(1) AS prioritization_segmentation,
store_category::VARCHAR(1) AS store_category,
store_code::VARCHAR(100) AS store_code,
store_name::VARCHAR(601) AS store_name,
store_grade::VARCHAR(20) AS store_grade,
store_size::VARCHAR(2) AS store_size,
region::VARCHAR(150) AS region,
zone_name::VARCHAR(150) AS zone_name,
city::VARCHAR(2) AS city,
rtrlatitude::VARCHAR(1) AS rtrlatitude,
rtrlongitude::VARCHAR(1) AS rtrlongitude,
customer_segment_key::VARCHAR(12) AS customer_segment_key,
customer_segment_description::VARCHAR(50) AS customer_segment_description,
retail_environment::VARCHAR(225) AS retail_environment,
sap_customer_channel_key::VARCHAR(12) AS sap_customer_channel_key,
sap_customer_channel_description::VARCHAR(112) AS sap_customer_channel_description,
sap_customer_sub_channel_key::VARCHAR(12) AS sap_customer_sub_channel_key,
sap_sub_channel_description::VARCHAR(112) AS sap_sub_channel_description,
sap_parent_customer_key::VARCHAR(12) AS sap_parent_customer_key,
sap_parent_customer_description::VARCHAR(112) AS sap_parent_customer_description,
sap_banner_key::VARCHAR(12) AS sap_banner_key,
sap_banner_description::VARCHAR(112) AS sap_banner_description,
sap_banner_format_key::VARCHAR(12) AS sap_banner_format_key,
sap_banner_format_description::VARCHAR(112) AS sap_banner_format_description,
customer_name::VARCHAR(100) AS customer_name,
customer_code::VARCHAR(10) AS customer_code,
product_code::VARCHAR(150) AS product_code,
product_name::VARCHAR(300) AS product_name,
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
sap_prod_sgmt_cd::VARCHAR(18) AS sap_prod_sgmt_cd,
sap_prod_sgmt_desc::VARCHAR(100) AS sap_prod_sgmt_desc,
sap_base_prod_desc::VARCHAR(100) AS sap_base_prod_desc,
sap_mega_brnd_desc::VARCHAR(100) AS sap_mega_brnd_desc,
sap_brnd_desc::VARCHAR(100) AS sap_brnd_desc,
sap_vrnt_desc::VARCHAR(100) AS sap_vrnt_desc,
sap_put_up_desc::VARCHAR(100) AS sap_put_up_desc,
sap_grp_frnchse_cd::VARCHAR(18) AS sap_grp_frnchse_cd,
sap_grp_frnchse_desc::VARCHAR(100) AS sap_grp_frnchse_desc,
sap_frnchse_cd::VARCHAR(18) AS sap_frnchse_cd,
sap_frnchse_desc::VARCHAR(100) AS sap_frnchse_desc,
sap_prod_frnchse_cd::VARCHAR(18) AS sap_prod_frnchse_cd,
sap_prod_frnchse_desc::VARCHAR(100) AS sap_prod_frnchse_desc,
sap_prod_mjr_cd::VARCHAR(18) AS sap_prod_mjr_cd,
sap_prod_mjr_desc::VARCHAR(100) AS sap_prod_mjr_desc,
sap_prod_mnr_cd::VARCHAR(18) AS sap_prod_mnr_cd,
sap_prod_mnr_desc::VARCHAR(100) AS sap_prod_mnr_desc,
sap_prod_hier_cd::VARCHAR(18) AS sap_prod_hier_cd,
sap_prod_hier_desc::VARCHAR(100) AS sap_prod_hier_desc,
pka_franchise_desc::VARCHAR(30) AS pka_franchise_desc,
pka_brand_desc::VARCHAR(30) AS pka_brand_desc,
pka_sub_brand_desc::VARCHAR(30) AS pka_sub_brand_desc,
pka_variant_desc::VARCHAR(30) AS pka_variant_desc,
pka_sub_variant_desc::VARCHAR(30) AS pka_sub_variant_desc,
global_product_franchise::VARCHAR(30) AS global_product_franchise,
global_product_brand::VARCHAR(30) AS global_product_brand,
global_product_sub_brand::VARCHAR(100) AS global_product_sub_brand,
global_product_variant::VARCHAR(100) AS global_product_variant,
global_product_segment::VARCHAR(50) AS global_product_segment,
global_product_subsegment::VARCHAR(100) AS global_product_subsegment,
global_product_category::VARCHAR(50) AS global_product_category,
global_product_subcategory::VARCHAR(50) AS global_product_subcategory,
global_put_up_description::VARCHAR(100) AS global_put_up_description,
ean::VARCHAR(150) AS ean,
sku_code::VARCHAR(40) AS sku_code,
sku_description::VARCHAR(300) AS sku_description,
pka_product_key::VARCHAR(68) AS pka_product_key,
pka_product_key_description::VARCHAR(255) AS pka_product_key_description,
sales_value::NUMERIC(38,6) AS sales_value,
sales_qty::NUMERIC(38,6) AS sales_qty,
avg_sales_qty::NUMERIC(10,2) AS avg_sales_qty,
sales_value_list_price::NUMERIC(38,12) AS sales_value_list_price,
lm_sales::NUMERIC(38,6) AS lm_sales,
lm_sales_qty::NUMERIC(38,6) AS lm_sales_qty,
lm_avg_sales_qty::NUMERIC(10,2) AS lm_avg_sales_qty,
lm_sales_lp::NUMERIC(38,12) AS lm_sales_lp,
p3m_sales::NUMERIC(38,6) AS p3m_sales,
p3m_qty::NUMERIC(38,6) AS p3m_qty,
p3m_avg_qty::NUMERIC(38,6) AS p3m_avg_qty,
p3m_sales_lp::NUMERIC(38,12) AS p3m_sales_lp,
p6m_sales::NUMERIC(38,6) AS p6m_sales,
p6m_qty::NUMERIC(38,6) AS p6m_qty,
p6m_avg_qty::NUMERIC(38,6) AS p6m_avg_qty,
p6m_sales_lp::NUMERIC(38,12) AS p6m_sales_lp,
p12m_sales::NUMERIC(38,6) AS p12m_sales,
p12m_qty::NUMERIC(38,6) AS p12m_qty,
p12m_avg_qty::NUMERIC(38,6) AS p12m_avg_qty,
p12m_sales_lp::NUMERIC(38,12) AS p12m_sales_lp,
f3m_sales::NUMERIC(38,6) AS f3m_sales,
f3m_qty::NUMERIC(38,6) AS f3m_qty,
f3m_avg_qty::NUMERIC(38,6) AS f3m_avg_qty,
lm_sales_flag::VARCHAR(1) AS lm_sales_flag,
p3m_sales_flag::VARCHAR(1) AS p3m_sales_flag,
p6m_sales_flag::VARCHAR(1) AS p6m_sales_flag,
p12m_sales_flag::VARCHAR(1) AS p12m_sales_flag,
mdp_flag::VARCHAR(1) AS mdp_flag,
target_complaince::numeric(38,6) AS target_complaince,
list_price::NUMERIC(38,6) AS list_price,
total_sales_lm::NUMERIC(38,6) AS total_sales_lm,
total_sales_p3m::NUMERIC(38,6) AS total_sales_p3m,
total_sales_p6m::NUMERIC(38,6) AS total_sales_p6m,
total_sales_p12m::NUMERIC(38,6) AS total_sales_p12m,
total_sales_by_store_lm::NUMERIC(38,6) AS total_sales_by_store_lm,
total_sales_by_store_p3m::NUMERIC(38,6) AS total_sales_by_store_p3m,
total_sales_by_store_p6m::NUMERIC(38,6) AS total_sales_by_store_p6m,
total_sales_by_store_p12m::NUMERIC(38,6) AS total_sales_by_store_p12m,
total_sales_by_sku_lm::NUMERIC(38,6) AS total_sales_by_sku_lm,
total_sales_by_sku_p3m::NUMERIC(38,6) AS total_sales_by_sku_p3m,
total_sales_by_sku_p6m::NUMERIC(38,6) AS total_sales_by_sku_p6m,
total_sales_by_sku_p12m::NUMERIC(38,6) AS total_sales_by_sku_p12m,
store_contribution_lm::NUMERIC(38,4) AS store_contribution_lm,
sku_contribution_lm::NUMERIC(38,4) AS sku_contribution_lm,
size_of_price_lm::NUMERIC(38,14) AS size_of_price_lm,
store_contribution_p3m::NUMERIC(38,4) AS store_contribution_p3m,
sku_contribution_p3m::NUMERIC(38,4) AS sku_contribution_p3m,
size_of_price_p3m::NUMERIC(38,14) AS size_of_price_p3m,
store_contribution_p6m::NUMERIC(38,4) AS store_contribution_p6m,
sku_contribution_p6m::NUMERIC(38,4) AS sku_contribution_p6m,
size_of_price_p6m::NUMERIC(38,14) AS size_of_price_p6m,
store_contribution_p12m::NUMERIC(38,4) AS store_contribution_p12m,
sku_contribution_p12m::NUMERIC(38,4) AS sku_contribution_p12m,
size_of_price_p12m::NUMERIC(38,14) AS size_of_price_p12m,
total_sales_lm_lp::NUMERIC(38,12) AS total_sales_lm_lp,
total_sales_p3m_lp::NUMERIC(38,12) AS total_sales_p3m_lp,
total_sales_p6m_lp::NUMERIC(38,12) AS total_sales_p6m_lp,
total_sales_p12m_lp::NUMERIC(38,12) AS total_sales_p12m_lp,
total_sales_by_store_lm_lp::NUMERIC(38,12) AS total_sales_by_store_lm_lp,
total_sales_by_store_p3m_lp::NUMERIC(38,12) AS total_sales_by_store_p3m_lp,
total_sales_by_store_p6m_lp::NUMERIC(38,12) AS total_sales_by_store_p6m_lp,
total_sales_by_store_p12m_lp::NUMERIC(38,12) AS total_sales_by_store_p12m_lp,
total_sales_by_sku_lm_lp::NUMERIC(38,12) AS total_sales_by_sku_lm_lp,
total_sales_by_sku_p3m_lp::NUMERIC(38,12) AS total_sales_by_sku_p3m_lp,
total_sales_by_sku_p6m_lp::NUMERIC(38,12) AS total_sales_by_sku_p6m_lp,
total_sales_by_sku_p12m_lp::NUMERIC(38,12) AS total_sales_by_sku_p12m_lp,
store_contribution_lm_lp::NUMERIC(38,4) AS store_contribution_lm_lp,
sku_contribution_lm_lp::NUMERIC(38,4) AS sku_contribution_lm_lp,
size_of_price_lm_lp::NUMERIC(38,20) AS size_of_price_lm_lp,
store_contribution_p3m_lp::NUMERIC(38,4) AS store_contribution_p3m_lp,
sku_contribution_p3m_lp::NUMERIC(38,4) AS sku_contribution_p3m_lp,
size_of_price_p3m_lp::NUMERIC(38,20) AS size_of_price_p3m_lp,
store_contribution_p6m_lp::NUMERIC(38,4) AS store_contribution_p6m_lp,
sku_contribution_p6m_lp::NUMERIC(38,4) AS sku_contribution_p6m_lp,
size_of_price_p6m_lp::NUMERIC(38,20) AS size_of_price_p6m_lp,
store_contribution_p12m_lp::NUMERIC(38,4) AS store_contribution_p12m_lp,
sku_contribution_p12m_lp::NUMERIC(38,4) AS sku_contribution_p12m_lp,
size_of_price_p12m_lp::NUMERIC(38,20) AS size_of_price_p12m_lp,
lm_sales_flag_count::BIGINT AS lm_sales_flag_count,
p3m_sales_flag_count::BIGINT AS p3m_sales_flag_count,
p6m_sales_flag_count::BIGINT AS p6m_sales_flag_count,
p12m_sales_flag_count::BIGINT AS p12m_sales_flag_count,
mdp_flag_count::BIGINT AS mdp_flag_count,
crt_dttm::timestamp without time zone AS crt_dttm
from edw_anz_sellout_rpt_re
)
--Final select
select * from final 