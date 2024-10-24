{{ 
    config(materialized='table', 
    transient=true )   
     
    }}
with wks_rpt_retail_excellence_sop as (
    select * from {{ ref('chnwks_integration__wks_rpt_retail_excellence_sop') }}
),
retail_excellence_msl_target_final as 
(
    select * from {{ ref('chnwks_integration__wks_cnsc_msl_target_final') }}
),

transformation as (
    SELECT
 DM.FISC_YR,
       DM.FISC_PER,
       DM."CLUSTER",
       DM.MARKET,
       'SELL-OUT' AS DATA_SRC,
       DM.SELL_OUT_CHANNEL AS CHANNEL_NAME,
       DM.DISTRIBUTOR_CODE,
       DM.DISTRIBUTOR_NAME,
       DM.SELL_OUT_CHANNEL,
       DM.STORE_TYPE,
       NULL AS PRIORITIZATION_SEGMENTATION,
       NULL AS STORE_CATEGORY,
       DM.STORE_CODE,
       DM.STORE_NAME,
       NULL AS STORE_GRADE,
       NULL AS STORE_SIZE,
       DM.REGION,
       DM.ZONE_NAME,
       NULL AS CITY,
       NULL AS RTRLATITUDE,
       NULL AS RTRLONGITUDE,
       DM.CUSTOMER_SEGMENT_KEY,
       DM.CUSTOMER_SEGMENT_DESCRIPTION,
       DM.SELL_OUT_RE AS RETAIL_ENVIRONMENT,
       DM.SAP_PARENT_CUSTOMER_KEY,
       DM.SAP_PARENT_CUSTOMER_DESCRIPTION,
       DM.SAP_CUSTOMER_CHANNEL_KEY,
       DM.SAP_CUSTOMER_CHANNEL_DESCRIPTION,
       DM.SAP_CUSTOMER_SUB_CHANNEL_KEY,
       DM.SAP_SUB_CHANNEL_DESCRIPTION,
       --DM.SAP_GO_TO_MDL_KEY,
       -- DM.SAP_GO_TO_MDL_DESCRIPTION,
       DM.SAP_BANNER_KEY,
       DM.SAP_BANNER_DESCRIPTION,
       DM.SAP_BANNER_FORMAT_KEY,
       DM.SAP_BANNER_FORMAT_DESCRIPTION,
       NULL AS CUSTOMER_NAME,
       NULL AS CUSTOMER_CODE,
       DM.PRODUCT_CODE,
       DM.PRODUCT_NAME,
       PROD_HIER_L1,
       NULL AS PROD_HIER_L2,
       NULL AS PROD_HIER_L3,
       DM.PROD_HIER_L4,
       DM.PROD_HIER_L5,
       DM.PROD_HIER_L6,
       NULL AS PROD_HIER_L7,
       NULL AS PROD_HIER_L8,
       DM.PROD_HIER_L9,
       --DM.MAPPED_SKU_CD,
       -- DM.SAP_MATL_NUM,
       -- DM.SAP_MAT_DESC,
       DM.SAP_PROD_SGMT_CD,
       DM.SAP_PROD_SGMT_DESC,
       --NULL AS SAP_BASE_PROD_CD,
       DM.SAP_BASE_PROD_DESC,
       --Null as SAP_MEGA_BRND_CD,
       DM.SAP_MEGA_BRND_DESC,
       --NULL AS SAP_BRND_CD,
       DM.SAP_BRND_DESC,
       --NULL AS SAP_VRNT_CD,
       DM.SAP_VRNT_DESC,
       --NULL AS SAP_PUT_UP_CD,
       DM.SAP_PUT_UP_DESC,
       DM.SAP_GRP_FRNCHSE_CD,
       DM.SAP_GRP_FRNCHSE_DESC,
       DM.SAP_FRNCHSE_CD,
       DM.SAP_FRNCHSE_DESC,
       DM.SAP_PROD_FRNCHSE_CD,
       DM.SAP_PROD_FRNCHSE_DESC,
       DM.SAP_PROD_MJR_CD,
       DM.SAP_PROD_MJR_DESC,
       DM.SAP_PROD_MNR_CD,
       DM.SAP_PROD_MNR_DESC,
       DM.SAP_PROD_HIER_CD,
       DM.SAP_PROD_HIER_DESC,
       --NULL AS PKA_FRANCHISE_CD, 
       DM.PKA_FRANCHISE_DESC,
       -- Null as PKA_BRAND_CD, 
       DM.PKA_BRAND_DESC,
       --null as PKA_SUB_BRAND_CD, 
       DM.PKA_SUB_BRAND_DESC,
       --null as PKA_VARIANT_CD, 
       DM.PKA_VARIANT_DESC,
       -- NULL AS PKA_SUB_VARIANT_CD, 
       DM.PKA_SUB_VARIANT_DESC,
       --GPH_REGION,
       --GPH_REG_FRNCHSE,
       -- GPH_REG_FRNCHSE_GRP,
       DM.GLOBAL_PRODUCT_FRANCHISE,
       DM.GLOBAL_PRODUCT_BRAND,
       DM.GLOBAL_PRODUCT_SUB_BRAND,
       DM.GLOBAL_PRODUCT_VARIANT,
       --GPH_PROD_NEEDSTATE,
       DM.GLOBAL_PRODUCT_CATEGORY,
       DM.GLOBAL_PRODUCT_SUBCATEGORY,
       DM.GLOBAL_PRODUCT_SEGMENT,
       DM.GLOBAL_PRODUCT_SUBSEGMENT,
       --GPH_PROD_PUT_UP_CD,
       DM.GLOBAL_PUT_UP_DESCRIPTION,
       ---GPH_PROD_SIZE,
       ---GPH_PROD_SIZE_UOM,
      DM.PRODUCT_CODE AS EAN,
       DM.SKU_CODE,
       DM.SKU_DESCRIPTION,
       -- NULL AS GREENLIGHT_SKU_FLAG,
       DM.PKA_PRODUCT_KEY,
       DM.PKA_PRODUCT_KEY_DESCRIPTION,
       DM.SALES_VALUE,
       DM.SALES_QTY,
       DM.AVG_SALES_QTY,
       DM.SALES_VALUE_LIST_PRICE,
       DM.LM_SALES,
       DM.LM_SALES_QTY,
       DM.LM_AVG_SALES_QTY,
       DM.LM_SALES_LP,
       DM.P3M_SALES,
       DM.P3M_QTY,
       DM.P3M_AVG_QTY,
       DM.P3M_SALES_LP,
       DM.P6M_SALES,
       DM.P6M_QTY,
       DM.P6M_AVG_QTY,
       DM.P6M_SALES_LP,
       DM.P12M_SALES,
       DM.P12M_QTY,
       DM.P12M_AVG_QTY,
       DM.P12M_SALES_LP,
       DM.F3M_SALES,
       DM.F3M_QTY,
       DM.F3M_AVG_QTY,
       DM.LM_SALES_FLAG,
       DM.P3M_SALES_FLAG,
       DM.P6M_SALES_FLAG,
       DM.P12M_SALES_FLAG,
       DM.MDP_FLAG,
       case when (DM.MDP_FLAG='Y' and msl_final.global_product_brand  is not null ) then msl_final.TARGET_COMPLAINCE else 1 end as TARGET_COMPLAINCE,
       DM.LIST_PRICE,
       DM.TOTAL_SALES_LM,
       DM.TOTAL_SALES_P3M,
       DM.TOTAL_SALES_P6M,
       DM.TOTAL_SALES_P12M,
       DM.TOTAL_SALES_BY_STORE_LM,
       DM.TOTAL_SALES_BY_STORE_P3M,
       DM.TOTAL_SALES_BY_STORE_P6M,
       DM.TOTAL_SALES_BY_STORE_P12M,
       DM.TOTAL_SALES_BY_SKU_LM,
       DM.TOTAL_SALES_BY_SKU_P3M,
       DM.TOTAL_SALES_BY_SKU_P6M,
       DM.TOTAL_SALES_BY_SKU_P12M,
       DM.TOTAL_SALES_LM_LP,
       DM.TOTAL_SALES_P3M_LP,
       DM.TOTAL_SALES_P6M_LP,
       DM.TOTAL_SALES_P12M_LP,
       DM.TOTAL_SALES_BY_STORE_LM_LP,
       DM.TOTAL_SALES_BY_STORE_P3M_LP,
       DM.TOTAL_SALES_BY_STORE_P6M_LP,
       DM.TOTAL_SALES_BY_STORE_P12M_LP,
       DM.TOTAL_SALES_BY_SKU_LM_LP,
       DM.TOTAL_SALES_BY_SKU_P3M_LP,
       DM.TOTAL_SALES_BY_SKU_P6M_LP,
       DM.TOTAL_SALES_BY_SKU_P12M_LP,
       DM.STORE_CONTRIBUTION_LM,
       DM.SKU_CONTRIBUTION_LM,
       DM.SIZE_OF_PRICE_LM,
       DM.STORE_CONTRIBUTION_P3M,
       DM.SKU_CONTRIBUTION_P3M,
       DM.SIZE_OF_PRICE_P3M,
       DM.STORE_CONTRIBUTION_P6M,
       DM.SKU_CONTRIBUTION_P6M,
       DM.SIZE_OF_PRICE_P6M,
       DM.STORE_CONTRIBUTION_P12M,
       DM.SKU_CONTRIBUTION_P12M,
       DM.SIZE_OF_PRICE_P12M,
       DM.STORE_CONTRIBUTION_LM_LP,
       DM.SKU_CONTRIBUTION_LM_LP,
       DM.SIZE_OF_PRICE_LM_LP,
       DM.STORE_CONTRIBUTION_P3M_LP,
       DM.SKU_CONTRIBUTION_P3M_LP,
       DM.SIZE_OF_PRICE_P3M_LP,
       DM.STORE_CONTRIBUTION_P6M_LP,
       DM.SKU_CONTRIBUTION_P6M_LP,
       DM.SIZE_OF_PRICE_P6M_LP,
       DM.STORE_CONTRIBUTION_P12M_LP,
       DM.SKU_CONTRIBUTION_P12M_LP,
       DM.SIZE_OF_PRICE_P12M_LP,
       COUNT(DM.LM_SALES_FLAG) OVER (PARTITION BY DM.CUSTOMER_AGG_DIM_KEY,DM.PRODUCT_AGG_DIM_KEY,DM.LM_SALES_FLAG,DM.MDP_FLAG) AS lM_SALES_FLAG_COUNT,
       COUNT(DM.P3M_SALES_FLAG) OVER (PARTITION BY DM.CUSTOMER_AGG_DIM_KEY,DM.PRODUCT_AGG_DIM_KEY,DM.P3M_SALES_FLAG,DM.MDP_FLAG) AS P3M_SALES_FLAG_COUNT,
       COUNT(DM.P6M_SALES_FLAG) OVER (PARTITION BY DM.CUSTOMER_AGG_DIM_KEY,DM.PRODUCT_AGG_DIM_KEY,DM.P6M_SALES_FLAG,DM.MDP_FLAG) AS P6M_SALES_FLAG_COUNT,
       COUNT(DM.P12M_SALES_FLAG) OVER (PARTITION BY DM.CUSTOMER_AGG_DIM_KEY,DM.PRODUCT_AGG_DIM_KEY,DM.P12M_SALES_FLAG,DM.MDP_FLAG) AS P12M_SALES_FLAG_COUNT,
       COUNT(DM.MDP_FLAG) OVER (PARTITION BY DM.CUSTOMER_AGG_DIM_KEY,DM.PRODUCT_AGG_DIM_KEY,DM.MDP_FLAG) AS MDP_FLAG_COUNT,
       SYSDATE() AS CREATED_DATE
FROM WKS_RPT_RETAIL_EXCELLENCE_SOP DM
left join retail_excellence_msl_target_final  msl_final on (DM.fisc_per=msl_final.fisc_per and upper(DM.global_product_brand)=upper(msl_final.global_product_brand)) 
),

final as (
select
fisc_yr::integer AS fisc_yr,
fisc_per::varchar(22) AS fisc_per,
cluster::varchar(100) AS cluster,
market::varchar(14) AS market,
data_src::varchar(8) AS data_src,
channel_name::varchar(50) AS channel_name,
distributor_code::varchar(100) AS distributor_code,
distributor_name::varchar(356) AS distributor_name,
sell_out_channel::varchar(500) AS sell_out_channel,
store_type::varchar(500) AS store_type,
prioritization_segmentation::varchar(1) AS prioritization_segmentation,
store_category::varchar(1) AS store_category,
store_code::varchar(100) AS store_code,
store_name::varchar(601) AS store_name,
store_grade::varchar(1) AS store_grade,
store_size::varchar(1) AS store_size,
region::varchar(150) AS region,
zone_name::varchar(150) AS zone_name,
city::varchar(1) AS city,
rtrlatitude::varchar(1) AS rtrlatitude,
rtrlongitude::varchar(1) AS rtrlongitude,
customer_segment_key::varchar(12) AS customer_segment_key,
customer_segment_description::varchar(50) AS customer_segment_description,
retail_environment::varchar(500) AS retail_environment,
sap_parent_customer_key::varchar(12) AS sap_parent_customer_key,
sap_parent_customer_description::varchar(75) AS sap_parent_customer_description,
sap_customer_channel_key::varchar(12) AS sap_customer_channel_key,
sap_customer_channel_description::varchar(75) AS sap_customer_channel_description,
sap_customer_sub_channel_key::varchar(12) AS sap_customer_sub_channel_key,
sap_sub_channel_description::varchar(75) AS sap_sub_channel_description,
sap_banner_key::varchar(12) AS sap_banner_key,
sap_banner_description::varchar(1) AS sap_banner_description,
sap_banner_format_key::varchar(1) AS sap_banner_format_key,
sap_banner_format_description::varchar(75) AS sap_banner_format_description,
customer_name::varchar(1) AS customer_name,
customer_code::varchar(1) AS customer_code,
product_code::varchar(150) AS product_code,
product_name::varchar(300) AS product_name,
prod_hier_l1::varchar(14) AS prod_hier_l1,
prod_hier_l2::varchar(1) AS prod_hier_l2,
prod_hier_l3::varchar(1) AS prod_hier_l3,
prod_hier_l4::varchar(1) AS prod_hier_l4,
prod_hier_l5::varchar(1) AS prod_hier_l5,
prod_hier_l6::varchar(1) AS prod_hier_l6,
prod_hier_l7::varchar(1) AS prod_hier_l7,
prod_hier_l8::varchar(1) AS prod_hier_l8,
prod_hier_l9::varchar(1) AS prod_hier_l9,
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
pka_franchise_desc::varchar(1) AS pka_franchise_desc,
pka_brand_desc::varchar(1) AS pka_brand_desc,
pka_sub_brand_desc::varchar(1) AS pka_sub_brand_desc,
pka_variant_desc::varchar(1) AS pka_variant_desc,
pka_sub_variant_desc::varchar(1) AS pka_sub_variant_desc,
global_product_franchise::varchar(1) AS global_product_franchise,
global_product_brand::varchar(200) AS global_product_brand,
global_product_sub_brand::varchar(1) AS global_product_sub_brand,
global_product_variant::varchar(1) AS global_product_variant,
global_product_category::varchar(1) AS global_product_category,
global_product_subcategory::varchar(1) AS global_product_subcategory,
global_product_segment::varchar(1) AS global_product_segment,
global_product_subsegment::varchar(1) AS global_product_subsegment,
global_put_up_description::varchar(1) AS global_put_up_description,
ean::varchar(150) AS ean,
sku_code::varchar(1) AS sku_code,
sku_description::varchar(1) AS sku_description,
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
p6m_sales::numeric(38,6) AS p6m_sales,
p6m_qty::numeric(38,6) AS p6m_qty,
p6m_avg_qty::numeric(10,2) AS p6m_avg_qty,
p6m_sales_lp::numeric(38,12) AS p6m_sales_lp,
p12m_sales::numeric(38,6) AS p12m_sales,
p12m_qty::numeric(38,6) AS p12m_qty,
p12m_avg_qty::numeric(10,2) AS p12m_avg_qty,
p12m_sales_lp::numeric(38,12) AS p12m_sales_lp,
f3m_sales::numeric(38,6) AS f3m_sales,
f3m_qty::numeric(38,6) AS f3m_qty,
f3m_avg_qty::numeric(10,2) AS f3m_avg_qty,
lm_sales_flag::varchar(1) AS lm_sales_flag,
p3m_sales_flag::varchar(1) AS p3m_sales_flag,
p6m_sales_flag::varchar(1) AS p6m_sales_flag,
p12m_sales_flag::varchar(1) AS p12m_sales_flag,
mdp_flag::varchar(1) AS mdp_flag,
target_complaince::numeric(38,6) AS target_complaince,
list_price::varchar(1) AS list_price,
total_sales_lm::numeric(38,6) AS total_sales_lm,
total_sales_p3m::numeric(38,6) AS total_sales_p3m,
total_sales_p6m::numeric(38,6) AS total_sales_p6m,
total_sales_p12m::numeric(38,6) AS total_sales_p12m,
total_sales_by_store_lm::numeric(38,6) AS total_sales_by_store_lm,
total_sales_by_store_p3m::numeric(38,6) AS total_sales_by_store_p3m,
total_sales_by_store_p6m::numeric(38,6) AS total_sales_by_store_p6m,
total_sales_by_store_p12m::numeric(38,6) AS total_sales_by_store_p12m,
total_sales_by_sku_lm::numeric(38,6) AS total_sales_by_sku_lm,
total_sales_by_sku_p3m::numeric(38,6) AS total_sales_by_sku_p3m,
total_sales_by_sku_p6m::numeric(38,6) AS total_sales_by_sku_p6m,
total_sales_by_sku_p12m::numeric(38,6) AS total_sales_by_sku_p12m,
total_sales_lm_lp::numeric(38,12) AS total_sales_lm_lp,
total_sales_p3m_lp::numeric(38,12) AS total_sales_p3m_lp,
total_sales_p6m_lp::numeric(38,12) AS total_sales_p6m_lp,
total_sales_p12m_lp::numeric(38,12) AS total_sales_p12m_lp,
total_sales_by_store_lm_lp::numeric(38,12) AS total_sales_by_store_lm_lp,
total_sales_by_store_p3m_lp::numeric(38,12) AS total_sales_by_store_p3m_lp,
total_sales_by_store_p6m_lp::numeric(38,12) AS total_sales_by_store_p6m_lp,
total_sales_by_store_p12m_lp::numeric(38,12) AS total_sales_by_store_p12m_lp,
total_sales_by_sku_lm_lp::numeric(38,12) AS total_sales_by_sku_lm_lp,
total_sales_by_sku_p3m_lp::numeric(38,12) AS total_sales_by_sku_p3m_lp,
total_sales_by_sku_p6m_lp::numeric(38,12) AS total_sales_by_sku_p6m_lp,
total_sales_by_sku_p12m_lp::numeric(38,12) AS total_sales_by_sku_p12m_lp,
store_contribution_lm::numeric(38,4) AS store_contribution_lm,
sku_contribution_lm::numeric(38,4) AS sku_contribution_lm,
size_of_price_lm::numeric(38,14) AS size_of_price_lm,
store_contribution_p3m::numeric(38,4) AS store_contribution_p3m,
sku_contribution_p3m::numeric(38,4) AS sku_contribution_p3m,
size_of_price_p3m::numeric(38,14) AS size_of_price_p3m,
store_contribution_p6m::numeric(38,4) AS store_contribution_p6m,
sku_contribution_p6m::numeric(38,4) AS sku_contribution_p6m,
size_of_price_p6m::numeric(38,14) AS size_of_price_p6m,
store_contribution_p12m::numeric(38,4) AS store_contribution_p12m,
sku_contribution_p12m::numeric(38,4) AS sku_contribution_p12m,
size_of_price_p12m::numeric(38,14) AS size_of_price_p12m,
store_contribution_lm_lp::numeric(38,4) AS store_contribution_lm_lp,
sku_contribution_lm_lp::numeric(38,4) AS sku_contribution_lm_lp,
size_of_price_lm_lp::numeric(38,20) AS size_of_price_lm_lp,
store_contribution_p3m_lp::numeric(38,4) AS store_contribution_p3m_lp,
sku_contribution_p3m_lp::numeric(38,4) AS sku_contribution_p3m_lp,
size_of_price_p3m_lp::numeric(38,20) AS size_of_price_p3m_lp,
store_contribution_p6m_lp::numeric(38,4) AS store_contribution_p6m_lp,
sku_contribution_p6m_lp::numeric(38,4) AS sku_contribution_p6m_lp,
size_of_price_p6m_lp::numeric(38,20) AS size_of_price_p6m_lp,
store_contribution_p12m_lp::numeric(38,4) AS store_contribution_p12m_lp,
sku_contribution_p12m_lp::numeric(38,4) AS sku_contribution_p12m_lp,
size_of_price_p12m_lp::numeric(38,20) AS size_of_price_p12m_lp,
lm_sales_flag_count::bigint AS lm_sales_flag_count,
p3m_sales_flag_count::bigint AS p3m_sales_flag_count,
p6m_sales_flag_count::bigint AS p6m_sales_flag_count,
p12m_sales_flag_count::bigint AS p12m_sales_flag_count,
mdp_flag_count::bigint AS mdp_flag_count,
created_date::timestamp AS created_date
from transformation
)

select * from final 