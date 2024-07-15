--import cte
with wks_korea_base_retail_excellence as (
    select * from {{ ref('natwks_integration__wks_korea_base_retail_excellence') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
--final cte
korea_re_basedim_values  as (
SELECT DISTINCT CNTRY_CD,
       CNTRY_NM,
       DIM_KEY,
       DATA_SOURCE,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       SOLDTO_CODE,
       STORE_CODE,
       STORE_NAME,
       STORE_TYPE,
	   CHANNEL,
       --MASTER_CODE,
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
       RETAIL_ENVIRONMENT,
       REGION,
       ZONE_OR_AREA,
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
       EAN,
	   --CUSTOMER_PRODUCT_DESC,
       SKU_CODE,
	   SKU_DESCRIPTION,
	   STORE_GRADE,
       PKA_PRODUCT_KEY,
       PKA_PRODUCT_KEY_DESCRIPTION
FROM WKS_KOREA_BASE_RETAIL_EXCELLENCE		
WHERE   MNTH_ID >= (SELECT last_27mnths
                              FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)::NUMERIC		                       
            AND   MNTH_ID <= (SELECT prev_mnth FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)::NUMERIC		

),
final as 
(
    select 
  cntry_cd::varchar(2) AS cntry_cd,
cntry_nm::varchar(50) AS cntry_nm,
dim_key::varchar(32) AS dim_key,
data_source::varchar(14) AS data_source,
distributor_code::varchar(150) AS distributor_code,
distributor_name::varchar(534) AS distributor_name,
soldto_code::varchar(255) AS soldto_code,
store_code::varchar(150) AS store_code,
store_name::varchar(901) AS store_name,
store_type::varchar(382) AS store_type,
channel::varchar(225) AS channel,
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
retail_environment::varchar(150) AS retail_environment,
region::varchar(150) AS region,
zone_or_area::varchar(150) AS zone_or_area,
customer_segment_key::varchar(12) AS customer_segment_key,
customer_segment_description::varchar(50) AS customer_segment_description,
global_product_franchise::varchar(30) AS global_product_franchise,
global_product_brand::varchar(30) AS global_product_brand,
global_product_sub_brand::varchar(100) AS global_product_sub_brand,
global_product_variant::varchar(100) AS global_product_variant,
global_product_segment::varchar(50) AS global_product_segment,
global_product_subsegment::varchar(100) AS global_product_subsegment,
global_product_category::varchar(50) AS global_product_category,
global_product_subcategory::varchar(50) AS global_product_subcategory,
global_put_up_description::varchar(100) AS global_put_up_description,
ean::varchar(150) AS ean,
sku_code::varchar(40) AS sku_code,
sku_description::varchar(300) AS sku_description,
store_grade::varchar(150) AS store_grade,
pka_product_key::varchar(68) AS pka_product_key,
pka_product_key_description::varchar(255) AS pka_product_key_description

from korea_re_basedim_values
)

--final select
select * from final