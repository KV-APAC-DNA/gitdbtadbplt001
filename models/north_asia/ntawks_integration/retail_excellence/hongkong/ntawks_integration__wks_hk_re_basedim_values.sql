--import cte

with wks_hk_base_retail_excellence as (
    select * from {{ ref('ntawks_integration__wks_hk_base_retail_excellence')}}
),

--final cte

wks_hk_re_basedim_values as 
(
    SELECT DISTINCT CNTRY_CD,
       CNTRY_NM,
       DIM_KEY,
       DATA_SOURCE,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       SOLDTO_CODE,
       STORE_CODE,
       STORE_NAME,
	   LIST_PRICE,
       STORE_TYPE,
	   CHANNEL,
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
       SKU_CODE,
	   SKU_DESCRIPTION,
	   STORE_GRADE,
       PKA_PRODUCT_KEY,
       PKA_PRODUCT_KEY_DESCRIPTION
    FROM WKS_HK_BASE_RETAIL_EXCELLENCE
),

final as 
(
    select cntry_cd :: varchar(2) as cntry_cd,
    cntry_nm :: varchar(30) as cntry_nm,
    dim_key :: varchar(32) as dim_key,
    data_source :: varchar(14) as data_source,
    distributor_code :: varchar(150) as distributor_code,
    distributor_name :: varchar(534) as distributor_name,
    soldto_code :: varchar(255) as soldto_code,
    store_code :: varchar(150) as store_code,
    store_name :: varchar(901) as store_name,
    list_price :: numeric(38,6) as list_price,
    store_type :: varchar(382) as store_type,
    channel :: varchar(225) as channel,
    sap_parent_customer_key :: varchar(12) as sap_parent_customer_key,
    sap_parent_customer_description :: varchar(75) as sap_parent_customer_description,
    sap_customer_channel_key :: varchar(12) as sap_customer_channel_key,
    sap_customer_channel_description :: varchar(75) as sap_customer_channel_description,
    sap_customer_sub_channel_key :: varchar(12) as sap_customer_sub_channel_key,
    sap_sub_channel_description :: varchar(75) as sap_sub_channel_description,
    sap_go_to_mdl_key :: varchar(12) as sap_go_to_mdl_key,
    sap_go_to_mdl_description :: varchar(75) as sap_go_to_mdl_description,
    sap_banner_key :: varchar(12) as sap_banner_key,
    sap_banner_description :: varchar(75) as sap_banner_description,
    sap_banner_format_key :: varchar(12) as sap_banner_format_key,
    sap_banner_format_description :: varchar(75) as sap_banner_format_description,
    retail_environment :: varchar(225) as retail_environment,
    region :: varchar(150) as region,
    zone_or_area :: varchar(150) as zone_or_area,
    customer_segment_key :: varchar(12) as customer_segment_key,
    customer_segment_description :: varchar(50) as customer_segment_description,
    global_product_franchise :: varchar(30) as global_product_franchise,
    global_product_brand :: varchar(30) as global_product_brand,
    global_product_sub_brand :: varchar(100) as global_product_sub_brand,
    global_product_variant :: varchar(100) as global_product_variant,
    global_product_segment :: varchar(50) as global_product_segment,
    global_product_subsegment :: varchar(100) as global_product_subsegment,
    global_product_category :: varchar(50) as global_product_category,
    global_product_subcategory :: varchar(50) as global_product_subcategory,
    global_put_up_description :: varchar(100) as global_put_up_description,
    ean :: varchar(150) as ean,
    sku_code :: varchar(40) as sku_code,
    sku_description :: varchar(300) as sku_description,
    store_grade :: varchar(150) as store_grade,
    pka_product_key :: varchar(68) as pka_product_key,
    pka_product_key_description :: varchar(255) as pka_product_key_description
    from wks_hk_re_basedim_values
)

--final select

select * from final
