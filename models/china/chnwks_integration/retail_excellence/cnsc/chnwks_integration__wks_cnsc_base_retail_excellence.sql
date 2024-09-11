with EDW_RPT_REGIONAL_SELLOUT_OFFTAKE as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),
itg_CS_re_store as (
    select * from {{ source('chnitg_integration', 'itg_cs_re_store') }}
),
edw_vw_cal_Retail_excellence_Dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
itg_mds_cn_otc_product_mapping as (
    select * from {{ source('chnitg_integration', 'itg_mds_cn_otc_product_mapping') }}
),

itg_query_parameters as (
    select * from {{ source('aspitg_integration' , 'itg_query_parameters')}}
), 


transformation as (
SELECT COUNTRY_CODE AS CNTRY_CD,
MD5(NVL (DISTRIBUTOR_CODE,'DC') ||NVL (DISTRIBUTOR_NAME,'DN') ||NVL (STORE_CODE,'SC') || NVL (STORE_NAME,'SN') 
|| NVL (msl_product_code,'PC') ||NVL (msl_product_desc,'PN') ||NVL (CUSTOMER_PRODUCT_DESC,'CPD') || NVL (STORE_TYPE,'ST') 
||NVL (SAP_PARENT_CUSTOMER_KEY,'SPCK') ||NVL (SAP_PARENT_CUSTOMER_DESCRIPTION,'SPSCD') || NVL (SAP_CUSTOMER_CHANNEL_KEY,'SCCK') 
||NVL (SAP_CUSTOMER_CHANNEL_DESCRIPTION,'SCCD') ||NVL (SAP_CUSTOMER_SUB_CHANNEL_KEY,'SCSCK') ||NVL (SAP_SUB_CHANNEL_DESCRIPTION,'SSCD') 
||NVL (CUSTOMER_SEGMENT_KEY,'CSK') ||NVL (CUSTOMER_SEGMENT_DESCRIPTION,'CSD') || NVL (SAP_GO_TO_MDL_KEY,'SGMK') ||NVL (SAP_GO_TO_MDL_DESCRIPTION,'SGMD')
||NVL (SAP_BANNER_KEY,'SBK') ||NVL (SAP_BANNER_DESCRIPTION,'SBD') ||NVL (SAP_BANNER_FORMAT_KEY,'SBFK') ||NVL (SAP_BANNER_FORMAT_DESCRIPTION,'SBFD') 
||NVL (retail_env,'RE') ||NVL (PKA_PRODUCT_KEY,'PK') ||NVL (REGION,'region') ||NVL (ZONE_OR_AREA,'zone') ||NVL (PKA_PRODUCT_KEY_DESCRIPTION,'PKPD') 
||NVL (SKU_DESCRIPTION,'SD') ||NVL (SKU_CODE,'SC')||NVL (PRODUCT_BRAND,'PD')||NVL (CHANNEL,'CHANNEL'))
 AS SELLOUT_DIM_KEY,
       COUNTRY_NAME AS CNTRY_NM,
       DATA_SOURCE AS DATA_SRC,
       YEAR,
       MNTH_ID,
       DISTRIBUTOR_CODE,
       Distributor_Name,
       STORE_CODE,
       store_name,
       store_type,
       msl_product_code,-------------------------------------ADDED
       msl_product_desc,----------------------------------------ADDED
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
             retail_env,
             REGION,
             ZONE_OR_AREA,
             CUSTOMER_SEGMENT_KEY,
             CUSTOMER_SEGMENT_DESCRIPTION,
             PKA_PRODUCT_KEY,
             PKA_PRODUCT_KEY_DESCRIPTION,  
             SKU_CODE,
             SKU_DESCRIPTION,			
            SUM(SELLOUT_SALES_QUANTITY) AS SLS_QTY,
            SUM(SELLOUT_SALES_VALUE) AS SLS_VALUE,
            CAST(AVG(SELLOUT_SALES_QUANTITY) AS numeric(38,6)) AS AVG_QTY,
            SUM(SALES_VALUE_LIST_PRICE)AS SALES_VALUE_LIST_PRICE,
            PRODUCT_BRAND,
			      channel,
            SYSDATE() as Created_date
FROM (SELECT COUNTRY_CODE,
             COUNTRY_NAME,
             DATA_SOURCE,
             YEAR,
             MNTH_ID,
             DISTRIBUTOR_CODE,
             SELLOUT.Distributor_Name||'#'||LTRIM(SELLOUT.Distributor_CODE,'0') AS Distributor_Name,
             STORE_CODE,
             SELLOUT.STORE_Name||'#'||ltrim(SELLOUT.STORE_CODE,'0') AS STORE_NAME,
             SELLOUT.store_type,
             msl_product_CODE,
             msl_product_desc,      
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
             SELLOUT.retail_env,
             REGION,
             ZONE_OR_AREA,
             CUSTOMER_SEGMENT_KEY,
             CUSTOMER_SEGMENT_DESCRIPTION,
             SKU_CODE,
             SKU_DESCRIPTION,
             PKA_PRODUCT_KEY,
             PKA_PRODUCT_KEY_DESCRIPTION,
             SELLOUT_SALES_QUANTITY,
             SELLOUT_SALES_VALUE,
             SALES_VALUE_LIST_PRICE,
             LOCALBRAND.brand_en AS PRODUCT_BRAND,
			       st.channel as channel
       FROM(SELECT COUNTRY_CODE,
             COUNTRY_NAME,
             DATA_SOURCE,
             YEAR,
             MNTH_ID,
             DISTRIBUTOR_CODE,
             max(distributor_name) over (PARTITION BY distributor_code ORDER BY cal_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS distributor_name,
             STORE_CODE,
             max(store_name) over (PARTITION BY store_code ORDER BY cal_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS STORE_NAME,
             store_type,
             msl_product_CODE,
             --msl_product_desc,  
             max(msl_product_desc) over (PARTITION BY msl_product_CODE ORDER BY cal_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msl_product_desc,			 
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
             retail_env,
             REGION,
             ZONE_OR_AREA,
             CUSTOMER_SEGMENT_KEY,
             CUSTOMER_SEGMENT_DESCRIPTION,
             SKU_CODE,
             SKU_DESCRIPTION,
             PKA_PRODUCT_KEY,
             PKA_PRODUCT_KEY_DESCRIPTION,
             SELLOUT_SALES_QUANTITY,
             SELLOUT_SALES_VALUE,
             sellout_value_list_price as SALES_VALUE_LIST_PRICE
      FROM EDW_RPT_REGIONAL_SELLOUT_OFFTAKE 
       WHERE COUNTRY_NAME='China Selfcare' and lower(store_type) not in 
       (Select distinct parameter_value from itg_query_parameters where parameter_name='EXCLUDE_RE_BASE_RET_ENV' and country_code='CNSC')
       --AND MNTH_ID >= (select last_37mnths. from edw_vw_cal_Retail_excellence_Dim)
       --CHANGED MONTH LOGIC 37 -> 28
       AND MNTH_ID >= (select last_28mnths from edw_vw_cal_Retail_excellence_Dim)
	  and mnth_id <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)

	   )SELLOUT
      LEFT JOIN (SELECT DISTINCT xjp_code, brand_en FROM itg_mds_cn_otc_product_mapping)localbrand ON localbrand.xjp_code=SELLOUT.msl_product_CODE 
      LEFT JOIN (SELECT distinct store_type,
                          channel,
                          product_code
						   FROM itg_CS_re_store) st
               ON SELLOUT.Retail_Env = st.store_type
              AND SELLOUT.msl_product_code = st.product_code	  
      )
GROUP BY  COUNTRY_CODE,
         COUNTRY_NAME,
         DATA_SOURCE,
         YEAR,
         MNTH_ID,
         DISTRIBUTOR_CODE,
         DISTRIBUTOR_NAME,
         STORE_CODE,
         STORE_NAME,
         STORE_TYPE,
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
         retail_env,
         MSL_PRODUCT_CODE,
         MSL_PRODUCT_DESC,
         REGION,
		     channel,
         ZONE_OR_AREA,
         CUSTOMER_SEGMENT_KEY,
         CUSTOMER_SEGMENT_DESCRIPTION,
         SKU_CODE,
		     SKU_DESCRIPTION,
		     PRODUCT_BRAND,
         PKA_PRODUCT_KEY,
         PKA_PRODUCT_KEY_DESCRIPTION),

final as (
    select
cntry_cd::varchar(2) AS cntry_cd,
sellout_dim_key::varchar(32) AS sellout_dim_key,
cntry_nm::varchar(50) AS cntry_nm,
data_src::varchar(14) AS data_src,
year::integer AS year,
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
sku_code::varchar(40) AS sku_code,
sku_description::varchar(150) AS sku_description,
sls_qty::numeric(38,6) AS sls_qty,
sls_value::numeric(38,6) AS sls_value,
avg_qty::numeric(38,6) AS avg_qty,
sales_value_list_price::numeric(38,12) AS sales_value_list_price,
product_brand::varchar(200) AS product_brand,
channel::varchar(50) AS channel,
created_date::timestamp AS created_date
from transformation
)         


select * from final         