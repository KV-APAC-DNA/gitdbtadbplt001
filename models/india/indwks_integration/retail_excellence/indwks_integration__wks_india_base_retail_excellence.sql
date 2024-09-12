with edw_list_price as (
select * from {{ ref('aspedw_integration__edw_list_price') }}
),
edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),
edw_sales_org_dim as 
(
select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
sdl_raw_sap_bw_price_list as
(
select * from {{ ref('aspitg_integration__sdl_raw_sap_bw_price_list') }}
),
EDW_VW_CAL_RETAIL_EXCELLENCE_DIM as
(
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),

wks_india_base_retail_excellence as (

    SELECT COUNTRY_CODE AS CNTRY_CD,
       COUNTRY_NAME AS CNTRY_NM,
       DATA_SOURCE,
       MD5(nvl(DISTRIBUTOR_CODE,'dc')||nvl(DISTRIBUTOR_NAME,'dn')||nvl (STORE_CODE,'rtr')||nvl(STORE_NAME,'sn')
           ||nvl(STORE_TYPE,'st')||nvl(REGION_NAME,'rn')||nvl(ZONE_NAME,'zn')||nvl(TOWN_NAME,'tn')
           ||nvl(BUSINESS_CHANNEL,'bc')||nvl(RETAILER_CATEGORY_NAME,'rcn')|| nvl(CLASS_DESC,'cd')
           ||MOTHERSKU_CODE||nvl(MOTHERSKU_NAME,'msn')
           ||nvl(SAP_PARENT_CUSTOMER_KEY, 'spck')||nvl(SAP_PARENT_CUSTOMER_DESCRIPTION,'spscd') 
           ||nvl(SAP_CUSTOMER_CHANNEL_KEY,'scck')||nvl(SAP_CUSTOMER_CHANNEL_DESCRIPTION,'sccd') 
           ||nvl(SAP_CUSTOMER_SUB_CHANNEL_KEY, 'scsck')||nvl(SAP_SUB_CHANNEL_DESCRIPTION,'sscd')
           ||nvl(CUSTOMER_SEGMENT_KEY,'csk')||nvl(CUSTOMER_SEGMENT_DESCRIPTION,'csd')
           ||nvl(SAP_GO_TO_MDL_KEY,'sgmk')||nvl(SAP_GO_TO_MDL_DESCRIPTION,'sgmd')
           ||nvl(SAP_BANNER_KEY,'sbk')||nvl(SAP_BANNER_DESCRIPTION,'sbd') 
           ||nvl(SAP_BANNER_FORMAT_KEY,'sbfk')||nvl (SAP_BANNER_FORMAT_DESCRIPTION,'sbfd') 
           ||nvl(RETAIL_ENVIRONMENT,'re')
           ||nvl (GLOBAL_PRODUCT_FRANCHISE,'gpf') ||nvl (GLOBAL_PRODUCT_BRAND,'gpb') 
           ||nvl (GLOBAL_PRODUCT_SUB_BRAND,'gpsb') ||nvl (GLOBAL_PRODUCT_VARIANT,'gpv') 
           ||nvl (GLOBAL_PRODUCT_SEGMENT,'gps')||nvl (GLOBAL_PRODUCT_SUBSEGMENT,'gpss') 
           ||nvl (GLOBAL_PRODUCT_CATEGORY,'gpc') ||nvl (GLOBAL_PRODUCT_SUBCATEGORY,'gpsc') 
           ||nvl (GLOBAL_PUT_UP_DESCRIPTION,'gpud') ||nvl(PKA_PRODUCT_KEY,'pk') ||nvl(PKA_PRODUCT_KEY_DESCRIPTION,'pkd')
           ) AS SELLOUT_DIM_KEY,--add gcph hier as well   
       YEAR,
       MNTH_ID,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       --SOLDTO_CODE,
       STORE_CODE,
       STORE_NAME,
       STORE_TYPE,
       REGION_NAME,
       ZONE_NAME,
       TOWN_NAME,
       BUSINESS_CHANNEL,
       RETAILER_CATEGORY_NAME,
       CLASS_DESC,
       --RTRLATITUDE,
       --RTRLONGITUDE,
       MOTHERSKU_CODE,
       MOTHERSKU_NAME,
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
       --REGION,
       --ZONE_OR_AREA,
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
       --EAN,
       --SKU_CODE,SKU_DESCRIPTION,
       PKA_PRODUCT_KEY,
       PKA_PRODUCT_KEY_DESCRIPTION,
       CAST(SUM(SELLOUT_SALES_QUANTITY) AS DECIMAL(38,6)) AS SO_SLS_QTY,
       CAST(SUM(SELLOUT_SALES_VALUE) AS NUMERIC(38,6)) AS SO_SLS_VALUE,
       CAST(AVG(SELLOUT_SALES_QUANTITY) AS DECIMAL(38,6)) AS SO_AVG_QTY,
       SUM(SALES_VALUE_LIST_PRICE) AS SALES_VALUE_LIST_PRICE,
	   SYSDATE() AS CRT_DTTM from
	   
(SELECT COUNTRY_CODE ,
       COUNTRY_NAME,
       DATA_SOURCE, 
       YEAR,
       MNTH_ID,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       --SOLDTO_CODE,
       distributor_additional_attribute1 as STORE_CODE,--RTRUNIQUECODE AS STORE_CODE,
       upper(STORE_NAME) as STORE_NAME,
       STORE_TYPE,
       region as REGION_NAME,
       zone_or_area as ZONE_NAME,
       'NA' as TOWN_NAME,
       channel as BUSINESS_CHANNEL,
       retail_env as RETAILER_CATEGORY_NAME,
       store_grade as CLASS_DESC,
       --RTRLATITUDE, distributor_additional_attribute2 as RTRLATITUDE,
       --RTRLONGITUDE, distributor_additional_attribute3 as RTRLONGITUDE,
       msl_product_code as MOTHERSKU_CODE,
       msl_product_desc as MOTHERSKU_NAME,
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
       --REGION,
       --ZONE_OR_AREA,
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
       SKU_CODE,SKU_DESCRIPTION,
       PKA_PRODUCT_KEY,
       PKA_PRODUCT_KEY_DESCRIPTION,
       SELLOUT_SALES_QUANTITY,
       SELLOUT_SALES_VALUE,
       ((NVL(LP1.AMOUNT::NUMERIC(38,6),LP2.AMOUNT::NUMERIC(38,6)))*MAIN.SELLOUT_SALES_QUANTITY) AS SALES_VALUE_LIST_PRICE
        FROM (select * FROM EDW_RPT_REGIONAL_SELLOUT_OFFTAKE 
WHERE COUNTRY_CODE = 'IN' and DATA_SOURCE = 'SELL-OUT') MAIN --IN_EDW.EDW_INDIA_REGIONAL_SELLOUT MAIN
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
	   ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
	   (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list where sls_org IN 
	   --('5100','6100','510A','6000')
	   (select distinct sls_org from edw_sales_org_dim where stats_crncy IN ('INR', 'LKR', 'BDT'))
	   group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
	   from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
	   TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
	   to_date(valid_to, 'YYYYMMDD'))   
	   left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
	   ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
	   WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key = 'IN')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0'))
        
      WHERE DATA_SOURCE = 'SELL-OUT'
	  and MNTH_ID >= (select last_27mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
	  and mnth_id <= (select prev_mnth from edw_vw_cal_Retail_excellence_Dim)::numeric
      --AND   MNTH_ID >(SELECT TO_CHAR(add_months ((SELECT to_date(MAX(mnth_id),'YYYYMM') FROM edw_rpt_regional_sellout_offtake where country_code='IN' and data_source='SELL-OUT'  ),-(SELECT CAST(parameter_value AS INT) FROM in_itg.itg_query_parameters WHERE parameter_name = 'RETAIL_EXCELLENCE_BASE_NO_OF_MONTHS')),'yyyymm'))
     -- AND   MNTH_ID <= TO_CHAR(SYSDATE,'YYYYMM')
     --AND  NOT (NVL(SELLOUT_SALES_QUANTITY,0) = 0 AND NVL(SELLOUT_SALES_QUANTITY,0) = 0) 
     GROUP BY COUNTRY_CODE,
       COUNTRY_NAME,
       DATA_SOURCE,
        YEAR,
       MNTH_ID,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       --SOLDTO_CODE,
       STORE_CODE,
       STORE_NAME,
       STORE_TYPE,
       REGION_NAME ,
       ZONE_NAME ,
       TOWN_NAME,
       BUSINESS_CHANNEL,
       RETAILER_CATEGORY_NAME,
       CLASS_DESC,
       --RTRLATITUDE,
       --RTRLONGITUDE,
       MOTHERSKU_CODE,
       MOTHERSKU_NAME,
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
       --REGION,
       --ZONE_OR_AREA,
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
       --EAN,
       --SKU_CODE,SKU_DESCRIPTION,
       PKA_PRODUCT_KEY,
       PKA_PRODUCT_KEY_DESCRIPTION
),

final as 
(
select 
cntry_cd::varchar(2) AS cntry_cd,
cntry_nm::varchar(50) AS cntry_nm,
data_source::varchar(14) AS data_source,
sellout_dim_key::varchar(32) AS sellout_dim_key,
year::numeric(18,0) AS year,
mnth_id::varchar(23) AS mnth_id,
distributor_code::varchar(100) AS distributor_code,
distributor_name::varchar(255) AS distributor_name,
store_code::varchar(150) AS store_code,
store_name::varchar(750) AS store_name,
store_type::varchar(255) AS store_type,
region_name::varchar(150) AS region_name,
zone_name::varchar(150) AS zone_name,
town_name::varchar(2) AS town_name,
business_channel::varchar(150) AS business_channel,
retailer_category_name::varchar(150) AS retailer_category_name,
class_desc::varchar(150) AS class_desc,
mothersku_code::varchar(150) AS mothersku_code,
mothersku_name::varchar(300) AS mothersku_name,
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
retail_environment::varchar(50) AS retail_environment,
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
pka_product_key::varchar(68) AS pka_product_key,
pka_product_key_description::varchar(255) AS pka_product_key_description,
so_sls_qty::numeric(38,6) AS so_sls_qty,
so_sls_value::numeric(38,6) AS so_sls_value,
so_avg_qty::numeric(38,6) AS so_avg_qty,
sales_value_list_price::numeric(38,12) AS sales_value_list_price,
crt_dttm::timestamp AS crt_dttm

from wks_india_base_retail_excellence

)

select * from final 