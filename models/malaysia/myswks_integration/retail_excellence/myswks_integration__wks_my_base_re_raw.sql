--import cte
with edw_rpt_regional_sellout_offtake as (   
    select * from {{ source('aspedw_integration','edw_rpt_regional_sellout_offtake') }}
),
wks_my_regional_sellout_pos_ean_lookup as (
    select * from {{ ref('myswks_integration__wks_my_regional_sellout_pos_ean_lookup') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),

MY_BASE_RE_RAW  as (
SELECT country_code,
             country_name,
             data_source,
             DISTRIBUTOR_CODE,
             DISTRIBUTOR_NAME,
             SOLDTO_CODE,
             cal_date,
             ltrim(STORE_CODE,0) as store_code,
		    case when data_source = 'SELL-OUT' then msl_product_code
		         
			      when data_source = 'POS' then LTRIM(EANLKUP.EAN,0)	
		     END as EAN,
		     store_name,
		     list_price,
		     msl_product_desc,
		     BASE.SKU_CODE,		
 			 Region,zone_or_area,
 			 store_type,
			 store_grade,
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
             MAX(GLOBAL_PUT_UP_DESCRIPTION) over ( PARTITION BY nvl(base.EAN,eanlkup.ean) ORDER BY base.mnth_id DESC  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS GLOBAL_PUT_UP_DESCRIPTION ,
             PKA_PRODUCT_KEY,
             PKA_PRODUCT_KEY_DESCRIPTION,
             YEAR,
             BASE.MNTH_ID,		
             sellout_value_list_price,
             SELLOUT_SALES_QUANTITY,
             SELLOUT_SALES_VALUE
      from  edw_rpt_regional_sellout_offtake base		
      LEFT JOIN (SELECT MNTH_ID,SKU_CODE,EAN FROM wks_my_regional_sellout_pos_ean_lookup) EANLKUP ON BASE.MNTH_ID = EANLKUP.MNTH_ID and		
																											BASE.SKU_CODE = EANLKUP.SKU_CODE		
	  WHERE COUNTRY_CODE = 'MY' and data_source in ('SELL-OUT','POS')
      and base.MNTH_ID >= (select last_28mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
	  and base.mnth_id <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
	  ),
	  
	--final select
final as
(
 select 
country_code::varchar(2) as country_code ,
country_name::varchar(30) as country_name ,
data_source::varchar(14) as data_source ,
distributor_code::varchar(100) as distributor_code ,
distributor_name::varchar(255) as distributor_name ,
soldto_code::varchar(255) as soldto_code ,
cal_date::date as cal_date ,
store_code::varchar(100) as store_code ,
ean::varchar(500) as ean ,
store_name::varchar(500) as store_name ,
list_price::numeric(38,6) as list_price ,
msl_product_desc::varchar(300) as msl_product_desc ,
sku_code::varchar(40) as sku_code ,
region::varchar(150) as region ,
zone_or_area::varchar(150) as zone_or_area ,
store_type::varchar(255) as store_type ,
store_grade::varchar(150) as store_grade ,
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
year::integer as year ,
mnth_id::varchar(23) as mnth_id ,
sellout_value_list_price::numeric(38,12) as sellout_value_list_price ,
sellout_sales_quantity::numeric(38,6) as sellout_sales_quantity ,
sellout_sales_value::numeric(38,6) as sellout_sales_value 
 from MY_BASE_RE_RAW 
)
select * from final 