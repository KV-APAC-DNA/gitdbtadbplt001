with wks_vietnam_regional_sellout_npd as (
select * from {{ ref('vnmwks_integration__wks_vietnam_regional_sellout_npd') }}
),
itg_mds_ap_customer360_config as (
select * from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }}
),
transformed as (
select *,
	   case when sellout_sales_quantity<>0 then sellout_sales_value/sellout_sales_quantity else 0 end as selling_price,
	   count(case when first_scan_flag_market_level = 'Y' then first_scan_flag_market_level end) over (partition by country_name,pka_product_key) as cnt_mkt from (
select *,
	   case when first_scan_flag_parent_customer_level_initial='Y' and rn_cus=1 then 'Y' else 'N' end as first_scan_flag_parent_customer_level,
	   case when first_scan_flag_market_level_initial='Y' and first_scan_flag_parent_customer_level_initial='Y' and rn_mkt=1 then 'Y' else 'N' end as first_scan_flag_market_level from (
select *,
	   -- CASE WHEN rn_cus=1 AND Customer_Product_Min_Date>dateadd(week,(select param_value from rg_sdl.sdl_mds_ap_customer360_config where code='npd_buffer_weeks')::integer,Customer_Min_Date) THEN 'Y' else 'N' END AS FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
	   -- CASE WHEN rn_mkt=1 AND Market_Product_Min_Date>dateadd(week,(select param_value from rg_sdl.sdl_mds_ap_customer360_config where code='npd_buffer_weeks')::integer,Market_Min_Date) THEN 'Y' else 'N' END AS FIRST_SCAN_FLAG_MARKET_LEVEL
	   case when customer_product_min_date>dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_buffer_weeks')::integer,customer_min_date) then 'Y' else 'N' end as first_scan_flag_parent_customer_level_initial,
	   case when market_product_min_date>dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_buffer_weeks')::integer,market_min_date) then 'Y' else 'N' end as first_scan_flag_market_level_initial
from (
select year,
       qrtr_no,
       mnth_id,
       mnth_no,
       cal_date,
	   univ_year,
	   univ_month,
	   country_code,
       country_name,
       data_source,
       soldto_code,
       distributor_code,
       distributor_name,
       store_code,
       store_name,
	   store_type,
       distributor_additional_attribute1,
       distributor_additional_attribute2,
       distributor_additional_attribute3,
       sap_parent_customer_key,
       sap_parent_customer_description,
       sap_customer_channel_key,
       sap_customer_channel_description,
       sap_customer_sub_channel_key,
       sap_sub_channel_description,
       sap_go_to_mdl_key,
       sap_go_to_mdl_description,
       sap_banner_key,
       sap_banner_description,
       sap_banner_format_key,
       sap_banner_format_description,
       retail_environment,
       region,
       zone_or_area,
       customer_segment_key,
       customer_segment_description,
       global_product_franchise,
       global_product_brand,
       global_product_sub_brand,
       global_product_variant,
       global_product_segment,
       global_product_subsegment,
       global_product_category,
       global_product_subcategory,
       global_put_up_description,
       ean,
       sku_code,
       sku_description,
       --greenlight_sku_flag,
       pka_product_key,
       pka_product_key_description,
	   --sls_org,
	   customer_product_desc,
       from_currency,
       to_currency,
       exchange_rate,
       sellout_sales_quantity,
       sellout_sales_value,
       sellout_sales_value_usd,
       0 as sellout_value_list_price,
       0 as sellout_value_list_price_usd,
	   customer_min_date,
	   customer_product_min_date,
	   market_min_date,
	   market_product_min_date,
	   rn_cus,
	   rn_mkt,
       msl_product_code,
       msl_product_desc,
       retail_env,
       channel,
	   crtd_dttm,
	   updt_dttm   	   
from wks_vietnam_regional_sellout_npd
 )))
),
final as (
select
year::varchar(10) as year,
qrtr_no::varchar(14) as qrtr_no,
mnth_id::varchar(21) as mnth_id,
mnth_no::varchar(10) as mnth_no,
cal_date::date as cal_date,
univ_year::number(38,0) as univ_year,
univ_month::number(38,0) as univ_month,
country_code::varchar(2) as country_code,
country_name::varchar(7) as country_name,
data_source::varchar(8) as data_source,
soldto_code::varchar(300) as soldto_code,
distributor_code::varchar(30) as distributor_code,
distributor_name::varchar(750) as distributor_name,
store_code::varchar(100) as store_code,
store_name::varchar(100) as store_name,
store_type::varchar(300) as store_type,
distributor_additional_attribute1::varchar(2) as distributor_additional_attribute1,
distributor_additional_attribute2::varchar(2) as distributor_additional_attribute2,
distributor_additional_attribute3::varchar(2) as distributor_additional_attribute3,
sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
sap_parent_customer_description::varchar(75) as sap_parent_customer_description,
sap_customer_channel_key::varchar(12) as sap_customer_channel_key,
sap_customer_channel_description::varchar(75) as sap_customer_channel_description,
sap_customer_sub_channel_key::varchar(12) as sap_customer_sub_channel_key,
sap_sub_channel_description::varchar(75) as sap_sub_channel_description,
sap_go_to_mdl_key::varchar(12) as sap_go_to_mdl_key,
sap_go_to_mdl_description::varchar(75) as sap_go_to_mdl_description,
sap_banner_key::varchar(12) as sap_banner_key,
sap_banner_description::varchar(75) as sap_banner_description,
sap_banner_format_key::varchar(12) as sap_banner_format_key,
sap_banner_format_description::varchar(75) as sap_banner_format_description,
retail_environment::varchar(50) as retail_environment,
region::varchar(750) as region,
zone_or_area::varchar(750) as zone_or_area,
customer_segment_key::varchar(12) as customer_segment_key,
customer_segment_description::varchar(50) as customer_segment_description,
global_product_franchise::varchar(30) as global_product_franchise,
global_product_brand::varchar(30) as global_product_brand,
global_product_sub_brand::varchar(100) as global_product_sub_brand,
global_product_variant::varchar(100) as global_product_variant,
global_product_segment::varchar(50) as global_product_segment,
global_product_subsegment::varchar(100) as global_product_subsegment,
global_product_category::varchar(50) as global_product_category,
global_product_subcategory::varchar(50) as global_product_subcategory,
global_put_up_description::varchar(100) as global_put_up_description,
ean::varchar(100) as ean,
sku_code::varchar(40) as sku_code,
sku_description::varchar(150) as sku_description,
pka_product_key::varchar(68) as pka_product_key,
pka_product_key_description::varchar(255) as pka_product_key_description,
customer_product_desc::varchar(255) as customer_product_desc,
from_currency::varchar(3) as from_currency,
to_currency::varchar(3) as to_currency,
exchange_rate::number(15,5) as exchange_rate,
sellout_sales_quantity::number(38,23) as sellout_sales_quantity,
sellout_sales_value::number(38,23) as sellout_sales_value,
sellout_sales_value_usd::number(38,11) as sellout_sales_value_usd,
sellout_value_list_price::number(38,0) as sellout_value_list_price,
sellout_value_list_price_usd::number(38,0) as sellout_value_list_price_usd,
customer_min_date::date as customer_min_date,
customer_product_min_date::date as customer_product_min_date,
market_min_date::date as market_min_date,
market_product_min_date::date as market_product_min_date,
rn_cus::number(38,0) as rn_cus,
rn_mkt::number(38,0) as rn_mkt,
msl_product_code::varchar(100) as msl_product_code,
msl_product_desc::varchar(255) as msl_product_desc,
retail_env::varchar(450) as retail_env,
channel::varchar(300) as channel,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm,
first_scan_flag_parent_customer_level_initial::varchar(1) as first_scan_flag_parent_customer_level_initial,
first_scan_flag_market_level_initial::varchar(1) as first_scan_flag_market_level_initial,
first_scan_flag_parent_customer_level::varchar(1) as first_scan_flag_parent_customer_level,
first_scan_flag_market_level::varchar(1) as first_scan_flag_market_level,
selling_price::number(38,19) as selling_price,
cnt_mkt::number(38,0) as cnt_mkt
from transformed
)
select * from final