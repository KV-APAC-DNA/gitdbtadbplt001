with WKS_HONG_KONG_REGIONAL_SELLOUT as (
    select * from {{ ref('ntawks_integration__wks_hong_kong_regional_sellout') }}
),
transformed as (
SELECT *,
Min(cal_date) Over (Partition By sap_parent_customer_key) as Customer_Min_Date,
Min(cal_date) Over (Partition By country_name) as Market_Min_Date,
RANK() OVER (PARTITION BY sap_parent_customer_key,pka_product_key ORDER BY cal_date) AS rn_cus,
RANK() OVER (PARTITION BY country_name,pka_product_key ORDER BY cal_date) AS rn_mkt,
Min(cal_date) Over (Partition By sap_parent_customer_key,pka_product_key) as Customer_Product_Min_Date, 
Min(cal_date) Over (Partition By country_name,pka_product_key) as Market_Product_Min_Date
FROM WKS_HONG_KONG_REGIONAL_SELLOUT
),
final as (
select
year::varchar(10) as year,
qrtr_no::varchar(14) as qrtr_no,
mnth_id::varchar(21) as mnth_id,
mnth_no::varchar(10) as mnth_no,
cal_date::date as cal_date,
univ_year::number(18,0) as univ_year,
univ_month::number(18,0) as univ_month,
country_code::varchar(2) as country_code,
country_name::varchar(9) as country_name,
data_source::varchar(8) as data_source,
soldto_code::varchar(100) as soldto_code,
distributor_code::varchar(10) as distributor_code,
distributor_name::varchar(100) as distributor_name,
store_code::varchar(50) as store_code,
store_name::varchar(100) as store_name,
store_type::varchar(100) as store_type,
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
region::varchar(2) as region,
zone_or_area::varchar(2) as zone_or_area,
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
sku_code::varchar(255) as sku_code,
sku_description::varchar(150) as sku_description,
pka_product_key::varchar(68) as pka_product_key,
pka_product_key_description::varchar(255) as pka_product_key_description,
customer_product_desc::varchar(255) as customer_product_desc,
from_currency::varchar(3) as from_currency,
to_currency::varchar(3) as to_currency,
exchange_rate::number(15,5) as exchange_rate,
sellout_sales_quantity::number(38,0) as sellout_sales_quantity,
sellout_sales_value::number(38,5) as sellout_sales_value,
sellout_sales_value_usd::number(38,11) as sellout_sales_value_usd,
msl_product_code::varchar(20) as msl_product_code,
msl_product_desc::varchar(255) as msl_product_desc,
store_grade::varchar(200) as store_grade,
retail_env::varchar(300) as retail_env,
channel::varchar(500) as channel,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm,
customer_min_date::date as customer_min_date,
market_min_date::date as market_min_date,
rn_cus::number(38,0) as rn_cus,
rn_mkt::number(38,0) as rn_mkt,
customer_product_min_date::date as customer_product_min_date,
market_product_min_date::date as market_product_min_date
from transformed
)
select * from final