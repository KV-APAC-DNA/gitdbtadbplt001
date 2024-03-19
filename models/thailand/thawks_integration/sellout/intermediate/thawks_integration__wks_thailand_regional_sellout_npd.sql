with wks_thailand_regional_sellout as (
select * from {{ ref('thawks_integration__wks_thailand_regional_sellout') }}
),
transformed as (
select 
  *, 
  min(cal_date) over (
    partition by sap_parent_customer_key
  ) as customer_min_date, 
  min(cal_date) over (partition by country_name) as market_min_date, 
  rank() over (
    partition by sap_parent_customer_key, 
    pka_product_key 
    order by 
      cal_date
  ) as rn_cus, 
  rank() over (
    partition by country_name, 
    pka_product_key 
    order by 
      cal_date
  ) as rn_mkt, 
  min(cal_date) over (
    partition by sap_parent_customer_key, 
    pka_product_key
  ) as customer_product_min_date, 
  min(cal_date) over (
    partition by country_name, pka_product_key
  ) as market_product_min_date 
from 
  wks_thailand_regional_sellout
),
final as (
select
    year::number(18,0) as year,
    qrtr_no::varchar(11) as qrtr_no,
    mnth_id::varchar(23) as mnth_id,
    mnth_no::number(18,0) as mnth_no,
    cal_date::date as cal_date,
    day::timestamp_ntz(9) as day,
    country_code::varchar(2) as country_code,
    country_name::varchar(8) as country_name,
    data_source::varchar(14) as data_source,
    soldto_code::varchar(255) as soldto_code,
    distributor_code::varchar(12) as distributor_code,
    distributor_name::varchar(100) as distributor_name,
    store_code::varchar(20) as store_code,
    store_name::varchar(500) as store_name,
    store_type::varchar(50) as store_type,
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
    customer_segment_key::varchar(1) as customer_segment_key,
    customer_segment_description::varchar(1) as customer_segment_description,
    global_product_franchise::varchar(30) as global_product_franchise,
    global_product_brand::varchar(30) as global_product_brand,
    global_product_sub_brand::varchar(100) as global_product_sub_brand,
    global_product_variant::varchar(100) as global_product_variant,
    global_product_segment::varchar(50) as global_product_segment,
    global_product_subsegment::varchar(100) as global_product_subsegment,
    global_product_category::varchar(50) as global_product_category,
    global_product_subcategory::varchar(50) as global_product_subcategory,
    global_put_up_description::varchar(100) as global_put_up_description,
    ean::varchar(20) as ean,
    sku_code::varchar(40) as sku_code,
    sku_description::varchar(150) as sku_description,
    pka_product_key::varchar(68) as pka_product_key,
    pka_product_key_description::varchar(255) as pka_product_key_description,
    customer_product_desc::varchar(500) as customer_product_desc,
    region::varchar(100) as region,
    zone_or_area::varchar(100) as zone_or_area,
    from_currency::varchar(5) as from_currency,
    to_currecy::varchar(5) as to_currecy,
    exchange_rate::number(15,5) as exchange_rate,
    sellout_sales_quantity::number(38,6) as sellout_sales_quantity,
    sellout_sales_value::number(38,6) as sellout_sales_value,
    sellout_sales_value_usd::number(38,11) as sellout_sales_value_usd,
    msl_product_code::varchar(20) as msl_product_code,
    msl_product_desc::varchar(100) as msl_product_desc,
    retail_env::varchar(300) as retail_env,
    channel::varchar(200) as channel,
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