
with wks_singapore_regional_sellout_npd as (
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_npd') }}
),
itg_mds_ap_customer360_config as (
    select * from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }}
),
filter_1 as (
      select
        year,
        qrtr_no,
        mnth_id,
        mnth_no,
        cal_date,
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
        from_currency, /* sls_org, */
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
        crtd_dttm,
	    updt_dttm
      /* sellout_value_invoice_price, */ /* sellout_value_invoice_price_usd, */ /* null as npd_flag_market_level, */ /* null as npd_flag_parent_customer_level   	   */
      from wks_singapore_regional_sellout_npd
      
    ),
filter_2 as 
(
    select
      *,
      case
        when customer_product_min_date > dateadd(
          week,
          cast((
            select
              param_value
            from itg_mds_ap_customer360_config
            where
              code = 'npd_buffer_weeks'
          ) as int),
          cast(customer_min_date as timestampntz)
        )
        then 'y'
        else 'n'
      end /* case when rn_cus=1 and customer_product_min_date>dateadd(week,(select param_value from rg_sdl.sdl_mds_ap_customer360_config where code='npd_buffer_weeks')::integer,customer_min_date) then 'y' else 'n' end as first_scan_flag_parent_customer_level, */ /* case when rn_mkt=1 and market_product_min_date>dateadd(week,(select param_value from rg_sdl.sdl_mds_ap_customer360_config where code='npd_buffer_weeks')::integer,market_min_date) then 'y' else 'n' end as first_scan_flag_market_level */ as first_scan_flag_parent_customer_level_initial,
      case
        when market_product_min_date > dateadd(
          week,
          cast((
            select
              param_value
            from itg_mds_ap_customer360_config
            where
              code = 'npd_buffer_weeks'
          ) as int),
          cast(market_min_date as timestampntz)
        )
        then 'y'
        else 'n'
      end as first_scan_flag_market_level_initial
    from filter_1
  ),
filter_3 as 
(
  select
    *,
    case
      when first_scan_flag_parent_customer_level_initial = 'y' and rn_cus = 1
      then 'y'
      else 'n'
    end as first_scan_flag_parent_customer_level,
    case
      when first_scan_flag_market_level_initial = 'y'
      and first_scan_flag_parent_customer_level_initial = 'y'
      and rn_mkt = 1
      then 'y'
      else 'n'
    end as first_scan_flag_market_level
  from filter_2
),
transformed as (select
  *,
  case
    when sellout_sales_quantity <> 0
    then sellout_sales_value / sellout_sales_quantity
    else 0
  end as selling_price,
  count(
    case when first_scan_flag_market_level = 'y' then first_scan_flag_market_level end
  ) over (partition by country_name, pka_product_key) as cnt_mkt
from filter_3),
final as 
(
    select 
    year::varchar(10) as year,
    qrtr_no::varchar(14) as qrtr_no,
    mnth_id::varchar(21) as mnth_id,
    mnth_no::varchar(10) as mnth_no,
    cal_date::date as cal_date,
    country_code::varchar(2) as country_code,
    country_name::varchar(9) as country_name,
    data_source::varchar(3) as data_source,
    soldto_code::varchar(200) as soldto_code,
    distributor_code::varchar(200) as distributor_code,
    distributor_name::varchar(10) as distributor_name,
    store_code::varchar(300) as store_code,
    store_name::varchar(300) as store_name,
    store_type::varchar(200) as store_type,
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
    ean::varchar(255) as ean,
    sku_code::varchar(40) as sku_code,
    sku_description::varchar(150) as sku_description,
    --   greenlight_sku_flag::varchar(300) as greenlight_sku_flag,
    case when pka_product_key in ('N/A', 'NA') then 'NA' else pka_product_key end as pka_product_key,
    case
    when pka_product_key_description::varchar(500) in ('N/A', 'NA')
    then 'NA'
    else pka_product_key_description::varchar(500)
    end as pka_product_key_description,
    from_currency::varchar(3) as from_currency, /* trim(nvl (nullif(product.sls_org,''),'NA')) as sls_org, */
    to_currency::varchar(3) as to_currency,
    exchange_rate::numeric(15,5) as exchange_rate,
    sellout_sales_quantity::numeric(38,0) as sellout_sales_quantity,
    sellout_sales_value::numeric(38,6) as sellout_sales_value,
    sellout_sales_value_usd::numeric(38,11) as sellout_sales_value_usd,
    sellout_value_list_price::numeric(18,0) as sellout_value_list_price,		
    sellout_value_list_price_usd::numeric(18,0) as sellout_value_list_price_usd,
    customer_min_date::date as customer_min_date,		
    customer_product_min_date::date as customer_product_min_date,	
    market_min_date::date as market_min_date,		
    market_product_min_date::date as market_product_min_date,		
    rn_cus::numeric(18,0) as rn_cus,		
    rn_mkt::numeric(18,0) as rn_mkt,		
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,	
    updt_dttm::timestamp_ntz(9) as updt_dttm,		
    first_scan_flag_parent_customer_level_initial::varchar(1) as first_scan_flag_parent_customer_level_initial,		
    first_scan_flag_market_level_initial::varchar(1) as first_scan_flag_market_level_initial,		
    first_scan_flag_parent_customer_level::varchar(1) as first_scan_flag_parent_customer_level,		
    first_scan_flag_market_level::varchar(1) as first_scan_flag_market_level,		
    selling_price::numeric(38,6) as selling_price,		
    cnt_mkt::numeric(18,0) as cnt_mkt
    from transformed
)
select * from final