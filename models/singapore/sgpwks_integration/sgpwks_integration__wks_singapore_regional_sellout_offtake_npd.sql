
with wks_singapore_regional_sellout_npd as (
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_npd') }}
),
itg_mds_ap_customer360_config as (
    select * from {{ source('source_name', 'itg_mds_ap_customer360_config') }}
),
final as (select
  *,
  case
    when sellout_sales_quantity <> 0
    then sellout_sales_value / sellout_sales_quantity
    else 0
  end as selling_price,
  count(
    case when first_scan_flag_market_level = 'y' then first_scan_flag_market_level end
  ) over (partition by country_name, pka_product_key) as cnt_mkt
from (
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
  from (
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
    from (
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
        greenlight_sku_flag,
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
        rn_mkt
      /* sellout_value_invoice_price, */ /* sellout_value_invoice_price_usd, */ /* null as npd_flag_market_level, */ /* null as npd_flag_parent_customer_level   	   */
      from wks_singapore_regional_sellout_npd
      
    )
  )
))