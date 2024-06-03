{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ["sls_grp", "forecast_on_year", "bu_version", "forecast_on_month"],
        pre_hook= ["{% if is_incremental() %}
        update {{this}} SET sls_grp = cust.sls_grp, channel = cust.channel, sls_ofc = cust.sls_ofc, sls_ofc_desc = cust.sls_ofc_desc FROM {{this}} itg, ( SELECT sls_grp, target_sls_grp, channel, sls_ofc, sls_ofc_desc FROM ( SELECT DISTINCT wks.sls_grp, wks.target_sls_grp, wks.channel, sls_ofc, sls_ofc_desc, CASE  WHEN wks.sls_grp <> wks.target_sls_grp THEN 'Y' ELSE 'N' END AS flag FROM  {{ source('ntawks_integration', 'wks_edw_customer_attr_flat_dim') }} wks ) WHERE flag = 'Y' ) cust WHERE cust.target_sls_grp = itg.sls_grp;
        {% endif %}",
        "{% if is_incremental() %}
        delete from {{this}} itg_tw_bu_forecast_sku using {{ ref('ntawks_integration__wks_itg_tw_bu_forecast_sku') }} wks_sku where itg_tw_bu_forecast_sku.sls_grp=wks_sku.sls_grp and itg_tw_bu_forecast_sku.bu_version=wks_sku.bu_version and itg_tw_bu_forecast_sku.forecast_on_year=wks_sku.forecast_on_year and itg_tw_bu_forecast_sku.forecast_on_month=wks_sku.forecast_on_month;
        {% endif %}"]
        )
}}


with source as(
    select * from {{ ref('ntawks_integration__wks_itg_tw_bu_forecast_sku') }}
),
final as(
    select 
        bu_version::varchar(10) as bu_version,
        forecast_on_year::varchar(10) as forecast_on_year,
        forecast_on_month::varchar(10) as forecast_on_month,
        forecast_for_year::varchar(10) as forecast_for_year,
        forecast_for_mnth::varchar(10) as forecast_for_mnth,
        sls_grp::varchar(100) as sls_grp,
        channel::varchar(100) as channel,
        sls_ofc::varchar(100) as sls_ofc,
        sls_ofc_desc::varchar(255) as sls_ofc_desc,
        strategy_customer_hierachy_name::varchar(255) as strategy_customer_hierachy_name,
        sap_code::varchar(30) as sap_code,
        system_list_price::float as system_list_price,
        gross_invoice_price::float as gross_invoice_price,
        gross_invoice_price_less_terms::float as gross_invoice_price_lesst_terms,
        rf_sell_out_qty::float as rf_sellout_qty,
        rf_sell_in_qty::float as rf_sellin_qty,
        price_off::float as price_off,
        pre_sales_before_returns::float as pre_sales_before_returns,
        load_date::timestamp_ntz(9) as load_date
    from source
)
select * from final