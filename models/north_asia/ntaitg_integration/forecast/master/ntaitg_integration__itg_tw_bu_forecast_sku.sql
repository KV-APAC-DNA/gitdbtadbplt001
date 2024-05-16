{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["sls_grp","bu_version", "forecast_on_year", "forecast_on_month"],
        merge_exclude_columns = ["load_date"]
    )
}}

with source as(
    select * from DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.wks_itg_tw_bu_forecast_sku
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