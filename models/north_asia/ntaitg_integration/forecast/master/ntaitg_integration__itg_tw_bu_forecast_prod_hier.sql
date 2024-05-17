{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['sap_code', 'item_idnt'],
        pre_hook= ["UPDATE {{this}} SET sls_grp = cust.sls_grp,
                    channel = cust.channel,
                    sls_ofc = cust.sls_ofc,
                    sls_ofc_desc = cust.sls_ofc_desc 
                    FROM {{this}} itg,
                    (
                    SELECT sls_grp ,target_sls_grp,channel,sls_ofc,sls_ofc_desc
                    FROM (SELECT DISTINCT wks.sls_grp ,wks.target_sls_grp,wks.channel,sls_ofc,sls_ofc_desc,
                    CASE
                     WHEN wks.sls_grp <> wks.target_sls_grp THEN 'Y'
                     ELSE 'N'
                    END AS flag
                    FROM  {{ source('ntawks_integration', 'wks_edw_customer_attr_flat_dim') }}
                      wks)
                    WHERE flag = 'Y') cust
                    Where cust.target_sls_grp =itg.sls_grp
                    ","delete from {{this}} itg_tw_bu_forecast_prod_hier
                        using
                        {{ ref('ntawks_integration__wks_itg_tw_bu_forecast_prod_hier') }} wks_prod_hier
                        where
                        itg_tw_bu_forecast_prod_hier.sls_grp=wks_prod_hier.sls_grp
                        and
                        itg_tw_bu_forecast_prod_hier.bu_version=wks_prod_hier.bu_version
                        and
                        itg_tw_bu_forecast_prod_hier.forecast_on_year=wks_prod_hier.forecast_on_year
                        and
                        itg_tw_bu_forecast_prod_hier.forecast_on_month=wks_prod_hier.forecast_on_month;
                    "]
        )
}}


with wks_itg_tw_bu_forecast_prod_hier as (
    select * from {{ ref('ntawks_integration__wks_itg_tw_bu_forecast_prod_hier') }}
),
final as (
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
lph_level_6::varchar(255) as lph_level_6,
price_off::float as price_off,
display::float as display,
dm::float as dm,
other_support::float as other_support,
sr::float as sr,
pre_sales_before_returns::float as pre_sales_before_returns,
pre_sales::float as pre_sales,
load_date::timestamp_ntz(9) as load_date
from 
wks_itg_tw_bu_forecast_prod_hier
)
select * from final