with itg_tw_bu_forecast_prod_hier as (
    select * from {{ ref('ntaitg_integration__itg_tw_bu_forecast_prod_hier') }}
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
from itg_tw_bu_forecast_prod_hier
)
select * from final