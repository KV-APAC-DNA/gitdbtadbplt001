with sdl_tw_bu_forecast_prod_hier as (
select * from {{ source('ntasdl_raw', 'sdl_tw_bu_forecast_prod_hier') }}
),
edw_customer_attr_hier_dim as (
select * from {{ ref('aspedw_integration__edw_customer_attr_hier_dim') }}
),
edw_customer_attr_flat_dim as (
select * from {{ source('aspedw_integration', 'edw_customer_attr_flat_dim') }}
),
transformed as (
select
bu_frcst_prod_hier.bu_version,
bu_frcst_prod_hier.forecast_on_year,
bu_frcst_prod_hier.forecast_on_month,
bu_frcst_prod_hier.forecast_for_year,
bu_frcst_prod_hier.forecast_for_mnth,
coalesce(cust_flat.sls_grp,'#N/A') AS sls_grp,
coalesce(cust_flat.channel,'#N/A') AS channel,
coalesce(cust_flat.sls_ofc,'#N/A') AS sls_ofc,
coalesce(cust_flat.sls_ofc_desc,'#N/A') as sls_ofc_desc,
coalesce(cust_attr.strategy_customer_hierachy_name,'#N/A') AS strategy_customer_hierachy_name,
bu_frcst_prod_hier.lph_level_6,
(bu_frcst_prod_hier.price_off*1000) as price_off,
(bu_frcst_prod_hier.display*1000) as display,
(bu_frcst_prod_hier.dm*1000) as dm,
(bu_frcst_prod_hier.other_support*1000) as other_support,
(bu_frcst_prod_hier.sr*1000) as sr,
(bu_frcst_prod_hier.pre_sales_before_returns*1000) as pre_sales_before_returns,
(bu_frcst_prod_hier.pre_sales*1000) as pre_sales,
bu_frcst_prod_hier.load_date
from
sdl_tw_bu_forecast_prod_hier bu_frcst_prod_hier
left join
(select sold_to_party,max(strategy_customer_hierachy_name) strategy_customer_hierachy_name 
                                        from edw_customer_attr_hier_dim where cntry='Taiwan' group by sold_to_party) cust_attr
on
bu_frcst_prod_hier.representative_cust_no=cust_attr.sold_to_party
left join
(select sold_to_party,max(sls_grp) sls_grp,max(channel) channel,max(sls_ofc) sls_ofc,max(sls_ofc_desc) sls_ofc_desc
                                        from edw_customer_attr_flat_dim where cntry='Taiwan' group by sold_to_party) cust_flat
on
bu_frcst_prod_hier.representative_cust_no=cust_flat.sold_to_party
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
sls_ofc::varchar(4) as sls_ofc,
sls_ofc_desc::varchar(40) as sls_ofc_desc,
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
from transformed
)
select * from final