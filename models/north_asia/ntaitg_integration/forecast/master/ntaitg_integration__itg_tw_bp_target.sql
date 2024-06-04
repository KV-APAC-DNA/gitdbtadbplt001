{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        unique_key=["bu_version", "forecast_on_year", "forecast_on_month"],
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} using {{ source('ntasdl_raw', 'sdl_tw_bp_forecast') }} bp_sdl where {{this}}.bp_version=bp_sdl.bp_version and {{this}}.forecast_on_year=bp_sdl.forecast_on_year and {{this}}.forecast_on_month=bp_sdl.forecast_on_month;
        {% endif %}"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw', 'sdl_tw_bp_forecast') }}
),
edw_customer_attr_hier_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_attr_hier_dim') }}
),
edw_customer_attr_flat_dim as (
    select * from aspedw_integration.edw_customer_attr_flat_dim
),

bp_frcst_prod_hier as(
  SELECT *
	FROM source
	WHERE prod_name <> 'TTL'
),
cust_attr as(
  SELECT sold_to_party,
		max(sls_grp) sls_grp,
		max(channel) channel,
		max(cust_hier_l1) cust_hier_l1,
		max(strategy_customer_hierachy_name) strategy_customer_hierachy_name
	FROM edw_customer_attr_hier_dim
	WHERE cntry = 'Taiwan'
	GROUP BY sold_to_party
),
cust_flat as(
  SELECT sold_to_party,
		max(sls_grp) sls_grp,
		max(channel) channel,
		max(sls_ofc) sls_ofc,
		max(sls_ofc_desc) sls_ofc_desc
	FROM edw_customer_attr_flat_dim
	WHERE cntry = 'Taiwan'
	GROUP BY sold_to_party
),
transformed as(
    SELECT bp_frcst_prod_hier.bp_version::varchar(5) as bp_version,
	bp_frcst_prod_hier.forecast_on_year::varchar(10) as forecast_on_year,
	bp_frcst_prod_hier.forecast_on_month::varchar(10) as forecast_on_month,
	bp_frcst_prod_hier.forecast_for_year::varchar(10) as forecast_for_year,
	bp_frcst_prod_hier.forecast_for_mnth::varchar(10) as forecast_for_mnth,
	coalesce(cust_flat.sls_grp, '#N/A')::varchar(100) as sls_grp,
	coalesce(cust_flat.channel, '#N/A')::varchar(100) as channel,
	coalesce(cust_flat.sls_ofc, '#N/A')::varchar(100) AS sls_ofc,
	coalesce(cust_flat.sls_ofc_desc, '#N/A')::varchar(255) AS sls_ofc_desc,
	coalesce(cust_attr.strategy_customer_hierachy_name, '#N/A')::varchar(255) as strategy_customer_hierachy_name,
	bp_frcst_prod_hier.region::varchar(100) as region,
	bp_frcst_prod_hier.lph_level_6::varchar(255) as lph_level_6,
	nvl((bp_frcst_prod_hier.pre_sales * 1000), 0)::float as pre_sales,
	nvl((bp_frcst_prod_hier.tp * 1000), 0)::float as tp,
	nvl((bp_frcst_prod_hier.nts * 1000), 0)::float as nts,
	bp_frcst_prod_hier.load_date::timestamp_ntz(9) as load_date
    FROM bp_frcst_prod_hier
    LEFT JOIN cust_attr ON bp_frcst_prod_hier.representative_cust_no = cust_attr.sold_to_party
    LEFT JOIN cust_flat ON bp_frcst_prod_hier.representative_cust_no = cust_flat.sold_to_party
)
select * from transformed