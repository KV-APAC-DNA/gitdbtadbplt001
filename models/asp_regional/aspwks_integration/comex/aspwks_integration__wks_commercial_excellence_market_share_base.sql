with edw_market_mirror_fact as (
    select * from {{ source('snapaspedw_integration', 'edw_market_mirror_fact') }}
),
itg_query_parameters as ( 
    select * from {{ source('aspitg_integration', 'itg_query_parameters') }}
),
itg_mds_ap_sales_ops_map as (
    select * from {{ ref('aspitg_integration__itg_mds_ap_sales_ops_map') }}
),
final as (
    SELECT market :: varchar(40) as market,
    "cluster" :: varchar(100) as "cluster",
    month_id :: varchar(23) as month_id,
    kpi :: varchar(100) as kpi,
    kv_val_usd :: numeric(38,5) as kv_val_usd,
    KV_VAL_LC :: numeric(38,5) as KV_VAL_LC,
    CAT_VAL_USD :: numeric(38,5) as CAT_VAL_USD,
    CAT_VAL_LC :: numeric(38,5) as CAT_VAL_LC
     FROM (
select CASE
	WHEN market = 'China' AND trans.supplier = 'IQVIA' THEN 'China Selfcare'
	WHEN market = 'China' AND trans.supplier <> 'IQVIA' THEN 'China Personal Care'
	ELSE market_map.destination_market
END AS market,
destination_cluster AS "cluster",
extract(year from time_period)||LPAD(extract(month from time_period),2,'00') AS month_id,
'MARKET SHARE' AS kpi,
sum(case when upper(manufacturer) in (SELECT DISTINCT parameter_value AS manufacturer
	FROM itg_query_parameters
	WHERE country_code = 'APAC' AND parameter_name = 'price_tracker_mfr')
	then sku_value_sales_usd else 0 end) as kv_val_usd,
sum(case when upper(manufacturer) in (SELECT DISTINCT parameter_value AS manufacturer
	FROM itg_query_parameters
	WHERE country_code = 'APAC' AND parameter_name = 'price_tracker_mfr')
	then sku_value_sales_lc else 0 end) as kv_val_lc,
sum(sku_value_sales_usd) as cat_val_usd,
sum(sku_value_sales_lc) as cat_val_lc
FROM edw_market_mirror_fact trans left join itg_mds_ap_sales_ops_map market_map
ON UPPER(market_map.source_market) = UPPER(trans.ggh_country) AND market_map.dataset = 'Market Share QSD'
where channel_type = 'Total'
and date_type = 'Monthly'
and ggh_region = 'APAC'
group by 1,2,3,4
	) WHERE month_id::INT<=(select (extract(year from min(latest_date))  || lpad(extract(month from min(latest_date)),2,'0'))::INT  as mkt_shr_max_dt from(
	select market, max(time_period) as latest_date from edw_market_mirror_fact
	where channel_type = 'Total'
	and date_type = 'Monthly'
	and ggh_region = 'APAC'
	group by 1 order by 1))
)
select * from final 