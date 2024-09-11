with itg_mds_ap_dso as (
    select * from {{ ref('aspitg_integration__itg_mds_ap_dso') }}
),
itg_mds_ap_sales_ops_map as (
    select * from {{ ref('aspitg_integration__itg_mds_ap_sales_ops_map') }}
),
final as ( 
   select NVL(market,'NA') :: varchar(40) AS market,
	NVL(ctry_map.destination_cluster,'NA') :: varchar(100) AS "cluster",
	'DSO' :: varchar(100) AS kpi,
	(year||lpad(month,2,'0')) :: varchar(23) AS month_id,
	max(jnj_days) :: numeric(31) as jnj_days,
	sum(gts) :: numeric(38,5) as gts,
    sum(gross_account_receivable) :: numeric(38,5) as gross_account_receivable
 from itg_mds_ap_dso dso,
itg_mds_ap_sales_ops_map ctry_map
where ctry_map.dataset = 'DSO' and ctry_map.source_market  = dso.market
group by market, ctry_map.destination_cluster, (year||lpad(month,2,'0'))

)
select * from final 