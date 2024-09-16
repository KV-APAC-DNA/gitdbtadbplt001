with edw_rpt_ecomm_oneview as (
    select * from {{ source('aspedw_integration', 'edw_rpt_ecomm_oneview') }}
),
final as ( 
    select 
NVL(base_dist.market,'NA') :: varchar(40) AS market, 
NVL(base_dist."cluster",'NA') :: varchar(100) AS "cluster",
'ECOM NTS' :: varchar(100) AS kpi,
base_dist.month_id :: varchar(23) as month_id,
sum(base.value_usd) :: numeric(38,5) as mtd_usd, sum(base.value_lcy) :: numeric(38,5) as mtd_lcy
 from (select * from
(select distinct CASE WHEN market like '%Japan%' THEN 'Japan'
		WHEN market = 'Pacific' THEN 'Australia'
        ELSE market END as market,
	cluster as "cluster" from edw_rpt_ecomm_oneview where kpi = 'NTS')t cross join
(select distinct
		left(fisc_yr_per, 4)||right(fisc_yr_per, 2) as month_id
	from edw_rpt_ecomm_oneview where kpi = 'NTS') u ) base_dist
left join
	(select CASE WHEN market like '%Japan%' THEN 'Japan'
		WHEN market = 'Pacific' THEN 'Australia'
        ELSE market END as market,
        cluster as "cluster",
		left(fisc_yr_per, 4)||right(fisc_yr_per, 2) as month_id,
		value_usd, value_lcy, kpi
        from edw_rpt_ecomm_oneview where kpi = 'NTS') base
on base_dist.market = base.market AND base_dist."cluster" = base."cluster" AND base_dist.month_id = base.month_id
group by base_dist.market, base_dist."cluster", base_dist.month_id

)
select * from final 