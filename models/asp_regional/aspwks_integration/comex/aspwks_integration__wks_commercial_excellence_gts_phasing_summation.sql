with wks_commercial_excellence_gts_base as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_gts_base') }}
),
final as (
Select base_dist.market :: varchar(40) AS market,
	base_dist."cluster" :: varchar(100) AS "cluster",
	base_dist.mega_brand :: varchar(100) as mega_brand,
	'GTS PHASING' :: varchar(100) as kpi, 
	base_dist.month_id :: varchar(23) as month_id,
	base_dist.week,
	Sum(NVL(base.gts_lcy,0)) :: numeric(38,5) AS val_lcy,
	Sum(NVL(base.gts_usd,0)) :: numeric(38,5) AS val_usd
From 
(select * from 
(select distinct market, "cluster", mega_brand, kpi from wks_commercial_excellence_gts_base) as t cross join 
(select distinct month_id,week from wks_commercial_excellence_gts_base)as u) base_dist
left join wks_commercial_excellence_gts_base base
on base_dist.market = base.market AND base_dist."cluster" = base."cluster" AND base_dist.mega_brand = base.mega_brand AND base_dist.kpi = base.kpi
AND base_dist.month_id = base.month_id AND base_dist.week = base.week 
Group by base_dist.market, base_dist."cluster", base_dist.mega_brand, --base_dist.kpi,
 base_dist.month_id, base_dist.week
)
select * from final 