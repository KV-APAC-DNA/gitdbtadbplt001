with wks_commercial_excellence_gts_base as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_gts_base') }}
),
final as (
Select base_dist.market :: varchar(40) AS market, 
	base_dist."cluster" :: varchar(100) AS "cluster", 
	base_dist.cust_seg :: varchar(500) as cust_seg,
	base_dist.mega_brand :: varchar(100) as mega_brand,
	base_dist.kpi :: varchar(100) as kpi, 
	base_dist.month_id :: varchar(23) as month_id,
	Sum(NVL(base.gts_lcy,0)) :: numeric(38,5) AS mtd_lcy,
	Sum(NVL(base.gts_usd,0)) :: numeric(38,5) AS mtd_usd
From 
(select * from 
(select distinct market, "cluster", cust_seg, mega_brand, kpi from wks_commercial_excellence_gts_base) as t cross join
(select distinct month_id from wks_commercial_excellence_gts_base) as u) base_dist
left join wks_commercial_excellence_gts_base base
on base_dist.market = base.market AND base_dist."cluster" = base."cluster" AND base.cust_seg = base_dist.cust_seg AND base_dist.mega_brand = base.mega_brand AND base_dist.kpi = base.kpi
AND base_dist.month_id = base.month_id 
Group by base_dist.market, base_dist."cluster", base_dist.cust_seg, base_dist.mega_brand, base_dist.kpi, base_dist.month_id


)
select * from final 