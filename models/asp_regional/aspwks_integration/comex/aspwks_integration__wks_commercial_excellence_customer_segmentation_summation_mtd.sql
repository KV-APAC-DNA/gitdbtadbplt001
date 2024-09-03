with wks_commercial_excellence_all_nts_base as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_all_nts_base') }}
),
final as (
Select base_dist.market :: varchar(40) AS market, 
	base_dist."cluster" :: varchar(100) AS "cluster", 
	base_dist.cust_seg :: varchar(500) as cust_seg,
    base_dist.retail_env,
	base_dist.mega_brand :: varchar(100) as mega_brand,
	base_dist.kpi :: varchar(100) as kpi, 
	base_dist.month_id :: varchar(23) as month_id,
    base_dist.ytd_week_passed,
	Sum(NVL(base.nts_lcy,0)) :: numeric(38,5) AS mtd_lcy,
    Sum(NVL(base.nts_usd,0)) :: numeric(38,5) AS mtd_usd
From (select * from 
(select distinct market, "cluster", cust_seg, retail_env, mega_brand, kpi from wks_commercial_excellence_all_nts_base)t cross join
(select distinct month_id,ytd_week_passed from wks_commercial_excellence_all_nts_base)u) base_dist
	left join wks_commercial_excellence_all_nts_base base
	on base.market = base_dist.market AND base."cluster" = base_dist."cluster" AND base.cust_seg = base_dist.cust_seg AND base.retail_env = base_dist.retail_env AND base.mega_brand = base_dist.mega_brand AND base.kpi = base_dist.kpi
         and base.month_id = base_dist.month_id
Group by base_dist.market, base_dist."cluster", base_dist.cust_seg, base_dist.retail_env, base_dist.mega_brand, base_dist.kpi, base_dist.month_id, base_dist.ytd_week_passed
)
select * from final 