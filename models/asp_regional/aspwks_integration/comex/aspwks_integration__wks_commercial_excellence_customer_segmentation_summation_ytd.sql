with wks_commercial_excellence_customer_segmentation_summation_mtd as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_customer_segmentation_summation_mtd') }}
),
final as (
Select base.market :: varchar(40) AS market, 
	base."cluster" :: varchar(100) AS "cluster", 
	base.cust_seg :: varchar(500) as cust_seg,
    base.retail_env,
	base.mega_brand :: varchar(100) as mega_brand,
	base.kpi :: varchar(100) as kpi, 
	base.month_id :: varchar(23) as month_id,
    base.ytd_week_passed,
    base.mtd_lcy :: numeric(38,5) AS mtd_lcy,
	base.mtd_usd :: numeric(38,5) AS mtd_usd,
	Sum(base_ytd.mtd_lcy) :: numeric(38,5) AS ytd_lcy,
	Sum(base_ytd.mtd_usd) :: numeric(38,5) AS ytd_usd
From wks_commercial_excellence_customer_segmentation_summation_mtd base
   left join wks_commercial_excellence_customer_segmentation_summation_mtd base_ytd
      on base_ytd.market = base.market AND base_ytd."cluster" = base."cluster" AND base_ytd.cust_seg = base.cust_seg AND base_ytd.retail_env = base.retail_env AND base_ytd.mega_brand = base.mega_brand AND base_ytd.kpi = base.kpi
         and CAST(left(base.month_id,4) as INT) = CAST(left(base_ytd.month_id,4) as INT)
         and CAST(right(base_ytd.month_id,2) as INT) <= CAST(right(base.month_id,2) as INT)
Group by base.market, base."cluster", base.cust_seg, base.retail_env, base.mega_brand, base.kpi, base.month_id, base.ytd_week_passed, base.mtd_lcy, base.mtd_usd
)
select * from final 