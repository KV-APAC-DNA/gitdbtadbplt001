with wks_commercial_excellence_dso_summation_mtd as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_dso_summation_mtd') }}
),
final as ( 
   Select base.market :: varchar(40) AS market,
	base."cluster" :: varchar(100) AS "cluster",
	base.kpi :: varchar(100) AS kpi,
	base.month_id :: varchar(23) AS month_id,
	base.jnj_days :: numeric(31) as dso_jnj_days,
	Sum(base_ytd.mtd_gts) :: numeric(38,5) AS ytd_dso_gts,
	Sum(base_ytd.mtd_gross_account_receivable) :: numeric(38,5) AS ytd_dso_gross_account_receivable
From wks_commercial_excellence_dso_summation_mtd base
   left join wks_commercial_excellence_dso_summation_mtd base_ytd
      on base_ytd.market = base.market AND base_ytd."cluster" = base."cluster" AND base_ytd.kpi = base.kpi
         and CAST(left(base.month_id,4) as INT) = CAST(left(base_ytd.month_id,4) as INT)
         and CAST(right(base_ytd.month_id,2) as INT) <= CAST(right(base.month_id,2) as INT)
Group by base.market, base."cluster", base.kpi, base.month_id, base.jnj_days

)
select * from final 