with wks_commercial_excellence_dso_base as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_dso_base') }}
),
final as ( 
   Select base_dist.market :: varchar(40) AS market,
	base_dist."cluster" :: varchar(100) AS "cluster",
	base_dist.kpi :: varchar(100) AS kpi,
	base_dist.jnj_days :: numeric(31) as jnj_days,
	base_dist.month_id :: varchar(23) AS month_id,
	Sum(NVL(base.gts,0)) :: numeric(38,5) AS mtd_gts,
	Sum(NVL(base.gross_account_receivable,0)) :: numeric(38,5) AS mtd_gross_account_receivable
From
(select * from
(select distinct market, "cluster", kpi from wks_commercial_excellence_dso_base)t cross join
(select distinct month_id,jnj_days from wks_commercial_excellence_dso_base)u) base_dist
left join wks_commercial_excellence_dso_base base
on base_dist.market = base.market AND base_dist."cluster" = base."cluster" AND base_dist.kpi = base.kpi
AND base_dist.month_id = base.month_id
Group by base_dist.market, base_dist."cluster", base_dist.kpi, base_dist.month_id,base_dist.jnj_days

)
select * from final 