with wks_rpt_okr_dashboard_intermediate as
(
    select * from {{ ref('aspwks_integration__wks_rpt_okr_dashboard_intermediate') }}
),
wks_okr_balance_to_go as
(
    select * from {{ ref('aspwks_integration__wks_okr_balance_to_go') }}
),
trans as
(
    SELECT DISTINCT base.year_month,
      base.fisc_year,
      base.quarter,
      base.measure,
      base.brand,
      base.segment,
      base.CLUSTER,
      base.market,
      base.core_brand,
      base.ppm,
      base.nts_grwng_share_size,
      base.cy_actual,
      base.py_actual,
      base.pm_actual,
      base.bp_target,
      base.ju_target,
      base.nu_target,
      base.ytd_cy_actual,
      base.ytd_py_actual,
      base.ytd_bp_target,
      base.ytd_ju_target,
      base.ytd_nu_target,
      base.le_target,
      base.ytd_le_target,
      btg.blnc_to_go as balance_to_go
    FROM wks_rpt_okr_dashboard_intermediate base
    LEFT JOIN wks_okr_balance_to_go btg ON base.fisc_year = btg.fisc_year
      AND nvl(base.year_month, '9999') = nvl(btg.year_month, '9999')
      AND nvl(base.measure, '9999') = nvl(btg.measure, '9999')
      AND nvl(base.brand, '9999') = nvl(btg.brand, '9999')
      AND nvl(base.segment, '9999') = nvl(btg.segment, '9999')
      AND nvl(base.market, '9999') = nvl(btg.market, '9999')
      AND nvl(base.cluster, '9999') = nvl(btg.cluster, '9999')
      AND base.year_month IS NOT NULL   
),
final as
(
    select
    year_month::varchar(10) as year_month,
	fisc_year::varchar(10) as fisc_year,
	quarter::number(38,0) as quarter,
	measure::varchar(100) as measure,
	brand::varchar(200) as brand,
	segment::varchar(200) as segment,
	cluster::varchar(100) as cluster,
	market::varchar(100) as market,
	core_brand::varchar(50) as core_brand,
	ppm::varchar(50) as ppm,
	nts_grwng_share_size::number(38,0) as nts_grwng_share_size,
	cy_actual::number(38,4) as cy_actual,
	py_actual::number(38,4) as py_actual,
	pm_actual::number(38,4) as pm_actual,
	bp_target::number(38,4) as bp_target,
	ju_target::number(38,4) as ju_target,
	nu_target::number(38,4) as nu_target,
	ytd_cy_actual::number(38,4) as ytd_cy_actual,
	ytd_py_actual::number(38,4) as ytd_py_actual,
	ytd_bp_target::number(38,4) as ytd_bp_target,
	ytd_ju_target::number(38,4) as ytd_ju_target,
	ytd_nu_target::number(38,4) as ytd_nu_target,
	le_target::number(38,4) as le_target,
	ytd_le_target::number(38,4) as ytd_le_target,
	balance_to_go::number(38,4) as balance_to_go
    from trans
)
select * from final