with wks_okr_perfect_store as
(
    select * from DEV_DNA_CORE.ASPWKS_INTEGRATION.WKS_OKR_PERFECT_STORE
),
trans as
(
    SELECT 'Actual' AS data_type,
      'Offline_Perfect_Store' AS kpi,
      year_month,
      year as fisc_year,
      CASE 
        WHEN cast(substring(year_month, 5, 6) AS INTEGER) BETWEEN 1
            AND 3
          THEN 1
        WHEN cast(substring(year_month, 5, 6) AS INTEGER) BETWEEN 4
            AND 6
          THEN 2
        WHEN cast(substring(year_month, 5, 6) AS INTEGER) BETWEEN 7
            AND 9
          THEN 3
        ELSE 4
        END AS qtr,
      NULL AS brand,
      NULL AS franchise,
      cluster,
      market,
      NULL AS NTS_GRWNG_SHARE_SIZE,
      avg(ps) AS actual_value,
      NULL AS bp,
      NULL AS ju,
      NULL AS nu,
      NULL AS YTD_ACTUAL,
      NULL AS YTD_BP_TARGET,
      NULL AS YTD_JU_TARGET,
      NULL AS YTD_NU_TARGET
    FROM wks_okr_perfect_store
    GROUP BY year,
      year_month,
      cluster,
      market
),
final as
(
    select
    data_type::varchar(20) as data_type,
	kpi::varchar(100) as kpi,
	year_month::varchar(10) as year_month,
	fisc_year::varchar(10) as fisc_year,
	qtr::number(38,0) as quarter,
	brand::varchar(200) as brand,
	franchise::varchar(200) as franchise,
	cluster::varchar(100) as cluster,
	market::varchar(100) as market,
	nts_grwng_share_size::number(38,0) as nts_grwng_share_size,
	actual_value::number(38,4) as actual_value,
	bp::number(38,4) as bp_target,
	ju::number(38,4) as ju_target,
	nu::number(38,4) as nu_target,
	ytd_actual::number(38,4) as ytd_actual,	
	ytd_bp_target::number(38,4) as ytd_bp_target,
	ytd_ju_target::number(38,4) as ytd_ju_target,
	ytd_nu_target::number(38,4) as ytd_nu_target
    from trans
)
select * from final