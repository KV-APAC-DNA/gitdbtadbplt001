with edw_rpt_okr_dashboard_temp as
(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_RPT_OKR_DASHBOARD_TEMP
),
edw_okr_core_ppm as
(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_OKR_CORE_PPM
),
trans as
(   
    SELECT DISTINCT cy.year_month as year_month,
      cy.fisc_year as fisc_year,
      cast(cy.quarter AS INTEGER) as quarter,
      cy.kpi as measure,
      cy.brand as brand,
      cy.franchise as segment,
      cy.cluster as CLUSTER,
      cy.market as market,
      cy.core as core_brand,
      cy.ppm as ppm,
      cy.nts_grwng_share_size as nts_grwng_share_size,
      cy.cy_act as cy_actual,
      py.py_act as py_actual,
      pm.pm_act as pm_actual,
      cy.bp_target as bp_target,
      cy.ju_target as ju_target,
      cy.nu_target as nu_target,
      CASE 
        WHEN cy.nu_target IS NOT NULL
          THEN cy.nu_target
        ELSE cy.ju_target
        END AS le_target,
      CASE 
        WHEN cy.cy_act IS NULL
          THEN NULL
        ELSE cy.cy_ytd
        END AS ytd_cy_actual,
      py.py_ytd as ytd_py_actual,
      cy.ytd_bp_target as ytd_bp_target,
      cy.ytd_ju_target as ytd_ju_target,
      cy.ytd_nu_target as ytd_nu_target,
      CASE 
        WHEN cy.ytd_nu_target IS NOT NULL
          THEN cy.ytd_nu_target
        ELSE cy.ytd_ju_target
        END AS ytd_le_target
    FROM (
      SELECT year_month,
        fisc_year,
        quarter,
        kpi,
        (substring(year_month, 1, 4) - 1) || (substring(year_month, 5, 6)) AS pm,
        (fisc_year - 1) AS py,
        TO_CHAR(add_months(TO_DATE(year_month, 'YYYYMM'), - 1), 'YYYYMM') AS pm1,
        substring(TO_CHAR(add_months(TO_DATE(year_month, 'YYYYMM'), - 1), 'YYYYMM'), 1, 4) AS pmy,
        base.brand,
        franchise,
        cluster,
        base.market,
        base.nts_grwng_share_size,
        ppm.core,
        ppm.ppm,
        actual_value AS cy_act,
        bp_target,
        ju_target,
        nu_target,
        ytd_actual AS cy_ytd,
        ytd_bp_target,
        ytd_ju_target,
        ytd_nu_target
      FROM (
        SELECT kpi,
          year_month,
          fisc_year,
          quarter,
          brand,
          franchise,
          cluster,
          CASE 
            WHEN cluster = 'Pacific'
              AND market IS NULL
              THEN 'Pacific'
            WHEN market = 'Korea'
              THEN 'South Korea'
            ELSE market
            END AS market,
          nts_grwng_share_size,
          sum(actual_value) AS actual_value,
          sum(bp_target) AS bp_target,
          sum(ju_target) AS ju_target,
          sum(nu_target) AS nu_target,
          sum(ytd_actual) AS ytd_actual,
          sum(ytd_bp_target) ytd_bp_target,
          sum(ytd_ju_target) ytd_ju_target,
          sum(ytd_nu_target) ytd_nu_target
        FROM edw_rpt_okr_dashboard_temp
        GROUP BY kpi,
          year_month,
          fisc_year,
          quarter,
          brand,
          franchise,
          cluster,
          nts_grwng_share_size,
          CASE 
            WHEN cluster = 'Pacific'
              AND market IS NULL
              THEN 'Pacific'
            WHEN market = 'Korea'
              THEN 'South Korea'
            ELSE market
            END
        ) base
      LEFT JOIN edw_okr_core_ppm ppm ON upper(base.market) = upper(ppm.market)
        AND upper(base.brand) = upper(ppm.brand)
      ) cy
    LEFT JOIN (
      SELECT year_month AS pm,
        fisc_year AS py,
        quarter AS pq,
        brand,
        franchise,
        cluster,
        CASE 
          WHEN cluster = 'Pacific'
            AND market IS NULL
            THEN 'Pacific'
          WHEN market = 'Korea'
            THEN 'South Korea'
          ELSE market
          END market,
        kpi,
        coalesce(actual_value,0) AS py_act,
        ytd_actual AS py_ytd
      FROM edw_rpt_okr_dashboard_temp
      WHERE data_type = 'Actual'
      ) py ON coalesce(cy.pm, '9999') = coalesce(py.pm, '9999')
      AND coalesce(cy.brand, '9999') = coalesce(py.brand, '9999')
      AND coalesce(cy.franchise, '9999') = coalesce(py.franchise, '9999')
      AND coalesce(cy.cluster, '9999') = coalesce(py.cluster, '9999')
      AND coalesce(cy.market, '9999') = coalesce(py.market, '9999')
      AND py.kpi = cy.kpi
      AND coalesce(cy.quarter, 9999) = coalesce(py.pq, 9999)
      AND coalesce(cy.py, '9999') = coalesce(py.py, '9999')
    LEFT JOIN (
      SELECT year_month AS pm,
        fisc_year AS pmy,
        brand,
        franchise,
        cluster,
        CASE
          WHEN cluster = 'Pacific'
            AND market IS NULL
            THEN 'Pacific'
          ELSE market
          END market,
        kpi,
        actual_value AS pm_act
      FROM edw_rpt_okr_dashboard_temp
      WHERE data_type = 'Actual'
        AND upper(kpi) = 'NTS% GROWING SHARE'
        AND year_month IS NOT NULL
      ) pm ON coalesce(cy.pm1, '9999') = coalesce(pm.pm, '9999')
      AND coalesce(cy.brand, '9999') = coalesce(pm.brand, '9999')
      AND coalesce(cy.franchise, '9999') = coalesce(pm.franchise, '9999')
      AND coalesce(cy.cluster, '9999') = coalesce(pm.cluster, '9999')
      AND coalesce(cy.market, '9999') = coalesce(pm.market, '9999')
      AND pm.kpi = cy.kpi
      AND cy.pm IS NOT NULL
      AND coalesce(cy.pmy, '9999') = coalesce(pm.pmy, '9999')
    WHERE cy.fisc_year > (date_part(year, current_timestamp()) - 3)
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
	ytd_le_target::number(38,4) as ytd_le_target
    from trans
)
select * from final