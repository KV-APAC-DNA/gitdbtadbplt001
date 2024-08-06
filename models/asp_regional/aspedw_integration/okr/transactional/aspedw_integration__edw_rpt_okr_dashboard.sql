with edw_rpt_okr_dashboard_intermediate as
(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.edw_rpt_okr_dashboard_intermediate
),
trans as
(
    WITH base AS (
    SELECT DISTINCT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      core_brand,
      ppm
    FROM edw_rpt_okr_dashboard_intermediate
    ),
  nts AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target,
      sum(balance_to_go) AS btg
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'NTS'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  gts AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'GTS'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  gp AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target,
      sum(balance_to_go) AS btg
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'GP'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  bme AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'BME$'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  bme_percent AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'BME % OF NTS'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ciw_percent AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'CIW % OF NTS'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ibt AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target,
      sum(balance_to_go) AS btg
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) LIKE 'IBT%'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ccc AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'CASH_CONVERSION_CYCLE'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  cm AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'CONTRIBUTION_MARGIN'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  cust_dqi AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'CUSTOMER_DQI'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  div_scr AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'DIVERSITY, EQUITY & INCLUSION SCORE'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  emp_eng AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'EMPLOYEE_ENGAGEMENT'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  fcf AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target,
      sum(balance_to_go) AS btg
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'FREE_CASH_FLOW'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  incl_scr AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'INCLUSION_SCORE'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  nts_grw_shr AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      nts_grwng_share_size,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(pm_actual) AS pm_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'NTS% GROWING SHARE'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      nts_grwng_share_size
    ),
  npd_nts AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target,
      sum(balance_to_go) AS btg
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'NTS_FROM_NPD'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  otifd AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target,
      sum(ytd_cy_actual) AS ytd_cy_actual,
      sum(ytd_py_actual) AS ytd_py_actual,
      sum(ytd_bp_target) AS ytd_bp_target,
      sum(ytd_ju_target) AS ytd_ju_target,
      sum(ytd_nu_target) AS ytd_nu_target,
      sum(ytd_le_target) AS ytd_le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'OTIF-D'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ps AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'OFFLINE_PERFECT_STORE'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ple AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'PEOPLE_LEADER_EFFECTIVENESS_INDEX'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  rgm AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target,
      sum(balance_to_go) AS btg
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) LIKE 'RGM%'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ppm_grw AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'SPIKE_PPM_GROWTH'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  tr AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'TALENT_RETENTION'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  twd AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'TOTAL WEIGHTED DISTRIBUTION'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  vsg AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'VALUE SHARE GROWTH'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  cic AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'ZERO_DISRUPTIONS_AS_CIC'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ecomm_nts AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target,
      sum(balance_to_go) AS btg
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'ECOMMERCE_NTS'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ecomm_rank AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'ECOM RANK'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  brand_pwr AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'BRAND POWER'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  sos AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = '6PAI'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  bmc_grw_shr AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'BMC GROWING SHARE'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ap_imprvmnt AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'AP_&_OAL_IMPROVEMENT'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ar_imprvmnt AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'AR_IMPROVEMENT'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  dnd_iq_imprvmnt AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'DIGITAL_&_DATA_IQ_IMPROVEMENT'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  inventory_reduction AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'INVENTORY_REDUCTION'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  sustainability_score AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'SUSTAINABILITY_SCORE_ON_LTR'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  slob AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target,
      sum(ytd_cy_actual) AS ytd_cy_actual,
      sum(ytd_py_actual) AS ytd_py_actual,
      sum(ytd_bp_target) AS ytd_bp_target,
      sum(ytd_ju_target) AS ytd_ju_target,
      sum(ytd_nu_target) AS ytd_nu_target,
      sum(ytd_le_target) AS ytd_le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'SLOB'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  sys_rat_auto AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'SYSTEM RAT. & AUTOMATION'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  spike_brnd_grwth AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target,
      sum(ytd_cy_actual) AS ytd_cy_actual,
      sum(ytd_py_actual) AS ytd_py_actual,
      sum(ytd_bp_target) AS ytd_bp_target,
      sum(ytd_ju_target) AS ytd_ju_target,
      sum(ytd_nu_target) AS ytd_nu_target,
      sum(ytd_le_target) AS ytd_le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) LIKE 'SPIKE BRAND GROWTH'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  mape AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target,
      sum(ytd_cy_actual) AS ytd_cy_actual,
      sum(ytd_py_actual) AS ytd_py_actual,
      sum(ytd_bp_target) AS ytd_bp_target,
      sum(ytd_ju_target) AS ytd_ju_target,
      sum(ytd_nu_target) AS ytd_nu_target,
      sum(ytd_le_target) AS ytd_le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'MAPE'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  bias AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target,
      sum(ytd_cy_actual) AS ytd_cy_actual,
      sum(ytd_py_actual) AS ytd_py_actual,
      sum(ytd_bp_target) AS ytd_bp_target,
      sum(ytd_ju_target) AS ytd_ju_target,
      sum(ytd_nu_target) AS ytd_nu_target,
      sum(ytd_le_target) AS ytd_le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'BIAS'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  sustainability_score_avn AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target,
      sum(le_target) AS le_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'SUSTAINABILITY_SCORE_ON_AVN'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  cogs AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'COGS'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  dio AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'DIO'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  mo_val_crtn AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'MO VALUE CREATION'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  timely_dcsn AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'TIMELY DECISION'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  bmc_cntrbtn_nts AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'BMC CONTRIBUTION OF % NTS'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  phrmcy_share AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'PHARMACY SHARE'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ecom_shr_cn AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'ECOM SHARE CHINA'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ecom_shr_rest_asia AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'ECOM SHARE REST OF ASIA'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ecom_share AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'ECOM SHARE'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  tdp_share AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'TDP SHARE'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  dqi AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'DQI:PHYSICAL AVAILABILITY,PLASTIC & TRANSACTIONAL DATA'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  mdp_share AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'MDP SHARE GROWTH'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  phrmcy_hb_chnl_grwth AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'PHARMACY AND H&B CHANNEL GROWTH'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  sos_growth AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'SHARE OF SEARCH GROWTH'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  e2e_invst AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'E2E DEMAND GENERATING INVESTMENT'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  dos AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'DOS'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  dso AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'DSO'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  sc_brand_nts AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = '% SPIKE & CORE BRANDS OF TOTAL NTS'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  days_of_sales AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'DAYS OF SALES'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  per_nts_from_ecom AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = '% OF NTS FROM ECOMM'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  ecomm_growth AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'ECOMM GROWTH'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  td_initiative AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'TECH & DATA INITIATIVES (AU)'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  tsa_on_time AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'TSA ON TIME'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    ),
  gross_inventory AS (
    SELECT year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market,
      sum(cy_actual) AS cy_actual,
      sum(py_actual) AS py_actual,
      sum(bp_target) AS bp_target,
      sum(ju_target) AS ju_target,
      sum(nu_target) AS nu_target
    FROM edw_rpt_okr_dashboard_intermediate
    WHERE upper(measure) = 'GROSS_INVENTORY'
    GROUP BY year_month,
      fisc_year,
      quarter,
      brand,
      segment,
      cluster,
      market
    )
SELECT to_date(base.year_month, 'YYYYMM') AS year_month,
  base.fisc_year,
  base.quarter,
  CASE 
    WHEN base.brand = 'Ben Gay'
      THEN 'Bengay'
    ELSE base.brand
    END AS brand,
  CASE 
    WHEN base.segment = 'Skin Health'
      THEN 'Skin Health & Beauty'
    ELSE base.segment
    END AS segment,
  base.cluster,
  base.market,
  base.core_brand,
  base.ppm,
  nts.cy_actual AS nts_cy_actual,
  nts.py_actual AS nts_py_actual,
  nts.bp_target AS nts_bp_target,
  nts.ju_target AS nts_ju_target,
  nts.nu_target AS nts_nu_target,
  nts.le_target AS nts_le_target,
  nts.btg AS nts_btg,
  gts.cy_actual AS gts_cy_actual,
  gts.py_actual AS gts_py_actual,
  gts.bp_target AS gts_bp_target,
  gts.ju_target AS gts_ju_target,
  gts.nu_target AS gts_nu_target,
  gts.le_target AS gts_le_target,
  NULL AS ciw_cy_actual,
  NULL AS ciw_py_actual,
  NULL AS ciw_bp_target,
  NULL AS ciw_ju_target,
  NULL AS ciw_nu_target,
  NULL AS ciw_le_target,
  gp.cy_actual AS gp_cy_actual,
  gp.py_actual AS gp_py_actual,
  gp.bp_target AS gp_bp_target,
  gp.ju_target AS gp_ju_target,
  gp.nu_target AS gp_nu_target,
  gp.le_target AS gp_le_target,
  gp.btg AS gp_btg,
  bme.cy_actual AS bme_cy_actual,
  bme.py_actual AS bme_py_actual,
  bme.bp_target AS bme_bp_target,
  bme.ju_target AS bme_ju_target,
  bme.nu_target AS bme_nu_target,
  bme.le_target AS bme_le_target,
  bme_percent.cy_actual AS bme_percent_cy_actual,
  bme_percent.py_actual AS bme_percent_py_actual,
  bme_percent.bp_target AS bme_percent_bp_target,
  bme_percent.ju_target AS bme_percent_ju_target,
  bme_percent.nu_target AS bme_percent_nu_target,
  bme_percent.le_target AS bme_percent_le_target,
  ciw_percent.cy_actual AS ciw_percent_cy_actual,
  ciw_percent.py_actual AS ciw_percent_py_actual,
  ciw_percent.bp_target AS ciw_percent_bp_target,
  ciw_percent.ju_target AS ciw_percent_ju_target,
  ciw_percent.nu_target AS ciw_percent_nu_target,
  ciw_percent.le_target AS ciw_percent_le_target,
  ibt.cy_actual AS ibt_cy_actual,
  ibt.py_actual AS ibt_py_actual,
  ibt.bp_target AS ibt_bp_target,
  ibt.ju_target AS ibt_ju_target,
  ibt.nu_target AS ibt_nu_target,
  ibt.le_target AS ibt_le_target,
  ibt.btg AS ibt_btg,
  ccc.cy_actual AS ccc_cy_actual,
  ccc.py_actual AS ccc_py_actual,
  ccc.bp_target AS ccc_bp_target,
  ccc.ju_target AS ccc_ju_target,
  ccc.nu_target AS ccc_nu_target,
  ccc.le_target AS ccc_le_target,
  cm.cy_actual AS cm_cy_actual,
  cm.py_actual AS cm_py_actual,
  cm.bp_target AS cm_bp_target,
  cm.ju_target AS cm_ju_target,
  cm.nu_target AS cm_nu_target,
  cm.le_target AS cm_le_target,
  cust_dqi.cy_actual AS cust_dqi_cy_actual,
  cust_dqi.py_actual AS cust_dqi_py_actual,
  cust_dqi.bp_target AS cust_dqi_bp_target,
  cust_dqi.ju_target AS cust_dqi_ju_target,
  cust_dqi.nu_target AS cust_dqi_nu_target,
  cust_dqi.le_target AS cust_dqi_le_target,
  div_scr.cy_actual AS div_scr_cy_actual,
  div_scr.py_actual AS div_scr_py_actual,
  div_scr.bp_target AS div_scr_bp_target,
  div_scr.ju_target AS div_scr_ju_target,
  div_scr.nu_target AS div_scr_nu_target,
  div_scr.le_target AS div_scr_le_target,
  emp_eng.cy_actual AS emp_eng_cy_actual,
  emp_eng.py_actual AS emp_eng_py_actual,
  emp_eng.bp_target AS emp_eng_bp_target,
  emp_eng.ju_target AS emp_eng_ju_target,
  emp_eng.nu_target AS emp_eng_nu_target,
  emp_eng.le_target AS emp_eng_le_target,
  fcf.cy_actual AS fcf_cy_actual,
  fcf.py_actual AS fcf_py_actual,
  fcf.bp_target AS fcf_bp_target,
  fcf.ju_target AS fcf_ju_target,
  fcf.nu_target AS fcf_nu_target,
  fcf.le_target AS fcf_le_target,
  fcf.btg AS fcf_btg,
  incl_scr.cy_actual AS incl_scr_cy_actual,
  incl_scr.py_actual AS incl_scr_py_actual,
  incl_scr.bp_target AS incl_scr_bp_target,
  incl_scr.ju_target AS incl_scr_ju_target,
  incl_scr.nu_target AS incl_scr_nu_target,
  incl_scr.le_target AS incl_scr_le_target,
  nts_grw_shr.cy_actual AS nts_grwng_share_cy_actual,
  nts_grw_shr.py_actual AS nts_grwng_share_py_actual,
  nts_grw_shr.pm_actual AS nts_grwng_share_pm_actual,
  nts_grw_shr.bp_target AS nts_grwng_share_bp_target,
  nts_grw_shr.ju_target AS nts_grwng_share_ju_target,
  nts_grw_shr.nu_target AS nts_grwng_share_nu_target,
  nts_grw_shr.le_target AS nts_grwng_share_le_target,
  nts_grw_shr.nts_grwng_share_size AS nts_grwng_share_size,
  npd_nts.cy_actual AS npd_nts_cy_actual,
  npd_nts.py_actual AS npd_nts_py_actual,
  npd_nts.bp_target AS npd_nts_bp_target,
  npd_nts.ju_target AS npd_nts_ju_target,
  npd_nts.nu_target AS npd_nts_nu_target,
  npd_nts.le_target AS npd_nts_le_target,
  npd_nts.btg AS npd_btg,
  otifd.cy_actual AS otifd_cy_actual,
  otifd.py_actual AS otifd_py_actual,
  otifd.bp_target AS otifd_bp_target,
  otifd.ju_target AS otifd_ju_target,
  otifd.nu_target AS otifd_nu_target,
  otifd.le_target AS otifd_le_target,
  otifd.ytd_cy_actual AS otifd_ytd_cy_actual,
  otifd.ytd_py_actual AS otifd_ytd_py_actual,
  otifd.ytd_bp_target AS otifd_ytd_bp_target,
  otifd.ytd_ju_target AS otifd_ytd_ju_target,
  otifd.ytd_nu_target AS otifd_ytd_nu_target,
  otifd.ytd_le_target AS otifd_ytd_le_target,
  ps.cy_actual AS ps_cy_actual,
  ps.py_actual AS ps_py_actual,
  ps.bp_target AS ps_bp_target,
  ps.ju_target AS ps_ju_target,
  ps.nu_target AS ps_nu_target,
  ps.le_target AS ps_le_target,
  ple.cy_actual AS ple_cy_actual,
  ple.py_actual AS ple_py_actual,
  ple.bp_target AS ple_bp_target,
  ple.ju_target AS ple_ju_target,
  ple.nu_target AS ple_nu_target,
  ple.le_target AS ple_le_target,
  rgm.cy_actual AS rgm_cy_actual,
  rgm.py_actual AS rgm_py_actual,
  rgm.bp_target AS rgm_bp_target,
  rgm.ju_target AS rgm_ju_target,
  rgm.nu_target AS rgm_nu_target,
  rgm.le_target AS rgm_le_target,
  rgm.btg AS rgm_btg,
  ppm_grw.cy_actual AS ppm_grw_cy_actual,
  ppm_grw.py_actual AS ppm_grw_py_actual,
  ppm_grw.bp_target AS ppm_grw_bp_target,
  ppm_grw.ju_target AS ppm_grw_ju_target,
  ppm_grw.nu_target AS ppm_grw_nu_target,
  ppm_grw.le_target AS ppm_grw_le_target,
  tr.cy_actual AS tr_cy_actual,
  tr.py_actual AS tr_py_actual,
  tr.bp_target AS tr_bp_target,
  tr.ju_target AS tr_ju_target,
  tr.nu_target AS tr_nu_target,
  tr.le_target AS tr_le_target,
  twd.cy_actual AS ttl_weighted_dstrbtn_cy_actual,
  twd.py_actual AS ttl_weighted_dstrbtn_py_actual,
  twd.bp_target AS ttl_weighted_dstrbtn_bp_target,
  twd.ju_target AS ttl_weighted_dstrbtn_ju_target,
  twd.nu_target AS ttl_weighted_dstrbtn_nu_target,
  twd.le_target AS ttl_weighted_dstrbtn_le_target,
  vsg.cy_actual AS value_share_growth_cy_actual,
  vsg.py_actual AS value_share_growth_py_actual,
  vsg.bp_target AS value_share_growth_bp_target,
  vsg.ju_target AS value_share_growth_ju_target,
  vsg.nu_target AS value_share_growth_nu_target,
  vsg.le_target AS value_share_growth_le_target,
  cic.cy_actual AS cic_cy_actual,
  cic.py_actual AS cic_py_actual,
  cic.bp_target AS cic_bp_target,
  cic.ju_target AS cic_ju_target,
  cic.nu_target AS cic_nu_target,
  cic.le_target AS cic_le_target,
  ecomm_nts.cy_actual AS ecomm_nts_cy_actual,
  ecomm_nts.py_actual AS ecomm_nts_py_actual,
  ecomm_nts.bp_target AS ecomm_nts_bp_target,
  ecomm_nts.ju_target AS ecomm_nts_ju_target,
  ecomm_nts.nu_target AS ecomm_nts_nu_target,
  ecomm_nts.le_target AS ecomm_nts_le_target,
  ecomm_nts.btg AS ecomm_nts_btg,
  ecomm_rank.cy_actual AS ecomm_rank_cy_actual,
  ecomm_rank.py_actual AS ecomm_rank_py_actual,
  ecomm_rank.bp_target AS ecomm_rank_bp_target,
  ecomm_rank.ju_target AS ecomm_rank_ju_target,
  ecomm_rank.nu_target AS ecomm_rank_nu_target,
  ecomm_rank.le_target AS ecomm_rank_le_target,
  brand_pwr.cy_actual AS brand_power_cy_actual,
  brand_pwr.py_actual AS brand_power_py_actual,
  brand_pwr.bp_target AS brand_power_bp_target,
  brand_pwr.ju_target AS brand_power_ju_target,
  brand_pwr.nu_target AS brand_power_nu_target,
  brand_pwr.le_target AS brand_power_le_target,
  sos.cy_actual AS sos_cy_actual,
  sos.py_actual AS sos_py_actual,
  sos.bp_target AS sos_bp_target,
  sos.ju_target AS sos_ju_target,
  sos.nu_target AS sos_nu_target,
  sos.le_target AS sos_le_target,
  bmc_grw_shr.cy_actual AS bmc_grw_shr_cy_actual,
  bmc_grw_shr.py_actual AS bmc_grw_shr_py_actual,
  bmc_grw_shr.bp_target AS bmc_grw_shr_bp_target,
  bmc_grw_shr.ju_target AS bmc_grw_shr_ju_target,
  bmc_grw_shr.nu_target AS bmc_grw_shr_nu_target,
  bmc_grw_shr.le_target AS bmc_grw_shr_le_target,
  ap_imprvmnt.cy_actual AS ap_imprvmnt_cy_actual,
  ap_imprvmnt.py_actual AS ap_imprvmnt_py_actual,
  ap_imprvmnt.bp_target AS ap_imprvmnt_bp_target,
  ap_imprvmnt.ju_target AS ap_imprvmnt_ju_target,
  ap_imprvmnt.nu_target AS ap_imprvmnt_nu_target,
  ap_imprvmnt.le_target AS ap_imprvmnt_le_target,
  ar_imprvmnt.cy_actual AS ar_imprvmnt_cy_actual,
  ar_imprvmnt.py_actual AS ar_imprvmnt_py_actual,
  ar_imprvmnt.bp_target AS ar_imprvmnt_bp_target,
  ar_imprvmnt.ju_target AS ar_imprvmnt_ju_target,
  ar_imprvmnt.nu_target AS ar_imprvmnt_nu_target,
  ar_imprvmnt.le_target AS ar_imprvmnt_le_target,
  dnd_iq_imprvmnt.cy_actual AS dnd_iq_imprvmnt_cy_actual,
  dnd_iq_imprvmnt.py_actual AS dnd_iq_imprvmnt_py_actual,
  dnd_iq_imprvmnt.bp_target AS dnd_iq_imprvmnt_bp_target,
  dnd_iq_imprvmnt.ju_target AS dnd_iq_imprvmnt_ju_target,
  dnd_iq_imprvmnt.nu_target AS dnd_iq_imprvmnt_nu_target,
  dnd_iq_imprvmnt.le_target AS dnd_iq_imprvmnt_le_target,
  inventory_reduction.cy_actual AS inventory_reduction_cy_actual,
  inventory_reduction.py_actual AS inventory_reduction_py_actual,
  inventory_reduction.bp_target AS inventory_reduction_bp_target,
  inventory_reduction.ju_target AS inventory_reduction_ju_target,
  inventory_reduction.nu_target AS inventory_reduction_nu_target,
  inventory_reduction.le_target AS inventory_reduction_le_target,
  sustainability_score.cy_actual AS sustainability_score_cy_actual,
  sustainability_score.py_actual AS sustainability_score_py_actual,
  sustainability_score.bp_target AS sustainability_score_bp_target,
  sustainability_score.ju_target AS sustainability_score_ju_target,
  sustainability_score.nu_target AS sustainability_score_nu_target,
  sustainability_score.le_target AS sustainability_score_le_target,
  slob.cy_actual AS slob_cy_actual,
  slob.py_actual AS slob_py_actual,
  slob.bp_target AS slob_bp_target,
  slob.ju_target AS slob_ju_target,
  slob.nu_target AS slob_nu_target,
  slob.le_target AS slob_le_target,
  slob.ytd_cy_actual AS slob_ytd_cy_actual,
  slob.ytd_py_actual AS slob_ytd_py_actual,
  slob.ytd_bp_target AS slob_ytd_bp_target,
  slob.ytd_ju_target AS slob_ytd_ju_target,
  slob.ytd_nu_target AS slob_ytd_nu_target,
  slob.ytd_le_target AS slob_ytd_le_target,
  sys_rat_auto.cy_actual AS sys_rat_auto_cy_actual,
  sys_rat_auto.py_actual AS sys_rat_auto_py_actual,
  sys_rat_auto.bp_target AS sys_rat_auto_bp_target,
  sys_rat_auto.ju_target AS sys_rat_auto_ju_target,
  sys_rat_auto.nu_target AS sys_rat_auto_nu_target,
  sys_rat_auto.le_target AS sys_rat_auto_le_target,
  spike_brnd_grwth.cy_actual AS spike_brnd_grwth_cy_actual,
  spike_brnd_grwth.py_actual AS spike_brnd_grwth_py_actual,
  spike_brnd_grwth.bp_target AS spike_brnd_grwth_bp_target,
  spike_brnd_grwth.ju_target AS spike_brnd_grwth_ju_target,
  spike_brnd_grwth.nu_target AS spike_brnd_grwth_nu_target,
  spike_brnd_grwth.le_target AS spike_brnd_grwth_le_target,
  spike_brnd_grwth.ytd_cy_actual AS spike_brnd_grwth_ytd_cy_actual,
  spike_brnd_grwth.ytd_py_actual AS spike_brnd_grwth_ytd_py_actual,
  spike_brnd_grwth.ytd_bp_target AS spike_brnd_grwth_ytd_bp_target,
  spike_brnd_grwth.ytd_ju_target AS spike_brnd_grwth_ytd_ju_target,
  spike_brnd_grwth.ytd_nu_target AS spike_brnd_grwth_ytd_nu_target,
  spike_brnd_grwth.ytd_le_target AS spike_brnd_grwth_ytd_le_target,
  mape.cy_actual AS mape_cy_actual,
  mape.py_actual AS mape_py_actual,
  mape.bp_target AS mape_bp_target,
  mape.ju_target AS mape_ju_target,
  mape.nu_target AS mape_nu_target,
  mape.le_target AS mape_le_target,
  mape.ytd_cy_actual AS mape_ytd_cy_actual,
  mape.ytd_py_actual AS mape_ytd_py_actual,
  mape.ytd_bp_target AS mape_ytd_bp_target,
  mape.ytd_ju_target AS mape_ytd_ju_target,
  mape.ytd_nu_target AS mape_ytd_nu_target,
  mape.ytd_le_target AS mape_ytd_le_target,
  bias.cy_actual AS bias_cy_actual,
  bias.py_actual AS bias_py_actual,
  bias.bp_target AS bias_bp_target,
  bias.ju_target AS bias_ju_target,
  bias.nu_target AS bias_nu_target,
  bias.le_target AS bias_le_target,
  bias.ytd_cy_actual AS bias_ytd_cy_actual,
  bias.ytd_py_actual AS bias_ytd_py_actual,
  bias.ytd_bp_target AS bias_ytd_bp_target,
  bias.ytd_ju_target AS bias_ytd_ju_target,
  bias.ytd_nu_target AS bias_ytd_nu_target,
  bias.ytd_le_target AS bias_ytd_le_target,
  sustainability_score_avn.cy_actual AS sustainability_score_avn_cy_actual,
  sustainability_score_avn.py_actual AS sustainability_score_avn_py_actual,
  sustainability_score_avn.bp_target AS sustainability_score_avn_bp_target,
  sustainability_score_avn.ju_target AS sustainability_score_avn_ju_target,
  sustainability_score_avn.nu_target AS sustainability_score_avn_nu_target,
  sustainability_score_avn.le_target AS sustainability_score_avn_le_target,
  cogs.cy_actual AS cogs_cy_actual,
  cogs.py_actual AS cogs_py_actual,
  cogs.bp_target AS cogs_bp_target,
  cogs.ju_target AS cogs_ju_target,
  cogs.nu_target AS cogs_nu_target,
  dio.cy_actual AS dio_cy_actual,
  dio.py_actual AS dio_py_actual,
  dio.bp_target AS dio_bp_target,
  dio.ju_target AS dio_ju_target,
  dio.nu_target AS dio_nu_target,
  mo_val_crtn.cy_actual AS mo_val_crtn_cy_actual,
  mo_val_crtn.py_actual AS mo_val_crtn_py_actual,
  mo_val_crtn.bp_target AS mo_val_crtn_bp_target,
  mo_val_crtn.ju_target AS mo_val_crtn_ju_target,
  mo_val_crtn.nu_target AS mo_val_crtn_nu_target,
  timely_dcsn.cy_actual AS timely_dcsn_cy_actual,
  timely_dcsn.py_actual AS timely_dcsn_py_actual,
  timely_dcsn.bp_target AS timely_dcsn_bp_target,
  timely_dcsn.ju_target AS timely_dcsn_ju_target,
  timely_dcsn.nu_target AS timely_dcsn_nu_target,
  bmc_cntrbtn_nts.cy_actual AS bmc_cntrbtn_nts_cy_actual,
  bmc_cntrbtn_nts.py_actual AS bmc_cntrbtn_nts_py_actual,
  bmc_cntrbtn_nts.bp_target AS bmc_cntrbtn_nts_bp_target,
  bmc_cntrbtn_nts.ju_target AS bmc_cntrbtn_nts_ju_target,
  bmc_cntrbtn_nts.nu_target AS bmc_cntrbtn_nts_nu_target,
  phrmcy_share.cy_actual AS phrmcy_share_cy_actual,
  phrmcy_share.py_actual AS phrmcy_share_py_actual,
  phrmcy_share.bp_target AS phrmcy_share_bp_target,
  phrmcy_share.ju_target AS phrmcy_share_ju_target,
  phrmcy_share.nu_target AS phrmcy_share_nu_target,
  ecom_shr_cn.cy_actual AS ecom_shr_cn_cy_actual,
  ecom_shr_cn.py_actual AS ecom_shr_cn_py_actual,
  ecom_shr_cn.bp_target AS ecom_shr_cn_bp_target,
  ecom_shr_cn.ju_target AS ecom_shr_cn_ju_target,
  ecom_shr_cn.nu_target AS ecom_shr_cn_nu_target,
  ecom_shr_rest_asia.cy_actual AS ecom_shr_rest_asia_cy_actual,
  ecom_shr_rest_asia.py_actual AS ecom_shr_rest_asia_py_actual,
  ecom_shr_rest_asia.bp_target AS ecom_shr_rest_asia_bp_target,
  ecom_shr_rest_asia.ju_target AS ecom_shr_rest_asia_ju_target,
  ecom_shr_rest_asia.nu_target AS ecom_shr_rest_asia_nu_target,
  ecom_share.cy_actual AS ecom_share_cy_actual,
  ecom_share.py_actual AS ecom_share_py_actual,
  ecom_share.bp_target AS ecom_share_bp_target,
  ecom_share.ju_target AS ecom_share_ju_target,
  ecom_share.nu_target AS ecom_share_nu_target,
  tdp_share.cy_actual AS tdp_share_cy_actual,
  tdp_share.py_actual AS tdp_share_py_actual,
  tdp_share.bp_target AS tdp_share_bp_target,
  tdp_share.ju_target AS tdp_share_ju_target,
  tdp_share.nu_target AS tdp_share_nu_target,
  dqi.cy_actual AS dqi_cy_actual,
  dqi.py_actual AS dqi_py_actual,
  dqi.bp_target AS dqi_bp_target,
  dqi.ju_target AS dqi_ju_target,
  dqi.nu_target AS dqi_nu_target,
  mdp_share.cy_actual AS mdp_share_cy_actual,
  mdp_share.py_actual AS mdp_share_py_actual,
  mdp_share.bp_target AS mdp_share_bp_target,
  mdp_share.ju_target AS mdp_share_ju_target,
  mdp_share.nu_target AS mdp_share_nu_target,
  phrmcy_hb_chnl_grwth.cy_actual AS phrmcy_hb_chnl_grwth_cy_actual,
  phrmcy_hb_chnl_grwth.py_actual AS phrmcy_hb_chnl_grwth_py_actual,
  phrmcy_hb_chnl_grwth.bp_target AS phrmcy_hb_chnl_grwth_bp_target,
  phrmcy_hb_chnl_grwth.ju_target AS phrmcy_hb_chnl_grwth_ju_target,
  phrmcy_hb_chnl_grwth.nu_target AS phrmcy_hb_chnl_grwth_nu_target,
  sos_growth.cy_actual AS sos_growth_cy_actual,
  sos_growth.py_actual AS sos_growth_py_actual,
  sos_growth.bp_target AS sos_growth_bp_target,
  sos_growth.ju_target AS sos_growth_ju_target,
  sos_growth.nu_target AS sos_growth_nu_target,
  e2e_invst.cy_actual AS e2e_invst_cy_actual,
  e2e_invst.py_actual AS e2e_invst_py_actual,
  e2e_invst.bp_target AS e2e_invst_bp_target,
  e2e_invst.ju_target AS e2e_invst_ju_target,
  e2e_invst.nu_target AS e2e_invst_nu_target,
  dos.cy_actual AS dos_cy_actual,
  dos.py_actual AS dos_py_actual,
  dos.bp_target AS dos_bp_target,
  dos.ju_target AS dos_ju_target,
  dos.nu_target AS dos_nu_target,
  dso.cy_actual AS dso_cy_actual,
  dso.py_actual AS dso_py_actual,
  dso.bp_target AS dso_bp_target,
  dso.ju_target AS dso_ju_target,
  dso.nu_target AS dso_nu_target,
  sc_brand_nts.cy_actual AS sc_brand_nts_cy_actual,
  sc_brand_nts.py_actual AS sc_brand_nts_py_actual,
  sc_brand_nts.bp_target AS sc_brand_nts_bp_target,
  sc_brand_nts.ju_target AS sc_brand_nts_ju_target,
  sc_brand_nts.nu_target AS sc_brand_nts_nu_target,
  days_of_sales.cy_actual AS days_of_sales_cy_actual,
  days_of_sales.py_actual AS days_of_sales_py_actual,
  days_of_sales.bp_target AS days_of_sales_bp_target,
  days_of_sales.ju_target AS days_of_sales_ju_target,
  days_of_sales.nu_target AS days_of_sales_nu_target,
  per_nts_from_ecom.cy_actual AS per_nts_from_ecom_cy_actual,
  per_nts_from_ecom.py_actual AS per_nts_from_ecom_py_actual,
  per_nts_from_ecom.bp_target AS per_nts_from_ecom_bp_target,
  per_nts_from_ecom.ju_target AS per_nts_from_ecom_ju_target,
  per_nts_from_ecom.nu_target AS per_nts_from_ecom_nu_target,
  ecomm_growth.cy_actual AS ecomm_growth_cy_actual,
  ecomm_growth.py_actual AS ecomm_growth_py_actual,
  ecomm_growth.bp_target AS ecomm_growth_bp_target,
  ecomm_growth.ju_target AS ecomm_growth_ju_target,
  ecomm_growth.nu_target AS ecomm_growth_nu_target,
  td_initiative.cy_actual AS td_initiative_cy_actual,
  td_initiative.py_actual AS td_initiative_py_actual,
  td_initiative.bp_target AS td_initiative_bp_target,
  td_initiative.ju_target AS td_initiative_ju_target,
  td_initiative.nu_target AS td_initiative_nu_target,
  tsa_on_time.cy_actual AS tsa_on_time_cy_actual,
  tsa_on_time.py_actual AS tsa_on_time_py_actual,
  tsa_on_time.bp_target AS tsa_on_time_bp_target,
  tsa_on_time.ju_target AS tsa_on_time_ju_target,
  tsa_on_time.nu_target AS tsa_on_time_nu_target,
  gross_inventory.cy_actual AS gross_inventory_cy_actual,
  gross_inventory.py_actual AS gross_inventory_py_actual,
  gross_inventory.bp_target AS gross_inventory_bp_target,
  gross_inventory.ju_target AS gross_inventory_ju_target,
  gross_inventory.nu_target AS gross_inventory_nu_target
FROM base
---------------------------NTS---------------------------------------------------
LEFT JOIN nts ON nvl(base.year_month, '9999') = nvl(nts.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(nts.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(nts.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(nts.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(nts.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(nts.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(nts.market, '9999')
---------------------------GTS---------------------------------------------------
LEFT JOIN gts ON nvl(base.year_month, '9999') = nvl(gts.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(gts.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(gts.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(gts.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(gts.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(gts.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(gts.market, '9999')
----------------------------GP -------------------------------------------------------------
LEFT JOIN gp ON nvl(base.year_month, '9999') = nvl(gp.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(gp.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(gp.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(gp.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(gp.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(gp.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(gp.market, '9999')
---------------------------------------------------BME$----------------------------------------------------
LEFT JOIN bme ON nvl(base.year_month, '9999') = nvl(bme.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(bme.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(bme.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(bme.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(bme.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(bme.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(bme.market, '9999')
---------------------------------------------------BME%----------------------------------------------------
LEFT JOIN bme_percent ON nvl(base.year_month, '9999') = nvl(bme_percent.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(bme_percent.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(bme_percent.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(bme_percent.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(bme_percent.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(bme_percent.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(bme_percent.market, '9999')
---------------------------------------------------CIW%----------------------------------------------------
LEFT JOIN ciw_percent ON nvl(base.year_month, '9999') = nvl(ciw_percent.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ciw_percent.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ciw_percent.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ciw_percent.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ciw_percent.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ciw_percent.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ciw_percent.market, '9999')
-----------------------------------------------IBT-------------------------------------------------------------
LEFT JOIN ibt ON nvl(base.year_month, '9999') = nvl(ibt.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ibt.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ibt.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ibt.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ibt.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ibt.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ibt.market, '9999')
-----------------------------------------------Cash Conversion Cycle-------------------------------------------------------------
LEFT JOIN ccc ON nvl(base.year_month, '9999') = nvl(ccc.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ccc.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ccc.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ccc.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ccc.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ccc.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ccc.market, '9999')
-----------------------------------------------Contribution Margin------------------------------------------------------------
LEFT JOIN cm ON nvl(base.year_month, '9999') = nvl(cm.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(cm.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(cm.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(cm.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(cm.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(cm.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(cm.market, '9999')
-----------------------------------------------Customer DQI------------------------------------------------------------
LEFT JOIN cust_dqi ON nvl(base.year_month, '9999') = nvl(cust_dqi.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(cust_dqi.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(cust_dqi.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(cust_dqi.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(cust_dqi.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(cust_dqi.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(cust_dqi.market, '9999')
-----------------------------------------------Diversity Score------------------------------------------------------------
LEFT JOIN div_scr ON nvl(base.year_month, '9999') = nvl(div_scr.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(div_scr.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(div_scr.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(div_scr.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(div_scr.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(div_scr.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(div_scr.market, '9999')
-----------------------------------------------Employee Engagement------------------------------------------------------------
LEFT JOIN emp_eng ON nvl(base.year_month, '9999') = nvl(emp_eng.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(emp_eng.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(emp_eng.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(emp_eng.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(emp_eng.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(emp_eng.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(emp_eng.market, '9999')
-----------------------------------------------Free Cash Flow-----------------------------------------------------------
LEFT JOIN fcf ON nvl(base.year_month, '9999') = nvl(fcf.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(fcf.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(fcf.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(fcf.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(fcf.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(fcf.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(fcf.market, '9999')
-----------------------------------------------Inclusion Score-----------------------------------------------------------
LEFT JOIN incl_scr ON nvl(base.year_month, '9999') = nvl(incl_scr.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(incl_scr.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(incl_scr.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(incl_scr.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(incl_scr.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(incl_scr.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(incl_scr.market, '9999')
-----------------------------------------------NTS % growing Share-----------------------------------------------------------
LEFT JOIN nts_grw_shr ON nvl(base.year_month, '9999') = nvl(nts_grw_shr.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(nts_grw_shr.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(nts_grw_shr.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(nts_grw_shr.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(nts_grw_shr.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(nts_grw_shr.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(nts_grw_shr.market, '9999')
-----------------------------------------------NTS from NPD-----------------------------------------------------------
LEFT JOIN npd_nts ON nvl(base.year_month, '9999') = nvl(npd_nts.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(npd_nts.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(npd_nts.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(npd_nts.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(npd_nts.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(npd_nts.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(npd_nts.market, '9999')
-----------------------------------------------OTIF-D-----------------------------------------------------------
LEFT JOIN otifd ON nvl(base.year_month, '9999') = nvl(otifd.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(otifd.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(otifd.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(otifd.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(otifd.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(otifd.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(otifd.market, '9999')
-----------------------------------------------offline Perfect Store-----------------------------------------------------------
LEFT JOIN ps ON nvl(base.year_month, '9999') = nvl(ps.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ps.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ps.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ps.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ps.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ps.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ps.market, '9999')
-----------------------------------------------People_Leader_Effectiveness_index-----------------------------------------------------------
LEFT JOIN ple ON nvl(base.year_month, '9999') = nvl(ple.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ple.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ple.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ple.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ple.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ple.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ple.market, '9999')
-----------------------------------------------RGM-----------------------------------------------------------
LEFT JOIN rgm ON nvl(base.year_month, '9999') = nvl(rgm.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(rgm.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(rgm.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(rgm.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(rgm.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(rgm.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(rgm.market, '9999')
-----------------------------------------------Spike PPM Growth-----------------------------------------------------------
LEFT JOIN ppm_grw ON nvl(base.year_month, '9999') = nvl(ppm_grw.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ppm_grw.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ppm_grw.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ppm_grw.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ppm_grw.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ppm_grw.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ppm_grw.market, '9999')
-----------------------------------------------Talent_Retention-----------------------------------------------------------
LEFT JOIN tr ON nvl(base.year_month, '9999') = nvl(tr.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(tr.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(tr.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(tr.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(tr.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(tr.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(tr.market, '9999')
-----------------------------------------------Total weighted distribution-----------------------------------------------------------
LEFT JOIN twd ON nvl(base.year_month, '9999') = nvl(twd.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(twd.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(twd.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(twd.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(twd.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(twd.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(twd.market, '9999')
-----------------------------------------------Value SHare Growth-----------------------------------------------------------
LEFT JOIN vsg ON nvl(base.year_month, '9999') = nvl(vsg.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(vsg.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(vsg.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(vsg.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(vsg.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(vsg.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(vsg.market, '9999')
-----------------------------------------------Zero_disruptions_as_CiC-----------------------------------------------------------
LEFT JOIN cic ON nvl(base.year_month, '9999') = nvl(cic.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(cic.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(cic.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(cic.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(cic.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(cic.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(cic.market, '9999')
-----------------------------------------------Ecommerce NTS-----------------------------------------------------------
LEFT JOIN ecomm_nts ON nvl(base.year_month, '9999') = nvl(ecomm_nts.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ecomm_nts.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ecomm_nts.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ecomm_nts.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ecomm_nts.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ecomm_nts.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ecomm_nts.market, '9999')
-----------------------------------------------Ecommerce Rank-----------------------------------------------------------
LEFT JOIN ecomm_rank ON nvl(base.year_month, '9999') = nvl(ecomm_rank.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ecomm_rank.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ecomm_rank.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ecomm_rank.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ecomm_rank.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ecomm_rank.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ecomm_rank.market, '9999')
-----------------------------------------------Brand Power-----------------------------------------------------------
LEFT JOIN brand_pwr ON nvl(base.year_month, '9999') = nvl(brand_pwr.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(brand_pwr.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(brand_pwr.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(brand_pwr.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(brand_pwr.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(brand_pwr.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(brand_pwr.market, '9999')
-----------------------------------------------6PAI-----------------------------------------------------------
LEFT JOIN sos ON nvl(base.year_month, '9999') = nvl(sos.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(sos.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(sos.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(sos.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(sos.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(sos.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(sos.market, '9999')
-----------------------------------------------BMC Grpwing SHare-----------------------------------------------------------
LEFT JOIN bmc_grw_shr ON nvl(base.year_month, '9999') = nvl(bmc_grw_shr.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(bmc_grw_shr.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(bmc_grw_shr.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(bmc_grw_shr.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(bmc_grw_shr.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(bmc_grw_shr.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(bmc_grw_shr.market, '9999')
-----------------------------------------------AP Improvement-----------------------------------------------------------
LEFT JOIN ap_imprvmnt ON nvl(base.year_month, '9999') = nvl(ap_imprvmnt.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ap_imprvmnt.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ap_imprvmnt.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ap_imprvmnt.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ap_imprvmnt.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ap_imprvmnt.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ap_imprvmnt.market, '9999')
-----------------------------------------------AR Improvement-----------------------------------------------------------
LEFT JOIN ar_imprvmnt ON nvl(base.year_month, '9999') = nvl(ar_imprvmnt.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ar_imprvmnt.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ar_imprvmnt.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ar_imprvmnt.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ar_imprvmnt.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ar_imprvmnt.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ar_imprvmnt.market, '9999')
-----------------------------------------------Digital & Data IQ Improvement-----------------------------------------------------------
LEFT JOIN dnd_iq_imprvmnt ON nvl(base.year_month, '9999') = nvl(dnd_iq_imprvmnt.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(dnd_iq_imprvmnt.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(dnd_iq_imprvmnt.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(dnd_iq_imprvmnt.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(dnd_iq_imprvmnt.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(dnd_iq_imprvmnt.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(dnd_iq_imprvmnt.market, '9999')
-----------------------------------------------Inventory Reduction-----------------------------------------------------------
LEFT JOIN inventory_reduction ON nvl(base.year_month, '9999') = nvl(inventory_reduction.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(inventory_reduction.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(inventory_reduction.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(inventory_reduction.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(inventory_reduction.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(inventory_reduction.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(inventory_reduction.market, '9999')
-----------------------------------------------Sustainability Score on Listerine ---------------------------------------------------------
LEFT JOIN sustainability_score ON nvl(base.year_month, '9999') = nvl(sustainability_score.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(sustainability_score.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(sustainability_score.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(sustainability_score.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(sustainability_score.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(sustainability_score.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(sustainability_score.market, '9999')
-----------------------------------------------SLOB-----------------------------------------------------------
LEFT JOIN slob ON nvl(base.year_month, '9999') = nvl(slob.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(slob.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(slob.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(slob.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(slob.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(slob.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(slob.market, '9999')
-----------------------------------------------System Rat & Automation-----------------------------------------------------------
LEFT JOIN sys_rat_auto ON nvl(base.year_month, '9999') = nvl(sys_rat_auto.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(sys_rat_auto.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(sys_rat_auto.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(sys_rat_auto.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(sys_rat_auto.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(sys_rat_auto.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(sys_rat_auto.market, '9999')
-----------------------------------------------Spike Brand NTS % Growth-----------------------------------------------------------
LEFT JOIN spike_brnd_grwth ON nvl(base.year_month, '9999') = nvl(spike_brnd_grwth.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(spike_brnd_grwth.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(spike_brnd_grwth.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(spike_brnd_grwth.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(spike_brnd_grwth.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(spike_brnd_grwth.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(spike_brnd_grwth.market, '9999')
-----------------------------------------------MAPE-----------------------------------------------------------
LEFT JOIN mape ON nvl(base.year_month, '9999') = nvl(mape.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(mape.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(mape.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(mape.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(mape.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(mape.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(mape.market, '9999')
-----------------------------------------------BIAS-----------------------------------------------------------
LEFT JOIN bias ON nvl(base.year_month, '9999') = nvl(bias.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(bias.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(bias.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(bias.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(bias.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(bias.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(bias.market, '9999')
-----------------------------------------------Sustainability Score on Aveeno ---------------------------------------------------------
LEFT JOIN sustainability_score_avn ON nvl(base.year_month, '9999') = nvl(sustainability_score_avn.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(sustainability_score_avn.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(sustainability_score_avn.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(sustainability_score_avn.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(sustainability_score_avn.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(sustainability_score_avn.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(sustainability_score_avn.market, '9999')
-----------------------------------------------COGS ---------------------------------------------------------
LEFT JOIN cogs ON nvl(base.year_month, '9999') = nvl(cogs.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(cogs.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(cogs.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(cogs.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(cogs.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(cogs.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(cogs.market, '9999')
-----------------------------------------------DIO ---------------------------------------------------------
LEFT JOIN dio ON nvl(base.year_month, '9999') = nvl(dio.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(dio.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(dio.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(dio.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(dio.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(dio.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(dio.market, '9999')
-----------------------------------------------MO Value Creation ---------------------------------------------------------
LEFT JOIN mo_val_crtn ON nvl(base.year_month, '9999') = nvl(mo_val_crtn.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(mo_val_crtn.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(mo_val_crtn.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(mo_val_crtn.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(mo_val_crtn.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(mo_val_crtn.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(mo_val_crtn.market, '9999')
----------------------------------------------Timely Decision ---------------------------------------------------------
LEFT JOIN timely_dcsn ON nvl(base.year_month, '9999') = nvl(timely_dcsn.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(timely_dcsn.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(timely_dcsn.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(timely_dcsn.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(timely_dcsn.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(timely_dcsn.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(timely_dcsn.market, '9999')
----------------------------------------------BMC Contribution of % NTS ---------------------------------------------------------
LEFT JOIN bmc_cntrbtn_nts ON nvl(base.year_month, '9999') = nvl(bmc_cntrbtn_nts.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(bmc_cntrbtn_nts.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(bmc_cntrbtn_nts.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(bmc_cntrbtn_nts.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(bmc_cntrbtn_nts.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(bmc_cntrbtn_nts.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(bmc_cntrbtn_nts.market, '9999')
----------------------------------------------Pharmacy Share ---------------------------------------------------------
LEFT JOIN phrmcy_share ON nvl(base.year_month, '9999') = nvl(phrmcy_share.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(phrmcy_share.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(phrmcy_share.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(phrmcy_share.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(phrmcy_share.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(phrmcy_share.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(phrmcy_share.market, '9999')
----------------------------------------------Ecomm Share of China---------------------------------------------------------
LEFT JOIN ecom_shr_cn ON nvl(base.year_month, '9999') = nvl(ecom_shr_cn.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ecom_shr_cn.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ecom_shr_cn.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ecom_shr_cn.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ecom_shr_cn.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ecom_shr_cn.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ecom_shr_cn.market, '9999')
----------------------------------------------Ecomm Share Rest of Asia---------------------------------------------------------
LEFT JOIN ecom_shr_rest_asia ON nvl(base.year_month, '9999') = nvl(ecom_shr_rest_asia.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ecom_shr_rest_asia.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ecom_shr_rest_asia.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ecom_shr_rest_asia.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ecom_shr_rest_asia.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ecom_shr_rest_asia.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ecom_shr_rest_asia.market, '9999')
----------------------------------------------Ecomm Share---------------------------------------------------------
LEFT JOIN ecom_share ON nvl(base.year_month, '9999') = nvl(ecom_share.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ecom_share.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ecom_share.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ecom_share.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ecom_share.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ecom_share.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ecom_share.market, '9999')
----------------------------------------------TDP Share---------------------------------------------------------
LEFT JOIN tdp_share ON nvl(base.year_month, '9999') = nvl(tdp_share.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(tdp_share.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(tdp_share.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(tdp_share.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(tdp_share.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(tdp_share.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(tdp_share.market, '9999')
----------------------------------------------DQI---------------------------------------------------------
LEFT JOIN dqi ON nvl(base.year_month, '9999') = nvl(dqi.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(dqi.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(dqi.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(dqi.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(dqi.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(dqi.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(dqi.market, '9999')
----------------------------------------------MDP Share Growth---------------------------------------------------------
LEFT JOIN mdp_share ON nvl(base.year_month, '9999') = nvl(mdp_share.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(mdp_share.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(mdp_share.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(mdp_share.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(mdp_share.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(mdp_share.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(mdp_share.market, '9999')
----------------------------------------------Pharmacy and H&B Channel Growth---------------------------------------------------------
LEFT JOIN phrmcy_hb_chnl_grwth ON nvl(base.year_month, '9999') = nvl(phrmcy_hb_chnl_grwth.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(phrmcy_hb_chnl_grwth.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(phrmcy_hb_chnl_grwth.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(phrmcy_hb_chnl_grwth.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(phrmcy_hb_chnl_grwth.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(phrmcy_hb_chnl_grwth.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(phrmcy_hb_chnl_grwth.market, '9999')
----------------------------------------------Share of Search Growth---------------------------------------------------------
LEFT JOIN sos_growth ON nvl(base.year_month, '9999') = nvl(sos_growth.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(sos_growth.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(sos_growth.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(sos_growth.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(sos_growth.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(sos_growth.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(sos_growth.market, '9999')
----------------------------------------------E2E Demand Generating Investment---------------------------------------------------------
LEFT JOIN e2e_invst ON nvl(base.year_month, '9999') = nvl(e2e_invst.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(e2e_invst.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(e2e_invst.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(e2e_invst.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(e2e_invst.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(e2e_invst.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(e2e_invst.market, '9999')
----------------------------------------------Days of Supply ---------------------------------------------------------
LEFT JOIN dos ON nvl(base.year_month, '9999') = nvl(dos.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(dos.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(dos.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(dos.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(dos.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(dos.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(dos.market, '9999')
----------------------------------------------Days of Sales Outstanding (ANZ) ---------------------------------------------------------
LEFT JOIN dso ON nvl(base.year_month, '9999') = nvl(dso.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(dso.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(dso.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(dso.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(dso.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(dso.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(dso.market, '9999')
----------------------------------------------% Spike and Core Brand of NTS ---------------------------------------------------------
LEFT JOIN sc_brand_nts ON nvl(base.year_month, '9999') = nvl(sc_brand_nts.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(sc_brand_nts.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(sc_brand_nts.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(sc_brand_nts.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(sc_brand_nts.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(sc_brand_nts.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(sc_brand_nts.market, '9999')
----------------------------------------------Days of Sales(MA) ---------------------------------------------------------
LEFT JOIN days_of_sales ON nvl(base.year_month, '9999') = nvl(days_of_sales.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(days_of_sales.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(days_of_sales.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(days_of_sales.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(days_of_sales.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(days_of_sales.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(days_of_sales.market, '9999')
----------------------------------------------% of NTS from Ecomm ---------------------------------------------------------
LEFT JOIN per_nts_from_ecom ON nvl(base.year_month, '9999') = nvl(per_nts_from_ecom.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(per_nts_from_ecom.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(per_nts_from_ecom.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(per_nts_from_ecom.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(per_nts_from_ecom.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(per_nts_from_ecom.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(per_nts_from_ecom.market, '9999')
----------------------------------------------Ecomm Growth---------------------------------------------------------
LEFT JOIN ecomm_growth ON nvl(base.year_month, '9999') = nvl(ecomm_growth.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(ecomm_growth.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(ecomm_growth.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(ecomm_growth.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(ecomm_growth.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(ecomm_growth.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(ecomm_growth.market, '9999')
----------------------------------------------Tech and Data Initiative (AU)---------------------------------------------------------
LEFT JOIN td_initiative ON nvl(base.year_month, '9999') = nvl(td_initiative.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(td_initiative.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(td_initiative.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(td_initiative.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(td_initiative.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(td_initiative.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(td_initiative.market, '9999')
----------------------------------------------TSA on Time---------------------------------------------------------
LEFT JOIN tsa_on_time ON nvl(base.year_month, '9999') = nvl(tsa_on_time.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(tsa_on_time.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(tsa_on_time.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(tsa_on_time.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(tsa_on_time.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(tsa_on_time.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(tsa_on_time.market, '9999')
----------------------------------------------Gross Inventory ---------------------------------------------------------
LEFT JOIN gross_inventory ON nvl(base.year_month, '9999') = nvl(gross_inventory.year_month, '9999')
  AND nvl(base.fisc_year, '9999') = nvl(gross_inventory.fisc_year, '9999')
  AND nvl(base.quarter, 9999) = nvl(gross_inventory.quarter, 9999)
  AND nvl(base.brand, '9999') = nvl(gross_inventory.brand, '9999')
  AND nvl(base.segment, '9999') = nvl(gross_inventory.segment, '9999')
  AND nvl(base.cluster, '9999') = nvl(gross_inventory.cluster, '9999')
  AND nvl(base.market, '9999') = nvl(gross_inventory.market, '9999')
),
final as
(
    select
    YEAR_MONTH::DATE as YEAR_MONTH,
	FISC_YEAR::VARCHAR(10) as FISC_YEAR,
	QUARTER::NUMBER(38,0) as QUARTER,
	BRAND::VARCHAR(200) as BRAND,
	SEGMENT::VARCHAR(200) as SEGMENT,
	CLUSTER::VARCHAR(100) as CLUSTER,
	MARKET::VARCHAR(100) as MARKET,
	core_brand::VARCHAR(50) as BMC,
	PPM::VARCHAR(50) as PPM,
	NTS_CY_ACTUAL::NUMBER(38,4) as NTS_CY_ACTUAL,
	NTS_PY_ACTUAL::NUMBER(38,4) as NTS_PY_ACTUAL,
	NTS_BP_TARGET::NUMBER(38,4) as NTS_BP_TARGET,
	NTS_JU_TARGET::NUMBER(38,4) as NTS_JU_TARGET,
	NTS_NU_TARGET::NUMBER(38,4) as NTS_NU_TARGET,
	NTS_LE_TARGET::NUMBER(38,4) as NTS_LE_TARGET,
	NTS_BTG::NUMBER(38,4) as NTS_BTG,
	GTS_CY_ACTUAL::NUMBER(38,4) as GTS_CY_ACTUAL,
	GTS_PY_ACTUAL::NUMBER(38,4) as GTS_PY_ACTUAL,
	GTS_BP_TARGET::NUMBER(38,4) as GTS_BP_TARGET,
	GTS_JU_TARGET::NUMBER(38,4) as GTS_JU_TARGET,
	GTS_NU_TARGET::NUMBER(38,4) as GTS_NU_TARGET,
	GTS_LE_TARGET::NUMBER(38,4) as GTS_LE_TARGET,
	CIW_CY_ACTUAL::NUMBER(38,4) as CIW_CY_ACTUAL,
	CIW_PY_ACTUAL::NUMBER(38,4) as CIW_PY_ACTUAL,
	CIW_BP_TARGET::NUMBER(38,4) as CIW_BP_TARGET,
	CIW_JU_TARGET::NUMBER(38,4) as CIW_JU_TARGET,
	CIW_NU_TARGET::NUMBER(38,4) as CIW_NU_TARGET,
	CIW_LE_TARGET::NUMBER(38,4) as CIW_LE_TARGET,
	GP_CY_ACTUAL::NUMBER(38,4) as GP_CY_ACTUAL,
	GP_PY_ACTUAL::NUMBER(38,4) as GP_PY_ACTUAL,
	GP_BP_TARGET::NUMBER(38,4) as GP_BP_TARGET,
	GP_JU_TARGET::NUMBER(38,4) as GP_JU_TARGET,
	GP_NU_TARGET::NUMBER(38,4) as GP_NU_TARGET,
	GP_LE_TARGET::NUMBER(38,4) as GP_LE_TARGET,
	GP_BTG::NUMBER(38,4) as GP_BTG,
	BME_CY_ACTUAL::NUMBER(38,4) as BME_CY_ACTUAL,
	BME_PY_ACTUAL::NUMBER(38,4) as BME_PY_ACTUAL,
	BME_BP_TARGET::NUMBER(38,4) as BME_BP_TARGET,
	BME_JU_TARGET::NUMBER(38,4) as BME_JU_TARGET,
	BME_NU_TARGET::NUMBER(38,4) as BME_NU_TARGET,
	BME_LE_TARGET::NUMBER(38,4) as BME_LE_TARGET,
	bme_percent_cy_actual::NUMBER(38,4) as "bme_%_nts_cy_actual",
	bme_percent_py_actual::NUMBER(38,4) as "bme_%_nts_py_actual",
	bme_percent_bp_target::NUMBER(38,4) as "bme_%_nts_bp_target",
	bme_percent_ju_target::NUMBER(38,4) as "bme_%_nts_ju_target",
	bme_percent_nu_target::NUMBER(38,4) as "bme_%_nts_nu_target",
	bme_percent_le_target::NUMBER(38,4) as "bme_%_nts_le_target",
	ciw_percent_cy_actual::NUMBER(38,4) as "ciw_%_nts_cy_actual",
	ciw_percent_py_actual::NUMBER(38,4) as "ciw_%_nts_py_actual",
	ciw_percent_bp_target::NUMBER(38,4) as "ciw_%_nts_bp_target",
	ciw_percent_ju_target::NUMBER(38,4) as "ciw_%_nts_ju_target",
	ciw_percent_nu_target::NUMBER(38,4) as "ciw_%_nts_nu_target",
	CIW_PERCENT_LE_TARGET::NUMBER(38,4) as CIW_PERCENT_LE_TARGET,
	IBT_CY_ACTUAL::NUMBER(38,4) as IBT_CY_ACTUAL,
	IBT_PY_ACTUAL::NUMBER(38,4) as IBT_PY_ACTUAL,
	IBT_BP_TARGET::NUMBER(38,4) as IBT_BP_TARGET,
	IBT_JU_TARGET::NUMBER(38,4) as IBT_JU_TARGET,
	IBT_NU_TARGET::NUMBER(38,4) as IBT_NU_TARGET,
	IBT_LE_TARGET::NUMBER(38,4) as IBT_LE_TARGET,
	IBT_BTG::NUMBER(38,4) as IBT_BTG,
	CCC_CY_ACTUAL::NUMBER(38,4) as CCC_CY_ACTUAL,
	CCC_PY_ACTUAL::NUMBER(38,4) as CCC_PY_ACTUAL,
	CCC_BP_TARGET::NUMBER(38,4) as CCC_BP_TARGET,
	CCC_JU_TARGET::NUMBER(38,4) as CCC_JU_TARGET,
	CCC_NU_TARGET::NUMBER(38,4) as CCC_NU_TARGET,
	CCC_LE_TARGET::NUMBER(38,4) as CCC_LE_TARGET,
	CM_CY_ACTUAL::NUMBER(38,4) as CNTRBTN_MRGN_CY_ACTUAL,
	CM_PY_ACTUAL::NUMBER(38,4) as CNTRBTN_MRGN_PY_ACTUAL,
	CM_BP_TARGET::NUMBER(38,4) as CNTRBTN_MRGN_BP_TARGET,
	CM_JU_TARGET::NUMBER(38,4) as CNTRBTN_MRGN_JU_TARGET,
	CM_NU_TARGET::NUMBER(38,4) as CNTRBTN_MRGN_NU_TARGET,
	CM_LE_TARGET::NUMBER(38,4) as CNTRBTN_MRGN_LE_TARGET,
	CUST_DQI_CY_ACTUAL::NUMBER(38,4) as CUSTOMER_DQI_CY_ACTUAL,
	CUST_DQI_PY_ACTUAL::NUMBER(38,4) as CUSTOMER_DQI_PY_ACTUAL,
	CUST_DQI_BP_TARGET::NUMBER(38,4) as CUSTOMER_DQI_BP_TARGET,
	CUST_DQI_JU_TARGET::NUMBER(38,4) as CUSTOMER_DQI_JU_TARGET,
	CUST_DQI_NU_TARGET::NUMBER(38,4) as CUSTOMER_DQI_NU_TARGET,
	CUST_DQI_LE_TARGET::NUMBER(38,4) as CUSTOMER_DQI_LE_TARGET,
	DIV_SCR_CY_ACTUAL::NUMBER(38,4) as DIV_EQT_INCL_SCORE_CY_ACTUAL,
	DIV_SCR_PY_ACTUAL::NUMBER(38,4) as DIV_EQT_INCL_SCORE_PY_ACTUAL,
	DIV_SCR_BP_TARGET::NUMBER(38,4) as DIV_EQT_INCL_SCORE_BP_TARGET,
	DIV_SCR_JU_TARGET::NUMBER(38,4) as DIV_EQT_INCL_SCORE_JU_TARGET,
	DIV_SCR_NU_TARGET::NUMBER(38,4) as DIV_EQT_INCL_SCORE_NU_TARGET,
	DIV_SCR_LE_TARGET::NUMBER(38,4) as DIV_EQT_INCL_SCORE_LE_TARGET,
	EMP_ENG_CY_ACTUAL::NUMBER(38,4) as EMP_ENGMNT_CY_ACTUAL,
	EMP_ENG_PY_ACTUAL::NUMBER(38,4) as EMP_ENGMNT_PY_ACTUAL,
	EMP_ENG_BP_TARGET::NUMBER(38,4) as EMP_ENGMNT_BP_TARGET,
	EMP_ENG_JU_TARGET::NUMBER(38,4) as EMP_ENGMNT_JU_TARGET,
	EMP_ENG_NU_TARGET::NUMBER(38,4) as EMP_ENGMNT_NU_TARGET,
	EMP_ENG_LE_TARGET::NUMBER(38,4) as EMP_ENGMNT_LE_TARGET,
	FCF_CY_ACTUAL::NUMBER(38,4) as FREE_CASH_FLOW_CY_ACTUAL,
	FCF_PY_ACTUAL::NUMBER(38,4) as FREE_CASH_FLOW_PY_ACTUAL,
	FCF_BP_TARGET::NUMBER(38,4) as FREE_CASH_FLOW_BP_TARGET,
	FCF_JU_TARGET::NUMBER(38,4) as FREE_CASH_FLOW_JU_TARGET,
	FCF_NU_TARGET::NUMBER(38,4) as FREE_CASH_FLOW_NU_TARGET,
	FCF_LE_TARGET::NUMBER(38,4) as FREE_CASH_FLOW_LE_TARGET,
	FCF_BTG::NUMBER(38,4) as FREE_CASH_FLOW_BTG,
	INCL_SCR_CY_ACTUAL::NUMBER(38,4) as INCLUSION_SCORE_CY_ACTUAL,
	INCL_SCR_PY_ACTUAL::NUMBER(38,4) as INCLUSION_SCORE_PY_ACTUAL,
	INCL_SCR_BP_TARGET::NUMBER(38,4) as INCLUSION_SCORE_BP_TARGET,
	INCL_SCR_JU_TARGET::NUMBER(38,4) as INCLUSION_SCORE_JU_TARGET,
	INCL_SCR_NU_TARGET::NUMBER(38,4) as INCLUSION_SCORE_NU_TARGET,
	INCL_SCR_LE_TARGET::NUMBER(38,4) as INCLUSION_SCORE_LE_TARGET,
	NTS_GRWNG_SHARE_CY_ACTUAL::NUMBER(38,4) as NTS_GRWNG_SHARE_CY_ACTUAL,
	NTS_GRWNG_SHARE_PY_ACTUAL::NUMBER(38,4) as NTS_GRWNG_SHARE_PY_ACTUAL,
	NTS_GRWNG_SHARE_PM_ACTUAL::NUMBER(38,4) as NTS_GRWNG_SHARE_PM_ACTUAL,
	NTS_GRWNG_SHARE_BP_TARGET::NUMBER(38,4) as NTS_GRWNG_SHARE_BP_TARGET,
	NTS_GRWNG_SHARE_JU_TARGET::NUMBER(38,4) as NTS_GRWNG_SHARE_JU_TARGET,
	NTS_GRWNG_SHARE_NU_TARGET::NUMBER(38,4) as NTS_GRWNG_SHARE_NU_TARGET,
	NTS_GRWNG_SHARE_LE_TARGET::NUMBER(38,4) as NTS_GRWNG_SHARE_LE_TARGET,
	NTS_GRWNG_SHARE_SIZE::NUMBER(38,0) as NTS_GRWNG_SHARE_SIZE,
	NPD_NTS_CY_ACTUAL::NUMBER(38,4) as NTS_FROM_NPD_CY_ACTUAL,
	NPD_NTS_PY_ACTUAL::NUMBER(38,4) as NTS_FROM_NPD_PY_ACTUAL,
	NPD_NTS_BP_TARGET::NUMBER(38,4) as NTS_FROM_NPD_BP_TARGET,
	NPD_NTS_JU_TARGET::NUMBER(38,4) as NTS_FROM_NPD_JU_TARGET,
	NPD_NTS_NU_TARGET::NUMBER(38,4) as NTS_FROM_NPD_NU_TARGET,
	NPD_NTS_LE_TARGET::NUMBER(38,4) as NTS_FROM_NPD_LE_TARGET,
	npd_btg::NUMBER(38,4) as NTS_FROM_NPD_BTG,
	OTIFD_CY_ACTUAL::NUMBER(38,4) as OTIFD_CY_ACTUAL,
	OTIFD_PY_ACTUAL::NUMBER(38,4) as OTIFD_PY_ACTUAL,
	OTIFD_BP_TARGET::NUMBER(38,4) as OTIFD_BP_TARGET,
	OTIFD_JU_TARGET::NUMBER(38,4) as OTIFD_JU_TARGET,
	OTIFD_NU_TARGET::NUMBER(38,4) as OTIFD_NU_TARGET,
	OTIFD_LE_TARGET::NUMBER(38,4) as OTIFD_LE_TARGET,
	OTIFD_YTD_CY_ACTUAL::NUMBER(38,4) as OTIFD_YTD_CY_ACTUAL,
	OTIFD_YTD_PY_ACTUAL::NUMBER(38,4) as OTIFD_YTD_PY_ACTUAL,
	OTIFD_YTD_BP_TARGET::NUMBER(38,4) as OTIFD_YTD_BP_TARGET,
	OTIFD_YTD_JU_TARGET::NUMBER(38,4) as OTIFD_YTD_JU_TARGET,
	OTIFD_YTD_NU_TARGET::NUMBER(38,4) as OTIFD_YTD_NU_TARGET,
	OTIFD_YTD_LE_TARGET::NUMBER(38,4) as OTIFD_YTD_LE_TARGET,
	ps_cy_actual::NUMBER(38,4) as PERFECT_STORE_CY_ACTUAL,
	PS_PY_ACTUAL::NUMBER(38,4) as PERFECT_STORE_PY_ACTUAL,
	PS_BP_TARGET::NUMBER(38,4) as PERFECT_STORE_BP_TARGET,
	PS_JU_TARGET::NUMBER(38,4) as PERFECT_STORE_JU_TARGET,
	PS_NU_TARGET::NUMBER(38,4) as PERFECT_STORE_NU_TARGET,
	PS_LE_TARGET::NUMBER(38,4) as PERFECT_STORE_LE_TARGET,
	PLE_CY_ACTUAL::NUMBER(38,4) as PLE_INDEX_CY_ACTUAL,
	PLE_PY_ACTUAL::NUMBER(38,4) as PLE_INDEX_PY_ACTUAL,
	PLE_BP_TARGET::NUMBER(38,4) as PLE_INDEX_BP_TARGET,
	PLE_JU_TARGET::NUMBER(38,4) as PLE_INDEX_JU_TARGET,
	PLE_NU_TARGET::NUMBER(38,4) as PLE_INDEX_NU_TARGET,
	PLE_LE_TARGET::NUMBER(38,4) as PLE_INDEX_LE_TARGET,
	RGM_CY_ACTUAL::NUMBER(38,4) as RGM_CY_ACTUAL,
	RGM_PY_ACTUAL::NUMBER(38,4) as RGM_PY_ACTUAL,
	RGM_BP_TARGET::NUMBER(38,4) as RGM_BP_TARGET,
	RGM_JU_TARGET::NUMBER(38,4) as RGM_JU_TARGET,
	RGM_NU_TARGET::NUMBER(38,4) as RGM_NU_TARGET,
	RGM_LE_TARGET::NUMBER(38,4) as RGM_LE_TARGET,
	RGM_BTG::NUMBER(38,4) as RGM_BTG,
	PPM_GRW_CY_ACTUAL::NUMBER(38,4) as SPIKE_PPM_GROWTH_CY_ACTUAL,
	PPM_GRW_PY_ACTUAL::NUMBER(38,4) as SPIKE_PPM_GROWTH_PY_ACTUAL,
	PPM_GRW_BP_TARGET::NUMBER(38,4) as SPIKE_PPM_GROWTH_BP_TARGET,
	PPM_GRW_JU_TARGET::NUMBER(38,4) as SPIKE_PPM_GROWTH_JU_TARGET,
	PPM_GRW_NU_TARGET::NUMBER(38,4) as SPIKE_PPM_GROWTH_NU_TARGET,
	PPM_GRW_LE_TARGET::NUMBER(38,4) as SPIKE_PPM_GROWTH_LE_TARGET,
	TR_CY_ACTUAL::NUMBER(38,4) as TALENT_RETENTION_CY_ACTUAL,
	TR_PY_ACTUAL::NUMBER(38,4) as TALENT_RETENTION_PY_ACTUAL,
	TR_BP_TARGET::NUMBER(38,4) as TALENT_RETENTION_BP_TARGET,
	TR_JU_TARGET::NUMBER(38,4) as TALENT_RETENTION_JU_TARGET,
	TR_NU_TARGET::NUMBER(38,4) as TALENT_RETENTION_NU_TARGET,
	TR_LE_TARGET::NUMBER(38,4) as TALENT_RETENTION_LE_TARGET,
	TTL_WEIGHTED_DSTRBTN_CY_ACTUAL::NUMBER(38,4) as TTL_WEIGHTED_DSTRBTN_CY_ACTUAL,
	TTL_WEIGHTED_DSTRBTN_PY_ACTUAL::NUMBER(38,4) as TTL_WEIGHTED_DSTRBTN_PY_ACTUAL,
	TTL_WEIGHTED_DSTRBTN_BP_TARGET::NUMBER(38,4) as TTL_WEIGHTED_DSTRBTN_BP_TARGET,
	TTL_WEIGHTED_DSTRBTN_JU_TARGET::NUMBER(38,4) as TTL_WEIGHTED_DSTRBTN_JU_TARGET,
	TTL_WEIGHTED_DSTRBTN_NU_TARGET::NUMBER(38,4) as TTL_WEIGHTED_DSTRBTN_NU_TARGET,
	TTL_WEIGHTED_DSTRBTN_LE_TARGET::NUMBER(38,4) as TTL_WEIGHTED_DSTRBTN_LE_TARGET,
	VALUE_SHARE_GROWTH_CY_ACTUAL::NUMBER(38,4) as VALUE_SHARE_GROWTH_CY_ACTUAL,
	VALUE_SHARE_GROWTH_PY_ACTUAL::NUMBER(38,4) as VALUE_SHARE_GROWTH_PY_ACTUAL,
	VALUE_SHARE_GROWTH_BP_TARGET::NUMBER(38,4) as VALUE_SHARE_GROWTH_BP_TARGET,
	VALUE_SHARE_GROWTH_JU_TARGET::NUMBER(38,4) as VALUE_SHARE_GROWTH_JU_TARGET,
	VALUE_SHARE_GROWTH_NU_TARGET::NUMBER(38,4) as VALUE_SHARE_GROWTH_NU_TARGET,
	VALUE_SHARE_GROWTH_LE_TARGET::NUMBER(38,4) as VALUE_SHARE_GROWTH_LE_TARGET,
	CIC_CY_ACTUAL::NUMBER(38,4) as CIC_CY_ACTUAL,
	CIC_PY_ACTUAL::NUMBER(38,4) as CIC_PY_ACTUAL,
	CIC_BP_TARGET::NUMBER(38,4) as CIC_BP_TARGET,
	CIC_JU_TARGET::NUMBER(38,4) as CIC_JU_TARGET,
	CIC_NU_TARGET::NUMBER(38,4) as CIC_NU_TARGET,
	CIC_LE_TARGET::NUMBER(38,4) as CIC_LE_TARGET,
	ECOMM_NTS_CY_ACTUAL::NUMBER(38,4) as ECOMM_NTS_CY_ACTUAL,
	ECOMM_NTS_PY_ACTUAL::NUMBER(38,4) as ECOMM_NTS_PY_ACTUAL,
	ECOMM_NTS_BP_TARGET::NUMBER(38,4) as ECOMM_NTS_BP_TARGET,
	ECOMM_NTS_JU_TARGET::NUMBER(38,4) as ECOMM_NTS_JU_TARGET,
	ECOMM_NTS_NU_TARGET::NUMBER(38,4) as ECOMM_NTS_NU_TARGET,
	ECOMM_NTS_LE_TARGET::NUMBER(38,4) as ECOMM_NTS_LE_TARGET,
	ECOMM_NTS_BTG::NUMBER(38,4) as ECOMM_NTS_BTG,
	ECOMM_RANK_CY_ACTUAL::NUMBER(38,4) as ECOMM_RANK_CY_ACTUAL,
	ECOMM_RANK_PY_ACTUAL::NUMBER(38,4) as ECOMM_RANK_PY_ACTUAL,
	ECOMM_RANK_BP_TARGET::NUMBER(38,4) as ECOMM_RANK_BP_TARGET,
	ECOMM_RANK_JU_TARGET::NUMBER(38,4) as ECOMM_RANK_JU_TARGET,
	ECOMM_RANK_NU_TARGET::NUMBER(38,4) as ECOMM_RANK_NU_TARGET,
	ECOMM_RANK_LE_TARGET::NUMBER(38,4) as ECOMM_RANK_LE_TARGET,
	BRAND_POWER_CY_ACTUAL::NUMBER(38,4) as BRAND_POWER_CY_ACTUAL,
	BRAND_POWER_PY_ACTUAL::NUMBER(38,4) as BRAND_POWER_PY_ACTUAL,
	BRAND_POWER_BP_TARGET::NUMBER(38,4) as BRAND_POWER_BP_TARGET,
	BRAND_POWER_JU_TARGET::NUMBER(38,4) as BRAND_POWER_JU_TARGET,
	BRAND_POWER_NU_TARGET::NUMBER(38,4) as BRAND_POWER_NU_TARGET,
	BRAND_POWER_LE_TARGET::NUMBER(38,4) as BRAND_POWER_LE_TARGET,
	SOS_CY_ACTUAL::NUMBER(38,4) as SOS_6PAI_CY_ACTUAL,
	SOS_PY_ACTUAL::NUMBER(38,4) as SOS_6PAI_PY_ACTUAL,
	SOS_BP_TARGET::NUMBER(38,4) as SOS_6PAI_BP_TARGET,
	SOS_JU_TARGET::NUMBER(38,4) as SOS_6PAI_JU_TARGET,
	SOS_NU_TARGET::NUMBER(38,4) as SOS_6PAI_NU_TARGET,
	SOS_LE_TARGET::NUMBER(38,4) as SOS_6PAI_LE_TARGET,
	BMC_GRW_SHR_CY_ACTUAL::NUMBER(38,4) as BMC_GRWNG_SHARE_CY_ACTUAL,
	BMC_GRW_SHR_PY_ACTUAL::NUMBER(38,4) as BMC_GRWNG_SHARE_PY_ACTUAL,
	BMC_GRW_SHR_BP_TARGET::NUMBER(38,4) as BMC_GRWNG_SHARE_BP_TARGET,
	BMC_GRW_SHR_JU_TARGET::NUMBER(38,4) as BMC_GRWNG_SHARE_JU_TARGET,
	BMC_GRW_SHR_NU_TARGET::NUMBER(38,4) as BMC_GRWNG_SHARE_NU_TARGET,
	BMC_GRW_SHR_LE_TARGET::NUMBER(38,4) as BMC_GRWNG_SHARE_LE_TARGET,
	AP_IMPRVMNT_CY_ACTUAL::NUMBER(38,4) as AP_IMPRVMNT_CY_ACTUAL,
	AP_IMPRVMNT_PY_ACTUAL::NUMBER(38,4) as AP_IMPRVMNT_PY_ACTUAL,
	AP_IMPRVMNT_BP_TARGET::NUMBER(38,4) as AP_IMPRVMNT_BP_TARGET,
	AP_IMPRVMNT_JU_TARGET::NUMBER(38,4) as AP_IMPRVMNT_JU_TARGET,
	AP_IMPRVMNT_NU_TARGET::NUMBER(38,4) as AP_IMPRVMNT_NU_TARGET,
	AP_IMPRVMNT_LE_TARGET::NUMBER(38,4) as AP_IMPRVMNT_LE_TARGET,
	AR_IMPRVMNT_CY_ACTUAL::NUMBER(38,4) as AR_IMPRVMNT_CY_ACTUAL,
	AR_IMPRVMNT_PY_ACTUAL::NUMBER(38,4) as AR_IMPRVMNT_PY_ACTUAL,
	AR_IMPRVMNT_BP_TARGET::NUMBER(38,4) as AR_IMPRVMNT_BP_TARGET,
	AR_IMPRVMNT_JU_TARGET::NUMBER(38,4) as AR_IMPRVMNT_JU_TARGET,
	AR_IMPRVMNT_NU_TARGET::NUMBER(38,4) as AR_IMPRVMNT_NU_TARGET,
	AR_IMPRVMNT_LE_TARGET::NUMBER(38,4) as AR_IMPRVMNT_LE_TARGET,
	DND_IQ_IMPRVMNT_CY_ACTUAL::NUMBER(38,4) as DND_IQ_IMPRVMNT_CY_ACTUAL,
	DND_IQ_IMPRVMNT_PY_ACTUAL::NUMBER(38,4) as DND_IQ_IMPRVMNT_PY_ACTUAL,
	DND_IQ_IMPRVMNT_BP_TARGET::NUMBER(38,4) as DND_IQ_IMPRVMNT_BP_TARGET,
	DND_IQ_IMPRVMNT_JU_TARGET::NUMBER(38,4) as DND_IQ_IMPRVMNT_JU_TARGET,
	DND_IQ_IMPRVMNT_NU_TARGET::NUMBER(38,4) as DND_IQ_IMPRVMNT_NU_TARGET,
	DND_IQ_IMPRVMNT_LE_TARGET::NUMBER(38,4) as DND_IQ_IMPRVMNT_LE_TARGET,
	INVENTORY_REDUCTION_CY_ACTUAL::NUMBER(38,4) as INVENTORY_REDUCTION_CY_ACTUAL,
	INVENTORY_REDUCTION_PY_ACTUAL::NUMBER(38,4) as INVENTORY_REDUCTION_PY_ACTUAL,
	INVENTORY_REDUCTION_BP_TARGET::NUMBER(38,4) as INVENTORY_REDUCTION_BP_TARGET,
	INVENTORY_REDUCTION_JU_TARGET::NUMBER(38,4) as INVENTORY_REDUCTION_JU_TARGET,
	INVENTORY_REDUCTION_NU_TARGET::NUMBER(38,4) as INVENTORY_REDUCTION_NU_TARGET,
	INVENTORY_REDUCTION_LE_TARGET::NUMBER(38,4) as INVENTORY_REDUCTION_LE_TARGET,
	SUSTAINABILITY_SCORE_CY_ACTUAL::NUMBER(38,4) as SUSTAINABILITY_SCORE_CY_ACTUAL,
	SUSTAINABILITY_SCORE_PY_ACTUAL::NUMBER(38,4) as SUSTAINABILITY_SCORE_PY_ACTUAL,
	SUSTAINABILITY_SCORE_BP_TARGET::NUMBER(38,4) as SUSTAINABILITY_SCORE_BP_TARGET,
	SUSTAINABILITY_SCORE_JU_TARGET::NUMBER(38,4) as SUSTAINABILITY_SCORE_JU_TARGET,
	SUSTAINABILITY_SCORE_NU_TARGET::NUMBER(38,4) as SUSTAINABILITY_SCORE_NU_TARGET,
	SUSTAINABILITY_SCORE_LE_TARGET::NUMBER(38,4) as SUSTAINABILITY_SCORE_LE_TARGET,
	SLOB_CY_ACTUAL::NUMBER(38,4) as SLOB_CY_ACTUAL,
	SLOB_PY_ACTUAL::NUMBER(38,4) as SLOB_PY_ACTUAL,
	SLOB_BP_TARGET::NUMBER(38,4) as SLOB_BP_TARGET,
	SLOB_JU_TARGET::NUMBER(38,4) as SLOB_JU_TARGET,
	SLOB_NU_TARGET::NUMBER(38,4) as SLOB_NU_TARGET,
	SLOB_LE_TARGET::NUMBER(38,4) as SLOB_LE_TARGET,
	SLOB_YTD_CY_ACTUAL::NUMBER(38,4) as SLOB_YTD_CY_ACTUAL,
	SLOB_YTD_PY_ACTUAL::NUMBER(38,4) as SLOB_YTD_PY_ACTUAL,
	SLOB_YTD_BP_TARGET::NUMBER(38,4) as SLOB_YTD_BP_TARGET,
	SLOB_YTD_JU_TARGET::NUMBER(38,4) as SLOB_YTD_JU_TARGET,
	SLOB_YTD_NU_TARGET::NUMBER(38,4) as SLOB_YTD_NU_TARGET,
	SLOB_YTD_LE_TARGET::NUMBER(38,4) as SLOB_YTD_LE_TARGET,
	SYS_RAT_AUTO_CY_ACTUAL::NUMBER(38,4) as SYSTEM_RAT_AUTO_CY_ACTUAL,
	SYS_RAT_AUTO_PY_ACTUAL::NUMBER(38,4) as SYSTEM_RAT_AUTO_PY_ACTUAL,
	SYS_RAT_AUTO_BP_TARGET::NUMBER(38,4) as SYSTEM_RAT_AUTO_BP_TARGET,
	SYS_RAT_AUTO_JU_TARGET::NUMBER(38,4) as SYSTEM_RAT_AUTO_JU_TARGET,
	SYS_RAT_AUTO_NU_TARGET::NUMBER(38,4) as SYSTEM_RAT_AUTO_NU_TARGET,
	SYS_RAT_AUTO_LE_TARGET::NUMBER(38,4) as SYSTEM_RAT_AUTO_LE_TARGET,
	spike_brnd_grwth_cy_actual::NUMBER(38,4) as SPIKE_BRAND_GROWTH_CY_ACTUAL,
	spike_brnd_grwth_py_actual::NUMBER(38,4) as SPIKE_BRAND_GROWTH_PY_ACTUAL,
	spike_brnd_grwth_BP_TARGET::NUMBER(38,4) as SPIKE_BRAND_GROWTH_BP_TARGET,
	spike_brnd_grwth_JU_TARGET::NUMBER(38,4) as SPIKE_BRAND_GROWTH_JU_TARGET,
	spike_brnd_grwth_NU_TARGET::NUMBER(38,4) as SPIKE_BRAND_GROWTH_NU_TARGET,
	spike_brnd_grwth_LE_TARGET::NUMBER(38,4) as SPIKE_BRAND_GROWTH_LE_TARGET,
	spike_brnd_grwth_ytd_CY_ACTUAL::NUMBER(38,4) as SPIKE_BRAND_GROWTH_YTD_CY_ACTUAL,
	spike_brnd_grwth_ytd_PY_ACTUAL::NUMBER(38,4) as SPIKE_BRAND_GROWTH_YTD_PY_ACTUAL,
	spike_brnd_grwth_ytd_BP_TARGET::NUMBER(38,4) as SPIKE_BRAND_GROWTH_YTD_BP_TARGET,
	spike_brnd_grwth_ytd_JU_TARGET::NUMBER(38,4) as SPIKE_BRAND_GROWTH_YTD_JU_TARGET,
	spike_brnd_grwth_ytd_NU_TARGET::NUMBER(38,4) as SPIKE_BRAND_GROWTH_YTD_NU_TARGET,
	spike_brnd_grwth_ytd_le_target::NUMBER(38,4) as SPIKE_BRAND_GROWTH_YTD_LE_TARGET,
	MAPE_CY_ACTUAL::NUMBER(38,4) as MAPE_CY_ACTUAL,
	MAPE_PY_ACTUAL::NUMBER(38,4) as MAPE_PY_ACTUAL,
	MAPE_BP_TARGET::NUMBER(38,4) as MAPE_BP_TARGET,
	MAPE_JU_TARGET::NUMBER(38,4) as MAPE_JU_TARGET,
	MAPE_NU_TARGET::NUMBER(38,4) as MAPE_NU_TARGET,
	MAPE_LE_TARGET::NUMBER(38,4) as MAPE_LE_TARGET,
	MAPE_YTD_CY_ACTUAL::NUMBER(38,4) as MAPE_YTD_CY_ACTUAL,
	MAPE_YTD_PY_ACTUAL::NUMBER(38,4) as MAPE_YTD_PY_ACTUAL,
	MAPE_YTD_BP_TARGET::NUMBER(38,4) as MAPE_YTD_BP_TARGET,
	MAPE_YTD_JU_TARGET::NUMBER(38,4) as MAPE_YTD_JU_TARGET,
	MAPE_YTD_NU_TARGET::NUMBER(38,4) as MAPE_YTD_NU_TARGET,
	MAPE_YTD_LE_TARGET::NUMBER(38,4) as MAPE_YTD_LE_TARGET,
	BIAS_CY_ACTUAL::NUMBER(38,4) as BIAS_CY_ACTUAL,
	BIAS_PY_ACTUAL::NUMBER(38,4) as BIAS_PY_ACTUAL,
	BIAS_BP_TARGET::NUMBER(38,4) as BIAS_BP_TARGET,
	BIAS_JU_TARGET::NUMBER(38,4) as BIAS_JU_TARGET,
	BIAS_NU_TARGET::NUMBER(38,4) as BIAS_NU_TARGET,
	BIAS_LE_TARGET::NUMBER(38,4) as BIAS_LE_TARGET,
	BIAS_YTD_CY_ACTUAL::NUMBER(38,4) as BIAS_YTD_CY_ACTUAL,
	BIAS_YTD_PY_ACTUAL::NUMBER(38,4) as BIAS_YTD_PY_ACTUAL,
	BIAS_YTD_BP_TARGET::NUMBER(38,4) as BIAS_YTD_BP_TARGET,
	BIAS_YTD_JU_TARGET::NUMBER(38,4) as BIAS_YTD_JU_TARGET,
	BIAS_YTD_NU_TARGET::NUMBER(38,4) as BIAS_YTD_NU_TARGET,
	BIAS_YTD_LE_TARGET::NUMBER(38,4) as BIAS_YTD_LE_TARGET,
	SUSTAINABILITY_SCORE_AVN_CY_ACTUAL::NUMBER(38,4) as SUSTAINABILITY_SCORE_AVN_CY_ACTUAL,
	SUSTAINABILITY_SCORE_AVN_PY_ACTUAL::NUMBER(38,4) as SUSTAINABILITY_SCORE_AVN_PY_ACTUAL,
	SUSTAINABILITY_SCORE_AVN_BP_TARGET::NUMBER(38,4) as SUSTAINABILITY_SCORE_AVN_BP_TARGET,
	SUSTAINABILITY_SCORE_AVN_JU_TARGET::NUMBER(38,4) as SUSTAINABILITY_SCORE_AVN_JU_TARGET,
	SUSTAINABILITY_SCORE_AVN_NU_TARGET::NUMBER(38,4) as SUSTAINABILITY_SCORE_AVN_NU_TARGET,
	SUSTAINABILITY_SCORE_AVN_LE_TARGET::NUMBER(38,4) as SUSTAINABILITY_SCORE_AVN_LE_TARGET,
	COGS_CY_ACTUAL::NUMBER(38,4) as COGS_CY_ACTUAL,
	COGS_PY_ACTUAL::NUMBER(38,4) as COGS_PY_ACTUAL,
	COGS_BP_TARGET::NUMBER(38,4) as COGS_BP_TARGET,
	COGS_JU_TARGET::NUMBER(38,4) as COGS_JU_TARGET,
	COGS_NU_TARGET::NUMBER(38,4) as COGS_NU_TARGET,
	DIO_CY_ACTUAL::NUMBER(38,4) as DIO_CY_ACTUAL,
	DIO_PY_ACTUAL::NUMBER(38,4) as DIO_PY_ACTUAL,
	DIO_BP_TARGET::NUMBER(38,4) as DIO_BP_TARGET,
	DIO_JU_TARGET::NUMBER(38,4) as DIO_JU_TARGET,
	DIO_NU_TARGET::NUMBER(38,4) as DIO_NU_TARGET,
	MO_VAL_CRTN_CY_ACTUAL::NUMBER(38,4) as MO_VALUE_CRTN_CY_ACTUAL,
	MO_VAL_CRTN_PY_ACTUAL::NUMBER(38,4) as MO_VALUE_CRTN_PY_ACTUAL,
	MO_VAL_CRTN_BP_TARGET::NUMBER(38,4) as MO_VALUE_CRTN_BP_TARGET,
	MO_VAL_CRTN_JU_TARGET::NUMBER(38,4) as MO_VALUE_CRTN_JU_TARGET,
	MO_VAL_CRTN_NU_TARGET::NUMBER(38,4) as MO_VALUE_CRTN_NU_TARGET,
	TIMELY_DCSN_CY_ACTUAL::NUMBER(38,4) as TIMELY_DCSN_CY_ACTUAL,
	TIMELY_DCSN_PY_ACTUAL::NUMBER(38,4) as TIMELY_DCSN_PY_ACTUAL,
	TIMELY_DCSN_BP_TARGET::NUMBER(38,4) as TIMELY_DCSN_BP_TARGET,
	TIMELY_DCSN_JU_TARGET::NUMBER(38,4) as TIMELY_DCSN_JU_TARGET,
	TIMELY_DCSN_NU_TARGET::NUMBER(38,4) as TIMELY_DCSN_NU_TARGET,
	bmc_cntrbtn_nts_cy_actual::NUMBER(38,4) as "bmc_cntrbtn_%_cy_actual",
	bmc_cntrbtn_nts_py_actual::NUMBER(38,4) as "bmc_cntrbtn_%_py_actual",
	bmc_cntrbtn_nts_bp_target::NUMBER(38,4) as "bmc_cntrbtn_%_bp_target",
	bmc_cntrbtn_nts_ju_target::NUMBER(38,4) as "bmc_cntrbtn_%_ju_target",
	bmc_cntrbtn_nts_nu_target::NUMBER(38,4) as "bmc_cntrbtn_%_nu_target",
	PHRMCY_SHARE_CY_ACTUAL::NUMBER(38,4) as PHRMCY_SHARE_CY_ACTUAL,
	PHRMCY_SHARE_PY_ACTUAL::NUMBER(38,4) as PHRMCY_SHARE_PY_ACTUAL,
	PHRMCY_SHARE_BP_TARGET::NUMBER(38,4) as PHRMCY_SHARE_BP_TARGET,
	PHRMCY_SHARE_JU_TARGET::NUMBER(38,4) as PHRMCY_SHARE_JU_TARGET,
	PHRMCY_SHARE_NU_TARGET::NUMBER(38,4) as PHRMCY_SHARE_NU_TARGET,
	ECOM_SHR_CN_CY_ACTUAL::NUMBER(38,4) as ECOM_SHARE_CN_CY_ACTUAL,
	ECOM_SHR_CN_PY_ACTUAL::NUMBER(38,4) as ECOM_SHARE_CN_PY_ACTUAL,
	ECOM_SHR_CN_BP_TARGET::NUMBER(38,4) as ECOM_SHARE_CN_BP_TARGET,
	ECOM_SHR_CN_JU_TARGET::NUMBER(38,4) as ECOM_SHARE_CN_JU_TARGET,
	ECOM_SHR_CN_NU_TARGET::NUMBER(38,4) as ECOM_SHARE_CN_NU_TARGET,
	ECOM_SHR_REST_ASIA_CY_ACTUAL::NUMBER(38,4) as ECOM_SHARE_REST_ASIA_CY_ACTUAL,
	ECOM_SHR_REST_ASIA_PY_ACTUAL::NUMBER(38,4) as ECOM_SHARE_REST_ASIA_PY_ACTUAL,
	ECOM_SHR_REST_ASIA_BP_TARGET::NUMBER(38,4) as ECOM_SHARE_REST_ASIA_BP_TARGET,
	ECOM_SHR_REST_ASIA_JU_TARGET::NUMBER(38,4) as ECOM_SHARE_REST_ASIA_JU_TARGET,
	ECOM_SHR_REST_ASIA_NU_TARGET::NUMBER(38,4) as ECOM_SHARE_REST_ASIA_NU_TARGET,
	ECOM_SHARE_CY_ACTUAL::NUMBER(38,4) as ECOM_SHARE_CY_ACTUAL,
	ECOM_SHARE_PY_ACTUAL::NUMBER(38,4) as ECOM_SHARE_PY_ACTUAL,
	ECOM_SHARE_BP_TARGET::NUMBER(38,4) as ECOM_SHARE_BP_TARGET,
	ECOM_SHARE_JU_TARGET::NUMBER(38,4) as ECOM_SHARE_JU_TARGET,
	ECOM_SHARE_NU_TARGET::NUMBER(38,4) as ECOM_SHARE_NU_TARGET,
	TDP_SHARE_CY_ACTUAL::NUMBER(38,4) as INCR_SHARE_OF_TDP_CY_ACTUAL,
	TDP_SHARE_PY_ACTUAL::NUMBER(38,4) as INCR_SHARE_OF_TDP_PY_ACTUAL,
	TDP_SHARE_BP_TARGET::NUMBER(38,4) as INCR_SHARE_OF_TDP_BP_TARGET,
	TDP_SHARE_JU_TARGET::NUMBER(38,4) as INCR_SHARE_OF_TDP_JU_TARGET,
	TDP_SHARE_NU_TARGET::NUMBER(38,4) as INCR_SHARE_OF_TDP_NU_TARGET,
	DQI_CY_ACTUAL::NUMBER(38,4) as DQI_CY_ACTUAL,
	DQI_PY_ACTUAL::NUMBER(38,4) as DQI_PY_ACTUAL,
	DQI_BP_TARGET::NUMBER(38,4) as DQI_BP_TARGET,
	DQI_JU_TARGET::NUMBER(38,4) as DQI_JU_TARGET,
	DQI_NU_TARGET::NUMBER(38,4) as DQI_NU_TARGET,
	MDP_SHARE_CY_ACTUAL::NUMBER(38,4) as MDP_SHARE_GROWTH_CY_ACTUAL,
	MDP_SHARE_PY_ACTUAL::NUMBER(38,4) as MDP_SHARE_GROWTH_PY_ACTUAL,
	MDP_SHARE_BP_TARGET::NUMBER(38,4) as MDP_SHARE_GROWTH_BP_TARGET,
	MDP_SHARE_JU_TARGET::NUMBER(38,4) as MDP_SHARE_GROWTH_JU_TARGET,
	MDP_SHARE_NU_TARGET::NUMBER(38,4) as MDP_SHARE_GROWTH_NU_TARGET,
	PHRMCY_HB_CHNL_GRWTH_CY_ACTUAL::NUMBER(38,4) as PHRMCY_HB_CHNL_GRWTH_CY_ACTUAL,
	PHRMCY_HB_CHNL_GRWTH_PY_ACTUAL::NUMBER(38,4) as PHRMCY_HB_CHNL_GRWTH_PY_ACTUAL,
	PHRMCY_HB_CHNL_GRWTH_BP_TARGET::NUMBER(38,4) as PHRMCY_HB_CHNL_GRWTH_BP_TARGET,
	PHRMCY_HB_CHNL_GRWTH_JU_TARGET::NUMBER(38,4) as PHRMCY_HB_CHNL_GRWTH_JU_TARGET,
	PHRMCY_HB_CHNL_GRWTH_NU_TARGET::NUMBER(38,4) as PHRMCY_HB_CHNL_GRWTH_NU_TARGET,
	SOS_GROWTH_CY_ACTUAL::NUMBER(38,4) as SOS_GROWTH_CY_ACTUAL,
	SOS_GROWTH_PY_ACTUAL::NUMBER(38,4) as SOS_GROWTH_PY_ACTUAL,
	SOS_GROWTH_BP_TARGET::NUMBER(38,4) as SOS_GROWTH_BP_TARGET,
	SOS_GROWTH_JU_TARGET::NUMBER(38,4) as SOS_GROWTH_JU_TARGET,
	SOS_GROWTH_NU_TARGET::NUMBER(38,4) as SOS_GROWTH_NU_TARGET,
	E2E_INVST_CY_ACTUAL::NUMBER(38,4) as E2E_DMND_GNRT_INVST_CY_ACTUAL,
	e2e_invst_py_actual::NUMBER(38,4) as E2E_DMND_GNRT_INVST_PY_ACTUAL,
	e2e_invst_bp_target::NUMBER(38,4) as E2E_DMND_GNRT_INVST_BP_TARGET,
	e2e_invst_ju_target::NUMBER(38,4) as E2E_DMND_GNRT_INVST_JU_TARGET,
	e2e_invst_nu_target::NUMBER(38,4) as E2E_DMND_GNRT_INVST_NU_TARGET,
	DOS_CY_ACTUAL::NUMBER(38,4) as DOS_CY_ACTUAL,
	DOS_PY_ACTUAL::NUMBER(38,4) as DOS_PY_ACTUAL,
	DOS_BP_TARGET::NUMBER(38,4) as DOS_BP_TARGET,
	DOS_JU_TARGET::NUMBER(38,4) as DOS_JU_TARGET,
	DOS_NU_TARGET::NUMBER(38,4) as DOS_NU_TARGET,
	DSO_CY_ACTUAL::NUMBER(38,4) as DSO_CY_ACTUAL,
	DSO_PY_ACTUAL::NUMBER(38,4) as DSO_PY_ACTUAL,
	DSO_BP_TARGET::NUMBER(38,4) as DSO_BP_TARGET,
	DSO_JU_TARGET::NUMBER(38,4) as DSO_JU_TARGET,
	DSO_NU_TARGET::NUMBER(38,4) as DSO_NU_TARGET,
	sc_brand_nts_cy_actual::NUMBER(38,4) as "s&c_brnd_%_nts_cy_actual",
	sc_brand_nts_py_actual::NUMBER(38,4) as "s&c_brnd_%_nts_py_actual",
	sc_brand_nts_bp_target::NUMBER(38,4) as "s&c_brnd_%_nts_bp_target",
	sc_brand_nts_ju_target::NUMBER(38,4) as "s&c_brnd_%_nts_ju_target",
	sc_brand_nts_nu_target::NUMBER(38,4) as "s&c_brnd_%_nts_nu_target",
	DAYS_OF_SALES_CY_ACTUAL::NUMBER(38,4) as DAYS_OF_SALES_CY_ACTUAL,
	DAYS_OF_SALES_PY_ACTUAL::NUMBER(38,4) as DAYS_OF_SALES_PY_ACTUAL,
	DAYS_OF_SALES_BP_TARGET::NUMBER(38,4) as DAYS_OF_SALES_BP_TARGET,
	DAYS_OF_SALES_JU_TARGET::NUMBER(38,4) as DAYS_OF_SALES_JU_TARGET,
	DAYS_OF_SALES_NU_TARGET::NUMBER(38,4) as DAYS_OF_SALES_NU_TARGET,
	per_nts_from_ecom_cy_actual::NUMBER(38,4) as "%_nts_from_ecomm_cy_actual",
	per_nts_from_ecom_py_actual::NUMBER(38,4) as "%_nts_from_ecomm_py_actual",
	per_nts_from_ecom_bp_target::NUMBER(38,4) as "%_nts_from_ecomm_bp_target",
	per_nts_from_ecom_ju_target::NUMBER(38,4) as "%_nts_from_ecomm_ju_target",
	per_nts_from_ecom_nu_target::NUMBER(38,4) as "%_nts_from_ecomm_nu_target",
	ECOMM_GROWTH_CY_ACTUAL::NUMBER(38,4) as ECOMM_GROWTH_CY_ACTUAL,
	ECOMM_GROWTH_PY_ACTUAL::NUMBER(38,4) as ECOMM_GROWTH_PY_ACTUAL,
	ECOMM_GROWTH_BP_TARGET::NUMBER(38,4) as ECOMM_GROWTH_BP_TARGET,
	ECOMM_GROWTH_JU_TARGET::NUMBER(38,4) as ECOMM_GROWTH_JU_TARGET,
	ECOMM_GROWTH_NU_TARGET::NUMBER(38,4) as ECOMM_GROWTH_NU_TARGET,
	td_initiative_cy_actual::NUMBER(38,4) as "t&d_initiative_cy_actual",
	td_initiative_py_actual::NUMBER(38,4) as "t&d_initiative_py_actual",
	td_initiative_bp_target::NUMBER(38,4) as "t&d_initiative_bp_target",
	td_initiative_ju_target::NUMBER(38,4) as "t&d_initiative_ju_target",
	td_initiative_nu_target::NUMBER(38,4) as "t&d_initiative_nu_target",
	TSA_ON_TIME_CY_ACTUAL::NUMBER(38,4) as TSA_ON_TIME_CY_ACTUAL,
	TSA_ON_TIME_PY_ACTUAL::NUMBER(38,4) as TSA_ON_TIME_PY_ACTUAL,
	TSA_ON_TIME_BP_TARGET::NUMBER(38,4) as TSA_ON_TIME_BP_TARGET,
	TSA_ON_TIME_JU_TARGET::NUMBER(38,4) as TSA_ON_TIME_JU_TARGET,
	TSA_ON_TIME_NU_TARGET::NUMBER(38,4) as TSA_ON_TIME_NU_TARGET,
	GROSS_INVENTORY_CY_ACTUAL::NUMBER(38,4) as GROSS_INVT_CY_ACTUAL,
	GROSS_INVENTORY_PY_ACTUAL::NUMBER(38,4) as GROSS_INVT_PY_ACTUAL,
	GROSS_INVENTORY_BP_TARGET::NUMBER(38,4) as GROSS_INVT_BP_TARGET,
	GROSS_INVENTORY_JU_TARGET::NUMBER(38,4) as GROSS_INVT_JU_TARGET,
	GROSS_INVENTORY_NU_TARGET::NUMBER(38,4) as GROSS_INVT_NU_TARGET
    from trans
)
select * from final