with itg_mds_ap_okr_actuals as
(
    select * from DEV_DNA_CORE.ASPITG_INTEGRATION.ITG_MDS_AP_OKR_ACTUALS
),
Trans as
(
    SELECT DISTINCT 'Actual' AS data_type,
      kpi,
      year_month,
      year as fisc_year,
      quarter,
      CASE 
        WHEN upper(brand) = 'O.B'
          THEN 'o.b.'
        WHEN UPPER(brand) = 'CLEAN&CLEAR'
          THEN 'Clean & Clear'
        WHEN UPPER(brand) = 'DR.CI.LABO'
          THEN 'Dr. Ci: Labo'
        WHEN brand = 'Aveeno Other'
          THEN 'Aveeno'
        ELSE brand
        END AS brand,
      CASE 
        WHEN upper(franchise) = 'SKINHEALTH'
          THEN 'Skin Health'
        WHEN upper(franchise) = 'SELF CARE'
          THEN 'Self Care'
        WHEN brand = 'Aveeno Other'
          THEN 'Skin Health'
        WHEN brand = 'Listerine'
          THEN 'Essential Health'
        ELSE franchise
        END AS segment,
      cluster,
      CASE 
        WHEN market = 'JP DCL'
          THEN 'Japan DCL'
        ELSE market
        END AS market,
      nts_grwng_share_size,
      CASE 
        WHEN actual_value <> 0
          OR upper(kpi) IN ('BMC GROWING SHARE', 'NTS% GROWING SHARE', 'VALUE SHARE GROWTH')
          THEN actual_value
        ELSE NULL
        END AS actual_value,
      NULL as BP_TARGET,
      NULL as JU_TARGET,
	  NULL as NU_TARGET,
      ytd as ytd_actual,
      NULL as ytd_bp_target,
	  NULL as ytd_ju_target,
	  NULL as ytd_nu_target
    FROM itg_mds_ap_okr_actuals
    WHERE upper(kpi) NOT IN ('ECOMMERCE_NTS', 'OFFLINE_PERFECT_STORE', 'GP', 'IBT_NETINCOME', 'NTS', 'CONTRIBUTION_MARGIN', 'NTS_FROM_NPD', '6PAI')
),
final as
    (
        select
        	data_type::varchar(20) as data_type,
	        kpi::varchar(100) as kpi,
	        year_month::varchar(10) as year_month,
	        fisc_year::varchar(10) as fisc_year,
	        quarter::number(38,0) as quarter,
	        brand::varchar(200) as brand,
	        segment::varchar(200) as franchise,
	        cluster::varchar(100) as cluster,
	        market::varchar(100) as market,
	        nts_grwng_share_size::number(38,0) as nts_grwng_share_size,
	        actual_value::number(38,4) as actual_value,
	        bp_target::number(38,4) as bp_target,
	        ju_target::number(38,4) as ju_target,
	        nu_target::number(38,4) as nu_target,
	        ytd_actual::number(38,4) as ytd_actual,
	        ytd_bp_target::number(38,4) as ytd_bp_target,
	        ytd_ju_target::number(38,4) as ytd_ju_target,
	        ytd_nu_target::number(38,4) as ytd_nu_target
        from Trans
    )
select * from final