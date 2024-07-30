with itg_okr_alteryx_automation as
(
    select * from dev_dna_core.aspitg_integration.itg_okr_alteryx_automation
),
trans as
(
    SELECT datatype,
      kpi,
      yearmonth,
      year,
      quarter,
      brand,
      segment,
      CLUSTER,
      market,
      NULL AS nts_grwng_share_size,
      CASE 
        WHEN datatype = 'Actual'
          THEN actuals
        ELSE NULL
        END AS actual_value,
      NULL as bp_target,
	  NULL as ju_target,
	  NULL as nu_target,
	  NULL as ytd_actual,
	  NULL as ytd_bp_target,
	  NULL as ytd_ju_target,
	  NULL as ytd_nu_target
    FROM itg_okr_alteryx_automation
    WHERE datatype = 'Actual'
),
final as
(
        select
        	datatype::varchar(20) as data_type,
	        kpi::varchar(100) as kpi,
	        yearmonth::varchar(10) as year_month,
	        year::varchar(10) as fisc_year,
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