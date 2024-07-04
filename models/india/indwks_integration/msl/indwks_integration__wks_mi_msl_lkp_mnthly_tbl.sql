with edw_retailer_calendar_dim as(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
edw_msl_spike_mi_msku_list as(
    select * from {{ ref('indedw_integration__edw_msl_spike_mi_msku_list') }}
),
union1 as(		
        SELECT lkp.region_name,
		lkp.zone_name,
		lkp.mothersku_name,
		lkp.period,
		cal.qtr,
		cal.mth_mm FROM edw_msl_spike_mi_msku_list lkp INNER JOIN (
		SELECT qtr,
			mth_mm
		FROM edw_retailer_calendar_dim
		WHERE mth_mm BETWEEN '202304'
				AND TO_CHAR(current_timestamp()::timestamp_ntz(9), 'YYYYMM')
		GROUP BY 1,
			2
		) cal ON LEFT(lkp.period, 4) = LEFT(cal.mth_mm, 4)
		AND RIGHT(lkp.period, 1) = cal.qtr
),
union2 as(		
		SELECT lkp.region_name,
		lkp.zone_name,
		lkp.mothersku_name,
		LEFT(cal.mth_mm, 4) || 'Q' || cal.qtr AS period,
		cal.qtr,
		cal.mth_mm FROM edw_msl_spike_mi_msku_list lkp CROSS JOIN (
		SELECT qtr,
			mth_mm
		FROM edw_retailer_calendar_dim
		WHERE mth_mm BETWEEN '202201'
				AND '202303'
		GROUP BY 1,
			2
		) cal WHERE lkp.period = '2023Q2'
),
transformed as(
    select * from union1
    union all
    select * from union2
)
select * from transformed