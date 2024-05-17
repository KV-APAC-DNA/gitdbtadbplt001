with 
v_intrm_calendar as
(
    select * from {{ ref('ntaedw_integration__v_intrm_calendar') }}
),

final as
(
    SELECT 
     a.cal_day
	,a.fisc_yr_vrnt
	,a.wkday
	,a.cal_wk
	,a.cal_mo_1
	,a.cal_mo_2
	,a.cal_qtr_1
	,a.cal_qtr_2
	,a.half_yr
	,a.cal_yr
	,a.fisc_per
	,a.pstng_per
	,a.fisc_yr
	,a.rec_mode
	,a.promo_week
	,a.promo_month
	,a.promo_per
	,a.fisc_wk_num
	,a.max_wk_flg
	,b.no_of_wks
FROM (
	v_intrm_calendar a LEFT JOIN (
		SELECT DISTINCT v_intrm_calendar.fisc_per
			,v_intrm_calendar.fisc_wk_num AS no_of_wks
		FROM v_intrm_calendar
		WHERE ((v_intrm_calendar.max_wk_flg)::TEXT = ('Y'::CHARACTER VARYING)::TEXT)
		) b ON ((a.fisc_per = b.fisc_per))
	)
)
select * from final