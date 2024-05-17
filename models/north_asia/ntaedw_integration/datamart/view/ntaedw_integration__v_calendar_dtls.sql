with v_calendar_promo_univ_fisc as(
    select * from  {{ source('ntaedw_integration', 'v_calendar_promo_univ_fisc') }}
),
transformed as(

SELECT b.cal_day
	,b.cal_mo_2 AS cal_mo
	,b.cal_yr
	,b.univ_per
	,b.univ_wk
	,d.univ_wk_strt_dt
	,d.univ_wk_end_dt
	,b.univ_week_month
	,e.univ_wk_month_strt_dt
	,e.univ_wk_month_end_dt
	,b.fisc_per
	,b.fisc_wk
	,c.fisc_wk_strt_dt
	,c.fisc_wk_end_dt
	,b.promo_per
	,f.promo_wk
	,f.promo_wk_strt_dt
	,f.promo_wk_end_dt
FROM (
	(
		(
			(
				(
					SELECT x.cal_day
						,x.univ_week AS univ_wk
						,x.cal_wk AS fisc_wk
						,x.cal_yr
						,x.cal_mo_2
						,(
							((x.cal_yr)::CHARACTER VARYING)::TEXT || CASE 
								WHEN (length(((x.cal_mo_2)::CHARACTER VARYING)::TEXT) = 2)
									THEN (('0'::CHARACTER VARYING)::TEXT || ((x.cal_mo_2)::CHARACTER VARYING)::TEXT)
								ELSE (('00'::CHARACTER VARYING)::TEXT || ((x.cal_mo_2)::CHARACTER VARYING)::TEXT)
								END
							) AS univ_per
						,x.univ_week_month
						,x.fisc_per
						,x.promo_week AS promo_wk
						,x.promo_month
						,x.promo_per
					FROM v_calendar_promo_univ_fisc x
					) b LEFT JOIN (
					SELECT v_calendar_promo_univ_fisc.cal_wk AS fisc_wk
						,min(v_calendar_promo_univ_fisc.cal_day) AS fisc_wk_strt_dt
						,"max" (v_calendar_promo_univ_fisc.cal_day) AS fisc_wk_end_dt
					FROM v_calendar_promo_univ_fisc
					GROUP BY v_calendar_promo_univ_fisc.cal_wk
					) c ON ((b.fisc_wk = c.fisc_wk))
				) LEFT JOIN (
				SELECT v_calendar_promo_univ_fisc.univ_week AS univ_wk
					,min(v_calendar_promo_univ_fisc.cal_day) AS univ_wk_strt_dt
					,"max" (v_calendar_promo_univ_fisc.cal_day) AS univ_wk_end_dt
				FROM v_calendar_promo_univ_fisc
				GROUP BY v_calendar_promo_univ_fisc.univ_week
					,v_calendar_promo_univ_fisc.cal_yr
				) d ON ((b.univ_wk = d.univ_wk))
			) LEFT JOIN (
			SELECT v_calendar_promo_univ_fisc.univ_week_month
				,v_calendar_promo_univ_fisc.cal_yr
				,v_calendar_promo_univ_fisc.cal_mo_2
				,min(v_calendar_promo_univ_fisc.cal_day) AS univ_wk_month_strt_dt
				,"max" (v_calendar_promo_univ_fisc.cal_day) AS univ_wk_month_end_dt
			FROM v_calendar_promo_univ_fisc
			GROUP BY v_calendar_promo_univ_fisc.univ_week_month
				,v_calendar_promo_univ_fisc.cal_yr
				,v_calendar_promo_univ_fisc.cal_mo_2
			) e ON (
				(
					(
						(b.univ_week_month = e.univ_week_month)
						AND (b.cal_yr = e.cal_yr)
						)
					AND (b.cal_mo_2 = e.cal_mo_2)
					)
				)
		) LEFT JOIN (
		SELECT v_calendar_promo_univ_fisc.promo_per
			,v_calendar_promo_univ_fisc.promo_week AS promo_wk
			,min(v_calendar_promo_univ_fisc.cal_day) AS promo_wk_strt_dt
			,"max" (v_calendar_promo_univ_fisc.cal_day) AS promo_wk_end_dt
		FROM v_calendar_promo_univ_fisc
		GROUP BY v_calendar_promo_univ_fisc.promo_week
			,v_calendar_promo_univ_fisc.promo_per
		) f ON (
			(
				(b.promo_wk = f.promo_wk)
				AND (b.promo_per = f.promo_per)
				)
			)
	)
)
select * from transformed   