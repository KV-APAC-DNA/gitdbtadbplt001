with 
edw_intrm_calendar as 
(
    select * from {{ ref('ntaedw_integration__edw_intrm_calendar') }}
),
itg_mds_hk_pos_promo_calendar as 
(
    select * from {{ ref('ntaitg_integration__itg_mds_hk_pos_promo_calendar') }}
),
final as 
(
    SELECT derived_table1."year" as year
	,derived_table1.fisc_per
	,derived_table1.qrtr_no
	,derived_table1.qrtr
	,derived_table1.mnth_id
	,derived_table1.mnth_desc
	,derived_table1.mnth_no
	,derived_table1.mnth_shrt
	,derived_table1.mnth_long
	,(((derived_table1."year")::CHARACTER VARYING)::TEXT || lpad(((derived_table1.wk)::CHARACTER VARYING)::TEXT, 2, ('0'::CHARACTER VARYING)::TEXT)) AS wk
	,derived_table1.mnth_wk_no
	,row_number() OVER (
		PARTITION BY derived_table1.mnth_wk_no
		,derived_table1.fisc_per ORDER BY derived_table1.cal_day
		) AS fisc_week_day_num
	,derived_table1.mnth_day
	,derived_table1.cal_year
	,derived_table1.cal_qrtr_no
	,derived_table1.cal_mnth_id
	,derived_table1.cal_mnth_no
	,derived_table1.cal_mnth_nm
	,derived_table1.cal_mnth_day
	,derived_table1.cal_mnth_wk_num
	,derived_table1.cal_yr_wk_num
	,row_number() OVER (
		PARTITION BY derived_table1.cal_mnth_wk_num
		,derived_table1.cal_mnth_id ORDER BY derived_table1.cal_day
		) AS cal_week_day_num
	,(((derived_table1.promo_year)::CHARACTER VARYING)::TEXT || lpad(((derived_table1.promo_week)::CHARACTER VARYING)::TEXT, 2, ('0'::CHARACTER VARYING)::TEXT)) AS promo_week
	,derived_table1.promo_month_week_num
	,derived_table1.promo_week_day_num
	,upper(((derived_table1.promo_month)::CHARACTER VARYING)::TEXT) AS promo_month_shrt
	,derived_table1.promo_year
	,derived_table1.cal_day
	,derived_table1.cal_date_id
FROM (
	SELECT ecd.fisc_yr AS "year"
		,ecd.fisc_per
		,CASE 
			WHEN (ecd.pstng_per = 1)
				THEN 1
			WHEN (ecd.pstng_per = 2)
				THEN 1
			WHEN (ecd.pstng_per = 3)
				THEN 1
			WHEN (ecd.pstng_per = 4)
				THEN 2
			WHEN (ecd.pstng_per = 5)
				THEN 2
			WHEN (ecd.pstng_per = 6)
				THEN 2
			WHEN (ecd.pstng_per = 7)
				THEN 3
			WHEN (ecd.pstng_per = 8)
				THEN 3
			WHEN (ecd.pstng_per = 9)
				THEN 3
			WHEN (ecd.pstng_per = 10)
				THEN 4
			WHEN (ecd.pstng_per = 11)
				THEN 4
			WHEN (ecd.pstng_per = 12)
				THEN 4
			ELSE NULL::INTEGER
			END AS qrtr_no
		,(
			(((ecd.fisc_yr)::CHARACTER VARYING)::TEXT || ('/'::CHARACTER VARYING)::TEXT) || (
				CASE 
					WHEN (ecd.pstng_per = 1)
						THEN 'Q1'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 2)
						THEN 'Q1'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 3)
						THEN 'Q1'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 4)
						THEN 'Q2'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 5)
						THEN 'Q2'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 6)
						THEN 'Q2'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 7)
						THEN 'Q3'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 8)
						THEN 'Q3'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 9)
						THEN 'Q3'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 10)
						THEN 'Q4'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 11)
						THEN 'Q4'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 12)
						THEN 'Q4'::CHARACTER VARYING
					ELSE NULL::CHARACTER VARYING
					END
				)::TEXT
			) AS qrtr
		,(((ecd.fisc_yr)::CHARACTER VARYING)::TEXT || trim(to_char(ecd.pstng_per, ('00'::CHARACTER VARYING)::TEXT))) AS mnth_id
		,(
			(((ecd.fisc_yr)::CHARACTER VARYING)::TEXT || ('/'::CHARACTER VARYING)::TEXT) || (
				CASE 
					WHEN (ecd.pstng_per = 1)
						THEN 'JAN'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 2)
						THEN 'FEB'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 3)
						THEN 'MAR'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 4)
						THEN 'APR'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 5)
						THEN 'MAY'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 6)
						THEN 'JUN'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 7)
						THEN 'JUL'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 8)
						THEN 'AUG'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 9)
						THEN 'SEP'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 10)
						THEN 'OCT'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 11)
						THEN 'NOV'::CHARACTER VARYING
					WHEN (ecd.pstng_per = 12)
						THEN 'DEC'::CHARACTER VARYING
					ELSE NULL::CHARACTER VARYING
					END
				)::TEXT
			) AS mnth_desc
		,ecd.pstng_per AS mnth_no
		,CASE 
			WHEN (ecd.pstng_per = 1)
				THEN 'JAN'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 2)
				THEN 'FEB'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 3)
				THEN 'MAR'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 4)
				THEN 'APR'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 5)
				THEN 'MAY'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 6)
				THEN 'JUN'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 7)
				THEN 'JUL'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 8)
				THEN 'AUG'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 9)
				THEN 'SEP'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 10)
				THEN 'OCT'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 11)
				THEN 'NOV'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 12)
				THEN 'DEC'::CHARACTER VARYING
			ELSE NULL::CHARACTER VARYING
			END AS mnth_shrt
		,CASE 
			WHEN (ecd.pstng_per = 1)
				THEN 'JANUARY'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 2)
				THEN 'FEBRUARY'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 3)
				THEN 'MARCH'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 4)
				THEN 'APRIL'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 5)
				THEN 'MAY'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 6)
				THEN 'JUNE'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 7)
				THEN 'JULY'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 8)
				THEN 'AUGUST'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 9)
				THEN 'SEPTEMBER'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 10)
				THEN 'OCTOBER'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 11)
				THEN 'NOVEMBER'::CHARACTER VARYING
			WHEN (ecd.pstng_per = 12)
				THEN 'DECEMBER'::CHARACTER VARYING
			ELSE NULL::CHARACTER VARYING
			END AS mnth_long
		,cyrwkno.yr_wk_num AS wk
		,ecd.fisc_wk_num AS mnth_wk_no
		,row_number() OVER (
			PARTITION BY ecd.fisc_per ORDER BY ecd.cal_day
			) AS mnth_day
		,ecd.cal_yr AS cal_year
		,ecd.cal_qtr_1 AS cal_qrtr_no
		,ecd.cal_mo_1 AS cal_mnth_id
		,ecd.cal_mo_2 AS cal_mnth_no
		,CASE 
			WHEN (ecd.cal_mo_2 = 1)
				THEN 'JANUARY'::CHARACTER VARYING
			WHEN (ecd.cal_mo_2 = 2)
				THEN 'FEBRUARY'::CHARACTER VARYING
			WHEN (ecd.cal_mo_2 = 3)
				THEN 'MARCH'::CHARACTER VARYING
			WHEN (ecd.cal_mo_2 = 4)
				THEN 'APRIL'::CHARACTER VARYING
			WHEN (ecd.cal_mo_2 = 5)
				THEN 'MAY'::CHARACTER VARYING
			WHEN (ecd.cal_mo_2 = 6)
				THEN 'JUNE'::CHARACTER VARYING
			WHEN (ecd.cal_mo_2 = 7)
				THEN 'JULY'::CHARACTER VARYING
			WHEN (ecd.cal_mo_2 = 8)
				THEN 'AUGUST'::CHARACTER VARYING
			WHEN (ecd.cal_mo_2 = 9)
				THEN 'SEPTEMBER'::CHARACTER VARYING
			WHEN (ecd.cal_mo_2 = 10)
				THEN 'OCTOBER'::CHARACTER VARYING
			WHEN (ecd.cal_mo_2 = 11)
				THEN 'NOVEMBER'::CHARACTER VARYING
			WHEN (ecd.cal_mo_2 = 12)
				THEN 'DECEMBER'::CHARACTER VARYING
			ELSE NULL::CHARACTER VARYING
			END AS cal_mnth_nm
		,row_number() OVER (
			PARTITION BY ecd.cal_mo_1 ORDER BY ecd.cal_day
			) AS cal_mnth_day
		,to_char((ecd.cal_day)::TIMESTAMP without TIME zone, ('W'::CHARACTER VARYING)::TEXT) AS cal_mnth_wk_num
		,(ecd.cal_wk)::CHARACTER VARYING AS cal_yr_wk_num
		,((ecd.cal_day)::date) AS cal_day
		,"replace" (
			((ecd.cal_day)::CHARACTER VARYING)::TEXT
			,('-'::CHARACTER VARYING)::TEXT
			,(''::CHARACTER VARYING)::TEXT
			) AS cal_date_id
		,promo.promo_week
		,promo.promo_month_week_num
		,promo.promo_week_day_num
		,promo.promo_month
		,promo.promo_year
	FROM (
		SELECT row_number() OVER (
				PARTITION BY a.fisc_yr ORDER BY a.cal_wk
				) AS yr_wk_num
			,(dateadd(('DAY'), (- (6)::BIGINT), (a.cal_day)::TIMESTAMP without TIME zone)) AS cal_day_first
			,a.cal_day AS cal_day_last
		FROM edw_intrm_calendar a
		WHERE (
				a.cal_day IN (
					SELECT edw_intrm_calendar.cal_day
					FROM edw_intrm_calendar
					WHERE (edw_intrm_calendar.wkday = 7)
					)
				)
		ORDER BY a.cal_wk
		) cyrwkno
		,(
			edw_intrm_calendar ecd LEFT JOIN (
				SELECT itg_mds_hk_pos_promo_calendar.calendardate AS cal_day
					,itg_mds_hk_pos_promo_calendar.promoyear AS promo_year
					,itg_mds_hk_pos_promo_calendar.promomonth AS promo_month
					,itg_mds_hk_pos_promo_calendar.promoweekday AS promo_week_day_num
					,itg_mds_hk_pos_promo_calendar.promomonthweeknumber AS promo_month_week_num
					,itg_mds_hk_pos_promo_calendar.promoweeknumber AS promo_week
					,(((itg_mds_hk_pos_promo_calendar.promoyear)::CHARACTER VARYING)::TEXT || ((itg_mds_hk_pos_promo_calendar.promomonth)::CHARACTER VARYING)::TEXT) AS promo_per
				FROM itg_mds_hk_pos_promo_calendar
				WHERE ((itg_mds_hk_pos_promo_calendar.customer_name)::TEXT = ('Mannings'::CHARACTER VARYING)::TEXT)
				) promo ON (((ecd.cal_day)::TIMESTAMP without TIME zone = promo.cal_day))
			)
	WHERE (
			(ecd.cal_day >= cyrwkno.cal_day_first)
			AND (ecd.cal_day <= cyrwkno.cal_day_last)
			)
	) derived_table1
)
select * from final