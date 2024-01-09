{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}

--Import CTE
with edw_crncy_exch as (
    select * from {{ ref('aspedw_integration__edw_crncy_exch') }}
),

edw_calendar_dim as (
   select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
--Logical CTE

-- Final CTE
final as (
(
	SELECT derived_table1.ex_rt_typ
	,derived_table1.from_crncy
	,derived_table1.to_crncy
	,derived_table1.vld_from
	,derived_table1.ex_rt
	,derived_table1.fisc_per FROM (
	SELECT a.ex_rt_typ
		,a.from_crncy
		,a.to_crncy
		,calmonthstartdate.vld_from
		,a.ex_rt
		,calmonthstartdate.fisc_per
		,RANK() OVER (
			PARTITION BY a.from_crncy
			,a.to_crncy
			,calmonthstartdate.fisc_per ORDER BY calmonthstartdate.vld_from DESC
			) AS latest_ex_rt_by_fisc_per
	FROM 
		(
			SELECT drvd_crncy.ex_rt_typ
				,drvd_crncy.from_crncy
				,drvd_crncy.to_crncy
				,cal.fisc_yr AS "year"
				,MAX(CAST((
							CASE 
								WHEN (
										(CAST((drvd_crncy.to_crncy) AS TEXT) = CAST((CAST('USD' AS VARCHAR)) AS TEXT))
										AND (
											(
												(CAST((drvd_crncy.from_crncy) AS TEXT) = CAST((CAST('IDR' AS VARCHAR)) AS TEXT))
												OR (CAST((drvd_crncy.from_crncy) AS TEXT) = CAST((CAST('KRW' AS VARCHAR)) AS TEXT))
												)
											OR (CAST((drvd_crncy.from_crncy) AS TEXT) = CAST((CAST('VND' AS VARCHAR)) AS TEXT))
											)
										)
									THEN (drvd_crncy.ex_rt / CAST((CAST((1000) AS DECIMAL)) AS DECIMAL(18, 0)))
								WHEN (
										(CAST((drvd_crncy.to_crncy) AS TEXT) = CAST((CAST('USD' AS VARCHAR)) AS TEXT))
										AND (CAST((drvd_crncy.from_crncy) AS TEXT) = CAST((CAST('JPY' AS VARCHAR)) AS TEXT))
										)
									THEN (drvd_crncy.ex_rt / CAST((CAST((100) AS DECIMAL)) AS DECIMAL(18, 0)))
								ELSE drvd_crncy.ex_rt
								END
							) AS DECIMAL(20, 10))) AS ex_rt
			FROM (
				(
					SELECT DISTINCT edw_crncy_exch.ex_rt_typ
						,edw_crncy_exch.from_crncy
						,edw_crncy_exch.to_crncy
						,TO_DATE(CAST((CAST(((CAST((CAST((99999999) AS DECIMAL)) AS DECIMAL(18, 0)) - CAST((CAST((edw_crncy_exch.vld_from) AS DECIMAL)) AS DECIMAL(18, 0)))) AS VARCHAR)) AS TEXT), CAST((CAST('YYYYMMDD' AS VARCHAR)) AS TEXT)) AS vld_from
						,edw_crncy_exch.ex_rt
						,edw_crncy_exch.from_ratio
						,edw_crncy_exch.to_ratio
					FROM edw_crncy_exch AS edw_crncy_exch
					WHERE (
							(
								(
									(CAST((edw_crncy_exch.ex_rt_typ) AS TEXT) = CAST((CAST('BWAR' AS VARCHAR)) AS TEXT))
									AND (CAST((edw_crncy_exch.from_crncy) AS TEXT) <> CAST((CAST('LKR' AS VARCHAR)) AS TEXT))
									)
								AND (CAST((edw_crncy_exch.from_crncy) AS TEXT) <> CAST((CAST('BDT' AS VARCHAR)) AS TEXT))
								)
							AND (CAST((edw_crncy_exch.from_crncy) AS TEXT) <> CAST((CAST('NZD' AS VARCHAR)) AS TEXT))
							)
					) AS drvd_crncy JOIN edw_calendar_dim AS cal
					ON (
							(
								(drvd_crncy.vld_from = cal.cal_day)
								AND (CAST((cal.fisc_yr_vrnt) AS TEXT) = CAST((CAST('J1' AS VARCHAR)) AS TEXT))
								)
							)
				)
			GROUP BY drvd_crncy.ex_rt_typ
				,drvd_crncy.from_crncy
				,drvd_crncy.to_crncy
				,SUBSTRING(CAST((CAST((drvd_crncy.vld_from) AS VARCHAR)) AS TEXT), 1, 4)
				,cal.fisc_yr
			) AS a JOIN (
			SELECT edw_calendar_dim.fisc_yr AS "year"
				,edw_calendar_dim.fisc_per
				,MIN(edw_calendar_dim.cal_day) AS vld_from
			FROM edw_calendar_dim
			WHERE (CAST((edw_calendar_dim.fisc_yr_vrnt) AS TEXT) = CAST((CAST('J1' AS VARCHAR)) AS TEXT))
			GROUP BY edw_calendar_dim.fisc_yr
				,edw_calendar_dim.fisc_per
			) AS calmonthstartdate
			ON (
					(
						(a."year" = calmonthstartdate."year")
						AND (calmonthstartdate.vld_from <= TO_VARIANT(CURRENT_DATE ())::VARCHAR)
						)
					)
		)
	 AS derived_table1 WHERE (derived_table1.latest_ex_rt_by_fisc_per = 1)

UNION ALL
	
	SELECT DISTINCT edw_crncy_exch.ex_rt_typ
	,edw_crncy_exch.from_crncy
	,edw_crncy_exch.from_crncy AS to_crncy
	,calmonthstartdate.vld_from
	,1 AS ex_rt
	,calmonthstartdate.fisc_per FROM (
	edw_crncy_exch AS edw_crncy_exch JOIN (
		SELECT edw_calendar_dim.fisc_yr AS "year"
			,edw_calendar_dim.fisc_per
			,MIN(edw_calendar_dim.cal_day) AS vld_from
		FROM edw_calendar_dim
		WHERE (
				(CAST((edw_calendar_dim.fisc_yr_vrnt) AS TEXT) = CAST((CAST('J1' AS VARCHAR)) AS TEXT))
				AND (edw_calendar_dim.cal_day <= TO_VARIANT(CURRENT_DATE ())::VARCHAR)
				)
		GROUP BY edw_calendar_dim.fisc_yr
			,edw_calendar_dim.fisc_per
		) AS calmonthstartdate
		ON ((1 = 1))
	) WHERE (CAST((edw_crncy_exch.ex_rt_typ) AS TEXT) = CAST((CAST('BWAR' AS VARCHAR)) AS TEXT))
	)

UNION ALL

SELECT drvd_crncy.ex_rt_typ
	,drvd_crncy.from_crncy
	,drvd_crncy.to_crncy
	,drvd_crncy.vld_from
	,CAST((
			CASE 
				WHEN (
						(CAST((drvd_crncy.to_crncy) AS TEXT) = CAST((CAST('USD' AS VARCHAR)) AS TEXT))
						AND (
							(
								(CAST((drvd_crncy.from_crncy) AS TEXT) = CAST((CAST('IDR' AS VARCHAR)) AS TEXT))
								OR (CAST((drvd_crncy.from_crncy) AS TEXT) = CAST((CAST('KRW' AS VARCHAR)) AS TEXT))
								)
							OR (CAST((drvd_crncy.from_crncy) AS TEXT) = CAST((CAST('VND' AS VARCHAR)) AS TEXT))
							)
						)
					THEN (drvd_crncy.ex_rt / CAST((CAST((1000) AS DECIMAL)) AS DECIMAL(18, 0)))
				WHEN (
						(CAST((drvd_crncy.to_crncy) AS TEXT) = CAST((CAST('USD' AS VARCHAR)) AS TEXT))
						AND (CAST((drvd_crncy.from_crncy) AS TEXT) = CAST((CAST('JPY' AS VARCHAR)) AS TEXT))
						)
					THEN (drvd_crncy.ex_rt / CAST((CAST((100) AS DECIMAL)) AS DECIMAL(18, 0)))
				ELSE drvd_crncy.ex_rt
				END
			) AS DECIMAL(20, 10)) AS ex_rt
	,drvd_crncy.fisc_per
FROM (
	SELECT DISTINCT edw_crncy_exch.ex_rt_typ
		,edw_crncy_exch.from_crncy
		,edw_crncy_exch.to_crncy
		,edw_crncy_exch.vld_from
		,edw_crncy_exch.ex_rt
		,edw_crncy_exch.fisc_per
		,RANK() OVER (
			PARTITION BY edw_crncy_exch.from_crncy
			,edw_crncy_exch.to_crncy
			,edw_crncy_exch.fisc_per ORDER BY edw_crncy_exch.vld_from
			) AS latest_ex_rt_by_fisc_per
	FROM (
		SELECT a.ex_rt_typ
			,a.from_crncy
			,a.to_crncy
			,a.ex_rt
			,a.vld_from
			,b.fisc_per
		FROM (
			(
				SELECT a.ex_rt_typ
					,a.from_crncy
					,a.to_crncy
					,a.ex_rt
					,TO_DATE(CAST((CAST(((CAST((CAST((99999999) AS DECIMAL)) AS DECIMAL(18, 0)) - CAST((CAST((a.vld_from) AS DECIMAL)) AS DECIMAL(18, 0)))) AS VARCHAR)) AS TEXT), CAST((CAST('YYYYMMDD' AS VARCHAR)) AS TEXT)) AS vld_from
				FROM edw_crncy_exch AS a
				WHERE (
						(CAST((a.ex_rt_typ) AS TEXT) = CAST((CAST('BWAR' AS VARCHAR)) AS TEXT))
						AND (
							(
								(CAST((a.from_crncy) AS TEXT) = CAST((CAST('LKR' AS VARCHAR)) AS TEXT))
								OR (CAST((a.from_crncy) AS TEXT) = CAST((CAST('BDT' AS VARCHAR)) AS TEXT))
								)
							OR (CAST((a.from_crncy) AS TEXT) = CAST((CAST('NZD' AS VARCHAR)) AS TEXT))
							)
						)
				) AS a JOIN edw_calendar_dim AS b
				ON ((a.vld_from = b.cal_day))
			)
		WHERE (CAST((b.fisc_yr_vrnt) AS TEXT) = CAST((CAST('J1' AS VARCHAR)) AS TEXT))
		
		UNION ALL
		
		SELECT ex.ex_rt_typ
			,ex.from_crncy
			,ex.to_crncy
			,ex.ex_rt
			,ex.vld_from
			,c.fisc_per
		FROM (
			(
				SELECT a.ex_rt_typ
					,a.from_crncy
					,a.to_crncy
					,a.ex_rt
					,a.vld_from
					,b.fisc_per
				FROM (
					(
						SELECT a.ex_rt_typ
							,a.from_crncy
							,a.to_crncy
							,a.ex_rt
							,a.vld_from
						FROM (
							(
								SELECT edw_crncy_exch.ex_rt_typ
									,edw_crncy_exch.from_crncy
									,edw_crncy_exch.to_crncy
									,edw_crncy_exch.ex_rt
									,TO_DATE(CAST((CAST(((CAST((CAST((99999999) AS DECIMAL)) AS DECIMAL(18, 0)) - CAST((CAST((edw_crncy_exch.vld_from) AS DECIMAL)) AS DECIMAL(18, 0)))) AS VARCHAR)) AS TEXT), CAST((CAST('YYYYMMDD' AS VARCHAR)) AS TEXT)) AS vld_from
								FROM edw_crncy_exch
								WHERE (
										(CAST((edw_crncy_exch.ex_rt_typ) AS TEXT) = CAST((CAST('BWAR' AS VARCHAR)) AS TEXT))
										AND (
											(
												(CAST((edw_crncy_exch.from_crncy) AS TEXT) = CAST((CAST('LKR' AS VARCHAR)) AS TEXT))
												OR (CAST((edw_crncy_exch.from_crncy) AS TEXT) = CAST((CAST('BDT' AS VARCHAR)) AS TEXT))
												)
											OR (CAST((edw_crncy_exch.from_crncy) AS TEXT) = CAST((CAST('NZD' AS VARCHAR)) AS TEXT))
											)
										)
								) AS a JOIN (
								SELECT edw_crncy_exch.from_crncy
									,edw_crncy_exch.to_crncy
									,MAX(TO_DATE(CAST((CAST(((CAST((CAST((99999999) AS DECIMAL)) AS DECIMAL(18, 0)) - CAST((CAST((edw_crncy_exch.vld_from) AS DECIMAL)) AS DECIMAL(18, 0)))) AS VARCHAR)) AS TEXT), CAST((CAST('YYYYMMDD' AS VARCHAR)) AS TEXT))) AS vld_from
								FROM edw_crncy_exch
								WHERE (
										(CAST((edw_crncy_exch.ex_rt_typ) AS TEXT) = CAST((CAST('BWAR' AS VARCHAR)) AS TEXT))
										AND (
											(
												(CAST((edw_crncy_exch.from_crncy) AS TEXT) = CAST((CAST('LKR' AS VARCHAR)) AS TEXT))
												OR (CAST((edw_crncy_exch.from_crncy) AS TEXT) = CAST((CAST('BDT' AS VARCHAR)) AS TEXT))
												)
											OR (CAST((edw_crncy_exch.from_crncy) AS TEXT) = CAST((CAST('NZD' AS VARCHAR)) AS TEXT))
											)
										)
								GROUP BY edw_crncy_exch.from_crncy
									,edw_crncy_exch.to_crncy
								) AS b
								ON (
										(
											(
												(CAST((a.from_crncy) AS TEXT) = CAST((b.from_crncy) AS TEXT))
												AND (CAST((a.to_crncy) AS TEXT) = CAST((b.to_crncy) AS TEXT))
												)
											AND (a.vld_from = b.vld_from)
											)
										)
							)
						) AS a JOIN edw_calendar_dim AS b
						ON ((a.vld_from = b.cal_day))
					)
				WHERE (CAST((b.fisc_yr_vrnt) AS TEXT) = CAST((CAST('J1' AS VARCHAR)) AS TEXT))
				) AS ex JOIN (
				SELECT DISTINCT edw_calendar_dim.fisc_per
				FROM edw_calendar_dim
				WHERE (
						(CAST((edw_calendar_dim.fisc_yr_vrnt) AS TEXT) = CAST((CAST('J1' AS VARCHAR)) AS TEXT))
						AND (edw_calendar_dim.cal_day <= TO_VARIANT(CURRENT_DATE ())::VARCHAR)
						)
				) AS c
				ON ((ex.fisc_per < c.fisc_per))
			)
		) AS edw_crncy_exch
	) AS drvd_crncy
WHERE (drvd_crncy.latest_ex_rt_by_fisc_per = 1)
  
--   current_timestamp()::timestamp_ntz(9) as crt_dttm,
--   current_timestamp()::timestamp_ntz(9) as updt_dttm
)


--Final select
select * from final 