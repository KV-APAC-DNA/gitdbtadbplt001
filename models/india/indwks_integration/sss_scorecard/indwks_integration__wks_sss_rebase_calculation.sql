with WKS_SSS_SCORE_DET_FOR_REBASE as(
    select * from {{ ref('indwks_integration__wks_sss_score_det_for_rebase') }}
),
det as(
    SELECT DET.STORE_CODE,
				DET.PROGRAM_TYPE,
				--   DET.KPI,
				DET.QUARTER,
				DET.YEAR,
				FRANCHISE,
				max(kpi_score) AS kpi_score
			FROM WKS_SSS_SCORE_DET_FOR_REBASE DET
			GROUP BY DET.STORE_CODE,
				DET.PROGRAM_TYPE,
				--   DET.KPI,
				DET.QUARTER,
				DET.YEAR,
				DET.FRANCHISE
),
temp as(
    SELECT DET.STORE_CODE,
			DET.PROGRAM_TYPE,
			--   DET.KPI,
			DET.QUARTER,
			DET.YEAR,
			SUM(KPI_SCORE) AS SUM_KPI_SCORE,
			CASE 
				WHEN SUM(KPI_SCORE) = 0
					THEN 0
				WHEN SUM(KPI_SCORE) > 0
					THEN 1 / SUM(KPI_SCORE)
				END AS CALCULATED_KPI_SCORE
		FROM det
		GROUP BY STORE_CODE,
			PROGRAM_TYPE,
			-- KPI,
			QUARTER,
			YEAR
),
transformed as(
SELECT T.TABLE_RN,
	T.STORE_CODE,
	T.STORE_NAME,
	T.PROGRAM_TYPE,
	T.FRANCHISE,
	T.PROD_HIER_L4,
	T.KPI,
	T.QUARTER,
	T.YEAR,
	T.KPI_SCORE,
	TEMP.SUM_KPI_SCORE,
	TEMP.CALCULATED_KPI_SCORE,
	T.KPI_SCORE * (ROUND(TEMP.CALCULATED_KPI_SCORE, 4) * 100) AS REBASE_SCORE
FROM WKS_SSS_SCORE_DET_FOR_REBASE T, TEMP
WHERE T.STORE_CODE = TEMP.STORE_CODE
	AND T.PROGRAM_TYPE = TEMP.PROGRAM_TYPE
	--AND   T.KPI = TEMP.KPI
	AND T.QUARTER = TEMP.QUARTER
	AND T.YEAR = TEMP.YEAR
)
select * from transformed

