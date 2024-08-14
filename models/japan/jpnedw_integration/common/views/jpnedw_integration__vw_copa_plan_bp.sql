with edw_copa_plan_fact as(
    select * from {{ ref('aspedw_integration__edw_copa_plan_fact') }}
),
union1 as(
    SELECT (
				"left" (
					((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
					,4
					)
				)::CHARACTER VARYING AS year_445
			,CASE 
				WHEN (
						to_number("right" (
								((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
								,2
								), ('99'::CHARACTER VARYING)::TEXT) <= ((6)::NUMERIC)::NUMERIC(18, 0)
						)
					THEN '1'::CHARACTER VARYING
				ELSE '2'::CHARACTER VARYING
				END AS half_445
			,CASE 
				WHEN (
						to_number("right" (
								((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
								,2
								), ('99'::CHARACTER VARYING)::TEXT) <= ((3)::NUMERIC)::NUMERIC(18, 0)
						)
					THEN '1'::CHARACTER VARYING
				WHEN (
						to_number("right" (
								((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
								,2
								), ('99'::CHARACTER VARYING)::TEXT) <= ((6)::NUMERIC)::NUMERIC(18, 0)
						)
					THEN '2'::CHARACTER VARYING
				WHEN (
						to_number("right" (
								((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
								,2
								), ('99'::CHARACTER VARYING)::TEXT) <= ((9)::NUMERIC)::NUMERIC(18, 0)
						)
					THEN '3'::CHARACTER VARYING
				ELSE '4'::CHARACTER VARYING
				END AS quarter_445
			,(
				ltrim("right" (
						((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
						,2
						), ((0)::CHARACTER VARYING)::TEXT)
				)::CHARACTER VARYING AS month_445
			,PLAN.acct_hier_shrt_desc
			,PLAN.acct_hier_desc
			,PLAN.acct_num
			,PLAN.category
			,(sum(PLAN.amt_obj_crcy) * (- ((1)::NUMERIC)::NUMERIC(18, 0))) AS amt_obj_crcy
		FROM edw_copa_plan_fact PLAN
		WHERE (
				(
					(
						(
							(
								((PLAN.obj_crncy)::TEXT = ('JPY'::CHARACTER VARYING)::TEXT)
								AND ((PLAN.ctry_key)::TEXT = ('JP'::CHARACTER VARYING)::TEXT)
								)
							AND ((PLAN.acct_hier_shrt_desc)::TEXT <> ('SCOGS'::CHARACTER VARYING)::TEXT)
							)
						AND ((PLAN.acct_hier_shrt_desc)::TEXT <> ('NTS'::CHARACTER VARYING)::TEXT)
						)
					AND ((PLAN.acct_hier_shrt_desc)::TEXT <> ('GTS'::CHARACTER VARYING)::TEXT)
					)
				AND ((PLAN.category)::TEXT = ('BP'::CHARACTER VARYING)::TEXT)
				)
		GROUP BY "left" (
				((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
				,4
				)
			,CASE 
				WHEN (
						"right" (
							((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
							,2
							) <= ((6)::CHARACTER VARYING)::TEXT
						)
					THEN 1
				ELSE 2
				END
			,CASE 
				WHEN (
						"right" (
							((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
							,2
							) <= ((3)::CHARACTER VARYING)::TEXT
						)
					THEN 1
				WHEN (
						"right" (
							((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
							,2
							) <= ((6)::CHARACTER VARYING)::TEXT
						)
					THEN 2
				WHEN (
						"right" (
							((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
							,2
							) <= ((9)::CHARACTER VARYING)::TEXT
						)
					THEN 3
				ELSE 4
				END
			,"right" (
				((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
				,2
				)
			,PLAN.acct_hier_shrt_desc
			,PLAN.acct_hier_desc
			,PLAN.acct_num
			,PLAN.category
),
union2 as(
 	SELECT (
				"left" (
					((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
					,4
					)
				)::CHARACTER VARYING AS year_445
			,CASE 
				WHEN (
						to_number("right" (
								((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
								,2
								), ('99'::CHARACTER VARYING)::TEXT) <= ((6)::NUMERIC)::NUMERIC(18, 0)
						)
					THEN '1'::CHARACTER VARYING
				ELSE '2'::CHARACTER VARYING
				END AS half_445
			,CASE 
				WHEN (
						to_number("right" (
								((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
								,2
								), ('99'::CHARACTER VARYING)::TEXT) <= ((3)::NUMERIC)::NUMERIC(18, 0)
						)
					THEN '1'::CHARACTER VARYING
				WHEN (
						to_number("right" (
								((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
								,2
								), ('99'::CHARACTER VARYING)::TEXT) <= ((6)::NUMERIC)::NUMERIC(18, 0)
						)
					THEN '2'::CHARACTER VARYING
				WHEN (
						to_number("right" (
								((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
								,2
								), ('99'::CHARACTER VARYING)::TEXT) <= ((9)::NUMERIC)::NUMERIC(18, 0)
						)
					THEN '3'::CHARACTER VARYING
				ELSE '4'::CHARACTER VARYING
				END AS quarter_445
			,(
				ltrim("right" (
						((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
						,2
						), ((0)::CHARACTER VARYING)::TEXT)
				)::CHARACTER VARYING AS month_445
			,PLAN.acct_hier_shrt_desc
			,PLAN.acct_hier_desc
			,PLAN.acct_num
			,PLAN.category
			,sum(PLAN.amt_obj_crcy) AS amt_obj_crcy
		FROM edw_copa_plan_fact PLAN
		WHERE (
				(
					(
						((PLAN.obj_crncy)::TEXT = ('JPY'::CHARACTER VARYING)::TEXT)
						AND ((PLAN.ctry_key)::TEXT = ('JP'::CHARACTER VARYING)::TEXT)
						)
					AND ((PLAN.acct_hier_shrt_desc)::TEXT = ('GTS'::CHARACTER VARYING)::TEXT)
					)
				AND ((PLAN.category)::TEXT = ('BP'::CHARACTER VARYING)::TEXT)
				)
		GROUP BY "left" (
				((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
				,4
				)
			,CASE 
				WHEN (
						"right" (
							((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
							,2
							) <= ((6)::CHARACTER VARYING)::TEXT
						)
					THEN 1
				ELSE 2
				END
			,CASE 
				WHEN (
						"right" (
							((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
							,2
							) <= ((3)::CHARACTER VARYING)::TEXT
						)
					THEN 1
				WHEN (
						"right" (
							((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
							,2
							) <= ((6)::CHARACTER VARYING)::TEXT
						)
					THEN 2
				WHEN (
						"right" (
							((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
							,2
							) <= ((9)::CHARACTER VARYING)::TEXT
						)
					THEN 3
				ELSE 4
				END
			,"right" (
				((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
				,2
				)
			,PLAN.acct_hier_shrt_desc
			,PLAN.acct_hier_desc
			,PLAN.acct_num
			,PLAN.category
),
union3 as(
    SELECT * from union1
    union all
    select * from union2
),
union4 as(
    SELECT (
		"left" (
			((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
			,4
			)
		)::CHARACTER VARYING AS year_445
	,CASE 
		WHEN (
				to_number("right" (
						((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
						,2
						), ('99'::CHARACTER VARYING)::TEXT) <= ((6)::NUMERIC)::NUMERIC(18, 0)
				)
			THEN '1'::CHARACTER VARYING
		ELSE '2'::CHARACTER VARYING
		END AS half_445
	,CASE 
		WHEN (
				to_number("right" (
						((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
						,2
						), ('99'::CHARACTER VARYING)::TEXT) <= ((3)::NUMERIC)::NUMERIC(18, 0)
				)
			THEN '1'::CHARACTER VARYING
		WHEN (
				to_number("right" (
						((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
						,2
						), ('99'::CHARACTER VARYING)::TEXT) <= ((6)::NUMERIC)::NUMERIC(18, 0)
				)
			THEN '2'::CHARACTER VARYING
		WHEN (
				to_number("right" (
						((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
						,2
						), ('99'::CHARACTER VARYING)::TEXT) <= ((9)::NUMERIC)::NUMERIC(18, 0)
				)
			THEN '3'::CHARACTER VARYING
		ELSE '4'::CHARACTER VARYING
		END AS quarter_445
	,(
		ltrim("right" (
				((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
				,2
				), ((0)::CHARACTER VARYING)::TEXT)
		)::CHARACTER VARYING AS month_445
	,PLAN.acct_hier_shrt_desc
	,PLAN.acct_hier_desc
	,'0099999999' AS acct_num
	,PLAN.category
	,sum(PLAN.amt_obj_crcy) AS amt_obj_crcy
FROM edw_copa_plan_fact PLAN
WHERE (
		(
			(
				((PLAN.obj_crncy)::TEXT = ('JPY'::CHARACTER VARYING)::TEXT)
				AND ((PLAN.ctry_key)::TEXT = ('JP'::CHARACTER VARYING)::TEXT)
				)
			AND ((PLAN.acct_hier_shrt_desc)::TEXT = ('NTS'::CHARACTER VARYING)::TEXT)
			)
		AND ((PLAN.category)::TEXT = ('BP'::CHARACTER VARYING)::TEXT)
		)
GROUP BY "left" (
		((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
		,4
		)
	,CASE 
		WHEN (
				"right" (
					((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
					,2
					) <= ((6)::CHARACTER VARYING)::TEXT
				)
			THEN 1
		ELSE 2
		END
	,CASE 
		WHEN (
				"right" (
					((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
					,2
					) <= ((3)::CHARACTER VARYING)::TEXT
				)
			THEN 1
		WHEN (
				"right" (
					((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
					,2
					) <= ((6)::CHARACTER VARYING)::TEXT
				)
			THEN 2
		WHEN (
				"right" (
					((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
					,2
					) <= ((9)::CHARACTER VARYING)::TEXT
				)
			THEN 3
		ELSE 4
		END
	,"right" (
		((PLAN.fisc_yr_per)::CHARACTER VARYING)::TEXT
		,2
		)
	,PLAN.acct_hier_shrt_desc
	,PLAN.acct_hier_desc
	,PLAN.category
),
transformed as(
    SELECT * from union3
    union all
    select * from union4
)
select * from transformed