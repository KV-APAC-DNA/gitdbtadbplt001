with v_intrm_calendar_ims as(
	select * from {{ ref('ntaedw_integration__v_intrm_calendar_ims') }}
),
edw_ims_fact as(
	select * from {{ ref('ntaedw_integration__edw_ims_fact') }}
),
itg_gt_msl_items as(
	select * from ntaitg_integration.itg_gt_msl_items
),
t2 as(
	SELECT v_intrm_calendar_ims.cal_day
			,v_intrm_calendar_ims.fisc_wk_num AS jj_wk_num
			,(
				(
					"substring" (
						((v_intrm_calendar_ims.fisc_per)::CHARACTER VARYING)::TEXT
						,1
						,4
						) || "substring" (
						((v_intrm_calendar_ims.fisc_per)::CHARACTER VARYING)::TEXT
						,6
						,2
						)
					)
				)::INTEGER AS jj_mnth_id
		FROM v_intrm_calendar_ims
		WHERE 
				(v_intrm_calendar_ims.cal_day::date <= current_timestamp()::date)
				AND (v_intrm_calendar_ims.wkday <> 7)
				
),
a as(
	SELECT DISTINCT edw_ims_fact.prod_cd
			,edw_ims_fact.ean_num
			,edw_ims_fact.ctry_cd
			,edw_ims_fact.dstr_cd
		FROM edw_ims_fact
),
transformed as(
SELECT t1.ctry_cd
	,t1.dstr_cd
	,t1.brand
	,t1.dstr_prod_cd
	,t1.sap_matl_cd
	,a.ean_num
	,t1.prod_desc_eng
	,t1.prod_desc_chnse
	,t1.store_class
	,t1.msl_flg
	,t2.cal_day AS msl_dt
	,t1.strt_yr_mnth
	,t1.end_yr_mnth
FROM (
	itg_gt_msl_items t1 LEFT JOIN a ON (
			(
				(
					((t1.sap_matl_cd)::TEXT = (a.prod_cd)::TEXT)
					AND ((t1.ctry_cd)::TEXT = (a.ctry_cd)::TEXT)
					)
				AND ((t1.dstr_cd)::TEXT = (a.dstr_cd)::TEXT)
				)
			)
	)
	,t2
WHERE (
		(((t2.jj_mnth_id)::CHARACTER VARYING)::TEXT >= (t1.strt_yr_mnth)::TEXT)
		AND (((t2.jj_mnth_id)::CHARACTER VARYING)::TEXT <= (t1.end_yr_mnth)::TEXT)
		)
)
select * from transformed