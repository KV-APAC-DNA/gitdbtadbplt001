with wks_tw_sales_incentive_offtake as(
	select * from {{ ref('ntawks_integration__wks_tw_sales_incentive_offtake') }}
),
wks_tw_sales_incentive_nts as(
	select * from {{ ref('ntawks_integration__wks_tw_sales_incentive_nts') }}
),
wks_tw_sales_incentive_tp_ciw as(
	select * from {{ ref('ntawks_integration__wks_tw_sales_incentive_tp_ciw') }}
),
union1 as(
		SELECT wks_tw_sales_incentive_offtake.source_type
			,wks_tw_sales_incentive_offtake.cntry_cd
			,wks_tw_sales_incentive_offtake.crncy_cd
			,wks_tw_sales_incentive_offtake.to_crncy
			,wks_tw_sales_incentive_offtake.psr_code
			,wks_tw_sales_incentive_offtake.psr_name
			,wks_tw_sales_incentive_offtake.year
			,wks_tw_sales_incentive_offtake.qrtr
			,wks_tw_sales_incentive_offtake.mnth_id
			,wks_tw_sales_incentive_offtake.report_to
			,wks_tw_sales_incentive_offtake.reportto_name
			,wks_tw_sales_incentive_offtake.reverse
			,wks_tw_sales_incentive_offtake.monthly_actual
			,wks_tw_sales_incentive_offtake.monthly_target
			,wks_tw_sales_incentive_offtake.monthly_achievement
			,wks_tw_sales_incentive_offtake.monthly_incentive_amount
			,wks_tw_sales_incentive_offtake.quarterly_actual
			,wks_tw_sales_incentive_offtake.quarterly_target
			,wks_tw_sales_incentive_offtake.quarterly_achievement
			,wks_tw_sales_incentive_offtake.quarterly_incentive_amount
		FROM wks_tw_sales_incentive_offtake
		
		UNION ALL
		
		SELECT wks_tw_sales_incentive_nts.source_type
			,wks_tw_sales_incentive_nts.cntry_cd
			,wks_tw_sales_incentive_nts.crncy_cd
			,wks_tw_sales_incentive_nts.to_crncy
			,wks_tw_sales_incentive_nts.psr_code
			,wks_tw_sales_incentive_nts.psr_name
			,wks_tw_sales_incentive_nts.year
			,wks_tw_sales_incentive_nts.qrtr
			,wks_tw_sales_incentive_nts.mnth_id
			,wks_tw_sales_incentive_nts.report_to
			,wks_tw_sales_incentive_nts.reportto_name
			,wks_tw_sales_incentive_nts.reverse
			,wks_tw_sales_incentive_nts.monthly_actual
			,wks_tw_sales_incentive_nts.monthly_target
			,wks_tw_sales_incentive_nts.monthly_achievement
			,wks_tw_sales_incentive_nts.monthly_incentive_amount
			,wks_tw_sales_incentive_nts.quarterly_actual
			,wks_tw_sales_incentive_nts.quarterly_target
			,wks_tw_sales_incentive_nts.quarterly_achievement
			,wks_tw_sales_incentive_nts.quarterly_incentive_amount
		FROM wks_tw_sales_incentive_nts
),
union2 as(
SELECT wks_tw_sales_incentive_tp_ciw.source_type
	,wks_tw_sales_incentive_tp_ciw.cntry_cd
	,wks_tw_sales_incentive_tp_ciw.crncy_cd
	,wks_tw_sales_incentive_tp_ciw.to_crncy
	,wks_tw_sales_incentive_tp_ciw.psr_code
	,wks_tw_sales_incentive_tp_ciw.psr_name
	,wks_tw_sales_incentive_tp_ciw.year
	,wks_tw_sales_incentive_tp_ciw.qrtr
	,wks_tw_sales_incentive_tp_ciw.mnth_id
	,wks_tw_sales_incentive_tp_ciw.report_to
	,wks_tw_sales_incentive_tp_ciw.reportto_name
	,wks_tw_sales_incentive_tp_ciw.reverse
	,wks_tw_sales_incentive_tp_ciw.monthly_actual
	,wks_tw_sales_incentive_tp_ciw.monthly_target
	,wks_tw_sales_incentive_tp_ciw.monthly_achievement
	,wks_tw_sales_incentive_tp_ciw.monthly_incentive_amount
	,wks_tw_sales_incentive_tp_ciw.quarterly_actual
	,wks_tw_sales_incentive_tp_ciw.quarterly_target
	,wks_tw_sales_incentive_tp_ciw.quarterly_achievement
	,wks_tw_sales_incentive_tp_ciw.quarterly_incentive_amount
FROM wks_tw_sales_incentive_tp_ciw
),
transformed as(
	select * from union1
	union all
	select * from union2
)
select * from transformed