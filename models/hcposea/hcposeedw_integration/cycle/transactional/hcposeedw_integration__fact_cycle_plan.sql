WITH WRK_FACT_CYCLE_PLAN
AS (
	SELECT *
	FROM DEV_DNA_CORE.PPAHIL01_WORKSPACE.HCPOSEEDW_INTEGRATION__WRK_FACT_CYCLE_PLAN
	),
itg_CYCLE_PLAN_DETAIL
AS (
	SELECT *
	FROM HCPOSEITG_INTEGRATION.itg_CYCLE_PLAN_DETAIL
	),
DIM_PRODUCT_INDICATION
AS (
	SELECT *
	FROM HCPOSEEDW_INTEGRATION.DIM_PRODUCT_INDICATION
	),
T1
AS (
	SELECT COUNTRY_KEY,
		NVL(EMPLOYEE_KEY, 'Not Applicable') AS EMPLOYEE_KEY,
		NVL(ORGANIZATION_KEY, 'Not Applicable') AS ORGANIZATION_KEY,
		TERRITORY_NAME,
		NVL(START_DATE_KEY, 0) AS START_DATE_KEY,
		NVL(END_DATE_KEY, 0) AS END_DATE_KEY,
		ACTIVE_FLAG,
		MANAGER_NAME,
		APPROVER_NAME,
		CLOSE_OUT_FLAG,
		READY_FOR_APPROVAL_FLAG,
		STATUS_TYPE,
		CYCLE_PLAN_NAME,
		CHANNEL_TYPE,
		ACTUAL_CALL_CNT_CP,
		PLANNED_CALL_CNT_CP,
		NULL AS CYCLE_PLAN_EXTERNAL_ID,
		NULL AS MANAGER,
		CP_APPROVAL_TIME,
		NUMBER_OF_TARGETS,
		NUMBER_OF_CFA_100_TARGETS,
		CYCLE_PLAN_ATTAINMENT_CPTARGET,
		MID_DATE,
		HCP_PRODUCT_ACHIEVED_100,
		HCP_PRODUCTS_PLANNED,
		CPA_100,
		NVL(HCP_KEY, 'Not Applicable') AS HCP_KEY,
		NVL(HCO_KEY, 'Not Applicable') AS HCO_KEY,
		SCHEDULED_CALL_COUNT,
		ACTUAL_CALL_COUNT,
		PLANNED_CALL_COUNT,
		REMAINING_CALL_COUNT,
		ATTAINMENT AS CYCLE_PLAN_TARGET_ATTAINMENT,
		ORIGINAL_PLANNED_CALLS,
		TOTAL_ACTUAL_CALLS,
		TOTAL_ATTAINMENT,
		TOTAL_PLANNED_CALLS,
		CYCLE_PLAN_TARGET_EXTERNAL_ID,
		TOTAL_SCHEDULED_CALLS,
		REMAINING,
		TOTAL_REMAINING,
		TOTAL_REMAINING_SCHEDULE,
		PRIMARY_PARENT_NAME as PRIMARY_PARENT,
		SPECIALTY_1 as ACCOUNTS_SPECIALTY_1,
		ACCOUNT_SOURCE_ID,
		CPT_CFA_100,
		CPT_CFA_66,
		CPT_CFA_33,
		NUMBER_OF_CFA_100_DETAILS,
		NUMBER_OF_PRODUCT_DETAILS,
		'Not Applicable' AS PRODUCT_INDICATION_KEY,
		NULL AS CLASSIFICATION_TYPE,
		NULL AS PLANNED_CALL_DETAIL_COUNT,
		NULL AS SCHEDULED_CALL_DETAIL_COUNT,
		NULL AS ACTUAL_CALL_DETAIL_COUNT,
		NULL AS REMAINING_CALL_DETAIL_COUNT,
		NULL AS CYCLE_PLAN_DETAIL_ATTAINMENT,
		NULL AS TOTAL_ACTUAL_DETAILS,
		NULL AS TOTAL_ATTAINMENT_DETAILS,
		NULL AS TOTAL_PLANNED_DETAILS,
		NULL AS TOTAL_SCHEDULED_DETAILS,
		NULL AS TOTAL_REMAINING_DETAILS,
		NULL AS CFA_100,
		NULL AS CFA_33,
		NULL AS CFA_66,
		CYCLE_PLAN_TYPE,
		CYCLE_PLAN_SOURCE_ID,
		CYCLE_PLAN_TARGET_SOURCE_ID,
		NULL AS CYCLE_PLAN_DETAIL_SOURCE_ID,
		CYCLE_PLAN_TARGET_NAME,
		NULL AS CYCLE_PLAN_DETAIL_NAME,
		CYCLE_PLAN_MODIFY_DT,
		CYCLE_PLAN_MODIFY_ID,
		CYCLE_PLAN_TARGET_MODIFY_DT,
		CYCLE_PLAN_TARGET_MODIFY_ID,
		null as CYCLE_PLAN_DETAIL_MODIFY_DT,
		NULL AS CYCLE_PLAN_DETAIL_MODIFY_ID,
		CASE 
			WHEN (TO_DATE(to_char(END_DATE_KEY), 'YYYYMMDD') - TO_DATE(to_char(START_DATE_KEY), 'YYYYMMDD')) <= 7
				THEN 'W' ---- WEEKLY
			WHEN (TO_DATE(to_char(END_DATE_KEY), 'YYYYMMDD') - TO_DATE(to_char(START_DATE_KEY), 'YYYYMMDD')) > 7
				AND (TO_DATE(to_char(END_DATE_KEY), 'YYYYMMDD') - TO_DATE(to_char(START_DATE_KEY), 'YYYYMMDD')) <= 31
				THEN 'M' -- MONTHLY
			WHEN (TO_DATE(to_char(END_DATE_KEY), 'YYYYMMDD') - TO_DATE(to_char(START_DATE_KEY), 'YYYYMMDD')) > 31
				AND (TO_DATE(to_char(END_DATE_KEY), 'YYYYMMDD') - TO_DATE(to_char(START_DATE_KEY), 'YYYYMMDD')) <= 92
				THEN 'Q' -- QUARTERLY
			WHEN (TO_DATE(to_char(END_DATE_KEY), 'YYYYMMDD') - TO_DATE(to_char(START_DATE_KEY), 'YYYYMMDD')) > 92
				AND (TO_DATE(to_char(END_DATE_KEY), 'YYYYMMDD') - TO_DATE(to_char(START_DATE_KEY), 'YYYYMMDD')) <= 182
				THEN 'H' -- HALF YEARLY
			WHEN (TO_DATE(to_char(END_DATE_KEY), 'YYYYMMDD') - TO_DATE(to_char(START_DATE_KEY), 'YYYYMMDD')) > 182
				THEN 'Y' -- YEARLY 
			ELSE 'Not Applicable'
			END AS CYCLE_PLAN_INDICATOR,
		CLASSIFICATION,
		current_timestamp() AS INSERTED_DATE,
		current_timestamp() AS UPDATED_DATE
	FROM WRK_FACT_CYCLE_PLAN
	),
T2
AS (
	SELECT TARGET.COUNTRY_KEY AS COUNTRY_KEY,
		NVL(TARGET.EMPLOYEE_KEY, 'Not Applicable') AS EMPLOYEE_KEY,
		NVL(TARGET.ORGANIZATION_KEY, 'Not Applicable') AS ORGANIZATION_KEY,
		TARGET.TERRITORY_NAME,
		NVL(TARGET.START_DATE_KEY, 0) AS START_DATE_KEY,
		NVL(TARGET.END_DATE_KEY, 0) AS END_DATE_KEY,
		TARGET.ACTIVE_FLAG,
		TARGET.MANAGER_NAME,
		TARGET.APPROVER_NAME,
		TARGET.CLOSE_OUT_FLAG,
		TARGET.READY_FOR_APPROVAL_FLAG,
		TARGET.STATUS_TYPE,
		TARGET.CYCLE_PLAN_NAME,
		TARGET.CHANNEL_TYPE,
		TARGET.ACTUAL_CALL_CNT_CP,
		TARGET.PLANNED_CALL_CNT_CP,
		NULL AS CYCLE_PLAN_EXTERNAL_ID,
		NULL AS MANAGER,
		NULL AS CP_APPROVAL_TIME,
		NULL AS NUMBER_OF_TARGETS,
		NULL AS NUMBER_OF_CFA_100_TARGETS,
		NULL AS CYCLE_PLAN_ATTAINMENT_CPTARGET,
		NULL AS MID_DATE,
		NULL AS HCP_PRODUCT_ACHIEVED_100,
		NULL AS HCP_PRODUCTS_PLANNED,
		NULL AS CPA_100,
		NVL(TARGET.HCP_KEY, 'Not Applicable') AS HCP_KEY,
		NVL(TARGET.HCO_KEY, 'Not Applicable') AS HCO_KEY,
		NULL AS SCHEDULED_CALL_COUNT,
		NULL AS ACTUAL_CALL_COUNT,
		NULL AS PLANNED_CALL_COUNT,
		NULL AS REMAINING_CALL_COUNT,
		NULL AS CYCLE_PLAN_TARGET_ATTAINMENT,
		NULL AS ORIGINAL_PLANNED_CALLS,
		NULL AS TOTAL_ACTUAL_CALLS,
		NULL AS TOTAL_ATTAINMENT,
		NULL AS TOTAL_PLANNED_CALLS,
		NULL AS CYCLE_PLAN_TARGET_EXTERNAL_ID,
		NULL AS TOTAL_SCHEDULED_CALLS,
		NULL AS REMAINING,
		NULL AS TOTAL_REMAINING,
		NULL AS TOTAL_REMAINING_SCHEDULE,
		NULL AS PRIMARY_PARENT,
		NULL AS ACCOUNTS_SPECIALTY_1,
		NULL AS ACCOUNT_SOURCE_ID,
		NULL AS CPT_CFA_100,
		NULL AS CPT_CFA_66,
		NULL AS CPT_CFA_33,
		NULL AS NUMBER_OF_CFA_100_DETAILS,
		NULL AS NUMBER_OF_PRODUCT_DETAILS,
		NVL(PROD.PRODUCT_INDICATION_KEY, 'Not Applicable') AS PRODUCT_INDICATION_KEY,
		DETAIL.CLASSIFICATION_TYPE AS CLASSIFICATION_TYPE,
		DETAIL.PLANNED_DETAILS AS PLANNED_CALL_DETAIL_COUNT,
		DETAIL.SCHEDULED_DETAILS AS SCHEDULED_CALL_DETAIL_COUNT,
		DETAIL.ACTUAL_DETAILS AS ACTUAL_CALL_DETAIL_COUNT,
		DETAIL.REMAINING AS REMAINING_CALL_DETAIL_COUNT,
		DETAIL.ATTAINMENT AS CYCLE_PLAN_DETAIL_ATTAINMENT,
		DETAIL.TOTAL_ACTUAL_DETAILS AS TOTAL_ACTUAL_DETAILS,
		DETAIL.TOTAL_ATTAINMENT AS TOTAL_ATTAINMENT_DETAILS,
		DETAIL.TOTAL_PLANNED_DETAILS AS TOTAL_PLANNED_DETAILS,
		DETAIL.TOTAL_SCHEDULED_DETAILS AS TOTAL_SCHEDULED_DETAILS,
		DETAIL.TOTAL_REMAINING AS TOTAL_REMAINING_DETAILS,
		DETAIL.CFA_100 AS CFA_100,
		DETAIL.CFA_33 AS CFA_33,
		DETAIL.CFA_66 AS CFA_66,
		'PRODUCT' AS CYCLE_PLAN_TYPE,
		TARGET.CYCLE_PLAN_SOURCE_ID,
		TARGET.CYCLE_PLAN_TARGET_SOURCE_ID,
		DETAIL.CYCLE_PLAN_DETAIL_SOURCE_ID,
		TARGET.CYCLE_PLAN_TARGET_NAME,
		DETAIL.CYCLE_PLAN_DETAIL_NAME,
		TARGET.CYCLE_PLAN_MODIFY_DT,
		TARGET.CYCLE_PLAN_MODIFY_ID,
		TARGET.CYCLE_PLAN_TARGET_MODIFY_DT,
		TARGET.CYCLE_PLAN_TARGET_MODIFY_ID,
		DETAIL.LAST_MODIFIED_DATE AS CYCLE_PLAN_DETAIL_MODIFY_DT,
		DETAIL.LAST_MODIFIED_BY_ID AS CYCLE_PLAN_DETAIL_MODIFY_ID,
		CASE 
			WHEN TO_DATE(END_DATE_KEY::VARCHAR, 'YYYYMMDD') - TO_DATE(START_DATE_KEY::VARCHAR, 'YYYYMMDD') <= 7
				THEN 'W' ---- WEEKLY
			WHEN TO_DATE(END_DATE_KEY::VARCHAR, 'YYYYMMDD') - TO_DATE(START_DATE_KEY::VARCHAR, 'YYYYMMDD') > 7
				AND TO_DATE(END_DATE_KEY::VARCHAR, 'YYYYMMDD') - TO_DATE(START_DATE_KEY::VARCHAR, 'YYYYMMDD') <= 31
				THEN 'M' -- MONTHLY
			WHEN TO_DATE(END_DATE_KEY::VARCHAR, 'YYYYMMDD') - TO_DATE(START_DATE_KEY::VARCHAR, 'YYYYMMDD') > 31
				AND TO_DATE(END_DATE_KEY::VARCHAR, 'YYYYMMDD') - TO_DATE(START_DATE_KEY::VARCHAR, 'YYYYMMDD') <= 92
				THEN 'Q' -- QUARTERLY
			WHEN TO_DATE(END_DATE_KEY::VARCHAR, 'YYYYMMDD') - TO_DATE(START_DATE_KEY::VARCHAR, 'YYYYMMDD') > 92
				AND TO_DATE(END_DATE_KEY::VARCHAR, 'YYYYMMDD') - TO_DATE(START_DATE_KEY::VARCHAR, 'YYYYMMDD') <= 182
				THEN 'H' -- HALF YEARLY
			WHEN TO_DATE(END_DATE_KEY::VARCHAR, 'YYYYMMDD') - TO_DATE(START_DATE_KEY::VARCHAR, 'YYYYMMDD') > 182
				THEN 'Y' -- YEARLY 
			ELSE 'Not Applicable'
			END AS CYCLE_PLAN_INDICATOR,
		NULL AS CLASSIFICATION,
		sysdate() INSERTED_DATE,
		sysdate() UPDATED_DATE
	FROM WRK_FACT_CYCLE_PLAN TARGET,
		itg_CYCLE_PLAN_DETAIL DETAIL,
		DIM_PRODUCT_INDICATION PROD
	WHERE trim(DETAIL.CYCLE_PLAN_TARGET_SOURCE_ID) = trim(TARGET.CYCLE_PLAN_TARGET_SOURCE_ID)
		AND trim(DETAIL.COUNTRY_CODE) = trim(TARGET.COUNTRY_CODE)
		AND trim(DETAIL.IS_DELETED) = '0'
		AND trim(DETAIL.PRODUCT_SOURCE_ID) = trim(PROD.PRODUCT_SOURCE_ID(+))
		AND trim(DETAIL.COUNTRY_CODE) = trim(PROD.COUNTRY_CODE(+))
	),
result
AS (
	SELECT *
	FROM T1
	
	UNION ALL
	
	SELECT *
	FROM t2
	),
FINAL
AS (
	SELECT country_key::VARCHAR(8) AS country_key,
		employee_key::VARCHAR(255) AS employee_key,
		organization_key::VARCHAR(255) AS organization_key,
		territory_name::VARCHAR(255) AS territory_name,
		start_date_key::number(38, 0) AS start_date_key,
		end_date_key::number(38, 0) AS end_date_key,
		active_flag::number(38, 0) AS active_flag,
		manager_name::VARCHAR(255) AS manager_name,
		approver_name::VARCHAR(255) AS approver_name,
		close_out_flag::number(38, 0) AS close_out_flag,
		ready_for_approval_flag::number(38, 0) AS ready_for_approval_flag,
		status_type::VARCHAR(255) AS status_type,
		cycle_plan_name::VARCHAR(255) AS cycle_plan_name,
		channel_type::VARCHAR(255) AS channel_type,
		actual_call_cnt_cp::number(38, 0) AS actual_call_cnt_cp,
		planned_call_cnt_cp::number(38, 0) AS planned_call_cnt_cp,
		cycle_plan_external_id::VARCHAR(255) AS cycle_plan_external_id,
		manager::VARCHAR(255) AS manager,
		cp_approval_time::timestamp_ntz(9) AS cp_approval_time,
		number_of_targets::number(38, 0) AS number_of_targets,
		number_of_cfa_100_targets::number(38, 0) AS number_of_cfa_100_targets,
		cycle_plan_attainment_cptarget::number(38, 1) AS cycle_plan_attainment_cptarget,
		mid_date::DATE AS mid_date,
		hcp_product_achieved_100::number(38, 0) AS hcp_product_achieved_100,
		hcp_products_planned::number(38, 0) AS hcp_products_planned,
		cpa_100::number(38, 1) AS cpa_100,
		hcp_key::VARCHAR(255) AS hcp_key,
		hco_key::VARCHAR(255) AS hco_key,
		scheduled_call_count::number(38, 0) AS scheduled_call_count,
		actual_call_count::number(38, 0) AS actual_call_count,
		planned_call_count::number(38, 0) AS planned_call_count,
		remaining_call_count::number(38, 0) AS remaining_call_count,
		cycle_plan_target_attainment::number(38, 0) AS cycle_plan_target_attainment,
		original_planned_calls::number(38, 0) AS original_planned_calls,
		total_actual_calls::number(38, 0) AS total_actual_calls,
		total_attainment::number(38, 0) AS total_attainment,
		total_planned_calls::number(38, 0) AS total_planned_calls,
		cycle_plan_target_external_id::VARCHAR(255) AS cycle_plan_target_external_id,
		total_scheduled_calls::number(38, 0) AS total_scheduled_calls,
		remaining::number(38, 0) AS remaining,
		total_remaining::number(38, 0) AS total_remaining,
		total_remaining_schedule::number(38, 0) AS total_remaining_schedule,
		primary_parent::VARCHAR(3000) AS primary_parent,
		accounts_specialty_1::VARCHAR(3000) AS accounts_specialty_1,
		account_source_id::VARCHAR(3000) AS account_source_id,
		cpt_cfa_100::number(38, 0) AS cpt_cfa_100,
		cpt_cfa_66::number(38, 0) AS cpt_cfa_66,
		cpt_cfa_33::number(38, 0) AS cpt_cfa_33,
		number_of_cfa_100_details::number(38, 0) AS number_of_cfa_100_details,
		number_of_product_details::number(38, 0) AS number_of_product_details,
		product_indication_key::VARCHAR(255) AS product_indication_key,
		classification_type::VARCHAR(255) AS classification_type,
		planned_call_detail_count::number(38, 0) AS planned_call_detail_count,
		scheduled_call_detail_count::number(38, 0) AS scheduled_call_detail_count,
		actual_call_detail_count::number(38, 0) AS actual_call_detail_count,
		remaining_call_detail_count::number(38, 0) AS remaining_call_detail_count,
		cycle_plan_detail_attainment::number(38, 0) AS cycle_plan_detail_attainment,
		total_actual_details::number(38, 0) AS total_actual_details,
		total_attainment_details::number(38, 0) AS total_attainment_details,
		total_planned_details::number(38, 0) AS total_planned_details,
		total_scheduled_details::number(38, 0) AS total_scheduled_details,
		total_remaining_details::number(38, 0) AS total_remaining_details,
		cfa_100::number(38, 0) AS cfa_100,
		cfa_33::number(38, 0) AS cfa_33,
		cfa_66::number(38, 0) AS cfa_66,
		cycle_plan_type::VARCHAR(255) AS cycle_plan_type,
		cycle_plan_source_id::VARCHAR(255) AS cycle_plan_source_id,
		cycle_plan_target_source_id::VARCHAR(255) AS cycle_plan_target_source_id,
		cycle_plan_detail_source_id::VARCHAR(255) AS cycle_plan_detail_source_id,
		cycle_plan_target_name::VARCHAR(255) AS cycle_plan_target_name,
		cycle_plan_detail_name::VARCHAR(255) AS cycle_plan_detail_name,
		cycle_plan_modify_dt::timestamp_ntz(9) AS cycle_plan_modify_dt,
		cycle_plan_modify_id::VARCHAR(255) AS cycle_plan_modify_id,
		cycle_plan_target_modify_dt::timestamp_ntz(9) AS cycle_plan_target_modify_dt,
		cycle_plan_target_modify_id::VARCHAR(255) AS cycle_plan_target_modify_id,
		cycle_plan_detail_modify_dt::timestamp_ntz(9) AS cycle_plan_detail_modify_dt,
		cycle_plan_detail_modify_id::VARCHAR(255) AS cycle_plan_detail_modify_id,
		cycle_plan_indicator::VARCHAR(255) AS cycle_plan_indicator,
		classification::VARCHAR(1300) AS classification,
		inserted_date::timestamp_ntz(9) AS inserted_date,
		updated_date::timestamp_ntz(9) AS updated_date
	FROM result
	)
SELECT *
FROM FINAL
