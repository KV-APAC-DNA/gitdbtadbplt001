WITH fact_cycle_plan
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__fact_cycle_plan') }}
    ),
edw_isight_dim_employee_snapshot_xref
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__edw_isight_dim_employee_snapshot_xref') }}
    ),
edw_isight_sector_mapping
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__edw_isight_sector_mapping') }}
    ),
edw_isight_sector_mapping
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__edw_isight_sector_mapping') }}
    ),
dim_employee
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__dim_employee') }}
    ),
edw_isight_sector_mapping
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__edw_isight_sector_mapping') }}
    ),
dim_hcp AS
(
    SELECT * FROM {{ ref('hcposeedw_integration__dim_hcp') }}
),
vw_employee_hier AS
(
    SELECT * FROM {{ ref('hcposeedw_integration__vw_employee_hier') }}
),
dim_hco AS
(
    SELECT * FROM {{ ref('hcposeedw_integration__dim_hco') }}
),
itg_cycle_plan AS
(
    SELECT * FROM {{ ref('hcposeitg_integration__itg_cycle_plan') }}
),
dim_product_indication AS
(
    SELECT * FROM {{ ref('hcposeedw_integration__dim_product_indication') }}
),
dim_employee AS
(
    SELECT * FROM {{ ref('hcposeedw_integration__dim_employee') }}
),
fact
AS (
    SELECT fact_cycle_plan.country_key,
        fact_cycle_plan.employee_key,
        fact_cycle_plan.organization_key,
        fact_cycle_plan.territory_name,
        fact_cycle_plan.start_date_key,
        fact_cycle_plan.end_date_key,
        fact_cycle_plan.active_flag,
        fact_cycle_plan.manager_name,
        fact_cycle_plan.approver_name,
        fact_cycle_plan.close_out_flag,
        fact_cycle_plan.ready_for_approval_flag,
        fact_cycle_plan.status_type,
        fact_cycle_plan.cycle_plan_name,
        fact_cycle_plan.channel_type,
        fact_cycle_plan.actual_call_cnt_cp,
        fact_cycle_plan.planned_call_cnt_cp,
        fact_cycle_plan.cycle_plan_external_id,
        fact_cycle_plan.manager,
        fact_cycle_plan.cp_approval_time,
        fact_cycle_plan.number_of_targets,
        fact_cycle_plan.number_of_cfa_100_targets,
        fact_cycle_plan.cycle_plan_attainment_cptarget,
        -- (((fact_cycle_plan.start_date_key)::CHARACTER VARYING(10))::DATE + ((((fact_cycle_plan.end_date_key)::CHARACTER -- VARYING(10))::DATE - ((fact_cycle_plan.start_date_key)::CHARACTER VARYING(10))::DATE) / 2)) AS mid_date,
        dateadd(day, floor(((fact_cycle_plan.end_date_key) - (fact_cycle_plan.start_date_key))/2)::NUMBER,TO_DATE(to_char(start_date_key), 'YYYYMMDD')) AS mid_date,
        fact_cycle_plan.hcp_product_achieved_100,
        fact_cycle_plan.hcp_products_planned,
        fact_cycle_plan.cpa_100,
        fact_cycle_plan.hcp_key,
        fact_cycle_plan.hco_key,
        fact_cycle_plan.scheduled_call_count,
        fact_cycle_plan.actual_call_count,
        fact_cycle_plan.planned_call_count,
        fact_cycle_plan.remaining_call_count,
        fact_cycle_plan.cycle_plan_target_attainment,
        fact_cycle_plan.original_planned_calls,
        fact_cycle_plan.total_actual_calls,
        fact_cycle_plan.total_attainment,
        fact_cycle_plan.total_planned_calls,
        fact_cycle_plan.cycle_plan_target_external_id,
        fact_cycle_plan.total_scheduled_calls,
        fact_cycle_plan.remaining,
        fact_cycle_plan.total_remaining,
        fact_cycle_plan.total_remaining_schedule,
        fact_cycle_plan.primary_parent,
        fact_cycle_plan.accounts_specialty_1,
        fact_cycle_plan.account_source_id,
        fact_cycle_plan.cpt_cfa_100,
        fact_cycle_plan.cpt_cfa_66,
        fact_cycle_plan.cpt_cfa_33,
        fact_cycle_plan.number_of_cfa_100_details,
        fact_cycle_plan.number_of_product_details,
        fact_cycle_plan.product_indication_key,
        fact_cycle_plan.classification_type,
        fact_cycle_plan.planned_call_detail_count,
        fact_cycle_plan.scheduled_call_detail_count,
        fact_cycle_plan.actual_call_detail_count,
        fact_cycle_plan.remaining_call_detail_count,
        fact_cycle_plan.cycle_plan_detail_attainment,
        fact_cycle_plan.total_actual_details,
        fact_cycle_plan.total_attainment_details,
        fact_cycle_plan.total_planned_details,
        fact_cycle_plan.total_scheduled_details,
        fact_cycle_plan.total_remaining_details,
        fact_cycle_plan.cfa_100,
        fact_cycle_plan.cfa_33,
        fact_cycle_plan.cfa_66,
        fact_cycle_plan.cycle_plan_type,
        fact_cycle_plan.cycle_plan_source_id,
        fact_cycle_plan.cycle_plan_target_source_id,
        fact_cycle_plan.cycle_plan_detail_source_id,
        fact_cycle_plan.cycle_plan_target_name,
        fact_cycle_plan.cycle_plan_detail_name,
        fact_cycle_plan.cycle_plan_modify_dt,
        fact_cycle_plan.cycle_plan_modify_id,
        fact_cycle_plan.cycle_plan_target_modify_dt,
        fact_cycle_plan.cycle_plan_target_modify_id,
        fact_cycle_plan.cycle_plan_detail_modify_dt,
        fact_cycle_plan.cycle_plan_detail_modify_id,
        fact_cycle_plan.cycle_plan_indicator,
        fact_cycle_plan.inserted_date,
        fact_cycle_plan.updated_date,
        fact_cycle_plan.classification
    FROM fact_cycle_plan
    WHERE ((fact_cycle_plan.cycle_plan_type)::TEXT = ('TARGET'::CHARACTER VARYING)::TEXT)
    ),
actve_usr
AS (
    SELECT stg_isight_active_user_snapshot.year,
        sect.sector,
        stg_isight_active_user_snapshot.month,
        stg_isight_active_user_snapshot.country_code,
        count(stg_isight_active_user_snapshot.employee_source_id) AS total_active
    FROM (
        edw_isight_dim_employee_snapshot_xref stg_isight_active_user_snapshot LEFT JOIN edw_isight_sector_mapping sect ON (
                (
                    ((sect.company)::TEXT = (stg_isight_active_user_snapshot.company_name)::TEXT)
                    AND ((sect.country)::TEXT = (stg_isight_active_user_snapshot.country_code)::TEXT)
                    )
                )
        )
    WHERE (
            (stg_isight_active_user_snapshot.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
            AND (
                ((stg_isight_active_user_snapshot.profile_name)::TEXT = ('AP_Core_Sales'::CHARACTER VARYING)::TEXT)
                OR ((stg_isight_active_user_snapshot.profile_name)::TEXT = ('AP_Core_MY_Sales'::CHARACTER VARYING)::TEXT)
                )
            )
    GROUP BY stg_isight_active_user_snapshot.year,
        stg_isight_active_user_snapshot.month,
        stg_isight_active_user_snapshot.country_code,
        sect.sector
    ),
emp1
AS (
    SELECT count(1) AS current_mnth_emp_cnt,
        dim_employee.country_code,
        sect.sector
    FROM (
        dim_employee LEFT JOIN edw_isight_sector_mapping sect ON (
                (
                    ((sect.company)::TEXT = (dim_employee.company_name)::TEXT)
                    AND ((sect.country)::TEXT = (dim_employee.country_code)::TEXT)
                    )
                )
        )
    WHERE (
            (dim_employee.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
            AND (
                ((dim_employee.profile_name)::TEXT = ('AP_Core_Sales'::CHARACTER VARYING)::TEXT)
                OR ((dim_employee.profile_name)::TEXT = ('AP_Core_MY_Sales'::CHARACTER VARYING)::TEXT)
                )
            )
    GROUP BY dim_employee.country_code,
        sect.sector
    ),
derived_table1
AS (
    SELECT fact.mid_date AS DATE,
        date_part (
            year,
            fact.mid_date
            ) AS year,
        date_part (
            month,
            fact.mid_date
            ) AS month,
        date_part (
            quarter,
            fact.mid_date
            ) AS quarter,
        fact.country_key AS country,
        dim_hcp.hcp_name,
        dim_hcp.hcp_key,
        dim_hcp.hcp_source_id,
        fact.accounts_specialty_1 AS specialty,
        hier.sector,
        dim_hco.hco_name AS business_account,
        fact.planned_call_count AS PLAN,
        fact.cycle_plan_target_attainment AS attainment,
        fact.actual_call_count AS actual,
        itg.cpa_100,
        fact.number_of_targets AS hcp_count,
        CASE 
            WHEN (
                    ((fact.status_type)::TEXT <> ('Approved'::CHARACTER VARYING)::TEXT)
                    AND (fact.ready_for_approval_flag = 1)
                    )
                THEN 'Ready for Approval'::CHARACTER VARYING
            WHEN ((fact.status_type)::TEXT = ('Approved'::CHARACTER VARYING)::TEXT)
                THEN 'Approved'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS cpa_status,
        hier.l2_wwid AS l3_wwid,
        hier.l2_username AS l3_username,
        hier.l2_manager_name AS l3_manager_name,
        hier.l3_wwid AS l2_wwid,
        hier.l3_username AS l2_username,
        hier.l3_manager_name AS l2_manager_name,
        hier.l4_wwid AS l1_wwid,
        hier.l4_username AS l1_username,
        hier.l4_manager_name AS l1_manager_name,
        hier.l1_username AS sales_rep_ntid,
        CASE 
            WHEN (hier.employee_name IS NULL)
                THEN dimemp.employee_name
            ELSE hier.employee_name
            END AS sales_rep,
        fact.classification_type,
        fact.planned_call_detail_count AS plan_product,
        fact.actual_call_detail_count AS actual_product,
        fact.cycle_plan_detail_attainment AS attainment_product,
        dpi.product_name AS product,
        dimemp.organization_l1_name,
        dimemp.organization_l2_name,
        dimemp.organization_l3_name,
        dimemp.organization_l4_name,
        dimemp.organization_l5_name,
        CASE 
            WHEN ((date_part (year,fact.mid_date)||date_part (month,fact.mid_date)) 
                = (date_part (year,sysdate()) || date_part (month,sysdate())))
                    
                THEN emp1.current_mnth_emp_cnt
            ELSE actve_usr.total_active
            END AS total_active,
        dim_hco.customer_code,
        fact.classification,
        dim_hco.hco_source_id
    FROM fact
    JOIN dim_hcp dim_hcp ON (((fact.hcp_key)::TEXT = (dim_hcp.hcp_key)::TEXT))
    JOIN vw_employee_hier hier ON (
            (
                ((fact.employee_key)::TEXT = (hier.employee_key)::TEXT)
                AND ((fact.country_key)::TEXT = (hier.country_code)::TEXT)
                )
            )
    LEFT JOIN dim_hco dim_hco ON (((fact.hco_key)::TEXT = (dim_hco.hco_key)::TEXT))
    LEFT JOIN itg_cycle_plan itg ON (((fact.cycle_plan_source_id)::TEXT = (itg.cycle_plan_source_id)::TEXT))
    LEFT JOIN dim_product_indication dpi ON (((fact.product_indication_key)::TEXT = (dpi.product_indication_key)::TEXT))
    LEFT JOIN dim_employee dimemp ON (
            (
                ((fact.employee_key)::TEXT = (dimemp.employee_key)::TEXT)
                AND ((fact.country_key)::TEXT = (dimemp.country_code)::TEXT)
                )
            )
    LEFT JOIN actve_usr ON (
            (
                (
                    (
                        (
                            date_part (
                                (year),
                                fact.mid_date
                                ) = actve_usr.year
                            )
                        AND (
                            (
                                (
                                    date_part (
                                        month,
                                        fact.mid_date
                                        )
                                    )::CHARACTER VARYING
                                )::TEXT = ((actve_usr.month)::CHARACTER VARYING)::TEXT
                            )
                        )
                    AND ((fact.country_key)::TEXT = (actve_usr.country_code)::TEXT)
                    )
                AND ((COALESCE(hier.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(actve_usr.sector, '#'::CHARACTER VARYING))::TEXT)
                )
            )
    LEFT JOIN emp1 ON (
            (
                ((fact.country_key)::TEXT = (emp1.country_code)::TEXT)
                AND ((COALESCE(hier.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(emp1.sector, '#'::CHARACTER VARYING))::TEXT)
                )
            )
    ),
final
AS (
    SELECT derived_table1.DATE,
        derived_table1.year,
        derived_table1.month,
        derived_table1.quarter,
        derived_table1.country,
        derived_table1.hcp_name,
        derived_table1.hcp_key,
        derived_table1.hcp_source_id,
        derived_table1.specialty,
        derived_table1.sector,
        derived_table1.business_account,
        derived_table1.PLAN,
        derived_table1.attainment,
        derived_table1.actual,
        derived_table1.cpa_100,
        derived_table1.hcp_count,
        derived_table1.cpa_status,
        derived_table1.l3_wwid,
        derived_table1.l3_username,
        derived_table1.l3_manager_name,
        derived_table1.l2_wwid,
        derived_table1.l2_username,
        derived_table1.l2_manager_name,
        derived_table1.l1_wwid,
        derived_table1.l1_username,
        derived_table1.l1_manager_name,
        derived_table1.sales_rep_ntid,
        derived_table1.sales_rep,
        derived_table1.classification_type,
        derived_table1.plan_product,
        derived_table1.actual_product,
        derived_table1.attainment_product,
        derived_table1.product,
        derived_table1.organization_l1_name,
        derived_table1.organization_l2_name,
        derived_table1.organization_l3_name,
        derived_table1.organization_l4_name,
        derived_table1.organization_l5_name,
        derived_table1.total_active,
        derived_table1.customer_code AS hcp_customer_code_2,
        derived_table1.classification AS account_segmentation,
        derived_table1.hco_source_id AS business_account_id
    FROM derived_table1
    WHERE (
            (derived_table1.cpa_status IS NOT NULL)
            AND ((derived_table1.country)::TEXT <> ('ZZ'::CHARACTER VARYING)::TEXT)
            )
    )
SELECT *
FROM final
