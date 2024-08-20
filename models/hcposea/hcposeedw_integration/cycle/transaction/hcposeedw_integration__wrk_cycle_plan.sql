WITH 
fact_cycle_plan AS
(
    SELECT * FROM hcposeedw_integration.fact_cycle_plan
),
dim_date AS
(
    SELECT * FROM hcposeedw_integration.dim_date
),
dim_hcp AS
(
    SELECT * FROM hcposeedw_integration.dim_hcp
),
vw_employee_hier AS
(
    SELECT * FROM hcposeedw_integration.vw_employee_hier
),
dim_hco AS
(
    SELECT * FROM hcposeedw_integration.dim_hco
),
DIM_EMPLOYEE_ICONNECT AS
(
    SELECT * FROM hcposeedw_integration.DIM_EMPLOYEE_ICONNECT
),
edw_isight_dim_employee_snapshot_xref AS
(
    SELECT * FROM hcposeedw_integration.edw_isight_dim_employee_snapshot_xref
),
edw_isight_sector_mapping AS
(
    SELECT * FROM hcposeedw_integration.edw_isight_sector_mapping
),
DIM_PRODUCT_INDICATION AS
(
    SELECT * FROM hcposeedw_integration.DIM_PRODUCT_INDICATION
),
T1
AS (
    SELECT NULL AS jnj_date_year,
        NULL AS jnj_date_month,
        NULL AS jnj_date_quarter,
        src1.date_year::CHARACTER VARYING AS date_year,
        src1.date_month::CHARACTER VARYING AS date_month,
        src1.date_quarter::CHARACTER VARYING AS date_quarter,
        NULL AS my_date_year,
        NULL AS my_date_month,
        NULL AS my_date_quarter,
        src1.country,
        src1.sector,
        src1.l3_wwid,
        src1.l3_username,
        src1.l3_manager_name,
        src1.l2_wwid,
        src1.l2_username,
        src1.l2_manager_name,
        src1.l1_wwid,
        src1.l1_username,
        src1.l1_manager_name,
        src1.sales_rep_ntid,
        src1.sales_rep,
        src1.email_id,
        src1.organization_l1_name,
        src1.organization_l2_name,
        src1.organization_l3_name,
        src1.organization_l4_name,
        src1.organization_l5_name,
        src1.total_active,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.speciality_1_type_english AS hcp_speciality,
        src1.customer_code AS hco_customer_code_2,
        src1.specialty AS cycle_speciality,
        src1.hco_source_id AS business_account_id,
        src1.business_account,
        src1.classification AS account_segmentation,
        src1.cpa_status,
        'TARGET' AS cycle_plan_type,
        SUM(src1.PLAN) AS planned_calls,
        SUM(src1.attainment) AS attainment,
        SUM(src1.actual) AS actual_calls,
        SUM(src1.cpa_100) AS cpa_100,
        NULL AS product,
        NULL AS planned_call_detail_count,
        NULL AS cycle_plan_detail_attainment,
        NULL AS actual_call_detail_count,
        NULL AS cfa_100,
        NULL AS cfa_33,
        NULL AS cfa_66
    FROM (
        SELECT dimdate.date_year AS date_year,
            dimdate.date_month AS date_month,
            dimdate.date_quarter AS date_quarter,
            fact.country_key AS country,
            dim_hcp.hcp_name,
            dim_hcp.hcp_source_id,
            dim_hcp.speciality_1_type_english,
            fact.accounts_specialty_1 AS specialty,
            hier.sector,
            dim_hco.hco_name AS business_account,
            fact.planned_call_count AS PLAN,
            fact.cycle_plan_target_attainment AS attainment,
            fact.actual_call_count AS actual,
            fact.CYCLE_PLAN_ATTAINMENT_CPTARGET AS cpa_100,
            fact.number_of_targets AS hcp_count,
            CASE 
                WHEN fact.status_type::TEXT <> 'Approved'::CHARACTER VARYING::TEXT
                    AND fact.ready_for_approval_flag = 1
                    THEN 'Ready for Approval'::CHARACTER VARYING
                WHEN fact.status_type::TEXT = 'Approved'::CHARACTER VARYING::TEXT
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
                WHEN hier.employee_name IS NULL
                    THEN dimemp.employee_name
                ELSE hier.employee_name
                END AS sales_rep,
            dimemp.email_id,
            dimemp.organization_l1_name,
            dimemp.organization_l2_name,
            dimemp.organization_l3_name,
            dimemp.organization_l4_name,
            dimemp.organization_l5_name,
            CASE 
                WHEN (
                        date_part (
                           year,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT || date_part (
                            month,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT
                        ) = (
                        date_part (
                           year,
                            sysdate()
                            )::CHARACTER VARYING::TEXT || date_part (
                            month,
                            sysdate()
                            )::CHARACTER VARYING::TEXT
                        )
                    THEN emp1.current_mnth_emp_cnt
                ELSE actve_usr.total_active
                END AS total_active,
            dim_hco.customer_code,
            fact.classification,
            dim_hco.hco_source_id
        FROM (
            SELECT country_key,
                employee_key,
                organization_key,
                territory_name,
                start_date_key,
                end_date_key,
                active_flag,
                close_out_flag,
                ready_for_approval_flag,
                status_type,
                cycle_plan_name,
                channel_type,
                cycle_plan_external_id,
                manager,
                number_of_targets,
                number_of_cfa_100_targets,
                cycle_plan_attainment_cptarget,
                start_date_key::CHARACTER VARYING(10)::DATE + (end_date_key::CHARACTER VARYING(10)::DATE - start_date_key::CHARACTER VARYING(10)::DATE) / 2 AS mid_date,
                hcp_product_achieved_100,
                hcp_products_planned,
                hcp_key,
                hco_key,
                scheduled_call_count,
                actual_call_count,
                planned_call_count,
                cycle_plan_target_attainment,
                accounts_specialty_1,
                account_source_id,
                number_of_cfa_100_details,
                product_indication_key,
                classification_type,
                cycle_plan_type,
                cycle_plan_indicator,
                classification
            FROM fact_cycle_plan
            WHERE cycle_plan_type::TEXT = 'TARGET'::CHARACTER VARYING::TEXT
            ) fact
        JOIN dim_date dimdate ON REPLACE(fact.mid_date::TEXT, '-', '')::NUMERIC::NUMERIC(18, 0) = dimdate.date_key::NUMERIC::NUMERIC(18, 0)
            AND DATE_YEAR BETWEEN (DATE_PART(year,sysdate()) - 2)
                AND DATE_PART(year,sysdate())
        JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
        JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
            AND fact.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
            AND fact.country_key::TEXT = dimemp.country_code::TEXT
        LEFT JOIN (
            SELECT stg_isight_active_user_snapshot.year,
                sect.sector,
                CASE 
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 1::TEXT
                        THEN 'Jan'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 2::TEXT
                        THEN 'Feb'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 3::TEXT
                        THEN 'Mar'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 4::TEXT
                        THEN 'Apr'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 5::TEXT
                        THEN 'May'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 6::TEXT
                        THEN 'Jun'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 7::TEXT
                        THEN 'Jul'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 8::TEXT
                        THEN 'Aug'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 9::TEXT
                        THEN 'Sep'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 10::TEXT
                        THEN 'Oct'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 11::TEXT
                        THEN 'Nov'::TEXT
                    ELSE 'Dec'::TEXT
                    END AS month,
                stg_isight_active_user_snapshot.country_code,
                COUNT(stg_isight_active_user_snapshot.employee_source_id) AS total_active
            FROM edw_isight_dim_employee_snapshot_xref stg_isight_active_user_snapshot
            LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = stg_isight_active_user_snapshot.company_name::TEXT
                AND sect.country::TEXT = stg_isight_active_user_snapshot.country_code::TEXT
            WHERE stg_isight_active_user_snapshot.active_flag = 1::NUMERIC::NUMERIC(18, 0)
                AND (
                    stg_isight_active_user_snapshot.profile_name::TEXT = 'AP_Core_Sales'::TEXT
                    OR stg_isight_active_user_snapshot.profile_name::TEXT = 'AP_Core_MY_Sales'::TEXT
                    )
            GROUP BY stg_isight_active_user_snapshot.year,
                stg_isight_active_user_snapshot.month,
                stg_isight_active_user_snapshot.country_code,
                sect.sector
            ) actve_usr ON dimdate.date_year = actve_usr.year
            AND dimdate.date_month = actve_usr.month::CHARACTER VARYING::TEXT
            AND fact.country_key::TEXT = actve_usr.country_code::TEXT
            AND COALESCE(hier.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(actve_usr.sector, '#'::CHARACTER VARYING)::TEXT
        LEFT JOIN (
            SELECT COUNT(1) AS current_mnth_emp_cnt,
                DIM_EMPLOYEE_ICONNECT.country_code,
                sect.sector
            FROM DIM_EMPLOYEE_ICONNECT
            LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = DIM_EMPLOYEE_ICONNECT.company_name::TEXT
                AND sect.country::TEXT = DIM_EMPLOYEE_ICONNECT.country_code::TEXT
            WHERE DIM_EMPLOYEE_ICONNECT.active_flag = 1::NUMERIC::NUMERIC(18, 0)
                AND (
                    DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_Sales'::CHARACTER VARYING::TEXT
                    OR DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_MY_Sales'::CHARACTER VARYING::TEXT
                    )
            GROUP BY DIM_EMPLOYEE_ICONNECT.country_code,
                sect.sector
            ) emp1 ON fact.country_key::TEXT = emp1.country_code::TEXT
            AND COALESCE(hier.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(emp1.sector, '#'::CHARACTER VARYING)::TEXT
        ) src1
    WHERE src1.cpa_status IS NOT NULL
        AND src1.country::TEXT <> 'ZZ'::CHARACTER VARYING::TEXT
    GROUP BY src1.date_year,
        src1.date_month,
        src1.date_quarter,
        src1.country,
        src1.sector,
        src1.l3_wwid,
        src1.l3_username,
        src1.l3_manager_name,
        src1.l2_wwid,
        src1.l2_username,
        src1.l2_manager_name,
        src1.l1_wwid,
        src1.l1_username,
        src1.l1_manager_name,
        src1.sales_rep_ntid,
        src1.sales_rep,
        src1.email_id,
        src1.organization_l1_name,
        src1.organization_l2_name,
        src1.organization_l3_name,
        src1.organization_l4_name,
        src1.organization_l5_name,
        src1.total_active,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.speciality_1_type_english,
        src1.customer_code,
        src1.specialty,
        src1.hco_source_id,
        src1.business_account,
        src1.classification,
        src1.cpa_status
    ),
T2
AS (
    SELECT src1.jnj_date_year::CHARACTER VARYING AS jnj_date_year,
        src1.jnj_date_month::CHARACTER VARYING AS jnj_date_month,
        src1.jnj_date_quarter::CHARACTER VARYING AS jnj_date_quarter,
        NULL AS date_year,
        NULL AS date_month,
        NULL AS date_quarter,
        NULL AS my_date_year,
        NULL AS my_date_month,
        NULL AS my_date_quarter,
        src1.country,
        src1.sector,
        src1.l3_wwid,
        src1.l3_username,
        src1.l3_manager_name,
        src1.l2_wwid,
        src1.l2_username,
        src1.l2_manager_name,
        src1.l1_wwid,
        src1.l1_username,
        src1.l1_manager_name,
        src1.sales_rep_ntid,
        src1.sales_rep,
        src1.email_id,
        src1.organization_l1_name,
        src1.organization_l2_name,
        src1.organization_l3_name,
        src1.organization_l4_name,
        src1.organization_l5_name,
        src1.total_active,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.speciality_1_type_english AS hcp_speciality,
        src1.customer_code AS hco_customer_code_2,
        src1.specialty AS cycle_speciality,
        src1.hco_source_id AS business_account_id,
        src1.business_account,
        src1.classification AS account_segmentation,
        src1.cpa_status,
        'TARGET' AS cycle_plan_type,
        SUM(src1.PLAN) AS planned_calls,
        SUM(src1.attainment) AS attainment,
        SUM(src1.actual) AS actual_calls,
        SUM(src1.cpa_100) AS cpa_100,
        NULL AS product,
        NULL AS planned_call_detail_count,
        NULL AS cycle_plan_detail_attainment,
        NULL AS actual_call_detail_count,
        NULL AS cfa_100,
        NULL AS cfa_33,
        NULL AS cfa_66
    FROM (
        SELECT dimdate.jnj_date_year,
            dimdate.jnj_date_month,
            dimdate.jnj_date_quarter,
            fact.country_key AS country,
            dim_hcp.hcp_name,
            dim_hcp.hcp_source_id,
            dim_hcp.speciality_1_type_english,
            fact.accounts_specialty_1 AS specialty,
            hier.sector,
            dim_hco.hco_name AS business_account,
            fact.planned_call_count AS PLAN,
            fact.cycle_plan_target_attainment AS attainment,
            fact.actual_call_count AS actual,
            fact.CYCLE_PLAN_ATTAINMENT_CPTARGET AS cpa_100,
            fact.number_of_targets AS hcp_count,
            CASE 
                WHEN fact.status_type::TEXT <> 'Approved'::CHARACTER VARYING::TEXT
                    AND fact.ready_for_approval_flag = 1
                    THEN 'Ready for Approval'::CHARACTER VARYING
                WHEN fact.status_type::TEXT = 'Approved'::CHARACTER VARYING::TEXT
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
                WHEN hier.employee_name IS NULL
                    THEN dimemp.employee_name
                ELSE hier.employee_name
                END AS sales_rep,
            dimemp.email_id,
            dimemp.organization_l1_name,
            dimemp.organization_l2_name,
            dimemp.organization_l3_name,
            dimemp.organization_l4_name,
            dimemp.organization_l5_name,
            CASE 
                WHEN (
                        date_part (
                           year,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT || date_part (
                            month,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT
                        ) = (
                        date_part (
                           year,
                            sysdate()
                            )::CHARACTER VARYING::TEXT || date_part (
                            month,
                            sysdate()
                            )::CHARACTER VARYING::TEXT
                        )
                    THEN emp1.current_mnth_emp_cnt
                ELSE actve_usr.total_active
                END AS total_active,
            dim_hco.customer_code,
            fact.classification,
            dim_hco.hco_source_id
        FROM (
            SELECT country_key,
                employee_key,
                organization_key,
                territory_name,
                start_date_key,
                end_date_key,
                active_flag,
                close_out_flag,
                ready_for_approval_flag,
                status_type,
                cycle_plan_name,
                channel_type,
                cycle_plan_external_id,
                manager,
                number_of_targets,
                number_of_cfa_100_targets,
                cycle_plan_attainment_cptarget,
                start_date_key::CHARACTER VARYING(10)::DATE + (end_date_key::CHARACTER VARYING(10)::DATE - start_date_key::CHARACTER VARYING(10)::DATE) / 2 AS mid_date,
                hcp_product_achieved_100,
                hcp_products_planned,
                hcp_key,
                hco_key,
                scheduled_call_count,
                actual_call_count,
                planned_call_count,
                cycle_plan_target_attainment,
                accounts_specialty_1,
                account_source_id,
                number_of_cfa_100_details,
                product_indication_key,
                classification_type,
                cycle_plan_type,
                cycle_plan_indicator,
                classification
            FROM fact_cycle_plan
            WHERE cycle_plan_type::TEXT = 'TARGET'::CHARACTER VARYING::TEXT
            ) fact
        JOIN dim_date dimdate ON REPLACE(fact.mid_date::TEXT, '-', '')::NUMERIC::NUMERIC(18, 0) = dimdate.date_key::NUMERIC::NUMERIC(18, 0)
            AND jnj_date_year BETWEEN (DATE_PART(year,sysdate()) - 2)
                AND DATE_PART(year,sysdate())
        JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
        JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
            AND fact.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
            AND fact.country_key::TEXT = dimemp.country_code::TEXT
        LEFT JOIN (
            SELECT stg_isight_active_user_snapshot.year,
                sect.sector,
                CASE 
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 1::TEXT
                        THEN 'Jan'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 2::TEXT
                        THEN 'Feb'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 3::TEXT
                        THEN 'Mar'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 4::TEXT
                        THEN 'Apr'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 5::TEXT
                        THEN 'May'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 6::TEXT
                        THEN 'Jun'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 7::TEXT
                        THEN 'Jul'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 8::TEXT
                        THEN 'Aug'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 9::TEXT
                        THEN 'Sep'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 10::TEXT
                        THEN 'Oct'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 11::TEXT
                        THEN 'Nov'::TEXT
                    ELSE 'Dec'::TEXT
                    END AS month,
                stg_isight_active_user_snapshot.country_code,
                COUNT(stg_isight_active_user_snapshot.employee_source_id) AS total_active
            FROM edw_isight_dim_employee_snapshot_xref stg_isight_active_user_snapshot
            LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = stg_isight_active_user_snapshot.company_name::TEXT
                AND sect.country::TEXT = stg_isight_active_user_snapshot.country_code::TEXT
            WHERE stg_isight_active_user_snapshot.active_flag = 1::NUMERIC::NUMERIC(18, 0)
                AND (
                    stg_isight_active_user_snapshot.profile_name::TEXT = 'AP_Core_Sales'::TEXT
                    OR stg_isight_active_user_snapshot.profile_name::TEXT = 'AP_Core_MY_Sales'::TEXT
                    )
            GROUP BY stg_isight_active_user_snapshot.year,
                stg_isight_active_user_snapshot.month,
                stg_isight_active_user_snapshot.country_code,
                sect.sector
            ) actve_usr ON dimdate.jnj_date_year = actve_usr.year
            AND dimdate.jnj_date_month = actve_usr.month::CHARACTER VARYING::TEXT
            AND fact.country_key::TEXT = actve_usr.country_code::TEXT
            AND COALESCE(hier.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(actve_usr.sector, '#'::CHARACTER VARYING)::TEXT
        LEFT JOIN (
            SELECT COUNT(1) AS current_mnth_emp_cnt,
                DIM_EMPLOYEE_ICONNECT.country_code,
                sect.sector
            FROM DIM_EMPLOYEE_ICONNECT
            LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = DIM_EMPLOYEE_ICONNECT.company_name::TEXT
                AND sect.country::TEXT = DIM_EMPLOYEE_ICONNECT.country_code::TEXT
            WHERE DIM_EMPLOYEE_ICONNECT.active_flag = 1::NUMERIC::NUMERIC(18, 0)
                AND (
                    DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_Sales'::CHARACTER VARYING::TEXT
                    OR DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_MY_Sales'::CHARACTER VARYING::TEXT
                    )
            GROUP BY DIM_EMPLOYEE_ICONNECT.country_code,
                sect.sector
            ) emp1 ON fact.country_key::TEXT = emp1.country_code::TEXT
            AND COALESCE(hier.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(emp1.sector, '#'::CHARACTER VARYING)::TEXT
        ) src1
    WHERE src1.cpa_status IS NOT NULL
        AND src1.country::TEXT <> 'ZZ'::CHARACTER VARYING::TEXT
    GROUP BY src1.jnj_date_year,
        src1.jnj_date_month,
        src1.jnj_date_quarter,
        src1.country,
        src1.sector,
        src1.l3_wwid,
        src1.l3_username,
        src1.l3_manager_name,
        src1.l2_wwid,
        src1.l2_username,
        src1.l2_manager_name,
        src1.l1_wwid,
        src1.l1_username,
        src1.l1_manager_name,
        src1.sales_rep_ntid,
        src1.sales_rep,
        src1.email_id,
        src1.organization_l1_name,
        src1.organization_l2_name,
        src1.organization_l3_name,
        src1.organization_l4_name,
        src1.organization_l5_name,
        src1.total_active,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.speciality_1_type_english,
        src1.customer_code,
        src1.specialty,
        src1.hco_source_id,
        src1.business_account,
        src1.classification,
        src1.cpa_status
    ),
T3
AS (
    SELECT NULL AS jnj_date_year,
        NULL AS jnj_date_month,
        NULL AS jnj_date_quarter,
        NULL AS date_year,
        NULL AS date_month,
        NULL AS date_quarter,
        src1.my_date_year::CHARACTER VARYING AS my_date_year,
        TRIM(src1.my_date_month::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS my_date_month,
        src1.my_date_quarter::CHARACTER VARYING AS my_date_quarter,
        src1.country,
        src1.sector,
        src1.l3_wwid,
        src1.l3_username,
        src1.l3_manager_name,
        src1.l2_wwid,
        src1.l2_username,
        src1.l2_manager_name,
        src1.l1_wwid,
        src1.l1_username,
        src1.l1_manager_name,
        src1.sales_rep_ntid,
        src1.sales_rep,
        src1.email_id,
        src1.organization_l1_name,
        src1.organization_l2_name,
        src1.organization_l3_name,
        src1.organization_l4_name,
        src1.organization_l5_name,
        src1.total_active,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.speciality_1_type_english AS hcp_speciality,
        src1.customer_code AS hco_customer_code_2,
        src1.specialty AS cycle_speciality,
        src1.hco_source_id AS business_account_id,
        src1.business_account,
        src1.classification AS account_segmentation,
        src1.cpa_status,
        'TARGET' AS cycle_plan_type,
        SUM(src1.PLAN) AS planned_calls,
        SUM(src1.attainment) AS attainment,
        SUM(src1.actual) AS actual_calls,
        SUM(src1.cpa_100) AS cpa_100,
        NULL AS product,
        NULL AS planned_call_detail_count,
        NULL AS cycle_plan_detail_attainment,
        NULL AS actual_call_detail_count,
        NULL AS cfa_100,
        NULL AS cfa_33,
        NULL AS cfa_66
    FROM (
        SELECT dimdate.my_date_year,
            dimdate.my_date_month,
            dimdate.my_date_quarter,
            fact.country_key AS country,
            dim_hcp.hcp_name,
            dim_hcp.hcp_source_id,
            dim_hcp.speciality_1_type_english,
            fact.accounts_specialty_1 AS specialty,
            hier.sector,
            dim_hco.hco_name AS business_account,
            fact.planned_call_count AS PLAN,
            fact.cycle_plan_target_attainment AS attainment,
            fact.actual_call_count AS actual,
            fact.CYCLE_PLAN_ATTAINMENT_CPTARGET AS cpa_100,
            fact.number_of_targets AS hcp_count,
            CASE 
                WHEN fact.status_type::TEXT <> 'Approved'::CHARACTER VARYING::TEXT
                    AND fact.ready_for_approval_flag = 1
                    THEN 'Ready for Approval'::CHARACTER VARYING
                WHEN fact.status_type::TEXT = 'Approved'::CHARACTER VARYING::TEXT
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
                WHEN hier.employee_name IS NULL
                    THEN dimemp.employee_name
                ELSE hier.employee_name
                END AS sales_rep,
            dimemp.email_id,
            dimemp.organization_l1_name,
            dimemp.organization_l2_name,
            dimemp.organization_l3_name,
            dimemp.organization_l4_name,
            dimemp.organization_l5_name,
            CASE 
                WHEN (
                        date_part (
                           year,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT || date_part (
                            month,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT
                        ) = (
                        date_part (
                           year,
                            sysdate()
                            )::CHARACTER VARYING::TEXT || date_part (
                            month,
                            sysdate()
                            )::CHARACTER VARYING::TEXT
                        )
                    THEN emp1.current_mnth_emp_cnt
                ELSE actve_usr.total_active
                END AS total_active,
            dim_hco.customer_code,
            fact.classification,
            dim_hco.hco_source_id
        FROM (
            SELECT country_key,
                employee_key,
                organization_key,
                territory_name,
                start_date_key,
                end_date_key,
                active_flag,
                close_out_flag,
                ready_for_approval_flag,
                status_type,
                cycle_plan_name,
                channel_type,
                cycle_plan_external_id,
                manager,
                number_of_targets,
                number_of_cfa_100_targets,
                cycle_plan_attainment_cptarget,
                start_date_key::CHARACTER VARYING(10)::DATE + (end_date_key::CHARACTER VARYING(10)::DATE - start_date_key::CHARACTER VARYING(10)::DATE) / 2 AS mid_date,
                hcp_product_achieved_100,
                hcp_products_planned,
                hcp_key,
                hco_key,
                scheduled_call_count,
                actual_call_count,
                planned_call_count,
                cycle_plan_target_attainment,
                accounts_specialty_1,
                account_source_id,
                number_of_cfa_100_details,
                product_indication_key,
                classification_type,
                cycle_plan_type,
                cycle_plan_indicator,
                classification
            FROM fact_cycle_plan
            WHERE cycle_plan_type::TEXT = 'TARGET'::CHARACTER VARYING::TEXT
            ) fact
        JOIN dim_date dimdate ON REPLACE(fact.mid_date::TEXT, '-', '')::NUMERIC::NUMERIC(18, 0) = dimdate.date_key::NUMERIC::NUMERIC(18, 0)
            AND my_date_year BETWEEN (DATE_PART(year,sysdate()) - 2)
                AND DATE_PART(year,sysdate())
        JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
        JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
            AND fact.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
            AND fact.country_key::TEXT = dimemp.country_code::TEXT
        LEFT JOIN (
            SELECT stg_isight_active_user_snapshot.year,
                sect.sector,
                CASE 
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 1::TEXT
                        THEN 'Jan'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 2::TEXT
                        THEN 'Feb'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 3::TEXT
                        THEN 'Mar'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 4::TEXT
                        THEN 'Apr'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 5::TEXT
                        THEN 'May'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 6::TEXT
                        THEN 'Jun'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 7::TEXT
                        THEN 'Jul'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 8::TEXT
                        THEN 'Aug'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 9::TEXT
                        THEN 'Sep'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 10::TEXT
                        THEN 'Oct'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 11::TEXT
                        THEN 'Nov'::TEXT
                    ELSE 'Dec'::TEXT
                    END AS month,
                stg_isight_active_user_snapshot.country_code,
                COUNT(stg_isight_active_user_snapshot.employee_source_id) AS total_active
            FROM edw_isight_dim_employee_snapshot_xref stg_isight_active_user_snapshot
            LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = stg_isight_active_user_snapshot.company_name::TEXT
                AND sect.country::TEXT = stg_isight_active_user_snapshot.country_code::TEXT
            WHERE stg_isight_active_user_snapshot.active_flag = 1::NUMERIC::NUMERIC(18, 0)
                AND (
                    stg_isight_active_user_snapshot.profile_name::TEXT = 'AP_Core_Sales'::TEXT
                    OR stg_isight_active_user_snapshot.profile_name::TEXT = 'AP_Core_MY_Sales'::TEXT
                    )
            GROUP BY stg_isight_active_user_snapshot.year,
                stg_isight_active_user_snapshot.month,
                stg_isight_active_user_snapshot.country_code,
                sect.sector
            ) actve_usr ON dimdate.my_date_year = actve_usr.year
            AND dimdate.my_date_quarter = actve_usr.month::CHARACTER VARYING::TEXT
            AND fact.country_key::TEXT = actve_usr.country_code::TEXT
            AND COALESCE(hier.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(actve_usr.sector, '#'::CHARACTER VARYING)::TEXT
        LEFT JOIN (
            SELECT COUNT(1) AS current_mnth_emp_cnt,
                DIM_EMPLOYEE_ICONNECT.country_code,
                sect.sector
            FROM DIM_EMPLOYEE_ICONNECT
            LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = DIM_EMPLOYEE_ICONNECT.company_name::TEXT
                AND sect.country::TEXT = DIM_EMPLOYEE_ICONNECT.country_code::TEXT
            WHERE DIM_EMPLOYEE_ICONNECT.active_flag = 1::NUMERIC::NUMERIC(18, 0)
                AND (
                    DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_Sales'::CHARACTER VARYING::TEXT
                    OR DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_MY_Sales'::CHARACTER VARYING::TEXT
                    )
            GROUP BY DIM_EMPLOYEE_ICONNECT.country_code,
                sect.sector
            ) emp1 ON fact.country_key::TEXT = emp1.country_code::TEXT
            AND COALESCE(hier.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(emp1.sector, '#'::CHARACTER VARYING)::TEXT
        ) src1
    WHERE src1.cpa_status IS NOT NULL
        AND src1.country::TEXT <> 'ZZ'::CHARACTER VARYING::TEXT
    GROUP BY src1.my_date_year,
        src1.my_date_month,
        src1.my_date_quarter,
        src1.country,
        src1.sector,
        src1.l3_wwid,
        src1.l3_username,
        src1.l3_manager_name,
        src1.l2_wwid,
        src1.l2_username,
        src1.l2_manager_name,
        src1.l1_wwid,
        src1.l1_username,
        src1.l1_manager_name,
        src1.sales_rep_ntid,
        src1.sales_rep,
        src1.email_id,
        src1.organization_l1_name,
        src1.organization_l2_name,
        src1.organization_l3_name,
        src1.organization_l4_name,
        src1.organization_l5_name,
        src1.total_active,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.speciality_1_type_english,
        src1.customer_code,
        src1.specialty,
        src1.hco_source_id,
        src1.business_account,
        src1.classification,
        src1.cpa_status
    ),
T4
AS (
    SELECT NULL AS jnj_date_year,
        NULL AS jnj_date_month,
        NULL AS jnj_date_quarter,
        src1.date_year::CHARACTER VARYING AS date_year,
        src1.date_month::CHARACTER VARYING AS date_month,
        src1.date_quarter::CHARACTER VARYING AS date_quarter,
        NULL AS my_date_year,
        NULL AS my_date_month,
        NULL AS my_date_quarter,
        src1.country,
        src1.sector,
        src1.l3_wwid,
        src1.l3_username,
        src1.l3_manager_name,
        src1.l2_wwid,
        src1.l2_username,
        src1.l2_manager_name,
        src1.l1_wwid,
        src1.l1_username,
        src1.l1_manager_name,
        src1.sales_rep_ntid,
        src1.sales_rep,
        src1.email_id,
        src1.organization_l1_name,
        src1.organization_l2_name,
        src1.organization_l3_name,
        src1.organization_l4_name,
        src1.organization_l5_name,
        src1.total_active,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.speciality_1_type_english AS hcp_speciality,
        src1.customer_code AS hco_customer_code_2,
        src1.specialty AS cycle_speciality,
        src1.hco_source_id AS business_account_id,
        src1.business_account,
        src1.classification AS account_segmentation,
        src1.cpa_status,
        'PRODUCT' AS cycle_plan_type,
        NULL AS planned_calls,
        NULL AS attainment,
        NULL AS actual_calls,
        NULL AS cpa_100,
        src1.product,
        SUM(src1.PLAN) AS planned_call_detail_count,
        SUM(src1.attainment) AS cycle_plan_detail_attainment,
        SUM(src1.actual) AS actual_call_detail_count,
        SUM(src1.cfa_100) AS cfa_100,
        SUM(src1.cfa_33) AS cfa_33,
        SUM(src1.cfa_66) AS cfa_66
    FROM (
        SELECT dimdate.date_year AS date_year,
            dimdate.date_month AS date_month,
            dimdate.date_quarter AS date_quarter,
            fact.country_key AS country,
            dim_hcp.hcp_name,
            dim_hcp.hcp_source_id,
            dim_hcp.speciality_1_type_english,
            fact.accounts_specialty_1 AS specialty,
            hier.sector,
            dim_hco.hco_name AS business_account,
            fact.planned_call_detail_count AS PLAN,
            fact.cycle_plan_detail_attainment AS attainment,
            fact.actual_call_detail_count AS actual,
            fact.cfa_100,
            fact.cfa_33,
            fact.cfa_66,
            fact.number_of_targets AS hcp_count,
            CASE 
                WHEN fact.status_type::TEXT <> 'Approved'::CHARACTER VARYING::TEXT
                    AND fact.ready_for_approval_flag = 1
                    THEN 'Ready for Approval'::CHARACTER VARYING
                WHEN fact.status_type::TEXT = 'Approved'::CHARACTER VARYING::TEXT
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
                WHEN hier.employee_name IS NULL
                    THEN dimemp.employee_name
                ELSE hier.employee_name
                END AS sales_rep,
            dimemp.email_id,
            dpi.product_name AS product,
            dimemp.organization_l1_name,
            dimemp.organization_l2_name,
            dimemp.organization_l3_name,
            dimemp.organization_l4_name,
            dimemp.organization_l5_name,
            CASE 
                WHEN (
                        date_part (
                           year,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT || date_part (
                            month,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT
                        ) = (
                        date_part (
                           year,
                            sysdate()
                            )::CHARACTER VARYING::TEXT || date_part (
                            month,
                            sysdate()
                            )::CHARACTER VARYING::TEXT
                        )
                    THEN emp1.current_mnth_emp_cnt
                ELSE actve_usr.total_active
                END AS total_active,
            dim_hco.customer_code,
            fact.classification_type AS classification,
            dim_hco.hco_source_id
        FROM (
            SELECT country_key,
                employee_key,
                organization_key,
                territory_name,
                start_date_key,
                end_date_key,
                active_flag,
                close_out_flag,
                ready_for_approval_flag,
                status_type,
                cycle_plan_name,
                channel_type,
                cycle_plan_external_id,
                manager,
                number_of_targets,
                number_of_cfa_100_targets,
                cycle_plan_attainment_cptarget,
                start_date_key::CHARACTER VARYING(10)::DATE + (end_date_key::CHARACTER VARYING(10)::DATE - start_date_key::CHARACTER VARYING(10)::DATE) / 2 AS mid_date,
                hcp_product_achieved_100,
                hcp_products_planned,
                hcp_key,
                hco_key,
                scheduled_call_detail_count,
                actual_call_detail_count,
                planned_call_detail_count,
                cycle_plan_detail_attainment,
                accounts_specialty_1,
                account_source_id,
                number_of_cfa_100_details,
                product_indication_key,
                classification_type,
                cycle_plan_type,
                cycle_plan_indicator,
                classification,
                cfa_100,
                cfa_33,
                cfa_66
            FROM fact_cycle_plan
            WHERE cycle_plan_type::TEXT = 'PRODUCT'::CHARACTER VARYING::TEXT
            ) fact
        JOIN dim_date dimdate ON REPLACE(fact.mid_date::TEXT, '-', '')::NUMERIC::NUMERIC(18, 0) = dimdate.date_key::NUMERIC::NUMERIC(18, 0)
            AND date_year BETWEEN (DATE_PART(year,sysdate()) - 2)
                AND DATE_PART(year,sysdate())
        JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
        JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
            AND fact.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
            AND fact.country_key::TEXT = dimemp.country_code::TEXT
        LEFT JOIN dim_product_indication dpi ON fact.product_indication_key::TEXT = dpi.product_indication_key::TEXT
        LEFT JOIN (
            SELECT stg_isight_active_user_snapshot.year,
                sect.sector,
                CASE 
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 1::TEXT
                        THEN 'Jan'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 2::TEXT
                        THEN 'Feb'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 3::TEXT
                        THEN 'Mar'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 4::TEXT
                        THEN 'Apr'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 5::TEXT
                        THEN 'May'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 6::TEXT
                        THEN 'Jun'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 7::TEXT
                        THEN 'Jul'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 8::TEXT
                        THEN 'Aug'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 9::TEXT
                        THEN 'Sep'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 10::TEXT
                        THEN 'Oct'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 11::TEXT
                        THEN 'Nov'::TEXT
                    ELSE 'Dec'::TEXT
                    END AS month,
                stg_isight_active_user_snapshot.country_code,
                COUNT(stg_isight_active_user_snapshot.employee_source_id) AS total_active
            FROM edw_isight_dim_employee_snapshot_xref stg_isight_active_user_snapshot
            LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = stg_isight_active_user_snapshot.company_name::TEXT
                AND sect.country::TEXT = stg_isight_active_user_snapshot.country_code::TEXT
            WHERE stg_isight_active_user_snapshot.active_flag = 1::NUMERIC::NUMERIC(18, 0)
                AND (
                    stg_isight_active_user_snapshot.profile_name::TEXT = 'AP_Core_Sales'::TEXT
                    OR stg_isight_active_user_snapshot.profile_name::TEXT = 'AP_Core_MY_Sales'::TEXT
                    )
            GROUP BY stg_isight_active_user_snapshot.year,
                stg_isight_active_user_snapshot.month,
                stg_isight_active_user_snapshot.country_code,
                sect.sector
            ) actve_usr ON dimdate.date_year = actve_usr.year
            AND dimdate.date_month = actve_usr.month::CHARACTER VARYING::TEXT
            AND fact.country_key::TEXT = actve_usr.country_code::TEXT
            AND COALESCE(hier.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(actve_usr.sector, '#'::CHARACTER VARYING)::TEXT
        LEFT JOIN (
            SELECT COUNT(1) AS current_mnth_emp_cnt,
                DIM_EMPLOYEE_ICONNECT.country_code,
                sect.sector
            FROM DIM_EMPLOYEE_ICONNECT
            LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = DIM_EMPLOYEE_ICONNECT.company_name::TEXT
                AND sect.country::TEXT = DIM_EMPLOYEE_ICONNECT.country_code::TEXT
            WHERE DIM_EMPLOYEE_ICONNECT.active_flag = 1::NUMERIC::NUMERIC(18, 0)
                AND (
                    DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_Sales'::CHARACTER VARYING::TEXT
                    OR DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_MY_Sales'::CHARACTER VARYING::TEXT
                    )
            GROUP BY DIM_EMPLOYEE_ICONNECT.country_code,
                sect.sector
            ) emp1 ON fact.country_key::TEXT = emp1.country_code::TEXT
            AND COALESCE(hier.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(emp1.sector, '#'::CHARACTER VARYING)::TEXT
        ) src1
    WHERE src1.cpa_status IS NOT NULL
        AND src1.country::TEXT <> 'ZZ'::CHARACTER VARYING::TEXT
    GROUP BY src1.date_year,
        src1.date_month,
        src1.date_quarter,
        src1.country,
        src1.sector,
        src1.l3_wwid,
        src1.l3_username,
        src1.l3_manager_name,
        src1.l2_wwid,
        src1.l2_username,
        src1.l2_manager_name,
        src1.l1_wwid,
        src1.l1_username,
        src1.l1_manager_name,
        src1.sales_rep_ntid,
        src1.sales_rep,
        src1.email_id,
        src1.organization_l1_name,
        src1.organization_l2_name,
        src1.organization_l3_name,
        src1.organization_l4_name,
        src1.organization_l5_name,
        src1.total_active,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.speciality_1_type_english,
        src1.customer_code,
        src1.specialty,
        src1.hco_source_id,
        src1.business_account,
        src1.classification,
        src1.cpa_status,
        src1.product
    ),
T5
AS (
    SELECT src1.jnj_date_year::CHARACTER VARYING AS jnj_date_year,
        src1.jnj_date_month::CHARACTER VARYING AS jnj_date_month,
        src1.jnj_date_quarter::CHARACTER VARYING AS jnj_date_quarter,
        NULL AS date_year,
        NULL AS date_month,
        NULL AS date_quarter,
        NULL AS my_date_year,
        NULL AS my_date_month,
        NULL AS my_date_quarter,
        src1.country,
        src1.sector,
        src1.l3_wwid,
        src1.l3_username,
        src1.l3_manager_name,
        src1.l2_wwid,
        src1.l2_username,
        src1.l2_manager_name,
        src1.l1_wwid,
        src1.l1_username,
        src1.l1_manager_name,
        src1.sales_rep_ntid,
        src1.sales_rep,
        src1.email_id,
        src1.organization_l1_name,
        src1.organization_l2_name,
        src1.organization_l3_name,
        src1.organization_l4_name,
        src1.organization_l5_name,
        src1.total_active,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.speciality_1_type_english AS hcp_speciality,
        src1.customer_code AS hco_customer_code_2,
        src1.specialty AS cycle_speciality,
        src1.hco_source_id AS business_account_id,
        src1.business_account,
        src1.classification AS account_segmentation,
        src1.cpa_status,
        'PRODUCT' AS cycle_plan_type,
        NULL AS planned_calls,
        NULL AS attainment,
        NULL AS actual_calls,
        NULL AS cpa_100,
        src1.product,
        SUM(src1.PLAN) AS planned_call_detail_count,
        SUM(src1.attainment) AS cycle_plan_detail_attainment,
        SUM(src1.actual) AS actual_call_detail_count,
        SUM(src1.cfa_100) AS cfa_100,
        SUM(src1.cfa_33) AS cfa_33,
        SUM(src1.cfa_66) AS cfa_66
    FROM (
        SELECT dimdate.jnj_date_year,
            dimdate.jnj_date_month,
            dimdate.jnj_date_quarter,
            fact.country_key AS country,
            dim_hcp.hcp_name,
            dim_hcp.hcp_source_id,
            dim_hcp.speciality_1_type_english,
            fact.accounts_specialty_1 AS specialty,
            hier.sector,
            dim_hco.hco_name AS business_account,
            fact.planned_call_detail_count AS PLAN,
            fact.cycle_plan_detail_attainment AS attainment,
            fact.actual_call_detail_count AS actual,
            fact.cfa_100,
            fact.cfa_33,
            fact.cfa_66,
            fact.number_of_targets AS hcp_count,
            CASE 
                WHEN fact.status_type::TEXT <> 'Approved'::CHARACTER VARYING::TEXT
                    AND fact.ready_for_approval_flag = 1
                    THEN 'Ready for Approval'::CHARACTER VARYING
                WHEN fact.status_type::TEXT = 'Approved'::CHARACTER VARYING::TEXT
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
                WHEN hier.employee_name IS NULL
                    THEN dimemp.employee_name
                ELSE hier.employee_name
                END AS sales_rep,
            dimemp.email_id,
            dpi.product_name AS product,
            dimemp.organization_l1_name,
            dimemp.organization_l2_name,
            dimemp.organization_l3_name,
            dimemp.organization_l4_name,
            dimemp.organization_l5_name,
            CASE 
                WHEN (
                        date_part (
                           year,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT || date_part (
                            month,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT
                        ) = (
                        date_part (
                           year,
                            sysdate()
                            )::CHARACTER VARYING::TEXT || date_part (
                            month,
                            sysdate()
                            )::CHARACTER VARYING::TEXT
                        )
                    THEN emp1.current_mnth_emp_cnt
                ELSE actve_usr.total_active
                END AS total_active,
            dim_hco.customer_code,
            fact.classification_type AS classification,
            dim_hco.hco_source_id
        FROM (
            SELECT country_key,
                employee_key,
                organization_key,
                territory_name,
                start_date_key,
                end_date_key,
                active_flag,
                close_out_flag,
                ready_for_approval_flag,
                status_type,
                cycle_plan_name,
                channel_type,
                cycle_plan_external_id,
                manager,
                number_of_targets,
                number_of_cfa_100_targets,
                cycle_plan_attainment_cptarget,
                start_date_key::CHARACTER VARYING(10)::DATE + (end_date_key::CHARACTER VARYING(10)::DATE - start_date_key::CHARACTER VARYING(10)::DATE) / 2 AS mid_date,
                hcp_product_achieved_100,
                hcp_products_planned,
                hcp_key,
                hco_key,
                scheduled_call_detail_count,
                actual_call_detail_count,
                planned_call_detail_count,
                cycle_plan_detail_attainment,
                accounts_specialty_1,
                account_source_id,
                number_of_cfa_100_details,
                product_indication_key,
                classification_type,
                cycle_plan_type,
                cycle_plan_indicator,
                classification,
                cfa_100,
                cfa_33,
                cfa_66
            FROM fact_cycle_plan
            WHERE cycle_plan_type::TEXT = 'PRODUCT'::CHARACTER VARYING::TEXT
            ) fact
        JOIN dim_date dimdate ON REPLACE(fact.mid_date::TEXT, '-', '')::NUMERIC::NUMERIC(18, 0) = dimdate.date_key::NUMERIC::NUMERIC(18, 0)
            AND jnj_date_year BETWEEN (DATE_PART(year,sysdate()) - 2)
                AND DATE_PART(year,sysdate())
        JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
        JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
            AND fact.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
            AND fact.country_key::TEXT = dimemp.country_code::TEXT
        LEFT JOIN dim_product_indication dpi ON fact.product_indication_key::TEXT = dpi.product_indication_key::TEXT
        LEFT JOIN (
            SELECT stg_isight_active_user_snapshot.year,
                sect.sector,
                CASE 
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 1::TEXT
                        THEN 'Jan'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 2::TEXT
                        THEN 'Feb'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 3::TEXT
                        THEN 'Mar'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 4::TEXT
                        THEN 'Apr'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 5::TEXT
                        THEN 'May'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 6::TEXT
                        THEN 'Jun'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 7::TEXT
                        THEN 'Jul'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 8::TEXT
                        THEN 'Aug'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 9::TEXT
                        THEN 'Sep'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 10::TEXT
                        THEN 'Oct'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 11::TEXT
                        THEN 'Nov'::TEXT
                    ELSE 'Dec'::TEXT
                    END AS month,
                stg_isight_active_user_snapshot.country_code,
                COUNT(stg_isight_active_user_snapshot.employee_source_id) AS total_active
            FROM edw_isight_dim_employee_snapshot_xref stg_isight_active_user_snapshot
            LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = stg_isight_active_user_snapshot.company_name::TEXT
                AND sect.country::TEXT = stg_isight_active_user_snapshot.country_code::TEXT
            WHERE stg_isight_active_user_snapshot.active_flag = 1::NUMERIC::NUMERIC(18, 0)
                AND (
                    stg_isight_active_user_snapshot.profile_name::TEXT = 'AP_Core_Sales'::TEXT
                    OR stg_isight_active_user_snapshot.profile_name::TEXT = 'AP_Core_MY_Sales'::TEXT
                    )
            GROUP BY stg_isight_active_user_snapshot.year,
                stg_isight_active_user_snapshot.month,
                stg_isight_active_user_snapshot.country_code,
                sect.sector
            ) actve_usr ON dimdate.jnj_date_year = actve_usr.year
            AND dimdate.jnj_date_month = actve_usr.month::CHARACTER VARYING::TEXT
            AND fact.country_key::TEXT = actve_usr.country_code::TEXT
            AND COALESCE(hier.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(actve_usr.sector, '#'::CHARACTER VARYING)::TEXT
        LEFT JOIN (
            SELECT COUNT(1) AS current_mnth_emp_cnt,
                DIM_EMPLOYEE_ICONNECT.country_code,
                sect.sector
            FROM DIM_EMPLOYEE_ICONNECT
            LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = DIM_EMPLOYEE_ICONNECT.company_name::TEXT
                AND sect.country::TEXT = DIM_EMPLOYEE_ICONNECT.country_code::TEXT
            WHERE DIM_EMPLOYEE_ICONNECT.active_flag = 1::NUMERIC::NUMERIC(18, 0)
                AND (
                    DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_Sales'::CHARACTER VARYING::TEXT
                    OR DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_MY_Sales'::CHARACTER VARYING::TEXT
                    )
            GROUP BY DIM_EMPLOYEE_ICONNECT.country_code,
                sect.sector
            ) emp1 ON fact.country_key::TEXT = emp1.country_code::TEXT
            AND COALESCE(hier.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(emp1.sector, '#'::CHARACTER VARYING)::TEXT
        ) src1
    WHERE src1.cpa_status IS NOT NULL
        AND src1.country::TEXT <> 'ZZ'::CHARACTER VARYING::TEXT
    GROUP BY src1.jnj_date_year,
        src1.jnj_date_month,
        src1.jnj_date_quarter,
        src1.country,
        src1.sector,
        src1.l3_wwid,
        src1.l3_username,
        src1.l3_manager_name,
        src1.l2_wwid,
        src1.l2_username,
        src1.l2_manager_name,
        src1.l1_wwid,
        src1.l1_username,
        src1.l1_manager_name,
        src1.sales_rep_ntid,
        src1.sales_rep,
        src1.email_id,
        src1.organization_l1_name,
        src1.organization_l2_name,
        src1.organization_l3_name,
        src1.organization_l4_name,
        src1.organization_l5_name,
        src1.total_active,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.speciality_1_type_english,
        src1.customer_code,
        src1.specialty,
        src1.hco_source_id,
        src1.business_account,
        src1.classification,
        src1.cpa_status,
        src1.product
    ),
T6
AS (
    SELECT NULL AS jnj_date_year,
        NULL AS jnj_date_month,
        NULL AS jnj_date_quarter,
        NULL AS date_year,
        NULL AS date_month,
        NULL AS date_quarter,
        src1.my_date_year::CHARACTER VARYING AS my_date_year,
        TRIM(src1.my_date_month::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS my_date_month,
        src1.my_date_quarter::CHARACTER VARYING AS my_date_quarter,
        src1.country,
        src1.sector,
        src1.l3_wwid,
        src1.l3_username,
        src1.l3_manager_name,
        src1.l2_wwid,
        src1.l2_username,
        src1.l2_manager_name,
        src1.l1_wwid,
        src1.l1_username,
        src1.l1_manager_name,
        src1.sales_rep_ntid,
        src1.sales_rep,
        src1.email_id,
        src1.organization_l1_name,
        src1.organization_l2_name,
        src1.organization_l3_name,
        src1.organization_l4_name,
        src1.organization_l5_name,
        src1.total_active,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.speciality_1_type_english AS hcp_speciality,
        src1.customer_code AS hco_customer_code_2,
        src1.specialty AS cycle_speciality,
        src1.hco_source_id AS business_account_id,
        src1.business_account,
        src1.classification AS account_segmentation,
        src1.cpa_status,
        'PRODUCT' AS cycle_plan_type,
        NULL AS planned_calls,
        NULL AS attainment,
        NULL AS actual_calls,
        NULL AS cpa_100,
        src1.product,
        SUM(src1.PLAN) AS planned_call_detail_count,
        SUM(src1.attainment) AS cycle_plan_detail_attainment,
        SUM(src1.actual) AS actual_call_detail_count,
        SUM(src1.cfa_100) AS cfa_100,
        SUM(src1.cfa_33) AS cfa_33,
        SUM(src1.cfa_66) AS cfa_66
    FROM (
        SELECT dimdate.my_date_year,
            dimdate.my_date_month,
            dimdate.my_date_quarter,
            fact.country_key AS country,
            dim_hcp.hcp_name,
            dim_hcp.hcp_source_id,
            dim_hcp.speciality_1_type_english,
            fact.accounts_specialty_1 AS specialty,
            hier.sector,
            dim_hco.hco_name AS business_account,
            fact.planned_call_detail_count AS PLAN,
            fact.cycle_plan_detail_attainment AS attainment,
            fact.actual_call_detail_count AS actual,
            fact.cfa_100,
            fact.cfa_33,
            fact.cfa_66,
            fact.number_of_targets AS hcp_count,
            CASE 
                WHEN fact.status_type::TEXT <> 'Approved'::CHARACTER VARYING::TEXT
                    AND fact.ready_for_approval_flag = 1
                    THEN 'Ready for Approval'::CHARACTER VARYING
                WHEN fact.status_type::TEXT = 'Approved'::CHARACTER VARYING::TEXT
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
                WHEN hier.employee_name IS NULL
                    THEN dimemp.employee_name
                ELSE hier.employee_name
                END AS sales_rep,
            dimemp.email_id,
            dpi.product_name AS product,
            dimemp.organization_l1_name,
            dimemp.organization_l2_name,
            dimemp.organization_l3_name,
            dimemp.organization_l4_name,
            dimemp.organization_l5_name,
            CASE 
                WHEN (
                        date_part (
                           year,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT || date_part (
                            month,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT
                        ) = (
                        date_part (
                           year,
                            sysdate()
                            )::CHARACTER VARYING::TEXT || date_part (
                            month,
                            sysdate()
                            )::CHARACTER VARYING::TEXT
                        )
                    THEN emp1.current_mnth_emp_cnt
                ELSE actve_usr.total_active
                END AS total_active,
            dim_hco.customer_code,
            fact.classification_type AS classification,
            dim_hco.hco_source_id
        FROM (
            SELECT country_key,
                employee_key,
                organization_key,
                territory_name,
                start_date_key,
                end_date_key,
                active_flag,
                close_out_flag,
                ready_for_approval_flag,
                status_type,
                cycle_plan_name,
                channel_type,
                cycle_plan_external_id,
                manager,
                number_of_targets,
                number_of_cfa_100_targets,
                cycle_plan_attainment_cptarget,
                start_date_key::CHARACTER VARYING(10)::DATE + (end_date_key::CHARACTER VARYING(10)::DATE - start_date_key::CHARACTER VARYING(10)::DATE) / 2 AS mid_date,
                hcp_product_achieved_100,
                hcp_products_planned,
                hcp_key,
                hco_key,
                scheduled_call_detail_count,
                actual_call_detail_count,
                planned_call_detail_count,
                cycle_plan_detail_attainment,
                accounts_specialty_1,
                account_source_id,
                number_of_cfa_100_details,
                product_indication_key,
                classification_type,
                cycle_plan_type,
                cycle_plan_indicator,
                classification,
                cfa_100,
                cfa_33,
                cfa_66
            FROM fact_cycle_plan
            WHERE cycle_plan_type::TEXT = 'PRODUCT'::CHARACTER VARYING::TEXT
            ) fact
        JOIN dim_date dimdate ON REPLACE(fact.mid_date::TEXT, '-', '')::NUMERIC::NUMERIC(18, 0) = dimdate.date_key::NUMERIC::NUMERIC(18, 0)
            AND my_date_year BETWEEN (DATE_PART(year,sysdate()) - 2)
                AND DATE_PART(year,sysdate())
        JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
        JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
            AND fact.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
            AND fact.country_key::TEXT = dimemp.country_code::TEXT
        LEFT JOIN dim_product_indication dpi ON fact.product_indication_key::TEXT = dpi.product_indication_key::TEXT
        LEFT JOIN (
            SELECT stg_isight_active_user_snapshot.year,
                sect.sector,
                CASE 
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 1::TEXT
                        THEN 'Jan'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 2::TEXT
                        THEN 'Feb'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 3::TEXT
                        THEN 'Mar'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 4::TEXT
                        THEN 'Apr'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 5::TEXT
                        THEN 'May'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 6::TEXT
                        THEN 'Jun'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 7::TEXT
                        THEN 'Jul'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 8::TEXT
                        THEN 'Aug'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 9::TEXT
                        THEN 'Sep'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 10::TEXT
                        THEN 'Oct'::TEXT
                    WHEN stg_isight_active_user_snapshot.month::TEXT = 11::TEXT
                        THEN 'Nov'::TEXT
                    ELSE 'Dec'::TEXT
                    END AS month,
                stg_isight_active_user_snapshot.country_code,
                COUNT(stg_isight_active_user_snapshot.employee_source_id) AS total_active
            FROM edw_isight_dim_employee_snapshot_xref stg_isight_active_user_snapshot
            LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = stg_isight_active_user_snapshot.company_name::TEXT
                AND sect.country::TEXT = stg_isight_active_user_snapshot.country_code::TEXT
            WHERE stg_isight_active_user_snapshot.active_flag = 1::NUMERIC::NUMERIC(18, 0)
                AND (
                    stg_isight_active_user_snapshot.profile_name::TEXT = 'AP_Core_Sales'::TEXT
                    OR stg_isight_active_user_snapshot.profile_name::TEXT = 'AP_Core_MY_Sales'::TEXT
                    )
            GROUP BY stg_isight_active_user_snapshot.year,
                stg_isight_active_user_snapshot.month,
                stg_isight_active_user_snapshot.country_code,
                sect.sector
            ) actve_usr ON dimdate.my_date_year = actve_usr.year
            AND dimdate.my_date_month = actve_usr.month::CHARACTER VARYING::TEXT
            AND fact.country_key::TEXT = actve_usr.country_code::TEXT
            AND COALESCE(hier.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(actve_usr.sector, '#'::CHARACTER VARYING)::TEXT
        LEFT JOIN (
            SELECT COUNT(1) AS current_mnth_emp_cnt,
                DIM_EMPLOYEE_ICONNECT.country_code,
                sect.sector
            FROM DIM_EMPLOYEE_ICONNECT
            LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = DIM_EMPLOYEE_ICONNECT.company_name::TEXT
                AND sect.country::TEXT = DIM_EMPLOYEE_ICONNECT.country_code::TEXT
            WHERE DIM_EMPLOYEE_ICONNECT.active_flag = 1::NUMERIC::NUMERIC(18, 0)
                AND (
                    DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_Sales'::CHARACTER VARYING::TEXT
                    OR DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_MY_Sales'::CHARACTER VARYING::TEXT
                    )
            GROUP BY DIM_EMPLOYEE_ICONNECT.country_code,
                sect.sector
            ) emp1 ON fact.country_key::TEXT = emp1.country_code::TEXT
            AND COALESCE(hier.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(emp1.sector, '#'::CHARACTER VARYING)::TEXT
        ) src1
    WHERE src1.cpa_status IS NOT NULL
        AND src1.country::TEXT <> 'ZZ'::CHARACTER VARYING::TEXT
    GROUP BY src1.my_date_year,
        src1.my_date_month,
        src1.my_date_quarter,
        src1.country,
        src1.sector,
        src1.l3_wwid,
        src1.l3_username,
        src1.l3_manager_name,
        src1.l2_wwid,
        src1.l2_username,
        src1.l2_manager_name,
        src1.l1_wwid,
        src1.l1_username,
        src1.l1_manager_name,
        src1.sales_rep_ntid,
        src1.sales_rep,
        src1.email_id,
        src1.organization_l1_name,
        src1.organization_l2_name,
        src1.organization_l3_name,
        src1.organization_l4_name,
        src1.organization_l5_name,
        src1.total_active,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.speciality_1_type_english,
        src1.customer_code,
        src1.specialty,
        src1.hco_source_id,
        src1.business_account,
        src1.classification,
        src1.cpa_status,
        src1.product
    ),
UNION_OF
AS (
    SELECT *
    FROM T1
    
    UNION ALL
    
    SELECT *
    FROM T2
    
    UNION ALL
    
    SELECT *
    FROM T3
    
    UNION ALL
    
    SELECT *
    FROM T4
    
    UNION ALL
    
    SELECT *
    FROM T5
    
    UNION ALL
    
    SELECT *
    FROM T6
    ),
FINAL
AS (
        SELECT JNJ_DATE_YEAR::VARCHAR(11) AS JNJ_DATE_YEAR,
        JNJ_DATE_MONTH::VARCHAR(5) AS JNJ_DATE_MONTH,
        JNJ_DATE_QUARTER::VARCHAR(5) AS JNJ_DATE_QUARTER,
        DATE_YEAR::VARCHAR(11) AS DATE_YEAR,
        DATE_MONTH::VARCHAR(3) AS DATE_MONTH,
        DATE_QUARTER::VARCHAR(2) AS DATE_QUARTER,
        MY_DATE_YEAR::VARCHAR(11) AS MY_DATE_YEAR,
        MY_DATE_MONTH::VARCHAR(3) AS MY_DATE_MONTH,
        MY_DATE_QUARTER::VARCHAR(2) AS MY_DATE_QUARTER,
        COUNTRY::VARCHAR(8) AS COUNTRY,
        SECTOR::VARCHAR(256) AS SECTOR,
        L3_WWID::VARCHAR(20) AS L3_WWID,
        L3_USERNAME::VARCHAR(80) AS L3_USERNAME,
        L3_MANAGER_NAME::VARCHAR(121) AS L3_MANAGER_NAME,
        L2_WWID::VARCHAR(20) AS L2_WWID,
        L2_USERNAME::VARCHAR(80)AS L2_USERNAME,
        L2_MANAGER_NAME::VARCHAR(121)AS L2_MANAGER_NAME,
        L1_WWID::VARCHAR(20)AS L1_WWID,
        L1_USERNAME::VARCHAR(80)AS L1_USERNAME,
        L1_MANAGER_NAME::VARCHAR(121)AS L1_MANAGER_NAME,
        SALES_REP_NTID::VARCHAR(80)AS SALES_REP_NTID,
        SALES_REP::VARCHAR(121)AS SALES_REP,
        EMAIL_ID::VARCHAR(128)AS EMAIL_ID,
        ORGANIZATION_L1_NAME::VARCHAR(80)AS ORGANIZATION_L1_NAME,
        ORGANIZATION_L2_NAME::VARCHAR(80)AS ORGANIZATION_L2_NAME,
        ORGANIZATION_L3_NAME::VARCHAR(80)AS ORGANIZATION_L3_NAME,
        ORGANIZATION_L4_NAME::VARCHAR(80)AS ORGANIZATION_L4_NAME,
        ORGANIZATION_L5_NAME::VARCHAR(80)AS ORGANIZATION_L5_NAME,
        TOTAL_ACTIVE::NUMBER(38,0)AS TOTAL_ACTIVE,
        HCP_NAME::VARCHAR(255)AS HCP_NAME,
        HCP_SOURCE_ID::VARCHAR(18)AS HCP_SOURCE_ID,
        HCP_SPECIALITY::VARCHAR(255)AS HCP_SPECIALITY,
        HCO_CUSTOMER_CODE_2::VARCHAR(60)AS HCO_CUSTOMER_CODE_2,
        CYCLE_SPECIALITY::VARCHAR(3000)AS CYCLE_SPECIALITY,
        BUSINESS_ACCOUNT_ID::VARCHAR(18)AS BUSINESS_ACCOUNT_ID,
        BUSINESS_ACCOUNT::VARCHAR(1300)AS BUSINESS_ACCOUNT,
        ACCOUNT_SEGMENTATION::VARCHAR(1300) AS ACCOUNT_SEGMENTATION,
        CPA_STATUS::VARCHAR(18) AS CPA_STATUS,
        CYCLE_PLAN_TYPE::VARCHAR(7) AS CYCLE_PLAN_TYPE,
        PLANNED_CALLS::NUMBER(38,0) AS PLANNED_CALLS,
        ATTAINMENT::NUMBER(38,0) AS ATTAINMENT,
        ACTUAL_CALLS::NUMBER(38,0) AS ACTUAL_CALLS,
        CPA_100::NUMBER(38,1) AS CPA_100,
        PRODUCT::VARCHAR(80) AS PRODUCT,
        PLANNED_CALL_DETAIL_COUNT::NUMBER(38,0) AS PLANNED_CALL_DETAIL_COUNT,
        CYCLE_PLAN_DETAIL_ATTAINMENT::NUMBER(38,0) AS CYCLE_PLAN_DETAIL_ATTAINMENT,
        ACTUAL_CALL_DETAIL_COUNT::NUMBER(38,0) AS ACTUAL_CALL_DETAIL_COUNT,
        CFA_100::NUMBER(38,0) AS CFA_100,
        CFA_33::NUMBER(38,0) AS CFA_33,
        CFA_66::NUMBER(38,0) AS CFA_66
    FROM UNION_OF
    )
SELECT *
FROM FINAL
