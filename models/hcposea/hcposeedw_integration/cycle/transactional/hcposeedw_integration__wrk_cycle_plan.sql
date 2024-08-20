with fact_cycle_plan
as (
    select *
    from dev_dna_core.hcposeedw_integration.fact_cycle_plan
    ),
dim_date
as (
    select *
    from dev_dna_core.hcposeedw_integration.dim_date
    ),
dim_hcp
as (
    select *
    from dev_dna_core.hcposeedw_integration.dim_hcp
    ),
vw_employee_hier
as (
    select *
    from dev_dna_core.hcposeedw_integration.vw_employee_hier
    ),
dim_hco
as (
    select *
    from dev_dna_core.hcposeedw_integration.dim_hco
    ),
dim_employee_iconnect
as (
    select *
    from dev_dna_core.hcposeedw_integration.dim_employee_iconnect
    ),
edw_isight_dim_employee_snapshot_xref
as (
    select *
    from dev_dna_core.hcposeedw_integration.edw_isight_dim_employee_snapshot_xref
    ),
edw_isight_sector_mapping
as (
    select *
    from dev_dna_core.hcposeedw_integration.edw_isight_sector_mapping
    ),
dim_product_indication
as (
    select *
    from dev_dna_core.hcposeedw_integration.dim_product_indication
    ),
cte1
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
                        "date_part" (
                            year,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT || "date_part" (
                            month,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT
                        ) = (
                        "date_part" (
                            year,
                            'now'::CHARACTER VARYING::TIMESTAMP WITHOUT TIME ZONE
                            )::CHARACTER VARYING::TEXT || "date_part" (
                            month,
                            'now'::CHARACTER VARYING::TIMESTAMP WITHOUT TIME ZONE
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
        JOIN dim_date dimdate ON REPLACE(fact.mid_date::TEXT, '-', '')::NUMERIC(18, 0) = dimdate.date_key::NUMERIC(18, 0)
            AND DATE_YEAR BETWEEN (DATE_PART(year, to_date(CURRENT_TIMESTAMP())) - 2)
                AND DATE_PART(year, to_date(CURRENT_TIMESTAMP()))
        --DATE_YEAR BETWEEN (DATE_PART_YEAR(TRUNC(current_timestamp())) - 2)
        --AND DATE_PART_YEAR(TRUNC(current_timestamp()))
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
cte2
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
                        "date_part" (
                            year,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT || "date_part" (
                            month,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT
                        ) = (
                        "date_part" (
                            year,
                            'now'::CHARACTER VARYING::TIMESTAMP WITHOUT TIME ZONE
                            )::CHARACTER VARYING::TEXT || "date_part" (
                            month,
                            'now'::CHARACTER VARYING::TIMESTAMP WITHOUT TIME ZONE
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
            AND jnj_date_year BETWEEN (DATE_PART(year, to_date(CURRENT_TIMESTAMP())) - 2)
                AND DATE_PART(year, to_date(CURRENT_TIMESTAMP()))
        --(DATE_PART_YEAR(to_date(current_timestamp())) - 2)   
        --AND DATE_PART_YEAR(to_date(current_timestamp()))
        JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
        JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
            AND fact.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
            AND fact.country_key::TEXT = dimemp.country_code::TEXT
        LEFT JOIN (
            SELECT stg_isight_active_user_snapshot.year AS year,
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
            AND dimdate.jnj_date_month = actve_usr.month
            AND fact.country_key::TEXT = actve_usr.country_code
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
insert1
AS (
    SELECT *
    FROM cte1
    
    UNION ALL
    
    SELECT *
    FROM cte2
    ),
insert2
AS (
    SELECT NULL AS jnj_date_year,
        NULL AS jnj_date_month,
        NULL AS jnj_date_quarter,
        NULL AS date_year,
        NULL AS date_month,
        NULL AS date_quarter,
        src1.my_date_year::CHARACTER VARYING AS my_date_year,
        RTRIM(src1.my_date_month::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS my_date_month,
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
                        "date_part" (
                            year,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT || "date_part" (
                            month,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT
                        ) = (
                        "date_part" (
                            year,
                            'now'::CHARACTER VARYING::TIMESTAMP WITHOUT TIME ZONE
                            )::CHARACTER VARYING::TEXT || "date_part" (
                            month,
                            'now'::CHARACTER VARYING::TIMESTAMP WITHOUT TIME ZONE
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
            AND my_date_year BETWEEN (DATE_PART(year, to_date(CURRENT_TIMESTAMP())) - 2)
                AND DATE_PART(year, to_date(CURRENT_TIMESTAMP()))
        --(DATE_PART_YEAR(TRUNC(current_timestamp())) - 2)
        --AND DATE_PART_YEAR(TRUNC(current_timestamp()))
        JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
        JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
            AND fact.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
            AND fact.country_key::TEXT = dimemp.country_code::TEXT
        LEFT JOIN (
            SELECT stg_isight_active_user_snapshot.year AS year,
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
insert3
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
                        "date_part" (
                            year,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT || "date_part" (
                            month,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT
                        ) = (
                        "date_part" (
                            year,
                            'now'::CHARACTER VARYING::TIMESTAMP WITHOUT TIME ZONE
                            )::CHARACTER VARYING::TEXT || "date_part" (
                            month,
                            'now'::CHARACTER VARYING::TIMESTAMP WITHOUT TIME ZONE
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
            AND date_year BETWEEN (DATE_PART(year, to_date(CURRENT_TIMESTAMP())) - 2)
                AND DATE_PART(year, to_date(CURRENT_TIMESTAMP()))
        --(DATE_PART_YEAR(TRUNC(current_timestamp())) - 2)
        --AND DATE_PART_YEAR(TRUNC(current_timestamp()))
        JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
        JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
            AND fact.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
            AND fact.country_key::TEXT = dimemp.country_code::TEXT
        LEFT JOIN dim_product_indication dpi ON fact.product_indication_key::TEXT = dpi.product_indication_key::TEXT
        LEFT JOIN (
            SELECT stg_isight_active_user_snapshot.year AS year,
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
cte3
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
                        "date_part" (
                            year,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT || "date_part" (
                            month,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT
                        ) = (
                        "date_part" (
                            year,
                            'now'::CHARACTER VARYING::TIMESTAMP WITHOUT TIME ZONE
                            )::CHARACTER VARYING::TEXT || "date_part" (
                            month,
                            'now'::CHARACTER VARYING::TIMESTAMP WITHOUT TIME ZONE
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
            AND jnj_date_year BETWEEN (DATE_PART(year, to_date(CURRENT_TIMESTAMP())) - 2)
                AND DATE_PART(year, to_date(CURRENT_TIMESTAMP()))
        --(DATE_PART_YEAR(TRUNC(current_timestamp())) - 2)
        --AND DATE_PART_YEAR(TRUNC(current_timestamp()))
        JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
        JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
            AND fact.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
            AND fact.country_key::TEXT = dimemp.country_code::TEXT
        LEFT JOIN dim_product_indication dpi ON fact.product_indication_key::TEXT = dpi.product_indication_key::TEXT
        LEFT JOIN (
            SELECT stg_isight_active_user_snapshot.year AS year,
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
cte4
AS (
    SELECT NULL AS jnj_date_year,
        NULL AS jnj_date_month,
        NULL AS jnj_date_quarter,
        NULL AS date_year,
        NULL AS date_month,
        NULL AS date_quarter,
        src1.my_date_year::CHARACTER VARYING AS my_date_year,
        RTRIM(src1.my_date_month::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS my_date_month,
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
                        "date_part" (
                            year,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT || "date_part" (
                            month,
                            fact.mid_date
                            )::CHARACTER VARYING::TEXT
                        ) = (
                        "date_part" (
                            year,
                            'now'::CHARACTER VARYING::TIMESTAMP WITHOUT TIME ZONE
                            )::CHARACTER VARYING::TEXT || "date_part" (
                            month,
                            'now'::CHARACTER VARYING::TIMESTAMP WITHOUT TIME ZONE
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
            AND my_date_year BETWEEN (DATE_PART(year, to_date(CURRENT_TIMESTAMP())) - 2)
                AND DATE_PART(year, to_date(CURRENT_TIMESTAMP()))
        --(DATE_PART_YEAR(TRUNC(current_timestamp())) - 2)
        --AND DATE_PART_YEAR(TRUNC(current_timestamp()))
        JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
        JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
            AND fact.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
            AND fact.country_key::TEXT = dimemp.country_code::TEXT
        LEFT JOIN dim_product_indication dpi ON fact.product_indication_key::TEXT = dpi.product_indication_key::TEXT
        LEFT JOIN (
            SELECT stg_isight_active_user_snapshot.year AS year,
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
insert4
AS (
    SELECT *
    FROM cte3
    
    UNION ALL
    
    SELECT *
    FROM cte4
    ),
result
AS (
    SELECT *
    FROM insert1
    
    UNION ALL
    
    SELECT *
    FROM insert2
    
    UNION ALL
    
    SELECT *
    FROM insert3
    
    UNION ALL
    
    SELECT *
    FROM insert4
    ),

final as (
    select 
        jnj_date_year::varchar(11) as jnj_date_year,
        jnj_date_month::varchar(5) as jnj_date_month,
        jnj_date_quarter::varchar(5) as jnj_date_quarter,
        date_year::varchar(11) as date_year,
        date_month::varchar(3) as date_month,
        date_quarter::varchar(2) as date_quarter,
        my_date_year::varchar(11) as my_date_year,
        my_date_month::varchar(3) as my_date_month,
        my_date_quarter::varchar(2) as my_date_quarter,
        country::varchar(8) as country,
        sector::varchar(256) as sector,
        l3_wwid::varchar(20) as l3_wwid,
        l3_username::varchar(80) as l3_username,
        l3_manager_name::varchar(121) as l3_manager_name,
        l2_wwid::varchar(20) as l2_wwid,
        l2_username::varchar(80) as l2_username,
        l2_manager_name::varchar(121) as l2_manager_name,
        l1_wwid::varchar(20) as l1_wwid,
        l1_username::varchar(80) as l1_username,
        l1_manager_name::varchar(121) as l1_manager_name,
        sales_rep_ntid::varchar(80) as sales_rep_ntid,
        sales_rep::varchar(121) as sales_rep,
        email_id::varchar(128) as email_id,
        organization_l1_name::varchar(80) as organization_l1_name,
        organization_l2_name::varchar(80) as organization_l2_name,
        organization_l3_name::varchar(80) as organization_l3_name,
        organization_l4_name::varchar(80) as organization_l4_name,
        organization_l5_name::varchar(80) as organization_l5_name,
        total_active::number(38,0) as total_active,
        hcp_name::varchar(255) as hcp_name,
        hcp_source_id::varchar(18) as hcp_source_id,
        hcp_speciality::varchar(255) as hcp_speciality,
        hco_customer_code_2::varchar(60) as hco_customer_code_2,
        cycle_speciality::varchar(3000) as cycle_speciality,
        business_account_id::varchar(18) as business_account_id,
        business_account::varchar(1300) as business_account,
        account_segmentation::varchar(1300) as account_segmentation,
        cpa_status::varchar(18) as cpa_status,
        cycle_plan_type::varchar(7) as cycle_plan_type,
        planned_calls::number(38,0) as planned_calls,
        attainment::number(38,0) as attainment,
        actual_calls::number(38,0) as actual_calls,
        cpa_100::number(38,1) as cpa_100,
        product::varchar(80) as product,
        planned_call_detail_count::number(38,0) as planned_call_detail_count,
        cycle_plan_detail_attainment::number(38,0) as cycle_plan_detail_attainment,
        actual_call_detail_count::number(38,0) as actual_call_detail_count,
        cfa_100::number(38,0) as cfa_100,
        cfa_33::number(38,0) as cfa_33,
        cfa_66::number(38,0) as cfa_66
    from result
)

select * from final
