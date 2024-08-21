WITH dim_date
AS (
    SELECT *
    FROM hcposeedw_integration.dim_date
    ),
vw_employee_hier
AS (
    SELECT *
    FROM hcposeedw_integration.vw_employee_hier
    ),
vw_employee_hier
AS (
    SELECT *
    FROM hcposeedw_integration.vw_employee_hier
    ),
holiday_list
AS (
    SELECT *
    FROM hcposeedw_integration.holiday_list
    ),
fact_timeoff_territory
AS (
    SELECT *
    FROM hcposeedw_integration.fact_timeoff_territory
    ),
fact_call_key_message
AS (
    SELECT *
    FROM hcposeedw_integration.fact_call_key_message
    ),
fact_call_detail
AS (
    SELECT *
    FROM hcposeedw_integration.fact_call_detail
    ),
edw_isight_sector_mapping
AS (
    SELECT *
    FROM hcposeedw_integration.edw_isight_sector_mapping
    ),
edw_isight_dim_employee_snapshot_xref
AS (
    SELECT *
    FROM hcposeedw_integration.edw_isight_dim_employee_snapshot_xref
    ),
dim_employee
AS (
    SELECT *
    FROM hcposeedw_integration.dim_employee
    ),
dim_date
AS (
    SELECT *
    FROM hcposeedw_integration.dim_date
    ),
T1
AS (
    SELECT src1.jnj_date_year AS jnj_year,
        src1.jnj_date_month AS jnj_month,
        src1.jnj_date_quarter AS jnj_quarter,
        NULL AS date_year,
        NULL AS date_month,
        NULL AS date_quarter,
        NULL AS my_year,
        NULL AS my_month,
        NULL AS my_quarter,
        src1.country,
        src1.sector,
        (src1.l3_wwid)::CHARACTER VARYING AS l3_wwid,
        (src1.l3_username)::CHARACTER VARYING AS l3_username,
        src1.l3_manager_name,
        (src1.l2_wwid)::CHARACTER VARYING AS l2_wwid,
        (src1.l2_username)::CHARACTER VARYING AS l2_username,
        (src1.l2_manager_name)::CHARACTER VARYING AS l2_manager_name,
        (src1.l1_wwid)::CHARACTER VARYING AS l1_wwid,
        (src1.l1_username)::CHARACTER VARYING AS l1_username,
        (src1.l1_manager_name)::CHARACTER VARYING AS l1_manager_name,
        (src1.organization_l1_name)::CHARACTER VARYING AS organization_l1_name,
        (src1.organization_l2_name)::CHARACTER VARYING AS organization_l2_name,
        src1.organization_l3_name,
        (src1.organization_l4_name)::CHARACTER VARYING AS organization_l4_name,
        (src1.organization_l5_name)::CHARACTER VARYING AS organization_l5_name,
        src1.sales_rep,
        src1.working_days,
        src1.total_cnt_call,
        src1.total_cnt_edetailing_calls,
        src1.total_cnt_call_delay,
        (src1.sales_rep_ntid)::CHARACTER VARYING AS sales_rep_ntid,
        src1.total_cnt_call_delay AS total_cnt_call_delay_sub,
        fct1.total_cnt_clm_flg,
        fct2.total_prnt_cnt_clm_flg,
        src1.total_cnt_submitted_calls,
        terr.cnt_total_time_on,
        terr1.cnt_total_time_off,
        CASE 
            WHEN (((((src1.jnj_date_year)::CHARACTER VARYING)::TEXT || ('-'::CHARACTER VARYING)::TEXT) || (src1.jnj_date_month)::TEXT) = ((((date_part(year, sysdate()))::CHARACTER VARYING(5))::TEXT || ('-'::CHARACTER VARYING)::TEXT) || ((date_part(month, sysdate()))::CHARACTER VARYING(5))::TEXT))
                THEN emp1.current_mnth_emp_cnt
            ELSE actve_usr.total_active
            END AS total_active,
        fct4.total_call_product AS detailed_products,
        fct3.total_sbmtd_calls_key_message
    FROM (
        (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        SELECT fact.jnj_date_year,
                                            fact.jnj_date_month,
                                            fact.jnj_date_quarter,
                                            fact.country,
                                            fact.sector,
                                            fact.l3_wwid,
                                            fact.l3_username,
                                            fact.l3_manager_name,
                                            fact.l2_wwid,
                                            fact.l2_username,
                                            fact.l2_manager_name,
                                            fact.l1_wwid,
                                            fact.l1_username,
                                            fact.l1_manager_name,
                                            fact.organization_l1_name,
                                            fact.organization_l2_name,
                                            fact.organization_l3_name,
                                            fact.organization_l4_name,
                                            fact.organization_l5_name,
                                            fact.sales_rep,
                                            fact.working_days,
                                            fact.total_cnt_call,
                                            fact.total_cnt_edetailing_calls,
                                            fact.total_cnt_call_delay,
                                            fact.sales_rep_ntid,
                                            fact.employee_key,
                                            fact.total_cnt_submitted_calls
                                        FROM (
                                            SELECT dimdate.jnj_date_year,
                                                CASE 
                                                    WHEN (
                                                            ((dimdate.jnj_date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_month IS NULL)
                                                                AND ('Jan' IS NULL)
                                                                )
                                                            )
                                                        THEN '1'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_month IS NULL)
                                                                AND ('Feb' IS NULL)
                                                                )
                                                            )
                                                        THEN '2'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_month IS NULL)
                                                                AND ('Mar' IS NULL)
                                                                )
                                                            )
                                                        THEN '3'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_month IS NULL)
                                                                AND ('Apr' IS NULL)
                                                                )
                                                            )
                                                        THEN '4'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_month IS NULL)
                                                                AND ('May' IS NULL)
                                                                )
                                                            )
                                                        THEN '5'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_month IS NULL)
                                                                AND ('Jun' IS NULL)
                                                                )
                                                            )
                                                        THEN '6'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_month IS NULL)
                                                                AND ('Jul' IS NULL)
                                                                )
                                                            )
                                                        THEN '7'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_month IS NULL)
                                                                AND ('Aug' IS NULL)
                                                                )
                                                            )
                                                        THEN '8'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_month IS NULL)
                                                                AND ('Sep' IS NULL)
                                                                )
                                                            )
                                                        THEN '9'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_month IS NULL)
                                                                AND ('Oct' IS NULL)
                                                                )
                                                            )
                                                        THEN '10'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_month IS NULL)
                                                                AND ('Nov' IS NULL)
                                                                )
                                                            )
                                                        THEN '11'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_month IS NULL)
                                                                AND ('Dec' IS NULL)
                                                                )
                                                            )
                                                        THEN '12'::CHARACTER VARYING
                                                    ELSE dimdate.jnj_date_month
                                                    END AS jnj_date_month,
                                                CASE 
                                                    WHEN (
                                                            ((dimdate.jnj_date_quarter)::TEXT = ('Q1'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_quarter IS NULL)
                                                                AND ('Q1' IS NULL)
                                                                )
                                                            )
                                                        THEN '1'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_quarter)::TEXT = ('Q2'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_quarter IS NULL)
                                                                AND ('Q2' IS NULL)
                                                                )
                                                            )
                                                        THEN '2'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_quarter)::TEXT = ('Q3'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_quarter IS NULL)
                                                                AND ('Q3' IS NULL)
                                                                )
                                                            )
                                                        THEN '3'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.jnj_date_quarter)::TEXT = ('Q4'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.jnj_date_quarter IS NULL)
                                                                AND ('Q4' IS NULL)
                                                                )
                                                            )
                                                        THEN '4'::CHARACTER VARYING
                                                    ELSE NULL::CHARACTER VARYING
                                                    END AS jnj_date_quarter,
                                                fact.country_key AS country,
                                                hier.sector,
                                                "max" ((hier.l2_wwid)::TEXT) AS l3_wwid,
                                                "max" (hier.l2_username) AS l3_username,
                                                hier.l2_manager_name AS l3_manager_name,
                                                "max" ((hier.l3_wwid)::TEXT) AS l2_wwid,
                                                "max" (hier.l3_username) AS l2_username,
                                                "max" ((hier.l3_manager_name)::TEXT) AS l2_manager_name,
                                                "max" ((hier.l4_wwid)::TEXT) AS l1_wwid,
                                                "max" (hier.l4_username) AS l1_username,
                                                "max" ((hier.l4_manager_name)::TEXT) AS l1_manager_name,
                                                "max" ((employee.organization_l1_name)::TEXT) AS organization_l1_name,
                                                "max" ((employee.organization_l2_name)::TEXT) AS organization_l2_name,
                                                employee.organization_l3_name,
                                                "max" ((employee.organization_l4_name)::TEXT) AS organization_l4_name,
                                                "max" ((employee.organization_l5_name)::TEXT) AS organization_l5_name,
                                                hier.employee_name AS sales_rep,
                                                "max" (((ds.working_days)::NUMERIC)::NUMERIC(18, 0)) AS working_days,
                                                count(fact.call_source_id) AS total_cnt_call,
                                                count(fact.call_clm_flag) AS total_cnt_edetailing_calls,
                                                sum(fact.call_submission_day) AS total_cnt_call_delay,
                                                fact.employee_key,
                                                hier.l1_username AS sales_rep_ntid,
                                                count(1) AS total_cnt_submitted_calls
                                            FROM (
                                                (
                                                    (
                                                        (
                                                            (
                                                                SELECT fact_call_detail.country_key,
                                                                    fact_call_detail.hcp_key,
                                                                    fact_call_detail.hco_key,
                                                                    fact_call_detail.employee_key,
                                                                    fact_call_detail.profile_key,
                                                                    fact_call_detail.organization_key,
                                                                    fact_call_detail.call_date_key,
                                                                    fact_call_detail.call_date_time,
                                                                    fact_call_detail.call_entry_datetime,
                                                                    fact_call_detail.call_mobile_last_modified_date_time,
                                                                    fact_call_detail.utc_call_date_time,
                                                                    fact_call_detail.utc_call_entry_time,
                                                                    fact_call_detail.call_status_type,
                                                                    fact_call_detail.call_channel_type,
                                                                    fact_call_detail.call_type,
                                                                    fact_call_detail.call_duration,
                                                                    fact_call_detail.call_clm_flag,
                                                                    fact_call_detail.parent_call_flag,
                                                                    fact_call_detail.attendee_type,
                                                                    fact_call_detail.call_comments,
                                                                    fact_call_detail.next_call_notes,
                                                                    fact_call_detail.pre_call_notes,
                                                                    fact_call_detail.manager_call_comment,
                                                                    fact_call_detail.last_activity_date,
                                                                    fact_call_detail.sample_card,
                                                                    fact_call_detail.account_plan,
                                                                    fact_call_detail.mobile_id,
                                                                    fact_call_detail.significant_event,
                                                                    fact_call_detail.location,
                                                                    fact_call_detail.signature_date,
                                                                    fact_call_detail.signature,
                                                                    fact_call_detail.territory,
                                                                    fact_call_detail.attendees,
                                                                    fact_call_detail.parent_call_source_id,
                                                                    fact_call_detail.user_name,
                                                                    fact_call_detail.medical_event,
                                                                    fact_call_detail.is_sampled_call,
                                                                    fact_call_detail.presentations,
                                                                    fact_call_detail.product_priority_1,
                                                                    fact_call_detail.product_priority_2,
                                                                    fact_call_detail.product_priority_3,
                                                                    fact_call_detail.product_priority_4,
                                                                    fact_call_detail.product_priority_5,
                                                                    fact_call_detail.attendee_list,
                                                                    fact_call_detail.msl_interaction_notes,
                                                                    fact_call_detail.sea_call_type,
                                                                    fact_call_detail.interaction_mode,
                                                                    fact_call_detail.hcp_kol_initiated,
                                                                    fact_call_detail.msl_interaction_type,
                                                                    fact_call_detail.call_objective,
                                                                    fact_call_detail.submission_delay,
                                                                    fact_call_detail.region,
                                                                    fact_call_detail.md_hsp_admin,
                                                                    fact_call_detail.hsp_minutes,
                                                                    fact_call_detail.ortho_on_call_case,
                                                                    fact_call_detail.ortho_volunteer_case,
                                                                    fact_call_detail.md_calc1,
                                                                    fact_call_detail.md_calculate_non_case_time,
                                                                    fact_call_detail.md_calculated_hours_field,
                                                                    fact_call_detail.md_casedeployment,
                                                                    fact_call_detail.md_case_coverage_12_hours,
                                                                    fact_call_detail.md_product_discussion,
                                                                    fact_call_detail.md_concurrent_call,
                                                                    fact_call_detail.courtesy_call,
                                                                    fact_call_detail.md_in_service,
                                                                    fact_call_detail.md_kol_course_discussion,
                                                                    fact_call_detail.kol_minutes,
                                                                    fact_call_detail.other_activities_time_12_hours,
                                                                    fact_call_detail.other_in_field_activities,
                                                                    fact_call_detail.md_overseas_workshop_visit,
                                                                    fact_call_detail.md_ra_activities2,
                                                                    fact_call_detail.sales_activity,
                                                                    fact_call_detail.sales_time_12_hours,
                                                                    fact_call_detail.time_spent,
                                                                    fact_call_detail.time_spent_on_other_activities_simp,
                                                                    fact_call_detail.time_spent_on_sales_activity,
                                                                    fact_call_detail.md_time_spent_on_a_call,
                                                                    fact_call_detail.md_case_type,
                                                                    fact_call_detail.md_sets_activities,
                                                                    fact_call_detail.md_time_spent_on_case,
                                                                    fact_call_detail.time_spent_on_other_activities,
                                                                    fact_call_detail.time_spent_per_call,
                                                                    fact_call_detail.md_case_conducted_in_hospital,
                                                                    fact_call_detail.calculated_field_2,
                                                                    fact_call_detail.calculated_hours_3,
                                                                    fact_call_detail.call_planned,
                                                                    fact_call_detail.call_submission_day,
                                                                    fact_call_detail.case_coverage,
                                                                    fact_call_detail.day_of_week,
                                                                    fact_call_detail.md_minutes,
                                                                    fact_call_detail.md_in_or_ot,
                                                                    fact_call_detail.md_d_call_type,
                                                                    fact_call_detail.md_hours,
                                                                    fact_call_detail.call_record_type_name,
                                                                    fact_call_detail.call_name,
                                                                    fact_call_detail.parent_call_name,
                                                                    fact_call_detail.submitted_by_mobile,
                                                                    fact_call_detail.call_source_id,
                                                                    fact_call_detail.product_indication_key,
                                                                    fact_call_detail.call_detail_priority,
                                                                    fact_call_detail.call_detail_type,
                                                                    fact_call_detail.call_discussion_record_type_source_id,
                                                                    fact_call_detail.comments,
                                                                    fact_call_detail.discussion_topics,
                                                                    fact_call_detail.discussion_type,
                                                                    fact_call_detail.call_discussion_type,
                                                                    fact_call_detail.effectiveness,
                                                                    fact_call_detail.follow_up_activity,
                                                                    fact_call_detail.outcomes,
                                                                    fact_call_detail.follow_up_additional_info,
                                                                    fact_call_detail.follow_up_date,
                                                                    fact_call_detail.materials_used,
                                                                    fact_call_detail.call_detail_source_id,
                                                                    fact_call_detail.call_detail_name,
                                                                    fact_call_detail.call_discussion_source_id,
                                                                    fact_call_detail.call_discussion_name,
                                                                    fact_call_detail.call_modify_dt,
                                                                    fact_call_detail.call_modify_id,
                                                                    fact_call_detail.call_detail_modify_dt,
                                                                    fact_call_detail.call_detail_modify_id,
                                                                    fact_call_detail.call_discussion_modify_dt,
                                                                    fact_call_detail.call_discussion_modify_id,
                                                                    fact_call_detail.inserted_date,
                                                                    fact_call_detail.updated_date
                                                                FROM fact_call_detail
                                                                WHERE (
                                                                        (
                                                                            (fact_call_detail.parent_call_flag = 1)
                                                                            AND ((fact_call_detail.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                                                                            )
                                                                        AND ((fact_call_detail.country_key)::TEXT <> ('ZZ'::CHARACTER VARYING)::TEXT)
                                                                        )
                                                                ) fact JOIN dim_date dimdate ON ((fact.call_date_key = dimdate.date_key))
                                                            ) JOIN vw_employee_hier hier ON (
                                                                (
                                                                    ((fact.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                                                    AND ((fact.country_key)::TEXT = (hier.country_code)::TEXT)
                                                                    )
                                                                )
                                                        ) LEFT JOIN dim_employee employee ON (((fact.employee_key)::TEXT = (employee.employee_key)::TEXT))
                                                    ) LEFT JOIN (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    SELECT 'SG'::CHARACTER VARYING AS country,
                                                                        ds.jnj_date_year,
                                                                        ds.jnj_date_month,
                                                                        count(*) AS working_days
                                                                    FROM dim_date ds
                                                                    WHERE (
                                                                            (
                                                                                (
                                                                                    (
                                                                                        NOT (
                                                                                            (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                                SELECT DISTINCT holiday_list.holiday_key
                                                                                                FROM holiday_list
                                                                                                WHERE ((holiday_list.country)::TEXT = ('SG'::CHARACTER VARYING)::TEXT)
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                                    )
                                                                                AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                            )
                                                                    GROUP BY 1,
                                                                        ds.jnj_date_year,
                                                                        ds.jnj_date_month
                                                                    
                                                                    UNION ALL
                                                                    
                                                                    SELECT 'MY'::CHARACTER VARYING AS country,
                                                                        ds.jnj_date_year,
                                                                        ds.jnj_date_month,
                                                                        count(*) AS working_days
                                                                    FROM dim_date ds
                                                                    WHERE (
                                                                            (
                                                                                (
                                                                                    (
                                                                                        NOT (
                                                                                            (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                                SELECT DISTINCT holiday_list.holiday_key
                                                                                                FROM holiday_list
                                                                                                WHERE ((holiday_list.country)::TEXT = ('MY'::CHARACTER VARYING)::TEXT)
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                                    )
                                                                                AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                            )
                                                                    GROUP BY 1,
                                                                        ds.jnj_date_year,
                                                                        ds.jnj_date_month
                                                                    )
                                                                
                                                                UNION ALL
                                                                
                                                                SELECT 'VN'::CHARACTER VARYING AS country,
                                                                    ds.jnj_date_year,
                                                                    ds.jnj_date_month,
                                                                    count(*) AS working_days
                                                                FROM dim_date ds
                                                                WHERE (
                                                                        (
                                                                            (
                                                                                (
                                                                                    NOT (
                                                                                        (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                            SELECT DISTINCT holiday_list.holiday_key
                                                                                            FROM holiday_list
                                                                                            WHERE ((holiday_list.country)::TEXT = ('VN'::CHARACTER VARYING)::TEXT)
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                            )
                                                                        AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                        )
                                                                GROUP BY 1,
                                                                    ds.jnj_date_year,
                                                                    ds.jnj_date_month
                                                                )
                                                            
                                                            UNION ALL
                                                            
                                                            SELECT 'TH'::CHARACTER VARYING AS country,
                                                                ds.jnj_date_year,
                                                                ds.jnj_date_month,
                                                                count(*) AS working_days
                                                            FROM dim_date ds
                                                            WHERE (
                                                                    (
                                                                        (
                                                                            (
                                                                                NOT (
                                                                                    (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                        SELECT DISTINCT holiday_list.holiday_key
                                                                                        FROM holiday_list
                                                                                        WHERE ((holiday_list.country)::TEXT = ('TH'::CHARACTER VARYING)::TEXT)
                                                                                        )
                                                                                    )
                                                                                )
                                                                            AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                            )
                                                                        AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                        )
                                                                    AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                    )
                                                            GROUP BY 1,
                                                                ds.jnj_date_year,
                                                                ds.jnj_date_month
                                                            )
                                                        
                                                        UNION ALL
                                                        
                                                        SELECT 'PH'::CHARACTER VARYING AS country,
                                                            ds.jnj_date_year,
                                                            ds.jnj_date_month,
                                                            count(*) AS working_days
                                                        FROM dim_date ds
                                                        WHERE (
                                                                (
                                                                    (
                                                                        (
                                                                            NOT (
                                                                                (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                    SELECT DISTINCT holiday_list.holiday_key
                                                                                    FROM holiday_list
                                                                                    WHERE ((holiday_list.country)::TEXT = ('PH'::CHARACTER VARYING)::TEXT)
                                                                                    )
                                                                                )
                                                                            )
                                                                        AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                        )
                                                                    AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                    )
                                                                AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                )
                                                        GROUP BY 1,
                                                            ds.jnj_date_year,
                                                            ds.jnj_date_month
                                                        )
                                                    
                                                    UNION ALL
                                                    
                                                    SELECT 'ID'::CHARACTER VARYING AS country,
                                                        ds.jnj_date_year,
                                                        ds.jnj_date_month,
                                                        count(*) AS working_days
                                                    FROM dim_date ds
                                                    WHERE (
                                                            (
                                                                (
                                                                    (
                                                                        NOT (
                                                                            (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                SELECT DISTINCT holiday_list.holiday_key
                                                                                FROM holiday_list
                                                                                WHERE ((holiday_list.country)::TEXT = ('ID'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            )
                                                                        )
                                                                    AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                    )
                                                                AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                )
                                                            AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                            )
                                                    GROUP BY 1,
                                                        ds.jnj_date_year,
                                                        ds.jnj_date_month
                                                    ) ds ON (
                                                        (
                                                            (
                                                                (((dimdate.jnj_date_year)::CHARACTER VARYING)::TEXT = ((ds.jnj_date_year)::CHARACTER VARYING)::TEXT)
                                                                AND ((dimdate.jnj_date_month)::TEXT = (ds.jnj_date_month)::TEXT)
                                                                )
                                                            AND ((fact.country_key)::TEXT = (ds.country)::TEXT)
                                                            )
                                                        )
                                                )
                                            GROUP BY dimdate.jnj_date_year,
                                                dimdate.jnj_date_month,
                                                dimdate.jnj_date_quarter,
                                                fact.country_key,
                                                hier.employee_name,
                                                fact.employee_key,
                                                hier.l1_username,
                                                hier.sector,
                                                hier.l2_manager_name,
                                                employee.organization_l3_name
                                            ) fact
                                        ) src1 LEFT JOIN (
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
                                        ) actve_usr ON (
                                            (
                                                (
                                                    (
                                                        (src1.jnj_date_year = actve_usr.year)
                                                        AND ((src1.jnj_date_month)::TEXT = ((actve_usr.month)::CHARACTER VARYING)::TEXT)
                                                        )
                                                    AND ((src1.country)::TEXT = (actve_usr.country_code)::TEXT)
                                                    )
                                                AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(actve_usr.sector, '#'::CHARACTER VARYING))::TEXT)
                                                )
                                            )
                                    ) LEFT JOIN (
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
                                    ) emp1 ON (
                                        (
                                            ((src1.country)::TEXT = (emp1.country_code)::TEXT)
                                            AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(emp1.sector, '#'::CHARACTER VARYING))::TEXT)
                                            )
                                        )
                                ) LEFT JOIN (
                                SELECT count(1) AS total_cnt_clm_flg,
                                    fct.employee_key,
                                    dimdate.jnj_date_year AS yr,
                                    CASE 
                                        WHEN (
                                                ((dimdate.jnj_date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.jnj_date_month IS NULL)
                                                    AND ('Jan' IS NULL)
                                                    )
                                                )
                                            THEN '1'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.jnj_date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.jnj_date_month IS NULL)
                                                    AND ('Feb' IS NULL)
                                                    )
                                                )
                                            THEN '2'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.jnj_date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.jnj_date_month IS NULL)
                                                    AND ('Mar' IS NULL)
                                                    )
                                                )
                                            THEN '3'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.jnj_date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.jnj_date_month IS NULL)
                                                    AND ('Apr' IS NULL)
                                                    )
                                                )
                                            THEN '4'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.jnj_date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.jnj_date_month IS NULL)
                                                    AND ('May' IS NULL)
                                                    )
                                                )
                                            THEN '5'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.jnj_date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.jnj_date_month IS NULL)
                                                    AND ('Jun' IS NULL)
                                                    )
                                                )
                                            THEN '6'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.jnj_date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.jnj_date_month IS NULL)
                                                    AND ('Jul' IS NULL)
                                                    )
                                                )
                                            THEN '7'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.jnj_date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.jnj_date_month IS NULL)
                                                    AND ('Aug' IS NULL)
                                                    )
                                                )
                                            THEN '8'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.jnj_date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.jnj_date_month IS NULL)
                                                    AND ('Sep' IS NULL)
                                                    )
                                                )
                                            THEN '9'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.jnj_date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.jnj_date_month IS NULL)
                                                    AND ('Oct' IS NULL)
                                                    )
                                                )
                                            THEN '10'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.jnj_date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.jnj_date_month IS NULL)
                                                    AND ('Nov' IS NULL)
                                                    )
                                                )
                                            THEN '11'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.jnj_date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.jnj_date_month IS NULL)
                                                    AND ('Dec' IS NULL)
                                                    )
                                                )
                                            THEN '12'::CHARACTER VARYING
                                        ELSE dimdate.jnj_date_month
                                        END AS mnth,
                                    fct.country_key,
                                    hier.l2_manager_name,
                                    hier.sector,
                                    employee.organization_l3_name
                                FROM (
                                    (
                                        (
                                            fact_call_detail fct JOIN dim_date dimdate ON ((fct.call_date_key = dimdate.date_key))
                                            ) JOIN vw_employee_hier hier ON (
                                                (
                                                    ((fct.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                                    AND ((fct.country_key)::TEXT = (hier.country_code)::TEXT)
                                                    )
                                                )
                                        ) LEFT JOIN dim_employee employee ON (((fct.employee_key)::TEXT = (employee.employee_key)::TEXT))
                                    )
                                WHERE (
                                        (
                                            (fct.call_clm_flag = 1)
                                            AND (fct.parent_call_flag = 1)
                                            )
                                        AND ((fct.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                                        )
                                GROUP BY fct.employee_key,
                                    dimdate.jnj_date_year,
                                    dimdate.jnj_date_month,
                                    fct.country_key,
                                    hier.l2_manager_name,
                                    hier.sector,
                                    employee.organization_l3_name
                                ) fct1 ON (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            ((src1.employee_key)::TEXT = (fct1.employee_key)::TEXT)
                                                            AND (src1.jnj_date_year = fct1.yr)
                                                            )
                                                        AND ((src1.jnj_date_month)::TEXT = (fct1.mnth)::TEXT)
                                                        )
                                                    AND ((fct1.country_key)::TEXT = (src1.country)::TEXT)
                                                    )
                                                AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct1.sector, '#'::CHARACTER VARYING))::TEXT)
                                                )
                                            AND ((COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT)
                                            )
                                        AND ((COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct1.l2_manager_name, '#'::CHARACTER VARYING))::TEXT)
                                        )
                                    )
                            ) LEFT JOIN (
                            SELECT count(1) AS total_prnt_cnt_clm_flg,
                                fct.employee_key,
                                dimdate.jnj_date_year AS yr,
                                CASE 
                                    WHEN (
                                            ((dimdate.jnj_date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.jnj_date_month IS NULL)
                                                AND ('Jan' IS NULL)
                                                )
                                            )
                                        THEN '1'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.jnj_date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.jnj_date_month IS NULL)
                                                AND ('Feb' IS NULL)
                                                )
                                            )
                                        THEN '2'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.jnj_date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.jnj_date_month IS NULL)
                                                AND ('Mar' IS NULL)
                                                )
                                            )
                                        THEN '3'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.jnj_date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.jnj_date_month IS NULL)
                                                AND ('Apr' IS NULL)
                                                )
                                            )
                                        THEN '4'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.jnj_date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.jnj_date_month IS NULL)
                                                AND ('May' IS NULL)
                                                )
                                            )
                                        THEN '5'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.jnj_date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.jnj_date_month IS NULL)
                                                AND ('Jun' IS NULL)
                                                )
                                            )
                                        THEN '6'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.jnj_date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.jnj_date_month IS NULL)
                                                AND ('Jul' IS NULL)
                                                )
                                            )
                                        THEN '7'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.jnj_date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.jnj_date_month IS NULL)
                                                AND ('Aug' IS NULL)
                                                )
                                            )
                                        THEN '8'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.jnj_date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.jnj_date_month IS NULL)
                                                AND ('Sep' IS NULL)
                                                )
                                            )
                                        THEN '9'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.jnj_date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.jnj_date_month IS NULL)
                                                AND ('Oct' IS NULL)
                                                )
                                            )
                                        THEN '10'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.jnj_date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.jnj_date_month IS NULL)
                                                AND ('Nov' IS NULL)
                                                )
                                            )
                                        THEN '11'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.jnj_date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.jnj_date_month IS NULL)
                                                AND ('Dec' IS NULL)
                                                )
                                            )
                                        THEN '12'::CHARACTER VARYING
                                    ELSE dimdate.jnj_date_month
                                    END AS mnth,
                                fct.country_key,
                                hier.l2_manager_name,
                                hier.sector,
                                employee.organization_l3_name
                            FROM (
                                (
                                    (
                                        fact_call_detail fct JOIN dim_date dimdate ON ((fct.call_date_key = dimdate.date_key))
                                        ) JOIN vw_employee_hier hier ON (
                                            (
                                                ((fct.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                                AND ((fct.country_key)::TEXT = (hier.country_code)::TEXT)
                                                )
                                            )
                                    ) LEFT JOIN dim_employee employee ON (((fct.employee_key)::TEXT = (employee.employee_key)::TEXT))
                                )
                            WHERE (
                                    (fct.parent_call_flag = 1)
                                    AND ((fct.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                                    )
                            GROUP BY fct.employee_key,
                                dimdate.jnj_date_year,
                                dimdate.jnj_date_month,
                                fct.country_key,
                                hier.l2_manager_name,
                                hier.sector,
                                employee.organization_l3_name
                            ) fct2 ON (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        ((src1.employee_key)::TEXT = (fct2.employee_key)::TEXT)
                                                        AND (src1.jnj_date_year = fct2.yr)
                                                        )
                                                    AND ((src1.jnj_date_month)::TEXT = (fct2.mnth)::TEXT)
                                                    )
                                                AND ((fct2.country_key)::TEXT = (src1.country)::TEXT)
                                                )
                                            AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct2.sector, '#'::CHARACTER VARYING))::TEXT)
                                            )
                                        AND ((COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct2.organization_l3_name, '#'::CHARACTER VARYING))::TEXT)
                                        )
                                    AND ((COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct2.l2_manager_name, '#'::CHARACTER VARYING))::TEXT)
                                    )
                                )
                        ) LEFT JOIN (
                        SELECT (sum(COALESCE(terr.sea_frml_hours_on, ((0)::NUMERIC)::NUMERIC(18, 0))) / ((8)::NUMERIC)::NUMERIC(18, 0)) AS cnt_total_time_on,
                            terr.employee_key,
                            terr.country_key,
                            date_dim.jnj_date_year AS yr,
                            CASE 
                                WHEN (
                                        ((date_dim.jnj_date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.jnj_date_month IS NULL)
                                            AND ('Jan' IS NULL)
                                            )
                                        )
                                    THEN '1'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.jnj_date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.jnj_date_month IS NULL)
                                            AND ('Feb' IS NULL)
                                            )
                                        )
                                    THEN '2'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.jnj_date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.jnj_date_month IS NULL)
                                            AND ('Mar' IS NULL)
                                            )
                                        )
                                    THEN '3'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.jnj_date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.jnj_date_month IS NULL)
                                            AND ('Apr' IS NULL)
                                            )
                                        )
                                    THEN '4'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.jnj_date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.jnj_date_month IS NULL)
                                            AND ('May' IS NULL)
                                            )
                                        )
                                    THEN '5'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.jnj_date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.jnj_date_month IS NULL)
                                            AND ('Jun' IS NULL)
                                            )
                                        )
                                    THEN '6'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.jnj_date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.jnj_date_month IS NULL)
                                            AND ('Jul' IS NULL)
                                            )
                                        )
                                    THEN '7'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.jnj_date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.jnj_date_month IS NULL)
                                            AND ('Aug' IS NULL)
                                            )
                                        )
                                    THEN '8'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.jnj_date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.jnj_date_month IS NULL)
                                            AND ('Sep' IS NULL)
                                            )
                                        )
                                    THEN '9'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.jnj_date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.jnj_date_month IS NULL)
                                            AND ('Oct' IS NULL)
                                            )
                                        )
                                    THEN '10'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.jnj_date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.jnj_date_month IS NULL)
                                            AND ('Nov' IS NULL)
                                            )
                                        )
                                    THEN '11'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.jnj_date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.jnj_date_month IS NULL)
                                            AND ('Dec' IS NULL)
                                            )
                                        )
                                    THEN '12'::CHARACTER VARYING
                                ELSE date_dim.jnj_date_month
                                END AS mnth
                        FROM (
                            fact_timeoff_territory terr JOIN dim_date date_dim ON (((terr.start_date_key)::TEXT = ((date_dim.date_key)::CHARACTER VARYING)::TEXT))
                            )
                        WHERE (
                                ((terr.sea_time_on_time_off)::TEXT = ('Time On'::CHARACTER VARYING)::TEXT)
                                AND ((terr.start_date_key)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                )
                        GROUP BY terr.employee_key,
                            terr.country_key,
                            date_dim.jnj_date_year,
                            date_dim.jnj_date_month
                        ) terr ON (
                            (
                                (
                                    (
                                        ((terr.employee_key)::TEXT = (src1.employee_key)::TEXT)
                                        AND ((terr.country_key)::TEXT = (src1.country)::TEXT)
                                        )
                                    AND (src1.jnj_date_year = terr.yr)
                                    )
                                AND ((src1.jnj_date_month)::TEXT = (terr.mnth)::TEXT)
                                )
                            )
                    ) LEFT JOIN (
                    SELECT (sum(COALESCE(terr.total_time_off, ((0)::NUMERIC)::NUMERIC(18, 0))) / ((8)::NUMERIC)::NUMERIC(18, 0)) AS cnt_total_time_off,
                        terr.employee_key,
                        terr.country_key,
                        date_dim.jnj_date_year AS yr,
                        CASE 
                            WHEN (
                                    ((date_dim.jnj_date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.jnj_date_month IS NULL)
                                        AND ('Jan' IS NULL)
                                        )
                                    )
                                THEN '1'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.jnj_date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.jnj_date_month IS NULL)
                                        AND ('Feb' IS NULL)
                                        )
                                    )
                                THEN '2'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.jnj_date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.jnj_date_month IS NULL)
                                        AND ('Mar' IS NULL)
                                        )
                                    )
                                THEN '3'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.jnj_date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.jnj_date_month IS NULL)
                                        AND ('Apr' IS NULL)
                                        )
                                    )
                                THEN '4'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.jnj_date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.jnj_date_month IS NULL)
                                        AND ('May' IS NULL)
                                        )
                                    )
                                THEN '5'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.jnj_date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.jnj_date_month IS NULL)
                                        AND ('Jun' IS NULL)
                                        )
                                    )
                                THEN '6'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.jnj_date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.jnj_date_month IS NULL)
                                        AND ('Jul' IS NULL)
                                        )
                                    )
                                THEN '7'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.jnj_date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.jnj_date_month IS NULL)
                                        AND ('Aug' IS NULL)
                                        )
                                    )
                                THEN '8'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.jnj_date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.jnj_date_month IS NULL)
                                        AND ('Sep' IS NULL)
                                        )
                                    )
                                THEN '9'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.jnj_date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.jnj_date_month IS NULL)
                                        AND ('Oct' IS NULL)
                                        )
                                    )
                                THEN '10'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.jnj_date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.jnj_date_month IS NULL)
                                        AND ('Nov' IS NULL)
                                        )
                                    )
                                THEN '11'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.jnj_date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.jnj_date_month IS NULL)
                                        AND ('Dec' IS NULL)
                                        )
                                    )
                                THEN '12'::CHARACTER VARYING
                            ELSE date_dim.jnj_date_month
                            END AS mnth
                    FROM (
                        fact_timeoff_territory terr JOIN dim_date date_dim ON (((terr.start_date_key)::TEXT = ((date_dim.date_key)::CHARACTER VARYING)::TEXT))
                        )
                    WHERE (
                            ((terr.sea_time_on_time_off)::TEXT = ('Time Off'::CHARACTER VARYING)::TEXT)
                            AND ((terr.start_date_key)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                            )
                    GROUP BY terr.employee_key,
                        terr.country_key,
                        date_dim.jnj_date_year,
                        date_dim.jnj_date_month
                    ) terr1 ON (
                        (
                            (
                                (
                                    ((terr1.employee_key)::TEXT = (src1.employee_key)::TEXT)
                                    AND ((terr1.country_key)::TEXT = (src1.country)::TEXT)
                                    )
                                AND (src1.jnj_date_year = terr1.yr)
                                )
                            AND ((src1.jnj_date_month)::TEXT = (terr1.mnth)::TEXT)
                            )
                        )
                ) LEFT JOIN (
                SELECT count(DISTINCT fct.call_name) AS total_sbmtd_calls_key_message,
                    fct.employee_key,
                    dimdate.jnj_date_year AS yr,
                    CASE 
                        WHEN (
                                ((dimdate.jnj_date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.jnj_date_month IS NULL)
                                    AND ('Jan' IS NULL)
                                    )
                                )
                            THEN '1'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.jnj_date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.jnj_date_month IS NULL)
                                    AND ('Feb' IS NULL)
                                    )
                                )
                            THEN '2'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.jnj_date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.jnj_date_month IS NULL)
                                    AND ('Mar' IS NULL)
                                    )
                                )
                            THEN '3'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.jnj_date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.jnj_date_month IS NULL)
                                    AND ('Apr' IS NULL)
                                    )
                                )
                            THEN '4'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.jnj_date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.jnj_date_month IS NULL)
                                    AND ('May' IS NULL)
                                    )
                                )
                            THEN '5'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.jnj_date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.jnj_date_month IS NULL)
                                    AND ('Jun' IS NULL)
                                    )
                                )
                            THEN '6'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.jnj_date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.jnj_date_month IS NULL)
                                    AND ('Jul' IS NULL)
                                    )
                                )
                            THEN '7'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.jnj_date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.jnj_date_month IS NULL)
                                    AND ('Aug' IS NULL)
                                    )
                                )
                            THEN '8'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.jnj_date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.jnj_date_month IS NULL)
                                    AND ('Sep' IS NULL)
                                    )
                                )
                            THEN '9'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.jnj_date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.jnj_date_month IS NULL)
                                    AND ('Oct' IS NULL)
                                    )
                                )
                            THEN '10'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.jnj_date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.jnj_date_month IS NULL)
                                    AND ('Nov' IS NULL)
                                    )
                                )
                            THEN '11'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.jnj_date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.jnj_date_month IS NULL)
                                    AND ('Dec' IS NULL)
                                    )
                                )
                            THEN '12'::CHARACTER VARYING
                        ELSE dimdate.jnj_date_month
                        END AS mnth,
                    fct.country_key,
                    hier.l2_manager_name,
                    hier.sector,
                    employee.organization_l3_name
                FROM (
                    (
                        (
                            (
                                fact_call_detail fct JOIN dim_date dimdate ON ((fct.call_date_key = dimdate.date_key))
                                ) JOIN fact_call_key_message fckm ON (((fct.call_source_id)::TEXT = (fckm.call_source_id)::TEXT))
                            ) JOIN vw_employee_hier hier ON (
                                (
                                    ((fct.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                    AND ((fct.country_key)::TEXT = (hier.country_code)::TEXT)
                                    )
                                )
                        ) LEFT JOIN dim_employee employee ON (((fct.employee_key)::TEXT = (employee.employee_key)::TEXT))
                    )
                WHERE (
                        (fct.parent_call_flag = 1)
                        AND ((fct.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                        )
                GROUP BY fct.employee_key,
                    dimdate.jnj_date_year,
                    dimdate.jnj_date_month,
                    fct.country_key,
                    hier.l2_manager_name,
                    hier.sector,
                    employee.organization_l3_name
                ) fct3 ON (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            ((src1.employee_key)::TEXT = (fct3.employee_key)::TEXT)
                                            AND (src1.jnj_date_year = fct3.yr)
                                            )
                                        AND ((src1.jnj_date_month)::TEXT = (fct3.mnth)::TEXT)
                                        )
                                    AND ((fct3.country_key)::TEXT = (src1.country)::TEXT)
                                    )
                                AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct3.sector, '#'::CHARACTER VARYING))::TEXT)
                                )
                            AND ((COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct3.organization_l3_name, '#'::CHARACTER VARYING))::TEXT)
                            )
                        AND ((COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct3.l2_manager_name, '#'::CHARACTER VARYING))::TEXT)
                        )
                    )
            ) LEFT JOIN (
            SELECT count(1) AS total_call_product,
                fct.employee_key,
                dimdate.jnj_date_year AS yr,
                CASE 
                    WHEN (
                            ((dimdate.jnj_date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.jnj_date_month IS NULL)
                                AND ('Jan' IS NULL)
                                )
                            )
                        THEN '1'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.jnj_date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.jnj_date_month IS NULL)
                                AND ('Feb' IS NULL)
                                )
                            )
                        THEN '2'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.jnj_date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.jnj_date_month IS NULL)
                                AND ('Mar' IS NULL)
                                )
                            )
                        THEN '3'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.jnj_date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.jnj_date_month IS NULL)
                                AND ('Apr' IS NULL)
                                )
                            )
                        THEN '4'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.jnj_date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.jnj_date_month IS NULL)
                                AND ('May' IS NULL)
                                )
                            )
                        THEN '5'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.jnj_date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.jnj_date_month IS NULL)
                                AND ('Jun' IS NULL)
                                )
                            )
                        THEN '6'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.jnj_date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.jnj_date_month IS NULL)
                                AND ('Jul' IS NULL)
                                )
                            )
                        THEN '7'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.jnj_date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.jnj_date_month IS NULL)
                                AND ('Aug' IS NULL)
                                )
                            )
                        THEN '8'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.jnj_date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.jnj_date_month IS NULL)
                                AND ('Sep' IS NULL)
                                )
                            )
                        THEN '9'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.jnj_date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.jnj_date_month IS NULL)
                                AND ('Oct' IS NULL)
                                )
                            )
                        THEN '10'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.jnj_date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.jnj_date_month IS NULL)
                                AND ('Nov' IS NULL)
                                )
                            )
                        THEN '11'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.jnj_date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.jnj_date_month IS NULL)
                                AND ('Dec' IS NULL)
                                )
                            )
                        THEN '12'::CHARACTER VARYING
                    ELSE dimdate.jnj_date_month
                    END AS mnth,
                fct.country_key,
                hier.l2_manager_name,
                hier.sector,
                employee.organization_l3_name
            FROM (
                (
                    (
                        fact_call_detail fct JOIN dim_date dimdate ON ((fct.call_date_key = dimdate.date_key))
                        ) JOIN vw_employee_hier hier ON (
                            (
                                ((fct.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                AND ((fct.country_key)::TEXT = (hier.country_code)::TEXT)
                                )
                            )
                    ) LEFT JOIN dim_employee employee ON (((fct.employee_key)::TEXT = (employee.employee_key)::TEXT))
                )
            WHERE (
                    (
                        (fct.parent_call_flag = 1)
                        AND ((fct.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                        )
                    AND (
                        (fct.detailed_products IS NOT NULL)
                        OR (
                            ((fct.detailed_products)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                            AND ((fct.detailed_products)::TEXT <> (' '::CHARACTER VARYING)::TEXT)
                            )
                        )
                    )
            GROUP BY fct.employee_key,
                dimdate.jnj_date_year,
                dimdate.jnj_date_month,
                fct.country_key,
                hier.l2_manager_name,
                hier.sector,
                employee.organization_l3_name
            ) fct4 ON (
                (
                    (
                        (
                            (
                                (
                                    (
                                        ((src1.employee_key)::TEXT = (fct4.employee_key)::TEXT)
                                        AND (src1.jnj_date_year = fct4.yr)
                                        )
                                    AND ((src1.jnj_date_month)::TEXT = (fct4.mnth)::TEXT)
                                    )
                                AND ((fct4.country_key)::TEXT = (src1.country)::TEXT)
                                )
                            AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct4.sector, '#'::CHARACTER VARYING))::TEXT)
                            )
                        AND ((COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct4.organization_l3_name, '#'::CHARACTER VARYING))::TEXT)
                        )
                    AND ((COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct4.l2_manager_name, '#'::CHARACTER VARYING))::TEXT)
                    )
                )
        )
    
    UNION ALL
    
    SELECT NULL AS jnj_year,
        NULL AS jnj_month,
        NULL AS jnj_quarter,
        NULL AS date_year,
        NULL AS date_month,
        NULL AS date_quarter,
        src1.my_date_year AS my_year,
        src1.my_date_month::CHARACTER VARYING AS my_month,
        src1.my_date_quarter AS my_quarter,
        src1.country,
        src1.sector,
        (src1.l3_wwid)::CHARACTER VARYING AS l3_wwid,
        (src1.l3_username)::CHARACTER VARYING AS l3_username,
        src1.l3_manager_name,
        (src1.l2_wwid)::CHARACTER VARYING AS l2_wwid,
        (src1.l2_username)::CHARACTER VARYING AS l2_username,
        (src1.l2_manager_name)::CHARACTER VARYING AS l2_manager_name,
        (src1.l1_wwid)::CHARACTER VARYING AS l1_wwid,
        (src1.l1_username)::CHARACTER VARYING AS l1_username,
        (src1.l1_manager_name)::CHARACTER VARYING AS l1_manager_name,
        (src1.organization_l1_name)::CHARACTER VARYING AS organization_l1_name,
        (src1.organization_l2_name)::CHARACTER VARYING AS organization_l2_name,
        src1.organization_l3_name,
        (src1.organization_l4_name)::CHARACTER VARYING AS organization_l4_name,
        (src1.organization_l5_name)::CHARACTER VARYING AS organization_l5_name,
        src1.sales_rep,
        src1.working_days,
        src1.total_cnt_call,
        src1.total_cnt_edetailing_calls,
        src1.total_cnt_call_delay,
        (src1.sales_rep_ntid)::CHARACTER VARYING AS sales_rep_ntid,
        src1.total_cnt_call_delay AS total_cnt_call_delay_sub,
        fct1.total_cnt_clm_flg,
        fct2.total_prnt_cnt_clm_flg,
        src1.total_cnt_submitted_calls,
        terr.cnt_total_time_on,
        terr1.cnt_total_time_off,
        CASE 
            WHEN (((((src1.my_date_year)::CHARACTER VARYING)::TEXT || ('-'::CHARACTER VARYING)::TEXT) || ((src1.my_date_month)::CHARACTER VARYING)::TEXT) = ((((date_part(year, sysdate()))::CHARACTER VARYING(5))::TEXT || ('-'::CHARACTER VARYING)::TEXT) || ((date_part(month, sysdate()))::CHARACTER VARYING(5))::TEXT))
                THEN emp1.current_mnth_emp_cnt
            ELSE actve_usr.total_active
            END AS total_active,
        fct4.total_call_product AS detailed_products,
        fct3.total_sbmtd_calls_key_message
    FROM (
        (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        SELECT fact.my_date_year,
                                            fact.my_date_month,
                                            fact.my_date_quarter,
                                            fact.country,
                                            fact.sector,
                                            fact.l3_wwid,
                                            fact.l3_username,
                                            fact.l3_manager_name,
                                            fact.l2_wwid,
                                            fact.l2_username,
                                            fact.l2_manager_name,
                                            fact.l1_wwid,
                                            fact.l1_username,
                                            fact.l1_manager_name,
                                            fact.organization_l1_name,
                                            fact.organization_l2_name,
                                            fact.organization_l3_name,
                                            fact.organization_l4_name,
                                            fact.organization_l5_name,
                                            fact.sales_rep,
                                            fact.working_days,
                                            fact.total_cnt_call,
                                            fact.total_cnt_edetailing_calls,
                                            fact.total_cnt_call_delay,
                                            fact.sales_rep_ntid,
                                            fact.employee_key,
                                            fact.total_cnt_submitted_calls
                                        FROM (
                                            SELECT dimdate.my_date_year,
                                                CASE 
                                                    WHEN (
                                                            (((dimdate.my_date_month)::CHARACTER VARYING)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_month IS NULL)
                                                                AND ('Jan' IS NULL)
                                                                )
                                                            )
                                                        THEN ('1'::CHARACTER VARYING)::CHAR
                                                    WHEN (
                                                            (((dimdate.my_date_month)::CHARACTER VARYING)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_month IS NULL)
                                                                AND ('Feb' IS NULL)
                                                                )
                                                            )
                                                        THEN ('2'::CHARACTER VARYING)::CHAR(256)
                                                    WHEN (
                                                            (((dimdate.my_date_month)::CHARACTER VARYING)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_month IS NULL)
                                                                AND ('Mar' IS NULL)
                                                                )
                                                            )
                                                        THEN ('3'::CHARACTER VARYING)::CHAR(256)
                                                    WHEN (
                                                            (((dimdate.my_date_month)::CHARACTER VARYING)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_month IS NULL)
                                                                AND ('Apr' IS NULL)
                                                                )
                                                            )
                                                        THEN ('4'::CHARACTER VARYING)::CHAR(256)
                                                    WHEN (
                                                            (((dimdate.my_date_month)::CHARACTER VARYING)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_month IS NULL)
                                                                AND ('May' IS NULL)
                                                                )
                                                            )
                                                        THEN ('5'::CHARACTER VARYING)::CHAR(256)
                                                    WHEN (
                                                            (((dimdate.my_date_month)::CHARACTER VARYING)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_month IS NULL)
                                                                AND ('Jun' IS NULL)
                                                                )
                                                            )
                                                        THEN ('6'::CHARACTER VARYING)::CHAR(256)
                                                    WHEN (
                                                            (((dimdate.my_date_month)::CHARACTER VARYING)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_month IS NULL)
                                                                AND ('Jul' IS NULL)
                                                                )
                                                            )
                                                        THEN ('7'::CHARACTER VARYING)::CHAR(256)
                                                    WHEN (
                                                            (((dimdate.my_date_month)::CHARACTER VARYING)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_month IS NULL)
                                                                AND ('Aug' IS NULL)
                                                                )
                                                            )
                                                        THEN ('8'::CHARACTER VARYING)::CHAR(256)
                                                    WHEN (
                                                            (((dimdate.my_date_month)::CHARACTER VARYING)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_month IS NULL)
                                                                AND ('Sep' IS NULL)
                                                                )
                                                            )
                                                        THEN ('9'::CHARACTER VARYING)::CHAR(256)
                                                    WHEN (
                                                            (((dimdate.my_date_month)::CHARACTER VARYING)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_month IS NULL)
                                                                AND ('Oct' IS NULL)
                                                                )
                                                            )
                                                        THEN ('10'::CHARACTER VARYING)::CHAR(256)
                                                    WHEN (
                                                            (((dimdate.my_date_month)::CHARACTER VARYING)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_month IS NULL)
                                                                AND ('Nov' IS NULL)
                                                                )
                                                            )
                                                        THEN ('11'::CHARACTER VARYING)::CHAR(256)
                                                    WHEN (
                                                            (((dimdate.my_date_month)::CHARACTER VARYING)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_month IS NULL)
                                                                AND ('Dec' IS NULL)
                                                                )
                                                            )
                                                        THEN ('12'::CHARACTER VARYING)::CHAR(256)
                                                    ELSE dimdate.my_date_month
                                                    END AS my_date_month,
                                                CASE 
                                                    WHEN (
                                                            (((dimdate.my_date_quarter)::CHARACTER VARYING)::TEXT = ('Q1'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_quarter IS NULL)
                                                                AND ('Q1' IS NULL)
                                                                )
                                                            )
                                                        THEN '1'::CHARACTER VARYING
                                                    WHEN (
                                                            (((dimdate.my_date_quarter)::CHARACTER VARYING)::TEXT = ('Q2'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_quarter IS NULL)
                                                                AND ('Q2' IS NULL)
                                                                )
                                                            )
                                                        THEN '2'::CHARACTER VARYING
                                                    WHEN (
                                                            (((dimdate.my_date_quarter)::CHARACTER VARYING)::TEXT = ('Q3'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_quarter IS NULL)
                                                                AND ('Q3' IS NULL)
                                                                )
                                                            )
                                                        THEN '3'::CHARACTER VARYING
                                                    WHEN (
                                                            (((dimdate.my_date_quarter)::CHARACTER VARYING)::TEXT = ('Q4'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.my_date_quarter IS NULL)
                                                                AND ('Q4' IS NULL)
                                                                )
                                                            )
                                                        THEN '4'::CHARACTER VARYING
                                                    ELSE NULL::CHARACTER VARYING
                                                    END AS my_date_quarter,
                                                fact.country_key AS country,
                                                hier.sector,
                                                "max" ((hier.l2_wwid)::TEXT) AS l3_wwid,
                                                "max" (hier.l2_username) AS l3_username,
                                                hier.l2_manager_name AS l3_manager_name,
                                                "max" ((hier.l3_wwid)::TEXT) AS l2_wwid,
                                                "max" (hier.l3_username) AS l2_username,
                                                "max" ((hier.l3_manager_name)::TEXT) AS l2_manager_name,
                                                "max" ((hier.l4_wwid)::TEXT) AS l1_wwid,
                                                "max" (hier.l4_username) AS l1_username,
                                                "max" ((hier.l4_manager_name)::TEXT) AS l1_manager_name,
                                                "max" ((employee.organization_l1_name)::TEXT) AS organization_l1_name,
                                                "max" ((employee.organization_l2_name)::TEXT) AS organization_l2_name,
                                                employee.organization_l3_name,
                                                "max" ((employee.organization_l4_name)::TEXT) AS organization_l4_name,
                                                "max" ((employee.organization_l5_name)::TEXT) AS organization_l5_name,
                                                hier.employee_name AS sales_rep,
                                                "max" (((ds.working_days)::NUMERIC)::NUMERIC(18, 0)) AS working_days,
                                                count(fact.call_source_id) AS total_cnt_call,
                                                count(fact.call_clm_flag) AS total_cnt_edetailing_calls,
                                                sum(fact.call_submission_day) AS total_cnt_call_delay,
                                                fact.employee_key,
                                                hier.l1_username AS sales_rep_ntid,
                                                count(1) AS total_cnt_submitted_calls
                                            FROM (
                                                (
                                                    (
                                                        (
                                                            (
                                                                SELECT fact_call_detail.country_key,
                                                                    fact_call_detail.hcp_key,
                                                                    fact_call_detail.hco_key,
                                                                    fact_call_detail.employee_key,
                                                                    fact_call_detail.profile_key,
                                                                    fact_call_detail.organization_key,
                                                                    fact_call_detail.call_date_key,
                                                                    fact_call_detail.call_date_time,
                                                                    fact_call_detail.call_entry_datetime,
                                                                    fact_call_detail.call_mobile_last_modified_date_time,
                                                                    fact_call_detail.utc_call_date_time,
                                                                    fact_call_detail.utc_call_entry_time,
                                                                    fact_call_detail.call_status_type,
                                                                    fact_call_detail.call_channel_type,
                                                                    fact_call_detail.call_type,
                                                                    fact_call_detail.call_duration,
                                                                    fact_call_detail.call_clm_flag,
                                                                    fact_call_detail.parent_call_flag,
                                                                    fact_call_detail.attendee_type,
                                                                    fact_call_detail.call_comments,
                                                                    fact_call_detail.next_call_notes,
                                                                    fact_call_detail.pre_call_notes,
                                                                    fact_call_detail.manager_call_comment,
                                                                    fact_call_detail.last_activity_date,
                                                                    fact_call_detail.sample_card,
                                                                    fact_call_detail.account_plan,
                                                                    fact_call_detail.mobile_id,
                                                                    fact_call_detail.significant_event,
                                                                    fact_call_detail.location,
                                                                    fact_call_detail.signature_date,
                                                                    fact_call_detail.signature,
                                                                    fact_call_detail.territory,
                                                                    fact_call_detail.attendees,
                                                                    fact_call_detail.parent_call_source_id,
                                                                    fact_call_detail.user_name,
                                                                    fact_call_detail.medical_event,
                                                                    fact_call_detail.is_sampled_call,
                                                                    fact_call_detail.presentations,
                                                                    fact_call_detail.product_priority_1,
                                                                    fact_call_detail.product_priority_2,
                                                                    fact_call_detail.product_priority_3,
                                                                    fact_call_detail.product_priority_4,
                                                                    fact_call_detail.product_priority_5,
                                                                    fact_call_detail.attendee_list,
                                                                    fact_call_detail.msl_interaction_notes,
                                                                    fact_call_detail.sea_call_type,
                                                                    fact_call_detail.interaction_mode,
                                                                    fact_call_detail.hcp_kol_initiated,
                                                                    fact_call_detail.msl_interaction_type,
                                                                    fact_call_detail.call_objective,
                                                                    fact_call_detail.submission_delay,
                                                                    fact_call_detail.region,
                                                                    fact_call_detail.md_hsp_admin,
                                                                    fact_call_detail.hsp_minutes,
                                                                    fact_call_detail.ortho_on_call_case,
                                                                    fact_call_detail.ortho_volunteer_case,
                                                                    fact_call_detail.md_calc1,
                                                                    fact_call_detail.md_calculate_non_case_time,
                                                                    fact_call_detail.md_calculated_hours_field,
                                                                    fact_call_detail.md_casedeployment,
                                                                    fact_call_detail.md_case_coverage_12_hours,
                                                                    fact_call_detail.md_product_discussion,
                                                                    fact_call_detail.md_concurrent_call,
                                                                    fact_call_detail.courtesy_call,
                                                                    fact_call_detail.md_in_service,
                                                                    fact_call_detail.md_kol_course_discussion,
                                                                    fact_call_detail.kol_minutes,
                                                                    fact_call_detail.other_activities_time_12_hours,
                                                                    fact_call_detail.other_in_field_activities,
                                                                    fact_call_detail.md_overseas_workshop_visit,
                                                                    fact_call_detail.md_ra_activities2,
                                                                    fact_call_detail.sales_activity,
                                                                    fact_call_detail.sales_time_12_hours,
                                                                    fact_call_detail.time_spent,
                                                                    fact_call_detail.time_spent_on_other_activities_simp,
                                                                    fact_call_detail.time_spent_on_sales_activity,
                                                                    fact_call_detail.md_time_spent_on_a_call,
                                                                    fact_call_detail.md_case_type,
                                                                    fact_call_detail.md_sets_activities,
                                                                    fact_call_detail.md_time_spent_on_case,
                                                                    fact_call_detail.time_spent_on_other_activities,
                                                                    fact_call_detail.time_spent_per_call,
                                                                    fact_call_detail.md_case_conducted_in_hospital,
                                                                    fact_call_detail.calculated_field_2,
                                                                    fact_call_detail.calculated_hours_3,
                                                                    fact_call_detail.call_planned,
                                                                    fact_call_detail.call_submission_day,
                                                                    fact_call_detail.case_coverage,
                                                                    fact_call_detail.day_of_week,
                                                                    fact_call_detail.md_minutes,
                                                                    fact_call_detail.md_in_or_ot,
                                                                    fact_call_detail.md_d_call_type,
                                                                    fact_call_detail.md_hours,
                                                                    fact_call_detail.call_record_type_name,
                                                                    fact_call_detail.call_name,
                                                                    fact_call_detail.parent_call_name,
                                                                    fact_call_detail.submitted_by_mobile,
                                                                    fact_call_detail.call_source_id,
                                                                    fact_call_detail.product_indication_key,
                                                                    fact_call_detail.call_detail_priority,
                                                                    fact_call_detail.call_detail_type,
                                                                    fact_call_detail.call_discussion_record_type_source_id,
                                                                    fact_call_detail.comments,
                                                                    fact_call_detail.discussion_topics,
                                                                    fact_call_detail.discussion_type,
                                                                    fact_call_detail.call_discussion_type,
                                                                    fact_call_detail.effectiveness,
                                                                    fact_call_detail.follow_up_activity,
                                                                    fact_call_detail.outcomes,
                                                                    fact_call_detail.follow_up_additional_info,
                                                                    fact_call_detail.follow_up_date,
                                                                    fact_call_detail.materials_used,
                                                                    fact_call_detail.call_detail_source_id,
                                                                    fact_call_detail.call_detail_name,
                                                                    fact_call_detail.call_discussion_source_id,
                                                                    fact_call_detail.call_discussion_name,
                                                                    fact_call_detail.call_modify_dt,
                                                                    fact_call_detail.call_modify_id,
                                                                    fact_call_detail.call_detail_modify_dt,
                                                                    fact_call_detail.call_detail_modify_id,
                                                                    fact_call_detail.call_discussion_modify_dt,
                                                                    fact_call_detail.call_discussion_modify_id,
                                                                    fact_call_detail.inserted_date,
                                                                    fact_call_detail.updated_date
                                                                FROM fact_call_detail
                                                                WHERE (
                                                                        (
                                                                            (fact_call_detail.parent_call_flag = 1)
                                                                            AND ((fact_call_detail.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                                                                            )
                                                                        AND ((fact_call_detail.country_key)::TEXT <> ('ZZ'::CHARACTER VARYING)::TEXT)
                                                                        )
                                                                ) fact JOIN dim_date dimdate ON ((fact.call_date_key = dimdate.date_key))
                                                            ) JOIN vw_employee_hier hier ON (
                                                                (
                                                                    ((fact.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                                                    AND ((fact.country_key)::TEXT = (hier.country_code)::TEXT)
                                                                    )
                                                                )
                                                        ) LEFT JOIN dim_employee employee ON (((fact.employee_key)::TEXT = (employee.employee_key)::TEXT))
                                                    ) LEFT JOIN (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    SELECT 'SG'::CHARACTER VARYING AS country,
                                                                        ds.my_date_year,
                                                                        ds.my_date_month,
                                                                        count(*) AS working_days
                                                                    FROM dim_date ds
                                                                    WHERE (
                                                                            (
                                                                                (
                                                                                    (
                                                                                        NOT (
                                                                                            (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                                SELECT DISTINCT holiday_list.holiday_key
                                                                                                FROM holiday_list
                                                                                                WHERE ((holiday_list.country)::TEXT = ('SG'::CHARACTER VARYING)::TEXT)
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                                    )
                                                                                AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                            )
                                                                    GROUP BY 1,
                                                                        ds.my_date_year,
                                                                        ds.my_date_month
                                                                    
                                                                    UNION ALL
                                                                    
                                                                    SELECT 'MY'::CHARACTER VARYING AS country,
                                                                        ds.my_date_year,
                                                                        ds.my_date_month,
                                                                        count(*) AS working_days
                                                                    FROM dim_date ds
                                                                    WHERE (
                                                                            (
                                                                                (
                                                                                    (
                                                                                        NOT (
                                                                                            (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                                SELECT DISTINCT holiday_list.holiday_key
                                                                                                FROM holiday_list
                                                                                                WHERE ((holiday_list.country)::TEXT = ('MY'::CHARACTER VARYING)::TEXT)
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                                    )
                                                                                AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                            )
                                                                    GROUP BY 1,
                                                                        ds.my_date_year,
                                                                        ds.my_date_month
                                                                    )
                                                                
                                                                UNION ALL
                                                                
                                                                SELECT 'VN'::CHARACTER VARYING AS country,
                                                                    ds.my_date_year,
                                                                    ds.my_date_month,
                                                                    count(*) AS working_days
                                                                FROM dim_date ds
                                                                WHERE (
                                                                        (
                                                                            (
                                                                                (
                                                                                    NOT (
                                                                                        (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                            SELECT DISTINCT holiday_list.holiday_key
                                                                                            FROM holiday_list
                                                                                            WHERE ((holiday_list.country)::TEXT = ('VN'::CHARACTER VARYING)::TEXT)
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                            )
                                                                        AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                        )
                                                                GROUP BY 1,
                                                                    ds.my_date_year,
                                                                    ds.my_date_month
                                                                )
                                                            
                                                            UNION ALL
                                                            
                                                            SELECT 'TH'::CHARACTER VARYING AS country,
                                                                ds.my_date_year,
                                                                ds.my_date_month,
                                                                count(*) AS working_days
                                                            FROM dim_date ds
                                                            WHERE (
                                                                    (
                                                                        (
                                                                            (
                                                                                NOT (
                                                                                    (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                        SELECT DISTINCT holiday_list.holiday_key
                                                                                        FROM holiday_list
                                                                                        WHERE ((holiday_list.country)::TEXT = ('TH'::CHARACTER VARYING)::TEXT)
                                                                                        )
                                                                                    )
                                                                                )
                                                                            AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                            )
                                                                        AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                        )
                                                                    AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                    )
                                                            GROUP BY 1,
                                                                ds.my_date_year,
                                                                ds.my_date_month
                                                            )
                                                        
                                                        UNION ALL
                                                        
                                                        SELECT 'PH'::CHARACTER VARYING AS country,
                                                            ds.my_date_year,
                                                            ds.my_date_month,
                                                            count(*) AS working_days
                                                        FROM dim_date ds
                                                        WHERE (
                                                                (
                                                                    (
                                                                        (
                                                                            NOT (
                                                                                (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                    SELECT DISTINCT holiday_list.holiday_key
                                                                                    FROM holiday_list
                                                                                    WHERE ((holiday_list.country)::TEXT = ('PH'::CHARACTER VARYING)::TEXT)
                                                                                    )
                                                                                )
                                                                            )
                                                                        AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                        )
                                                                    AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                    )
                                                                AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                )
                                                        GROUP BY 1,
                                                            ds.my_date_year,
                                                            ds.my_date_month
                                                        )
                                                    
                                                    UNION ALL
                                                    
                                                    SELECT 'ID'::CHARACTER VARYING AS country,
                                                        ds.my_date_year,
                                                        ds.my_date_month,
                                                        count(*) AS working_days
                                                    FROM dim_date ds
                                                    WHERE (
                                                            (
                                                                (
                                                                    (
                                                                        NOT (
                                                                            (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                SELECT DISTINCT holiday_list.holiday_key
                                                                                FROM holiday_list
                                                                                WHERE ((holiday_list.country)::TEXT = ('ID'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            )
                                                                        )
                                                                    AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                    )
                                                                AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                )
                                                            AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                            )
                                                    GROUP BY 1,
                                                        ds.my_date_year,
                                                        ds.my_date_month
                                                    ) ds ON (
                                                        (
                                                            (
                                                                (((dimdate.my_date_year)::CHARACTER VARYING)::TEXT = ((ds.my_date_year)::CHARACTER VARYING)::TEXT)
                                                                AND (((dimdate.my_date_month)::CHARACTER VARYING)::TEXT = ((ds.my_date_month)::CHARACTER VARYING)::TEXT)
                                                                )
                                                            AND ((fact.country_key)::TEXT = (ds.country)::TEXT)
                                                            )
                                                        )
                                                )
                                            GROUP BY dimdate.my_date_year,
                                                dimdate.my_date_month,
                                                dimdate.my_date_quarter,
                                                fact.country_key,
                                                hier.employee_name,
                                                fact.employee_key,
                                                hier.l1_username,
                                                hier.sector,
                                                hier.l2_manager_name,
                                                employee.organization_l3_name
                                            ) fact
                                        ) src1 LEFT JOIN (
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
                                        ) actve_usr ON (
                                            (
                                                (
                                                    (
                                                        (src1.my_date_year = actve_usr.year)
                                                        AND (((src1.my_date_month)::CHARACTER VARYING)::TEXT = ((actve_usr.month)::CHARACTER VARYING)::TEXT)
                                                        )
                                                    AND ((src1.country)::TEXT = (actve_usr.country_code)::TEXT)
                                                    )
                                                AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(actve_usr.sector, '#'::CHARACTER VARYING))::TEXT)
                                                )
                                            )
                                    ) LEFT JOIN (
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
                                    ) emp1 ON (
                                        (
                                            ((src1.country)::TEXT = (emp1.country_code)::TEXT)
                                            AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(emp1.sector, '#'::CHARACTER VARYING))::TEXT)
                                            )
                                        )
                                ) LEFT JOIN (
                                SELECT count(1) AS total_cnt_clm_flg,
                                    fct.employee_key,
                                    dimdate.my_date_year AS yr,
                                    CASE 
                                        WHEN (
                                                (dimdate.my_date_month = 'Jan'::CHAR(256))
                                                OR (
                                                    (dimdate.my_date_month IS NULL)
                                                    AND ('Jan' IS NULL)
                                                    )
                                                )
                                            THEN '1'::CHAR(256)
                                        WHEN (
                                                (dimdate.my_date_month = 'Feb'::CHAR(256))
                                                OR (
                                                    (dimdate.my_date_month IS NULL)
                                                    AND ('Feb' IS NULL)
                                                    )
                                                )
                                            THEN '2'::CHAR(256)
                                        WHEN (
                                                (dimdate.my_date_month = 'Mar'::CHAR(256))
                                                OR (
                                                    (dimdate.my_date_month IS NULL)
                                                    AND ('Mar' IS NULL)
                                                    )
                                                )
                                            THEN '3'::CHAR(256)
                                        WHEN (
                                                (dimdate.my_date_month = 'Apr'::CHAR(256))
                                                OR (
                                                    (dimdate.my_date_month IS NULL)
                                                    AND ('Apr' IS NULL)
                                                    )
                                                )
                                            THEN '4'::CHAR(256)
                                        WHEN (
                                                (dimdate.my_date_month = 'May'::CHAR(256))
                                                OR (
                                                    (dimdate.my_date_month IS NULL)
                                                    AND ('May' IS NULL)
                                                    )
                                                )
                                            THEN '5'::CHAR(256)
                                        WHEN (
                                                (dimdate.my_date_month = 'Jun'::CHAR(256))
                                                OR (
                                                    (dimdate.my_date_month IS NULL)
                                                    AND ('Jun' IS NULL)
                                                    )
                                                )
                                            THEN '6'::CHAR(256)
                                        WHEN (
                                                (dimdate.my_date_month = 'Jul'::CHAR(256))
                                                OR (
                                                    (dimdate.my_date_month IS NULL)
                                                    AND ('Jul' IS NULL)
                                                    )
                                                )
                                            THEN '7'::CHAR(256)
                                        WHEN (
                                                (dimdate.my_date_month = 'Aug'::CHAR(256))
                                                OR (
                                                    (dimdate.my_date_month IS NULL)
                                                    AND ('Aug' IS NULL)
                                                    )
                                                )
                                            THEN '8'::CHAR(256)
                                        WHEN (
                                                (dimdate.my_date_month = 'Sep'::CHAR(256))
                                                OR (
                                                    (dimdate.my_date_month IS NULL)
                                                    AND ('Sep' IS NULL)
                                                    )
                                                )
                                            THEN '9'::CHAR(256)
                                        WHEN (
                                                (dimdate.my_date_month = 'Oct'::CHAR(256))
                                                OR (
                                                    (dimdate.my_date_month IS NULL)
                                                    AND ('Oct' IS NULL)
                                                    )
                                                )
                                            THEN '10'::CHAR(256)
                                        WHEN (
                                                (dimdate.my_date_month = 'Nov'::CHAR(256))
                                                OR (
                                                    (dimdate.my_date_month IS NULL)
                                                    AND ('Nov' IS NULL)
                                                    )
                                                )
                                            THEN '11'::CHAR(256)
                                        WHEN (
                                                (dimdate.my_date_month = 'Dec'::CHAR(256))
                                                OR (
                                                    (dimdate.my_date_month IS NULL)
                                                    AND ('Dec' IS NULL)
                                                    )
                                                )
                                            THEN '12'::CHAR(256)
                                        ELSE dimdate.my_date_month
                                        END AS mnth,
                                    fct.country_key,
                                    hier.l2_manager_name,
                                    hier.sector,
                                    employee.organization_l3_name
                                FROM (
                                    (
                                        (
                                            fact_call_detail fct JOIN dim_date dimdate ON ((fct.call_date_key = dimdate.date_key))
                                            ) JOIN vw_employee_hier hier ON (
                                                (
                                                    ((fct.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                                    AND ((fct.country_key)::TEXT = (hier.country_code)::TEXT)
                                                    )
                                                )
                                        ) LEFT JOIN dim_employee employee ON (((fct.employee_key)::TEXT = (employee.employee_key)::TEXT))
                                    )
                                WHERE (
                                        (
                                            (fct.call_clm_flag = 1)
                                            AND (fct.parent_call_flag = 1)
                                            )
                                        AND ((fct.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                                        )
                                GROUP BY fct.employee_key,
                                    dimdate.my_date_year,
                                    dimdate.my_date_month,
                                    fct.country_key,
                                    hier.l2_manager_name,
                                    hier.sector,
                                    employee.organization_l3_name
                                ) fct1 ON (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            ((src1.employee_key)::TEXT = (fct1.employee_key)::TEXT)
                                                            AND (src1.my_date_year = fct1.yr)
                                                            )
                                                        AND (((src1.my_date_month)::CHARACTER VARYING)::TEXT = ((fct1.mnth)::CHARACTER VARYING)::TEXT)
                                                        )
                                                    AND ((fct1.country_key)::TEXT = (src1.country)::TEXT)
                                                    )
                                                AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct1.sector, '#'::CHARACTER VARYING))::TEXT)
                                                )
                                            AND ((COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT)
                                            )
                                        AND ((COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct1.l2_manager_name, '#'::CHARACTER VARYING))::TEXT)
                                        )
                                    )
                            ) LEFT JOIN (
                            SELECT count(1) AS total_prnt_cnt_clm_flg,
                                fct.employee_key,
                                dimdate.my_date_year AS yr,
                                CASE 
                                    WHEN (
                                            (dimdate.my_date_month = 'Jan'::CHAR(256))
                                            OR (
                                                (dimdate.my_date_month IS NULL)
                                                AND ('Jan' IS NULL)
                                                )
                                            )
                                        THEN '1'::CHAR(256)
                                    WHEN (
                                            (dimdate.my_date_month = 'Feb'::CHAR(256))
                                            OR (
                                                (dimdate.my_date_month IS NULL)
                                                AND ('Feb' IS NULL)
                                                )
                                            )
                                        THEN '2'::CHAR(256)
                                    WHEN (
                                            (dimdate.my_date_month = 'Mar'::CHAR(256))
                                            OR (
                                                (dimdate.my_date_month IS NULL)
                                                AND ('Mar' IS NULL)
                                                )
                                            )
                                        THEN '3'::CHAR(256)
                                    WHEN (
                                            (dimdate.my_date_month = 'Apr'::CHAR(256))
                                            OR (
                                                (dimdate.my_date_month IS NULL)
                                                AND ('Apr' IS NULL)
                                                )
                                            )
                                        THEN '4'::CHAR(256)
                                    WHEN (
                                            (dimdate.my_date_month = 'May'::CHAR(256))
                                            OR (
                                                (dimdate.my_date_month IS NULL)
                                                AND ('May' IS NULL)
                                                )
                                            )
                                        THEN '5'::CHAR(256)
                                    WHEN (
                                            (dimdate.my_date_month = 'Jun'::CHAR(256))
                                            OR (
                                                (dimdate.my_date_month IS NULL)
                                                AND ('Jun' IS NULL)
                                                )
                                            )
                                        THEN '6'::CHAR(256)
                                    WHEN (
                                            (dimdate.my_date_month = 'Jul'::CHAR(256))
                                            OR (
                                                (dimdate.my_date_month IS NULL)
                                                AND ('Jul' IS NULL)
                                                )
                                            )
                                        THEN '7'::CHAR(256)
                                    WHEN (
                                            (dimdate.my_date_month = 'Aug'::CHAR(256))
                                            OR (
                                                (dimdate.my_date_month IS NULL)
                                                AND ('Aug' IS NULL)
                                                )
                                            )
                                        THEN '8'::CHAR(256)
                                    WHEN (
                                            (dimdate.my_date_month = 'Sep'::CHAR(256))
                                            OR (
                                                (dimdate.my_date_month IS NULL)
                                                AND ('Sep' IS NULL)
                                                )
                                            )
                                        THEN '9'::CHAR(256)
                                    WHEN (
                                            (dimdate.my_date_month = 'Oct'::CHAR(256))
                                            OR (
                                                (dimdate.my_date_month IS NULL)
                                                AND ('Oct' IS NULL)
                                                )
                                            )
                                        THEN '10'::CHAR(256)
                                    WHEN (
                                            (dimdate.my_date_month = 'Nov'::CHAR(256))
                                            OR (
                                                (dimdate.my_date_month IS NULL)
                                                AND ('Nov' IS NULL)
                                                )
                                            )
                                        THEN '11'::CHAR(256)
                                    WHEN (
                                            (dimdate.my_date_month = 'Dec'::CHAR(256))
                                            OR (
                                                (dimdate.my_date_month IS NULL)
                                                AND ('Dec' IS NULL)
                                                )
                                            )
                                        THEN '12'::CHAR(256)
                                    ELSE dimdate.my_date_month
                                    END AS mnth,
                                fct.country_key,
                                hier.l2_manager_name,
                                hier.sector,
                                employee.organization_l3_name
                            FROM (
                                (
                                    (
                                        fact_call_detail fct JOIN dim_date dimdate ON ((fct.call_date_key = dimdate.date_key))
                                        ) JOIN vw_employee_hier hier ON (
                                            (
                                                ((fct.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                                AND ((fct.country_key)::TEXT = (hier.country_code)::TEXT)
                                                )
                                            )
                                    ) LEFT JOIN dim_employee employee ON (((fct.employee_key)::TEXT = (employee.employee_key)::TEXT))
                                )
                            WHERE (
                                    (fct.parent_call_flag = 1)
                                    AND ((fct.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                                    )
                            GROUP BY fct.employee_key,
                                dimdate.my_date_year,
                                dimdate.my_date_month,
                                fct.country_key,
                                hier.l2_manager_name,
                                hier.sector,
                                employee.organization_l3_name
                            ) fct2 ON (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        ((src1.employee_key)::TEXT = (fct2.employee_key)::TEXT)
                                                        AND (src1.my_date_year = fct2.yr)
                                                        )
                                                    AND (((src1.my_date_month)::CHARACTER VARYING)::TEXT = ((fct2.mnth)::CHARACTER VARYING)::TEXT)
                                                    )
                                                AND ((fct2.country_key)::TEXT = (src1.country)::TEXT)
                                                )
                                            AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct2.sector, '#'::CHARACTER VARYING))::TEXT)
                                            )
                                        AND ((COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct2.organization_l3_name, '#'::CHARACTER VARYING))::TEXT)
                                        )
                                    AND ((COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct2.l2_manager_name, '#'::CHARACTER VARYING))::TEXT)
                                    )
                                )
                        ) LEFT JOIN (
                        SELECT (sum(COALESCE(terr.sea_frml_hours_on, ((0)::NUMERIC)::NUMERIC(18, 0))) / ((8)::NUMERIC)::NUMERIC(18, 0)) AS cnt_total_time_on,
                            terr.employee_key,
                            terr.country_key,
                            date_dim.my_date_year AS yr,
                            CASE 
                                WHEN (
                                        (date_dim.my_date_month = 'Jan'::CHAR(256))
                                        OR (
                                            (date_dim.my_date_month IS NULL)
                                            AND ('Jan' IS NULL)
                                            )
                                        )
                                    THEN '1'::CHAR(256)
                                WHEN (
                                        (date_dim.my_date_month = 'Feb'::CHAR(256))
                                        OR (
                                            (date_dim.my_date_month IS NULL)
                                            AND ('Feb' IS NULL)
                                            )
                                        )
                                    THEN '2'::CHAR(256)
                                WHEN (
                                        (date_dim.my_date_month = 'Mar'::CHAR(256))
                                        OR (
                                            (date_dim.my_date_month IS NULL)
                                            AND ('Mar' IS NULL)
                                            )
                                        )
                                    THEN '3'::CHAR(256)
                                WHEN (
                                        (date_dim.my_date_month = 'Apr'::CHAR(256))
                                        OR (
                                            (date_dim.my_date_month IS NULL)
                                            AND ('Apr' IS NULL)
                                            )
                                        )
                                    THEN '4'::CHAR(256)
                                WHEN (
                                        (date_dim.my_date_month = 'May'::CHAR(256))
                                        OR (
                                            (date_dim.my_date_month IS NULL)
                                            AND ('May' IS NULL)
                                            )
                                        )
                                    THEN '5'::CHAR(256)
                                WHEN (
                                        (date_dim.my_date_month = 'Jun'::CHAR(256))
                                        OR (
                                            (date_dim.my_date_month IS NULL)
                                            AND ('Jun' IS NULL)
                                            )
                                        )
                                    THEN '6'::CHAR(256)
                                WHEN (
                                        (date_dim.my_date_month = 'Jul'::CHAR(256))
                                        OR (
                                            (date_dim.my_date_month IS NULL)
                                            AND ('Jul' IS NULL)
                                            )
                                        )
                                    THEN '7'::CHAR(256)
                                WHEN (
                                        (date_dim.my_date_month = 'Aug'::CHAR(256))
                                        OR (
                                            (date_dim.my_date_month IS NULL)
                                            AND ('Aug' IS NULL)
                                            )
                                        )
                                    THEN '8'::CHAR(256)
                                WHEN (
                                        (date_dim.my_date_month = 'Sep'::CHAR(256))
                                        OR (
                                            (date_dim.my_date_month IS NULL)
                                            AND ('Sep' IS NULL)
                                            )
                                        )
                                    THEN '9'::CHAR(256)
                                WHEN (
                                        (date_dim.my_date_month = 'Oct'::CHAR(256))
                                        OR (
                                            (date_dim.my_date_month IS NULL)
                                            AND ('Oct' IS NULL)
                                            )
                                        )
                                    THEN '10'::CHAR(256)
                                WHEN (
                                        (date_dim.my_date_month = 'Nov'::CHAR(256))
                                        OR (
                                            (date_dim.my_date_month IS NULL)
                                            AND ('Nov' IS NULL)
                                            )
                                        )
                                    THEN '11'::CHAR(256)
                                WHEN (
                                        (date_dim.my_date_month = 'Dec'::CHAR(256))
                                        OR (
                                            (date_dim.my_date_month IS NULL)
                                            AND ('Dec' IS NULL)
                                            )
                                        )
                                    THEN '12'::CHAR(256)
                                ELSE date_dim.my_date_month
                                END AS mnth
                        FROM (
                            fact_timeoff_territory terr JOIN dim_date date_dim ON (((terr.start_date_key)::TEXT = ((date_dim.date_key)::CHARACTER VARYING)::TEXT))
                            )
                        WHERE (
                                ((terr.sea_time_on_time_off)::TEXT = ('Time On'::CHARACTER VARYING)::TEXT)
                                AND ((terr.start_date_key)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                )
                        GROUP BY terr.employee_key,
                            terr.country_key,
                            date_dim.my_date_year,
                            date_dim.my_date_month
                        ) terr ON (
                            (
                                (
                                    (
                                        ((terr.employee_key)::TEXT = (src1.employee_key)::TEXT)
                                        AND ((terr.country_key)::TEXT = (src1.country)::TEXT)
                                        )
                                    AND (src1.my_date_year = terr.yr)
                                    )
                                AND (((src1.my_date_month)::CHARACTER VARYING)::TEXT = ((terr.mnth)::CHARACTER VARYING)::TEXT)
                                )
                            )
                    ) LEFT JOIN (
                    SELECT (sum(COALESCE(terr.total_time_off, ((0)::NUMERIC)::NUMERIC(18, 0))) / ((8)::NUMERIC)::NUMERIC(18, 0)) AS cnt_total_time_off,
                        terr.employee_key,
                        terr.country_key,
                        date_dim.my_date_year AS yr,
                        CASE 
                            WHEN (
                                    (date_dim.my_date_month = 'Jan'::CHAR(256))
                                    OR (
                                        (date_dim.my_date_month IS NULL)
                                        AND ('Jan' IS NULL)
                                        )
                                    )
                                THEN '1'::CHAR(256)
                            WHEN (
                                    (date_dim.my_date_month = 'Feb'::CHAR(256))
                                    OR (
                                        (date_dim.my_date_month IS NULL)
                                        AND ('Feb' IS NULL)
                                        )
                                    )
                                THEN '2'::CHAR(256)
                            WHEN (
                                    (date_dim.my_date_month = 'Mar'::CHAR(256))
                                    OR (
                                        (date_dim.my_date_month IS NULL)
                                        AND ('Mar' IS NULL)
                                        )
                                    )
                                THEN '3'::CHAR(256)
                            WHEN (
                                    (date_dim.my_date_month = 'Apr'::CHAR(256))
                                    OR (
                                        (date_dim.my_date_month IS NULL)
                                        AND ('Apr' IS NULL)
                                        )
                                    )
                                THEN '4'::CHAR(256)
                            WHEN (
                                    (date_dim.my_date_month = 'May'::CHAR(256))
                                    OR (
                                        (date_dim.my_date_month IS NULL)
                                        AND ('May' IS NULL)
                                        )
                                    )
                                THEN '5'::CHAR(256)
                            WHEN (
                                    (date_dim.my_date_month = 'Jun'::CHAR(256))
                                    OR (
                                        (date_dim.my_date_month IS NULL)
                                        AND ('Jun' IS NULL)
                                        )
                                    )
                                THEN '6'::CHAR(256)
                            WHEN (
                                    (date_dim.my_date_month = 'Jul'::CHAR(256))
                                    OR (
                                        (date_dim.my_date_month IS NULL)
                                        AND ('Jul' IS NULL)
                                        )
                                    )
                                THEN '7'::CHAR(256)
                            WHEN (
                                    (date_dim.my_date_month = 'Aug'::CHAR(256))
                                    OR (
                                        (date_dim.my_date_month IS NULL)
                                        AND ('Aug' IS NULL)
                                        )
                                    )
                                THEN '8'::CHAR(256)
                            WHEN (
                                    (date_dim.my_date_month = 'Sep'::CHAR(256))
                                    OR (
                                        (date_dim.my_date_month IS NULL)
                                        AND ('Sep' IS NULL)
                                        )
                                    )
                                THEN '9'::CHAR(256)
                            WHEN (
                                    (date_dim.my_date_month = 'Oct'::CHAR(256))
                                    OR (
                                        (date_dim.my_date_month IS NULL)
                                        AND ('Oct' IS NULL)
                                        )
                                    )
                                THEN '10'::CHAR(256)
                            WHEN (
                                    (date_dim.my_date_month = 'Nov'::CHAR(256))
                                    OR (
                                        (date_dim.my_date_month IS NULL)
                                        AND ('Nov' IS NULL)
                                        )
                                    )
                                THEN '11'::CHAR(256)
                            WHEN (
                                    (date_dim.my_date_month = 'Dec'::CHAR(256))
                                    OR (
                                        (date_dim.my_date_month IS NULL)
                                        AND ('Dec' IS NULL)
                                        )
                                    )
                                THEN '12'::CHAR(256)
                            ELSE date_dim.my_date_month
                            END AS mnth
                    FROM (
                        fact_timeoff_territory terr JOIN dim_date date_dim ON (((terr.start_date_key)::TEXT = ((date_dim.date_key)::CHARACTER VARYING)::TEXT))
                        )
                    WHERE (
                            ((terr.sea_time_on_time_off)::TEXT = ('Time Off'::CHARACTER VARYING)::TEXT)
                            AND ((terr.start_date_key)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                            )
                    GROUP BY terr.employee_key,
                        terr.country_key,
                        date_dim.my_date_year,
                        date_dim.my_date_month
                    ) terr1 ON (
                        (
                            (
                                (
                                    ((terr1.employee_key)::TEXT = (src1.employee_key)::TEXT)
                                    AND ((terr1.country_key)::TEXT = (src1.country)::TEXT)
                                    )
                                AND (src1.my_date_year = terr1.yr)
                                )
                            AND (((src1.my_date_month)::CHARACTER VARYING)::TEXT = ((terr1.mnth)::CHARACTER VARYING)::TEXT)
                            )
                        )
                ) LEFT JOIN (
                SELECT count(DISTINCT fct.call_name) AS total_sbmtd_calls_key_message,
                    fct.employee_key,
                    dimdate.my_date_year AS yr,
                    CASE 
                        WHEN (
                                (dimdate.my_date_month = 'Jan'::CHAR(256))
                                OR (
                                    (dimdate.my_date_month IS NULL)
                                    AND ('Jan' IS NULL)
                                    )
                                )
                            THEN '1'::CHAR(256)
                        WHEN (
                                (dimdate.my_date_month = 'Feb'::CHAR(256))
                                OR (
                                    (dimdate.my_date_month IS NULL)
                                    AND ('Feb' IS NULL)
                                    )
                                )
                            THEN '2'::CHAR(256)
                        WHEN (
                                (dimdate.my_date_month = 'Mar'::CHAR(256))
                                OR (
                                    (dimdate.my_date_month IS NULL)
                                    AND ('Mar' IS NULL)
                                    )
                                )
                            THEN '3'::CHAR(256)
                        WHEN (
                                (dimdate.my_date_month = 'Apr'::CHAR(256))
                                OR (
                                    (dimdate.my_date_month IS NULL)
                                    AND ('Apr' IS NULL)
                                    )
                                )
                            THEN '4'::CHAR(256)
                        WHEN (
                                (dimdate.my_date_month = 'May'::CHAR(256))
                                OR (
                                    (dimdate.my_date_month IS NULL)
                                    AND ('May' IS NULL)
                                    )
                                )
                            THEN '5'::CHAR(256)
                        WHEN (
                                (dimdate.my_date_month = 'Jun'::CHAR(256))
                                OR (
                                    (dimdate.my_date_month IS NULL)
                                    AND ('Jun' IS NULL)
                                    )
                                )
                            THEN '6'::CHAR(256)
                        WHEN (
                                (dimdate.my_date_month = 'Jul'::CHAR(256))
                                OR (
                                    (dimdate.my_date_month IS NULL)
                                    AND ('Jul' IS NULL)
                                    )
                                )
                            THEN '7'::CHAR(256)
                        WHEN (
                                (dimdate.my_date_month = 'Aug'::CHAR(256))
                                OR (
                                    (dimdate.my_date_month IS NULL)
                                    AND ('Aug' IS NULL)
                                    )
                                )
                            THEN '8'::CHAR(256)
                        WHEN (
                                (dimdate.my_date_month = 'Sep'::CHAR(256))
                                OR (
                                    (dimdate.my_date_month IS NULL)
                                    AND ('Sep' IS NULL)
                                    )
                                )
                            THEN '9'::CHAR(256)
                        WHEN (
                                (dimdate.my_date_month = 'Oct'::CHAR(256))
                                OR (
                                    (dimdate.my_date_month IS NULL)
                                    AND ('Oct' IS NULL)
                                    )
                                )
                            THEN '10'::CHAR(256)
                        WHEN (
                                (dimdate.my_date_month = 'Nov'::CHAR(256))
                                OR (
                                    (dimdate.my_date_month IS NULL)
                                    AND ('Nov' IS NULL)
                                    )
                                )
                            THEN '11'::CHAR(256)
                        WHEN (
                                (dimdate.my_date_month = 'Dec'::CHAR(256))
                                OR (
                                    (dimdate.my_date_month IS NULL)
                                    AND ('Dec' IS NULL)
                                    )
                                )
                            THEN '12'::CHAR(256)
                        ELSE dimdate.my_date_month
                        END AS mnth,
                    fct.country_key,
                    hier.l2_manager_name,
                    hier.sector,
                    employee.organization_l3_name
                FROM (
                    (
                        (
                            (
                                fact_call_detail fct JOIN dim_date dimdate ON ((fct.call_date_key = dimdate.date_key))
                                ) JOIN fact_call_key_message fckm ON (((fct.call_source_id)::TEXT = (fckm.call_source_id)::TEXT))
                            ) JOIN vw_employee_hier hier ON (
                                (
                                    ((fct.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                    AND ((fct.country_key)::TEXT = (hier.country_code)::TEXT)
                                    )
                                )
                        ) LEFT JOIN dim_employee employee ON (((fct.employee_key)::TEXT = (employee.employee_key)::TEXT))
                    )
                WHERE (
                        (fct.parent_call_flag = 1)
                        AND ((fct.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                        )
                GROUP BY fct.employee_key,
                    dimdate.my_date_year,
                    dimdate.my_date_month,
                    fct.country_key,
                    hier.l2_manager_name,
                    hier.sector,
                    employee.organization_l3_name
                ) fct3 ON (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            ((src1.employee_key)::TEXT = (fct3.employee_key)::TEXT)
                                            AND (src1.my_date_year = fct3.yr)
                                            )
                                        AND (((src1.my_date_month)::CHARACTER VARYING)::TEXT = ((fct3.mnth)::CHARACTER VARYING)::TEXT)
                                        )
                                    AND ((fct3.country_key)::TEXT = (src1.country)::TEXT)
                                    )
                                AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct3.sector, '#'::CHARACTER VARYING))::TEXT)
                                )
                            AND ((COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct3.organization_l3_name, '#'::CHARACTER VARYING))::TEXT)
                            )
                        AND ((COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct3.l2_manager_name, '#'::CHARACTER VARYING))::TEXT)
                        )
                    )
            ) LEFT JOIN (
            SELECT count(1) AS total_call_product,
                fct.employee_key,
                dimdate.my_date_year AS yr,
                CASE 
                    WHEN (
                            (dimdate.my_date_month = 'Jan'::CHAR(256))
                            OR (
                                (dimdate.my_date_month IS NULL)
                                AND ('Jan' IS NULL)
                                )
                            )
                        THEN '1'::CHAR(256)
                    WHEN (
                            (dimdate.my_date_month = 'Feb'::CHAR(256))
                            OR (
                                (dimdate.my_date_month IS NULL)
                                AND ('Feb' IS NULL)
                                )
                            )
                        THEN '2'::CHAR(256)
                    WHEN (
                            (dimdate.my_date_month = 'Mar'::CHAR(256))
                            OR (
                                (dimdate.my_date_month IS NULL)
                                AND ('Mar' IS NULL)
                                )
                            )
                        THEN '3'::CHAR(256)
                    WHEN (
                            (dimdate.my_date_month = 'Apr'::CHAR(256))
                            OR (
                                (dimdate.my_date_month IS NULL)
                                AND ('Apr' IS NULL)
                                )
                            )
                        THEN '4'::CHAR(256)
                    WHEN (
                            (dimdate.my_date_month = 'May'::CHAR(256))
                            OR (
                                (dimdate.my_date_month IS NULL)
                                AND ('May' IS NULL)
                                )
                            )
                        THEN '5'::CHAR(256)
                    WHEN (
                            (dimdate.my_date_month = 'Jun'::CHAR(256))
                            OR (
                                (dimdate.my_date_month IS NULL)
                                AND ('Jun' IS NULL)
                                )
                            )
                        THEN '6'::CHAR(256)
                    WHEN (
                            (dimdate.my_date_month = 'Jul'::CHAR(256))
                            OR (
                                (dimdate.my_date_month IS NULL)
                                AND ('Jul' IS NULL)
                                )
                            )
                        THEN '7'::CHAR(256)
                    WHEN (
                            (dimdate.my_date_month = 'Aug'::CHAR(256))
                            OR (
                                (dimdate.my_date_month IS NULL)
                                AND ('Aug' IS NULL)
                                )
                            )
                        THEN '8'::CHAR(256)
                    WHEN (
                            (dimdate.my_date_month = 'Sep'::CHAR(256))
                            OR (
                                (dimdate.my_date_month IS NULL)
                                AND ('Sep' IS NULL)
                                )
                            )
                        THEN '9'::CHAR(256)
                    WHEN (
                            (dimdate.my_date_month = 'Oct'::CHAR(256))
                            OR (
                                (dimdate.my_date_month IS NULL)
                                AND ('Oct' IS NULL)
                                )
                            )
                        THEN '10'::CHAR(256)
                    WHEN (
                            (dimdate.my_date_month = 'Nov'::CHAR(256))
                            OR (
                                (dimdate.my_date_month IS NULL)
                                AND ('Nov' IS NULL)
                                )
                            )
                        THEN '11'::CHAR(256)
                    WHEN (
                            (dimdate.my_date_month = 'Dec'::CHAR(256))
                            OR (
                                (dimdate.my_date_month IS NULL)
                                AND ('Dec' IS NULL)
                                )
                            )
                        THEN '12'::CHAR(256)
                    ELSE dimdate.my_date_month
                    END AS mnth,
                fct.country_key,
                hier.l2_manager_name,
                hier.sector,
                employee.organization_l3_name
            FROM (
                (
                    (
                        fact_call_detail fct JOIN dim_date dimdate ON ((fct.call_date_key = dimdate.date_key))
                        ) JOIN vw_employee_hier hier ON (
                            (
                                ((fct.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                AND ((fct.country_key)::TEXT = (hier.country_code)::TEXT)
                                )
                            )
                    ) LEFT JOIN dim_employee employee ON (((fct.employee_key)::TEXT = (employee.employee_key)::TEXT))
                )
            WHERE (
                    (
                        (fct.parent_call_flag = 1)
                        AND ((fct.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                        )
                    AND (
                        (fct.detailed_products IS NOT NULL)
                        OR (
                            ((fct.detailed_products)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                            AND ((fct.detailed_products)::TEXT <> (' '::CHARACTER VARYING)::TEXT)
                            )
                        )
                    )
            GROUP BY fct.employee_key,
                dimdate.my_date_year,
                dimdate.my_date_month,
                fct.country_key,
                hier.l2_manager_name,
                hier.sector,
                employee.organization_l3_name
            ) fct4 ON (
                (
                    (
                        (
                            (
                                (
                                    (
                                        ((src1.employee_key)::TEXT = (fct4.employee_key)::TEXT)
                                        AND (src1.my_date_year = fct4.yr)
                                        )
                                    AND (src1.my_date_month = fct4.mnth)
                                    )
                                AND ((fct4.country_key)::TEXT = (src1.country)::TEXT)
                                )
                            AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct4.sector, '#'::CHARACTER VARYING))::TEXT)
                            )
                        AND ((COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct4.organization_l3_name, '#'::CHARACTER VARYING))::TEXT)
                        )
                    AND ((COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct4.l2_manager_name, '#'::CHARACTER VARYING))::TEXT)
                    )
                )
        )
    ),
t2
AS (
    SELECT NULL AS jnj_year,
        NULL AS jnj_month,
        NULL AS jnj_quarter,
        src1.date_year,
        src1.date_month,
        src1.date_quarter,
        NULL AS my_year,
        NULL AS my_month,
        NULL AS my_quarter,
        src1.country,
        src1.sector,
        (src1.l3_wwid)::CHARACTER VARYING AS l3_wwid,
        (src1.l3_username)::CHARACTER VARYING AS l3_username,
        src1.l3_manager_name,
        (src1.l2_wwid)::CHARACTER VARYING AS l2_wwid,
        (src1.l2_username)::CHARACTER VARYING AS l2_username,
        (src1.l2_manager_name)::CHARACTER VARYING AS l2_manager_name,
        (src1.l1_wwid)::CHARACTER VARYING AS l1_wwid,
        (src1.l1_username)::CHARACTER VARYING AS l1_username,
        (src1.l1_manager_name)::CHARACTER VARYING AS l1_manager_name,
        (src1.organization_l1_name)::CHARACTER VARYING AS organization_l1_name,
        (src1.organization_l2_name)::CHARACTER VARYING AS organization_l2_name,
        src1.organization_l3_name,
        (src1.organization_l4_name)::CHARACTER VARYING AS organization_l4_name,
        (src1.organization_l5_name)::CHARACTER VARYING AS organization_l5_name,
        src1.sales_rep,
        src1.working_days,
        src1.total_cnt_call,
        src1.total_cnt_edetailing_calls,
        src1.total_cnt_call_delay,
        (src1.sales_rep_ntid)::CHARACTER VARYING AS sales_rep_ntid,
        src1.total_cnt_call_delay AS total_cnt_call_delay_sub,
        fct1.total_cnt_clm_flg,
        fct2.total_prnt_cnt_clm_flg,
        src1.total_cnt_submitted_calls,
        terr.cnt_total_time_on,
        terr1.cnt_total_time_off,
        CASE 
            WHEN (((((src1.date_year)::CHARACTER VARYING)::TEXT || ('-'::CHARACTER VARYING)::TEXT) || (src1.date_month)::TEXT) = ((((date_part(year, sysdate()))::CHARACTER VARYING(5))::TEXT || ('-'::CHARACTER VARYING)::TEXT) || ((date_part(month, sysdate()))::CHARACTER VARYING(5))::TEXT))
                THEN emp1.current_mnth_emp_cnt
            ELSE actve_usr.total_active
            END AS total_active,
        fct4.total_call_product AS detailed_products,
        fct3.total_sbmtd_calls_key_message
    FROM (
        (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        SELECT fact.date_year,
                                            fact.date_month,
                                            fact.date_quarter,
                                            fact.country,
                                            fact.sector,
                                            fact.l3_wwid,
                                            fact.l3_username,
                                            fact.l3_manager_name,
                                            fact.l2_wwid,
                                            fact.l2_username,
                                            fact.l2_manager_name,
                                            fact.l1_wwid,
                                            fact.l1_username,
                                            fact.l1_manager_name,
                                            fact.organization_l1_name,
                                            fact.organization_l2_name,
                                            fact.organization_l3_name,
                                            fact.organization_l4_name,
                                            fact.organization_l5_name,
                                            fact.sales_rep,
                                            fact.working_days,
                                            fact.total_cnt_call,
                                            fact.total_cnt_edetailing_calls,
                                            fact.total_cnt_call_delay,
                                            fact.sales_rep_ntid,
                                            fact.employee_key,
                                            fact.total_cnt_submitted_calls
                                        FROM (
                                            SELECT dimdate.date_year,
                                                CASE 
                                                    WHEN (
                                                            ((dimdate.date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_month IS NULL)
                                                                AND ('Jan' IS NULL)
                                                                )
                                                            )
                                                        THEN '1'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_month IS NULL)
                                                                AND ('Feb' IS NULL)
                                                                )
                                                            )
                                                        THEN '2'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_month IS NULL)
                                                                AND ('Mar' IS NULL)
                                                                )
                                                            )
                                                        THEN '3'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_month IS NULL)
                                                                AND ('Apr' IS NULL)
                                                                )
                                                            )
                                                        THEN '4'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_month IS NULL)
                                                                AND ('May' IS NULL)
                                                                )
                                                            )
                                                        THEN '5'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_month IS NULL)
                                                                AND ('Jun' IS NULL)
                                                                )
                                                            )
                                                        THEN '6'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_month IS NULL)
                                                                AND ('Jul' IS NULL)
                                                                )
                                                            )
                                                        THEN '7'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_month IS NULL)
                                                                AND ('Aug' IS NULL)
                                                                )
                                                            )
                                                        THEN '8'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_month IS NULL)
                                                                AND ('Sep' IS NULL)
                                                                )
                                                            )
                                                        THEN '9'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_month IS NULL)
                                                                AND ('Oct' IS NULL)
                                                                )
                                                            )
                                                        THEN '10'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_month IS NULL)
                                                                AND ('Nov' IS NULL)
                                                                )
                                                            )
                                                        THEN '11'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_month IS NULL)
                                                                AND ('Dec' IS NULL)
                                                                )
                                                            )
                                                        THEN '12'::CHARACTER VARYING
                                                    ELSE dimdate.date_month
                                                    END AS date_month,
                                                CASE 
                                                    WHEN (
                                                            ((dimdate.date_quarter)::TEXT = ('Q1'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_quarter IS NULL)
                                                                AND ('Q1' IS NULL)
                                                                )
                                                            )
                                                        THEN '1'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_quarter)::TEXT = ('Q2'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_quarter IS NULL)
                                                                AND ('Q2' IS NULL)
                                                                )
                                                            )
                                                        THEN '2'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_quarter)::TEXT = ('Q3'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_quarter IS NULL)
                                                                AND ('Q3' IS NULL)
                                                                )
                                                            )
                                                        THEN '3'::CHARACTER VARYING
                                                    WHEN (
                                                            ((dimdate.date_quarter)::TEXT = ('Q4'::CHARACTER VARYING)::TEXT)
                                                            OR (
                                                                (dimdate.date_quarter IS NULL)
                                                                AND ('Q4' IS NULL)
                                                                )
                                                            )
                                                        THEN '4'::CHARACTER VARYING
                                                    ELSE NULL::CHARACTER VARYING
                                                    END AS date_quarter,
                                                fact.country_key AS country,
                                                hier.sector,
                                                "max" ((hier.l2_wwid)::TEXT) AS l3_wwid,
                                                "max" (hier.l2_username) AS l3_username,
                                                hier.l2_manager_name AS l3_manager_name,
                                                "max" ((hier.l3_wwid)::TEXT) AS l2_wwid,
                                                "max" (hier.l3_username) AS l2_username,
                                                "max" ((hier.l3_manager_name)::TEXT) AS l2_manager_name,
                                                "max" ((hier.l4_wwid)::TEXT) AS l1_wwid,
                                                "max" (hier.l4_username) AS l1_username,
                                                "max" ((hier.l4_manager_name)::TEXT) AS l1_manager_name,
                                                "max" ((employee.organization_l1_name)::TEXT) AS organization_l1_name,
                                                "max" ((employee.organization_l2_name)::TEXT) AS organization_l2_name,
                                                employee.organization_l3_name,
                                                "max" ((employee.organization_l4_name)::TEXT) AS organization_l4_name,
                                                "max" ((employee.organization_l5_name)::TEXT) AS organization_l5_name,
                                                hier.employee_name AS sales_rep,
                                                "max" (((ds.working_days)::NUMERIC)::NUMERIC(18, 0)) AS working_days,
                                                count(fact.call_source_id) AS total_cnt_call,
                                                count(fact.call_clm_flag) AS total_cnt_edetailing_calls,
                                                sum(fact.call_submission_day) AS total_cnt_call_delay,
                                                fact.employee_key,
                                                hier.l1_username AS sales_rep_ntid,
                                                count(1) AS total_cnt_submitted_calls
                                            FROM (
                                                (
                                                    (
                                                        (
                                                            (
                                                                SELECT fact_call_detail.country_key,
                                                                    fact_call_detail.hcp_key,
                                                                    fact_call_detail.hco_key,
                                                                    fact_call_detail.employee_key,
                                                                    fact_call_detail.profile_key,
                                                                    fact_call_detail.organization_key,
                                                                    fact_call_detail.call_date_key,
                                                                    fact_call_detail.call_date_time,
                                                                    fact_call_detail.call_entry_datetime,
                                                                    fact_call_detail.call_mobile_last_modified_date_time,
                                                                    fact_call_detail.utc_call_date_time,
                                                                    fact_call_detail.utc_call_entry_time,
                                                                    fact_call_detail.call_status_type,
                                                                    fact_call_detail.call_channel_type,
                                                                    fact_call_detail.call_type,
                                                                    fact_call_detail.call_duration,
                                                                    fact_call_detail.call_clm_flag,
                                                                    fact_call_detail.parent_call_flag,
                                                                    fact_call_detail.attendee_type,
                                                                    fact_call_detail.call_comments,
                                                                    fact_call_detail.next_call_notes,
                                                                    fact_call_detail.pre_call_notes,
                                                                    fact_call_detail.manager_call_comment,
                                                                    fact_call_detail.last_activity_date,
                                                                    fact_call_detail.sample_card,
                                                                    fact_call_detail.account_plan,
                                                                    fact_call_detail.mobile_id,
                                                                    fact_call_detail.significant_event,
                                                                    fact_call_detail.location,
                                                                    fact_call_detail.signature_date,
                                                                    fact_call_detail.signature,
                                                                    fact_call_detail.territory,
                                                                    fact_call_detail.attendees,
                                                                    fact_call_detail.parent_call_source_id,
                                                                    fact_call_detail.user_name,
                                                                    fact_call_detail.medical_event,
                                                                    fact_call_detail.is_sampled_call,
                                                                    fact_call_detail.presentations,
                                                                    fact_call_detail.product_priority_1,
                                                                    fact_call_detail.product_priority_2,
                                                                    fact_call_detail.product_priority_3,
                                                                    fact_call_detail.product_priority_4,
                                                                    fact_call_detail.product_priority_5,
                                                                    fact_call_detail.attendee_list,
                                                                    fact_call_detail.msl_interaction_notes,
                                                                    fact_call_detail.sea_call_type,
                                                                    fact_call_detail.interaction_mode,
                                                                    fact_call_detail.hcp_kol_initiated,
                                                                    fact_call_detail.msl_interaction_type,
                                                                    fact_call_detail.call_objective,
                                                                    fact_call_detail.submission_delay,
                                                                    fact_call_detail.region,
                                                                    fact_call_detail.md_hsp_admin,
                                                                    fact_call_detail.hsp_minutes,
                                                                    fact_call_detail.ortho_on_call_case,
                                                                    fact_call_detail.ortho_volunteer_case,
                                                                    fact_call_detail.md_calc1,
                                                                    fact_call_detail.md_calculate_non_case_time,
                                                                    fact_call_detail.md_calculated_hours_field,
                                                                    fact_call_detail.md_casedeployment,
                                                                    fact_call_detail.md_case_coverage_12_hours,
                                                                    fact_call_detail.md_product_discussion,
                                                                    fact_call_detail.md_concurrent_call,
                                                                    fact_call_detail.courtesy_call,
                                                                    fact_call_detail.md_in_service,
                                                                    fact_call_detail.md_kol_course_discussion,
                                                                    fact_call_detail.kol_minutes,
                                                                    fact_call_detail.other_activities_time_12_hours,
                                                                    fact_call_detail.other_in_field_activities,
                                                                    fact_call_detail.md_overseas_workshop_visit,
                                                                    fact_call_detail.md_ra_activities2,
                                                                    fact_call_detail.sales_activity,
                                                                    fact_call_detail.sales_time_12_hours,
                                                                    fact_call_detail.time_spent,
                                                                    fact_call_detail.time_spent_on_other_activities_simp,
                                                                    fact_call_detail.time_spent_on_sales_activity,
                                                                    fact_call_detail.md_time_spent_on_a_call,
                                                                    fact_call_detail.md_case_type,
                                                                    fact_call_detail.md_sets_activities,
                                                                    fact_call_detail.md_time_spent_on_case,
                                                                    fact_call_detail.time_spent_on_other_activities,
                                                                    fact_call_detail.time_spent_per_call,
                                                                    fact_call_detail.md_case_conducted_in_hospital,
                                                                    fact_call_detail.calculated_field_2,
                                                                    fact_call_detail.calculated_hours_3,
                                                                    fact_call_detail.call_planned,
                                                                    fact_call_detail.call_submission_day,
                                                                    fact_call_detail.case_coverage,
                                                                    fact_call_detail.day_of_week,
                                                                    fact_call_detail.md_minutes,
                                                                    fact_call_detail.md_in_or_ot,
                                                                    fact_call_detail.md_d_call_type,
                                                                    fact_call_detail.md_hours,
                                                                    fact_call_detail.call_record_type_name,
                                                                    fact_call_detail.call_name,
                                                                    fact_call_detail.parent_call_name,
                                                                    fact_call_detail.submitted_by_mobile,
                                                                    fact_call_detail.call_source_id,
                                                                    fact_call_detail.product_indication_key,
                                                                    fact_call_detail.call_detail_priority,
                                                                    fact_call_detail.call_detail_type,
                                                                    fact_call_detail.call_discussion_record_type_source_id,
                                                                    fact_call_detail.comments,
                                                                    fact_call_detail.discussion_topics,
                                                                    fact_call_detail.discussion_type,
                                                                    fact_call_detail.call_discussion_type,
                                                                    fact_call_detail.effectiveness,
                                                                    fact_call_detail.follow_up_activity,
                                                                    fact_call_detail.outcomes,
                                                                    fact_call_detail.follow_up_additional_info,
                                                                    fact_call_detail.follow_up_date,
                                                                    fact_call_detail.materials_used,
                                                                    fact_call_detail.call_detail_source_id,
                                                                    fact_call_detail.call_detail_name,
                                                                    fact_call_detail.call_discussion_source_id,
                                                                    fact_call_detail.call_discussion_name,
                                                                    fact_call_detail.call_modify_dt,
                                                                    fact_call_detail.call_modify_id,
                                                                    fact_call_detail.call_detail_modify_dt,
                                                                    fact_call_detail.call_detail_modify_id,
                                                                    fact_call_detail.call_discussion_modify_dt,
                                                                    fact_call_detail.call_discussion_modify_id,
                                                                    fact_call_detail.inserted_date,
                                                                    fact_call_detail.updated_date
                                                                FROM fact_call_detail
                                                                WHERE (
                                                                        (
                                                                            (fact_call_detail.parent_call_flag = 1)
                                                                            AND ((fact_call_detail.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                                                                            )
                                                                        AND ((fact_call_detail.country_key)::TEXT <> ('ZZ'::CHARACTER VARYING)::TEXT)
                                                                        )
                                                                ) fact JOIN dim_date dimdate ON ((fact.call_date_key = dimdate.date_key))
                                                            ) JOIN vw_employee_hier hier ON (
                                                                (
                                                                    ((fact.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                                                    AND ((fact.country_key)::TEXT = (hier.country_code)::TEXT)
                                                                    )
                                                                )
                                                        ) LEFT JOIN dim_employee employee ON (((fact.employee_key)::TEXT = (employee.employee_key)::TEXT))
                                                    ) LEFT JOIN (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    SELECT 'SG'::CHARACTER VARYING AS country,
                                                                        ds.date_year,
                                                                        ds.date_month,
                                                                        count(*) AS working_days
                                                                    FROM dim_date ds
                                                                    WHERE (
                                                                            (
                                                                                (
                                                                                    (
                                                                                        NOT (
                                                                                            (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                                SELECT DISTINCT holiday_list.holiday_key
                                                                                                FROM holiday_list
                                                                                                WHERE ((holiday_list.country)::TEXT = ('SG'::CHARACTER VARYING)::TEXT)
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                                    )
                                                                                AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                            )
                                                                    GROUP BY 1,
                                                                        ds.date_year,
                                                                        ds.date_month
                                                                    
                                                                    UNION ALL
                                                                    
                                                                    SELECT 'MY'::CHARACTER VARYING AS country,
                                                                        ds.date_year,
                                                                        ds.date_month,
                                                                        count(*) AS working_days
                                                                    FROM dim_date ds
                                                                    WHERE (
                                                                            (
                                                                                (
                                                                                    (
                                                                                        NOT (
                                                                                            (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                                SELECT DISTINCT holiday_list.holiday_key
                                                                                                FROM holiday_list
                                                                                                WHERE ((holiday_list.country)::TEXT = ('MY'::CHARACTER VARYING)::TEXT)
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                                    )
                                                                                AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                            )
                                                                    GROUP BY 1,
                                                                        ds.date_year,
                                                                        ds.date_month
                                                                    )
                                                                
                                                                UNION ALL
                                                                
                                                                SELECT 'VN'::CHARACTER VARYING AS country,
                                                                    ds.date_year,
                                                                    ds.date_month,
                                                                    count(*) AS working_days
                                                                FROM dim_date ds
                                                                WHERE (
                                                                        (
                                                                            (
                                                                                (
                                                                                    NOT (
                                                                                        (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                            SELECT DISTINCT holiday_list.holiday_key
                                                                                            FROM holiday_list
                                                                                            WHERE ((holiday_list.country)::TEXT = ('VN'::CHARACTER VARYING)::TEXT)
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                            )
                                                                        AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                        )
                                                                GROUP BY 1,
                                                                    ds.date_year,
                                                                    ds.date_month
                                                                )
                                                            
                                                            UNION ALL
                                                            
                                                            SELECT 'TH'::CHARACTER VARYING AS country,
                                                                ds.date_year,
                                                                ds.date_month,
                                                                count(*) AS working_days
                                                            FROM dim_date ds
                                                            WHERE (
                                                                    (
                                                                        (
                                                                            (
                                                                                NOT (
                                                                                    (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                        SELECT DISTINCT holiday_list.holiday_key
                                                                                        FROM holiday_list
                                                                                        WHERE ((holiday_list.country)::TEXT = ('TH'::CHARACTER VARYING)::TEXT)
                                                                                        )
                                                                                    )
                                                                                )
                                                                            AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                            )
                                                                        AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                        )
                                                                    AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                    )
                                                            GROUP BY 1,
                                                                ds.date_year,
                                                                ds.date_month
                                                            )
                                                        
                                                        UNION ALL
                                                        
                                                        SELECT 'PH'::CHARACTER VARYING AS country,
                                                            ds.date_year,
                                                            ds.date_month,
                                                            count(*) AS working_days
                                                        FROM dim_date ds
                                                        WHERE (
                                                                (
                                                                    (
                                                                        (
                                                                            NOT (
                                                                                (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                    SELECT DISTINCT holiday_list.holiday_key
                                                                                    FROM holiday_list
                                                                                    WHERE ((holiday_list.country)::TEXT = ('PH'::CHARACTER VARYING)::TEXT)
                                                                                    )
                                                                                )
                                                                            )
                                                                        AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                        )
                                                                    AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                    )
                                                                AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                                )
                                                        GROUP BY 1,
                                                            ds.date_year,
                                                            ds.date_month
                                                        )
                                                    
                                                    UNION ALL
                                                    
                                                    SELECT 'ID'::CHARACTER VARYING AS country,
                                                        ds.date_year,
                                                        ds.date_month,
                                                        count(*) AS working_days
                                                    FROM dim_date ds
                                                    WHERE (
                                                            (
                                                                (
                                                                    (
                                                                        NOT (
                                                                            (ds.date_key)::CHARACTER VARYING(20) IN (
                                                                                SELECT DISTINCT holiday_list.holiday_key
                                                                                FROM holiday_list
                                                                                WHERE ((holiday_list.country)::TEXT = ('ID'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            )
                                                                        )
                                                                    AND (rtrim(ds.date_dayofweek)::TEXT <> ('Saturday'::CHARACTER VARYING)::TEXT)
                                                                    )
                                                                AND (rtrim(ds.date_dayofweek)::TEXT <> ('Sunday'::CHARACTER VARYING)::TEXT)
                                                                )
                                                            AND (((ds.date_key)::CHARACTER VARYING)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                                            )
                                                    GROUP BY 1,
                                                        ds.date_year,
                                                        ds.date_month
                                                    ) ds ON (
                                                        (
                                                            (
                                                                (((dimdate.date_year)::CHARACTER VARYING)::TEXT = ((ds.date_year)::CHARACTER VARYING)::TEXT)
                                                                AND ((dimdate.date_month)::TEXT = (ds.date_month)::TEXT)
                                                                )
                                                            AND ((fact.country_key)::TEXT = (ds.country)::TEXT)
                                                            )
                                                        )
                                                )
                                            GROUP BY dimdate.date_year,
                                                dimdate.date_month,
                                                dimdate.date_quarter,
                                                fact.country_key,
                                                hier.employee_name,
                                                fact.employee_key,
                                                hier.l1_username,
                                                hier.sector,
                                                hier.l2_manager_name,
                                                employee.organization_l3_name
                                            ) fact
                                        ) src1 LEFT JOIN (
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
                                        ) actve_usr ON (
                                            (
                                                (
                                                    (
                                                        (src1.date_year = actve_usr.year)
                                                        AND ((src1.date_month)::TEXT = ((actve_usr.month)::CHARACTER VARYING)::TEXT)
                                                        )
                                                    AND ((src1.country)::TEXT = (actve_usr.country_code)::TEXT)
                                                    )
                                                AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(actve_usr.sector, '#'::CHARACTER VARYING))::TEXT)
                                                )
                                            )
                                    ) LEFT JOIN (
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
                                    ) emp1 ON (
                                        (
                                            ((src1.country)::TEXT = (emp1.country_code)::TEXT)
                                            AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(emp1.sector, '#'::CHARACTER VARYING))::TEXT)
                                            )
                                        )
                                ) LEFT JOIN (
                                SELECT count(1) AS total_cnt_clm_flg,
                                    fct.employee_key,
                                    dimdate.date_year AS yr,
                                    CASE 
                                        WHEN (
                                                ((dimdate.date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.date_month IS NULL)
                                                    AND ('Jan' IS NULL)
                                                    )
                                                )
                                            THEN '1'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.date_month IS NULL)
                                                    AND ('Feb' IS NULL)
                                                    )
                                                )
                                            THEN '2'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.date_month IS NULL)
                                                    AND ('Mar' IS NULL)
                                                    )
                                                )
                                            THEN '3'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.date_month IS NULL)
                                                    AND ('Apr' IS NULL)
                                                    )
                                                )
                                            THEN '4'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.date_month IS NULL)
                                                    AND ('May' IS NULL)
                                                    )
                                                )
                                            THEN '5'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.date_month IS NULL)
                                                    AND ('Jun' IS NULL)
                                                    )
                                                )
                                            THEN '6'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.date_month IS NULL)
                                                    AND ('Jul' IS NULL)
                                                    )
                                                )
                                            THEN '7'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.date_month IS NULL)
                                                    AND ('Aug' IS NULL)
                                                    )
                                                )
                                            THEN '8'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.date_month IS NULL)
                                                    AND ('Sep' IS NULL)
                                                    )
                                                )
                                            THEN '9'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.date_month IS NULL)
                                                    AND ('Oct' IS NULL)
                                                    )
                                                )
                                            THEN '10'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.date_month IS NULL)
                                                    AND ('Nov' IS NULL)
                                                    )
                                                )
                                            THEN '11'::CHARACTER VARYING
                                        WHEN (
                                                ((dimdate.date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (dimdate.date_month IS NULL)
                                                    AND ('Dec' IS NULL)
                                                    )
                                                )
                                            THEN '12'::CHARACTER VARYING
                                        ELSE dimdate.date_month
                                        END AS mnth,
                                    fct.country_key,
                                    hier.l2_manager_name,
                                    hier.sector,
                                    employee.organization_l3_name
                                FROM (
                                    (
                                        (
                                            fact_call_detail fct JOIN dim_date dimdate ON ((fct.call_date_key = dimdate.date_key))
                                            ) JOIN vw_employee_hier hier ON (
                                                (
                                                    ((fct.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                                    AND ((fct.country_key)::TEXT = (hier.country_code)::TEXT)
                                                    )
                                                )
                                        ) LEFT JOIN dim_employee employee ON (((fct.employee_key)::TEXT = (employee.employee_key)::TEXT))
                                    )
                                WHERE (
                                        (
                                            (fct.call_clm_flag = 1)
                                            AND (fct.parent_call_flag = 1)
                                            )
                                        AND ((fct.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                                        )
                                GROUP BY fct.employee_key,
                                    dimdate.date_year,
                                    dimdate.date_month,
                                    fct.country_key,
                                    hier.l2_manager_name,
                                    hier.sector,
                                    employee.organization_l3_name
                                ) fct1 ON (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            ((src1.employee_key)::TEXT = (fct1.employee_key)::TEXT)
                                                            AND (src1.date_year = fct1.yr)
                                                            )
                                                        AND ((src1.date_month)::TEXT = (fct1.mnth)::TEXT)
                                                        )
                                                    AND ((fct1.country_key)::TEXT = (src1.country)::TEXT)
                                                    )
                                                AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct1.sector, '#'::CHARACTER VARYING))::TEXT)
                                                )
                                            AND ((COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT)
                                            )
                                        AND ((COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct1.l2_manager_name, '#'::CHARACTER VARYING))::TEXT)
                                        )
                                    )
                            ) LEFT JOIN (
                            SELECT count(1) AS total_prnt_cnt_clm_flg,
                                fct.employee_key,
                                dimdate.date_year AS yr,
                                CASE 
                                    WHEN (
                                            ((dimdate.date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.date_month IS NULL)
                                                AND ('Jan' IS NULL)
                                                )
                                            )
                                        THEN '1'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.date_month IS NULL)
                                                AND ('Feb' IS NULL)
                                                )
                                            )
                                        THEN '2'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.date_month IS NULL)
                                                AND ('Mar' IS NULL)
                                                )
                                            )
                                        THEN '3'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.date_month IS NULL)
                                                AND ('Apr' IS NULL)
                                                )
                                            )
                                        THEN '4'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.date_month IS NULL)
                                                AND ('May' IS NULL)
                                                )
                                            )
                                        THEN '5'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.date_month IS NULL)
                                                AND ('Jun' IS NULL)
                                                )
                                            )
                                        THEN '6'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.date_month IS NULL)
                                                AND ('Jul' IS NULL)
                                                )
                                            )
                                        THEN '7'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.date_month IS NULL)
                                                AND ('Aug' IS NULL)
                                                )
                                            )
                                        THEN '8'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.date_month IS NULL)
                                                AND ('Sep' IS NULL)
                                                )
                                            )
                                        THEN '9'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.date_month IS NULL)
                                                AND ('Oct' IS NULL)
                                                )
                                            )
                                        THEN '10'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.date_month IS NULL)
                                                AND ('Nov' IS NULL)
                                                )
                                            )
                                        THEN '11'::CHARACTER VARYING
                                    WHEN (
                                            ((dimdate.date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (dimdate.date_month IS NULL)
                                                AND ('Dec' IS NULL)
                                                )
                                            )
                                        THEN '12'::CHARACTER VARYING
                                    ELSE dimdate.date_month
                                    END AS mnth,
                                fct.country_key,
                                hier.l2_manager_name,
                                hier.sector,
                                employee.organization_l3_name
                            FROM (
                                (
                                    (
                                        fact_call_detail fct JOIN dim_date dimdate ON ((fct.call_date_key = dimdate.date_key))
                                        ) JOIN vw_employee_hier hier ON (
                                            (
                                                ((fct.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                                AND ((fct.country_key)::TEXT = (hier.country_code)::TEXT)
                                                )
                                            )
                                    ) LEFT JOIN dim_employee employee ON (((fct.employee_key)::TEXT = (employee.employee_key)::TEXT))
                                )
                            WHERE (
                                    (fct.parent_call_flag = 1)
                                    AND ((fct.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                                    )
                            GROUP BY fct.employee_key,
                                dimdate.date_year,
                                dimdate.date_month,
                                fct.country_key,
                                hier.l2_manager_name,
                                hier.sector,
                                employee.organization_l3_name
                            ) fct2 ON (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        ((src1.employee_key)::TEXT = (fct2.employee_key)::TEXT)
                                                        AND (src1.date_year = fct2.yr)
                                                        )
                                                    AND ((src1.date_month)::TEXT = (fct2.mnth)::TEXT)
                                                    )
                                                AND ((fct2.country_key)::TEXT = (src1.country)::TEXT)
                                                )
                                            AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct2.sector, '#'::CHARACTER VARYING))::TEXT)
                                            )
                                        AND ((COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct2.organization_l3_name, '#'::CHARACTER VARYING))::TEXT)
                                        )
                                    AND ((COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct2.l2_manager_name, '#'::CHARACTER VARYING))::TEXT)
                                    )
                                )
                        ) LEFT JOIN (
                        SELECT (sum(COALESCE(terr.sea_frml_hours_on, ((0)::NUMERIC)::NUMERIC(18, 0))) / ((8)::NUMERIC)::NUMERIC(18, 0)) AS cnt_total_time_on,
                            terr.employee_key,
                            terr.country_key,
                            date_dim.date_year AS yr,
                            CASE 
                                WHEN (
                                        ((date_dim.date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.date_month IS NULL)
                                            AND ('Jan' IS NULL)
                                            )
                                        )
                                    THEN '1'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.date_month IS NULL)
                                            AND ('Feb' IS NULL)
                                            )
                                        )
                                    THEN '2'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.date_month IS NULL)
                                            AND ('Mar' IS NULL)
                                            )
                                        )
                                    THEN '3'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.date_month IS NULL)
                                            AND ('Apr' IS NULL)
                                            )
                                        )
                                    THEN '4'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.date_month IS NULL)
                                            AND ('May' IS NULL)
                                            )
                                        )
                                    THEN '5'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.date_month IS NULL)
                                            AND ('Jun' IS NULL)
                                            )
                                        )
                                    THEN '6'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.date_month IS NULL)
                                            AND ('Jul' IS NULL)
                                            )
                                        )
                                    THEN '7'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.date_month IS NULL)
                                            AND ('Aug' IS NULL)
                                            )
                                        )
                                    THEN '8'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.date_month IS NULL)
                                            AND ('Sep' IS NULL)
                                            )
                                        )
                                    THEN '9'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.date_month IS NULL)
                                            AND ('Oct' IS NULL)
                                            )
                                        )
                                    THEN '10'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.date_month IS NULL)
                                            AND ('Nov' IS NULL)
                                            )
                                        )
                                    THEN '11'::CHARACTER VARYING
                                WHEN (
                                        ((date_dim.date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                                        OR (
                                            (date_dim.date_month IS NULL)
                                            AND ('Dec' IS NULL)
                                            )
                                        )
                                    THEN '12'::CHARACTER VARYING
                                ELSE date_dim.date_month
                                END AS mnth
                        FROM (
                            fact_timeoff_territory terr JOIN dim_date date_dim ON (((terr.start_date_key)::TEXT = ((date_dim.date_key)::CHARACTER VARYING)::TEXT))
                            )
                        WHERE (
                                ((terr.sea_time_on_time_off)::TEXT = ('Time On'::CHARACTER VARYING)::TEXT)
                                AND ((terr.start_date_key)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                                )
                        GROUP BY terr.employee_key,
                            terr.country_key,
                            date_dim.date_year,
                            date_dim.date_month
                        ) terr ON (
                            (
                                (
                                    (
                                        ((terr.employee_key)::TEXT = (src1.employee_key)::TEXT)
                                        AND ((terr.country_key)::TEXT = (src1.country)::TEXT)
                                        )
                                    AND (src1.date_year = terr.yr)
                                    )
                                AND ((src1.date_month)::TEXT = (terr.mnth)::TEXT)
                                )
                            )
                    ) LEFT JOIN (
                    SELECT (sum(COALESCE(terr.total_time_off, ((0)::NUMERIC)::NUMERIC(18, 0))) / ((8)::NUMERIC)::NUMERIC(18, 0)) AS cnt_total_time_off,
                        terr.employee_key,
                        terr.country_key,
                        date_dim.date_year AS yr,
                        CASE 
                            WHEN (
                                    ((date_dim.date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.date_month IS NULL)
                                        AND ('Jan' IS NULL)
                                        )
                                    )
                                THEN '1'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.date_month IS NULL)
                                        AND ('Feb' IS NULL)
                                        )
                                    )
                                THEN '2'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.date_month IS NULL)
                                        AND ('Mar' IS NULL)
                                        )
                                    )
                                THEN '3'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.date_month IS NULL)
                                        AND ('Apr' IS NULL)
                                        )
                                    )
                                THEN '4'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.date_month IS NULL)
                                        AND ('May' IS NULL)
                                        )
                                    )
                                THEN '5'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.date_month IS NULL)
                                        AND ('Jun' IS NULL)
                                        )
                                    )
                                THEN '6'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.date_month IS NULL)
                                        AND ('Jul' IS NULL)
                                        )
                                    )
                                THEN '7'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.date_month IS NULL)
                                        AND ('Aug' IS NULL)
                                        )
                                    )
                                THEN '8'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.date_month IS NULL)
                                        AND ('Sep' IS NULL)
                                        )
                                    )
                                THEN '9'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.date_month IS NULL)
                                        AND ('Oct' IS NULL)
                                        )
                                    )
                                THEN '10'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.date_month IS NULL)
                                        AND ('Nov' IS NULL)
                                        )
                                    )
                                THEN '11'::CHARACTER VARYING
                            WHEN (
                                    ((date_dim.date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (date_dim.date_month IS NULL)
                                        AND ('Dec' IS NULL)
                                        )
                                    )
                                THEN '12'::CHARACTER VARYING
                            ELSE date_dim.date_month
                            END AS mnth
                    FROM (
                        fact_timeoff_territory terr JOIN dim_date date_dim ON (((terr.start_date_key)::TEXT = ((date_dim.date_key)::CHARACTER VARYING)::TEXT))
                        )
                    WHERE (
                            ((terr.sea_time_on_time_off)::TEXT = ('Time Off'::CHARACTER VARYING)::TEXT)
                            AND ((terr.start_date_key)::TEXT < to_char(sysdate(), ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                            )
                    GROUP BY terr.employee_key,
                        terr.country_key,
                        date_dim.date_year,
                        date_dim.date_month
                    ) terr1 ON (
                        (
                            (
                                (
                                    ((terr1.employee_key)::TEXT = (src1.employee_key)::TEXT)
                                    AND ((terr1.country_key)::TEXT = (src1.country)::TEXT)
                                    )
                                AND (src1.date_year = terr1.yr)
                                )
                            AND ((src1.date_month)::TEXT = (terr1.mnth)::TEXT)
                            )
                        )
                ) LEFT JOIN (
                SELECT count(DISTINCT fct.call_name) AS total_sbmtd_calls_key_message,
                    fct.employee_key,
                    dimdate.date_year AS yr,
                    CASE 
                        WHEN (
                                ((dimdate.date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.date_month IS NULL)
                                    AND ('Jan' IS NULL)
                                    )
                                )
                            THEN '1'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.date_month IS NULL)
                                    AND ('Feb' IS NULL)
                                    )
                                )
                            THEN '2'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.date_month IS NULL)
                                    AND ('Mar' IS NULL)
                                    )
                                )
                            THEN '3'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.date_month IS NULL)
                                    AND ('Apr' IS NULL)
                                    )
                                )
                            THEN '4'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.date_month IS NULL)
                                    AND ('May' IS NULL)
                                    )
                                )
                            THEN '5'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.date_month IS NULL)
                                    AND ('Jun' IS NULL)
                                    )
                                )
                            THEN '6'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.date_month IS NULL)
                                    AND ('Jul' IS NULL)
                                    )
                                )
                            THEN '7'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.date_month IS NULL)
                                    AND ('Aug' IS NULL)
                                    )
                                )
                            THEN '8'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.date_month IS NULL)
                                    AND ('Sep' IS NULL)
                                    )
                                )
                            THEN '9'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.date_month IS NULL)
                                    AND ('Oct' IS NULL)
                                    )
                                )
                            THEN '10'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.date_month IS NULL)
                                    AND ('Nov' IS NULL)
                                    )
                                )
                            THEN '11'::CHARACTER VARYING
                        WHEN (
                                ((dimdate.date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                                OR (
                                    (dimdate.date_month IS NULL)
                                    AND ('Dec' IS NULL)
                                    )
                                )
                            THEN '12'::CHARACTER VARYING
                        ELSE dimdate.date_month
                        END AS mnth,
                    fct.country_key,
                    hier.l2_manager_name,
                    hier.sector,
                    employee.organization_l3_name
                FROM (
                    (
                        (
                            (
                                fact_call_detail fct JOIN dim_date dimdate ON ((fct.call_date_key = dimdate.date_key))
                                ) JOIN fact_call_key_message fckm ON (((fct.call_source_id)::TEXT = (fckm.call_source_id)::TEXT))
                            ) JOIN vw_employee_hier hier ON (
                                (
                                    ((fct.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                    AND ((fct.country_key)::TEXT = (hier.country_code)::TEXT)
                                    )
                                )
                        ) LEFT JOIN dim_employee employee ON (((fct.employee_key)::TEXT = (employee.employee_key)::TEXT))
                    )
                WHERE (
                        (fct.parent_call_flag = 1)
                        AND ((fct.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                        )
                GROUP BY fct.employee_key,
                    dimdate.date_year,
                    dimdate.date_month,
                    fct.country_key,
                    hier.l2_manager_name,
                    hier.sector,
                    employee.organization_l3_name
                ) fct3 ON (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            ((src1.employee_key)::TEXT = (fct3.employee_key)::TEXT)
                                            AND (src1.date_year = fct3.yr)
                                            )
                                        AND ((src1.date_month)::TEXT = (fct3.mnth)::TEXT)
                                        )
                                    AND ((fct3.country_key)::TEXT = (src1.country)::TEXT)
                                    )
                                AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct3.sector, '#'::CHARACTER VARYING))::TEXT)
                                )
                            AND ((COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct3.organization_l3_name, '#'::CHARACTER VARYING))::TEXT)
                            )
                        AND ((COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct3.l2_manager_name, '#'::CHARACTER VARYING))::TEXT)
                        )
                    )
            ) LEFT JOIN (
            SELECT count(1) AS total_call_product,
                fct.employee_key,
                dimdate.date_year AS yr,
                CASE 
                    WHEN (
                            ((dimdate.date_month)::TEXT = ('Jan'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.date_month IS NULL)
                                AND ('Jan' IS NULL)
                                )
                            )
                        THEN '1'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.date_month)::TEXT = ('Feb'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.date_month IS NULL)
                                AND ('Feb' IS NULL)
                                )
                            )
                        THEN '2'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.date_month)::TEXT = ('Mar'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.date_month IS NULL)
                                AND ('Mar' IS NULL)
                                )
                            )
                        THEN '3'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.date_month)::TEXT = ('Apr'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.date_month IS NULL)
                                AND ('Apr' IS NULL)
                                )
                            )
                        THEN '4'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.date_month)::TEXT = ('May'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.date_month IS NULL)
                                AND ('May' IS NULL)
                                )
                            )
                        THEN '5'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.date_month)::TEXT = ('Jun'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.date_month IS NULL)
                                AND ('Jun' IS NULL)
                                )
                            )
                        THEN '6'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.date_month)::TEXT = ('Jul'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.date_month IS NULL)
                                AND ('Jul' IS NULL)
                                )
                            )
                        THEN '7'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.date_month)::TEXT = ('Aug'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.date_month IS NULL)
                                AND ('Aug' IS NULL)
                                )
                            )
                        THEN '8'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.date_month)::TEXT = ('Sep'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.date_month IS NULL)
                                AND ('Sep' IS NULL)
                                )
                            )
                        THEN '9'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.date_month)::TEXT = ('Oct'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.date_month IS NULL)
                                AND ('Oct' IS NULL)
                                )
                            )
                        THEN '10'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.date_month)::TEXT = ('Nov'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.date_month IS NULL)
                                AND ('Nov' IS NULL)
                                )
                            )
                        THEN '11'::CHARACTER VARYING
                    WHEN (
                            ((dimdate.date_month)::TEXT = ('Dec'::CHARACTER VARYING)::TEXT)
                            OR (
                                (dimdate.date_month IS NULL)
                                AND ('Dec' IS NULL)
                                )
                            )
                        THEN '12'::CHARACTER VARYING
                    ELSE dimdate.date_month
                    END AS mnth,
                fct.country_key,
                hier.l2_manager_name,
                hier.sector,
                employee.organization_l3_name
            FROM (
                (
                    (
                        fact_call_detail fct JOIN dim_date dimdate ON ((fct.call_date_key = dimdate.date_key))
                        ) JOIN vw_employee_hier hier ON (
                            (
                                ((fct.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                AND ((fct.country_key)::TEXT = (hier.country_code)::TEXT)
                                )
                            )
                    ) LEFT JOIN dim_employee employee ON (((fct.employee_key)::TEXT = (employee.employee_key)::TEXT))
                )
            WHERE (
                    (
                        (fct.parent_call_flag = 1)
                        AND ((fct.call_status_type)::TEXT = ('Submitted'::CHARACTER VARYING)::TEXT)
                        )
                    AND (
                        (fct.detailed_products IS NOT NULL)
                        OR (
                            ((fct.detailed_products)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                            AND ((fct.detailed_products)::TEXT <> (' '::CHARACTER VARYING)::TEXT)
                            )
                        )
                    )
            GROUP BY fct.employee_key,
                dimdate.date_year,
                dimdate.date_month,
                fct.country_key,
                hier.l2_manager_name,
                hier.sector,
                employee.organization_l3_name
            ) fct4 ON (
                (
                    (
                        (
                            (
                                (
                                    (
                                        ((src1.employee_key)::TEXT = (fct4.employee_key)::TEXT)
                                        AND (src1.date_year = fct4.yr)
                                        )
                                    AND ((src1.date_month)::TEXT = (fct4.mnth)::TEXT)
                                    )
                                AND ((fct4.country_key)::TEXT = (src1.country)::TEXT)
                                )
                            AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct4.sector, '#'::CHARACTER VARYING))::TEXT)
                            )
                        AND ((COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct4.organization_l3_name, '#'::CHARACTER VARYING))::TEXT)
                        )
                    AND ((COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING))::TEXT = (COALESCE(fct4.l2_manager_name, '#'::CHARACTER VARYING))::TEXT)
                    )
                )
        )
    ),
union_of
AS (
    SELECT *
    FROM T1
    
    UNION ALL
    
    SELECT *
    FROM T2
    ),
FINAL
AS (
    SELECT *
    FROM UNION_OF
    )
SELECT *
FROM FINAL
