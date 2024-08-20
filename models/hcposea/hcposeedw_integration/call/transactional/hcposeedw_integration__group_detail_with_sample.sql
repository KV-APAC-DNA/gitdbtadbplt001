with itg_call as (
    select * from dev_dna_core.hcposeitg_integration.itg_call
),

itg_call_detail as (
    select * from dev_dna_core.hcposeitg_integration.itg_call_detail
),

itg_call_discussion as (
    select * from dev_dna_core.hcposeitg_integration.itg_call_discussion
),

itg_recordtype as (
    select * from dev_dna_core.hcposeitg_integration.itg_recordtype
),

dim_country as (
     select * from dev_dna_core.hcposeedw_integration.dim_country
),

dim_employee as (
    select * from dev_dna_core.hcposeedw_integration.dim_employee
),

dim_profile as (
    select * from dev_dna_core.hcposeedw_integration.dim_profile
),

dim_organization as (
    select * from dev_dna_core.hcposeedw_integration.dim_organization

dim_date as (
    select * from dev_dna_core.hcposeedw_integration.dim_date
),

dim_product_indication as (
    select * from dev_dna_core.hcposeedw_integration.dim_product_indication
),

dim_hcp as (
     select * from dev_dna_core.hcposeedw_integration.dim_hcp
),

dim_hco as (
    select * from dev_dna_core.hcposeedw_integration.dim_hco
),

itg_lookup_retention_period as (
    select * from dev_dna_core.snaposeitg_integration.itg_lookup_retention_period
    {# table is missing from snowflake #}
), 

call
AS (
    SELECT inn.*,
        p.product_indication_key
    FROM (
        SELECT DISTINCT ci.*,
            d.call_detail_source_id,
            d.call_detail_priority,
            d.product_source_id cd_product_source_id,
            d.country_code cd_country_code,
            d.is_parent_call is_parent_call_dtl,
            d.last_modified_date call_detail_modify_date,
            d.last_modified_by_id call_detail_modify_id,
            cd.call_discussion_source_id,
            cd.product_source_id cds_product_source_id,
            cd.country_code cds_country_code,
            cd.last_modified_date call_discussion_modify_date,
            cd.last_modified_by_id call_discussion_modify_id,
            rt.record_type_name,
            us_pr.employee_profile_id,
            nvl(us_pr.employee_key, 'Not Applicable') AS employee_key,
            nvl(us_pr.profile_key, 'Not Applicable') AS profile_key,
            nvl(organization_key, 'Not Applicable') AS organization_key,
            dd.date_key,
            cd.record_type_source_id call_discussion_record_type_source_id,
            cd.comments,
            cd.discussion_topics,
            cd.discussion_type,
            cd.call_discussion_type,
            cd.effectiveness,
            cd.follow_up_activity,
            cd.outcomes,
            cd.follow_up_additional_info,
            cd.follow_up_date,
            cd.materials_used,
            d.call_detail_name,
            cd.call_discussion_name
        FROM itg_call ci,
            (
                SELECT *
                FROM itg_call_detail d
                WHERE nvl(d.is_deleted, '0') = '0'
                ) d,
            (
                SELECT *
                FROM itg_call_discussion cd
                WHERE nvl(cd.is_deleted, '0') = '0'
                ) cd,
            itg_recordtype rt,
            dim_country ct,
            (
                SELECT employee_key,
                    employee_profile_id,
                    employee_source_id,
                    pf.profile_source_id,
                    us.profile_name,
                    profile_key,
                    us.country_code,
                    o.organization_key,
                    o.effective_from_date,
                    o.effective_to_date
                FROM dim_employee us,
                    dim_profile pf,
                    dim_organization o
                WHERE us.employee_profile_id = pf.profile_source_id
                    AND us.country_code = pf.country_code
                    AND us.my_organization_code = o.my_organization_code(+)
                    AND us.country_code = o.country_code(+)
                ) us_pr,
            dim_date dd
        WHERE ci.call_source_id = d.call_source_id(+)
            AND ci.country_code = d.country_code(+)
            --and nvl(d.is_deleted,'0') = '0'          
            AND d.call_source_id = cd.call_source_id(+)
            AND d.country_code = cd.country_code(+)
            --and nvl(cd.is_deleted,'0') = '0'            
            AND d.product_source_id = cd.product_source_id(+)
            AND ci.record_type_source_id = rt.record_type_source_id
            AND ci.country_code = rt.country_code
            AND ci.country_code = ct.country_key
            AND ci.owner_source_id = us_pr.employee_source_id(+)
            AND ci.country_code = us_pr.country_code(+)
            AND ci.call_date = date_value
            --and (ci.parent_call_source_id is NULL or ci.parent_call_source_id = '')
            AND ci.call_type = 'Group Detail with Sample'
            --and ci.load_flag  = 'N'
            AND ci.is_deleted = '0'
            AND (ci.msl_interaction_type IS NULL)
            AND ci.call_date > (select DATE_TRUNC('year', dateadd(day, - retention_years * 365 ,current_timestamp()))
                FROM itg_lookup_retention_period
                WHERE upper(table_name) = 'FACT_CALL_DETAIL'
                )
            AND ci.call_date >= nvl(us_pr.effective_from_date, to_date('1900-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss'))
            AND ci.call_date <= nvl(us_pr.effective_to_date, to_date('2099-12-31 00:00:00', 'yyyy-mm-dd hh24:mi:ss'))
        ) inn,
        DIM_PRODUCT_INDICATION p
    WHERE nvl(nullif(cds_product_source_id, ''), cd_product_source_id) = p.product_source_id(+)
        AND nvl(cds_country_code, cd_country_code) = p.country_code(+)
    ),
main
AS (
    SELECT c1.call_source_id,
        c1.parent_call_source_id,
        c1.call_name parent_call_name,
        c1.attendees,
        c1.attendee_type,
        C1.CALL_NAME,
        c1.is_parent_call,
        hcp.hcp_key,
        c1.country_code,
        c1.call_source_id main_call_source_id
    FROM itg_call c1,
        dim_hcp hcp
    WHERE (
            c1.parent_call_source_id IS NULL
            OR c1.parent_call_source_id = ''
            )
        AND c1.account_source_id = hcp_source_id
        AND c1.country_code = hcp.country_code
        AND c1.call_type = 'Group Detail with Sample'
        AND c1.is_deleted = '0'
    
    UNION ALL
    
    SELECT c2.call_source_id,
        c2.parent_call_source_id,
        c1.call_name parent_call_name,
        c2.attendees,
        c2.attendee_type,
        c2.call_name,
        c2.is_parent_call,
        hcp.hcp_key,
        c1.country_code,
        c1.call_source_id main_call_source_id
    FROM itg_call c1,
        itg_call c2,
        dim_hcp hcp,
        dim_hcp hcp1
    WHERE c1.call_type = 'Detail with Sample'
        AND c1.call_type = c2.call_type
        AND (
            c1.parent_call_source_id IS NULL
            OR c1.parent_call_source_id = ''
            )
        AND c2.parent_call_source_id = c1.call_source_id
        AND c2.account_source_id = hcp.hcp_source_id
        AND c2.country_code = hcp.country_code
        AND c1.account_source_id = hcp1.hcp_source_id
        AND c1.country_code = hcp1.country_code
        AND c2.is_deleted = '0'
    ),
child_rec
AS (
    SELECT child.call_source_id,
        child.parent_call_source_id,
        nvl(hco_key, 'Not Applicable') hco_key,
        child.country_code
    FROM itg_call child,
        dim_hco hco
    WHERE child.account_source_id = hco.hco_source_id
        AND child.country_code = hco.country_code
        AND child.call_type = 'Group Detail with Sample'
        AND parent_call_source_id IS NOT NULL
        AND child.is_deleted = '0'
    ),
cte1
AS (
    SELECT DISTINCT c1.country_code,
        nvl(main.hcp_key, 'Not Applicable') hcp_key,
        nvl(child_rec.hco_key, 'Not Applicable') hco_key,
        c1.employee_key,
        c1.profile_key,
        c1.organization_key,
        c1.date_key
        --, to_char(C1.CALL_DATE,'yyyymmdd') date_key
        ,
        CASE 
            WHEN c1.country_code = 'PH'
                THEN convert_timezone('PHT', C1.CALL_DATE_TIME)
            WHEN c1.country_code = 'SG'
                THEN convert_timezone('SGT', C1.CALL_DATE_TIME)
            WHEN c1.country_code = 'ID'
                THEN convert_timezone('UTC+07', C1.CALL_DATE_TIME)
            WHEN c1.country_code = 'MY'
                THEN convert_timezone('MYT', C1.CALL_DATE_TIME)
            WHEN c1.country_code = 'TH'
                THEN convert_timezone('ICT', C1.CALL_DATE_TIME)
            WHEN c1.country_code = 'VN'
                THEN convert_timezone('ICT', C1.CALL_DATE_TIME)
            WHEN c1.country_code = 'NP'
                THEN convert_timezone('NPT', C1.CALL_DATE_TIME)
            END CALL_DATE_TIME,
        CASE 
            WHEN c1.country_code = 'PH'
                THEN convert_timezone('PHT', C1.LAST_MODIFIED_DATE)
            WHEN c1.country_code = 'SG'
                THEN convert_timezone('SGT', C1.LAST_MODIFIED_DATE)
            WHEN c1.country_code = 'ID'
                THEN convert_timezone('UTC+07', C1.LAST_MODIFIED_DATE)
            WHEN c1.country_code = 'MY'
                THEN convert_timezone('MYT', C1.LAST_MODIFIED_DATE)
            WHEN c1.country_code = 'TH'
                THEN convert_timezone('ICT', C1.LAST_MODIFIED_DATE)
            WHEN c1.country_code = 'VN'
                THEN convert_timezone('ICT', C1.LAST_MODIFIED_DATE)
            WHEN c1.country_code = 'NP'
                THEN convert_timezone('NPT', C1.LAST_MODIFIED_DATE)
            END LAST_MODIFIED_DATE,
        CASE 
            WHEN c1.country_code = 'PH'
                THEN convert_timezone('PHT', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            WHEN c1.country_code = 'SG'
                THEN convert_timezone('SGT', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            WHEN c1.country_code = 'ID'
                THEN convert_timezone('UTC+07', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            WHEN c1.country_code = 'MY'
                THEN convert_timezone('MYT', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            WHEN c1.country_code = 'TH'
                THEN convert_timezone('ICT', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            WHEN c1.country_code = 'VN'
                THEN convert_timezone('ICT', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            WHEN c1.country_code = 'NP'
                THEN convert_timezone('NPT', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            END MOBILE_LAST_MODIFIED_DATE_TIME,
        C1.CALL_DATE_TIME utc_call_date_time,
        C1.LAST_MODIFIED_DATE utc_call_entry_time,
        substring(CALL_STATUS_TYPE, 1, length(call_status_type) - 4) call_status_type,
        'F2F' CALL_CHANNEL_TYP,
        C1.CALL_TYPE,
        C1.SEA_CALL_TYPE,
        C1.CALL_DURATION,
        C1.CALL_CLM
        --,case when ( main.parent_call_source_id is null and nvl(call_detail_priority,1) = 1 ) then 1 else 0 end is_parent_call
        ,
        row_number() OVER (
            PARTITION BY main.parent_call_name,
            main.country_code ORDER BY main.parent_call_source_id nulls first,
                nvl(hcp_key, 'Not Applicable'),
                nvl(hco_key, 'Not Applicable'),
                call_detail_priority
            ) is_parent_call_base,
        main.attendees,
        substring(main.attendee_type, 1, length(main.attendee_type) - 4) attendee_type,
        replace(c1.call_comments, chr(10), '') call_comments,
        c1.next_call_notes,
        C1.pre_call_notes,
        replace(c1.manager_call_comment, chr(10), '') manager_call_comment,
        C1.record_type_name,
        C1.CALL_NAME,
        main.parent_call_name,
        C1.SUBMITTED_BY_MOBILE,
        C1.call_source_id,
        main.PARENT_CALL_SOURCE_ID,
        nvl(product_indication_key, 'Not Applicable') AS product_indication_key,
        call_detail_priority,
        CASE 
            WHEN call_detail_modify_date IS NOT NULL
                THEN 'Detail'
            ELSE NULL
            END CALL_DETAIL_TYPE
        --, call_detail_duration 
        ,
        call_detail_source_id,
        call_discussion_source_id,
        C1.LAST_MODIFIED_DATE AS LASTMODIFIEDDATE,
        C1.LAST_MODIFIED_BY_ID,
        call_detail_modify_date,
        call_detail_modify_id,
        call_discussion_modify_date,
        call_discussion_modify_id
        -----new columns starts
        ,
        c1.is_deleted call_is_deleted_flag,
        c1.last_activity_date,
        c1.sample_card,
        c1.account_plan,
        c1.mobile_id,
        c1.significant_event,
        c1.location,
        c1.signature_date,
        c1.signature,
        c1.territory,
        c1.detailed_products,
        c1.user_name,
        c1.medical_event,
        c1.is_sampled_call,
        c1.presentations,
        c1.product_priority_1,
        c1.product_priority_2,
        c1.product_priority_3,
        c1.product_priority_4,
        c1.product_priority_5,
        c1.attendee_list,
        c1.msl_interaction_notes,
        c1.sea_call_type,
        c1.interaction_mode,
        c1.hcp_kol_initiated,
        c1.msl_interaction_type,
        c1.call_objective,
        c1.submission_delay,
        c1.region,
        c1.md_hsp_admin,
        c1.hsp_minutes,
        c1.ortho_on_call_case,
        c1.ortho_volunteer_case,
        c1.md_calc1,
        c1.md_calculate_non_case_time,
        c1.md_calculated_hours_field,
        c1.md_casedeployment,
        c1.md_case_coverage_12_hours,
        c1.md_product_discussion,
        c1.md_concurrent_call,
        c1.courtesy_call,
        c1.md_in_service,
        c1.md_kol_course_discussion,
        c1.kol_minutes,
        c1.other_activities_time_12_hours,
        c1.other_in_field_activities,
        c1.md_overseas_workshop_visit,
        c1.md_ra_activities2,
        c1.sales_activity,
        c1.sales_time_12_hours,
        c1.time_spent,
        c1.time_spent_on_other_activities_simp,
        c1.time_spent_on_sales_activity,
        c1.md_time_spent_on_a_call,
        c1.md_case_type,
        c1.md_sets_activities,
        c1.md_time_spent_on_case,
        c1.time_spent_on_other_activities,
        c1.time_spent_per_call,
        c1.md_case_conducted_in_hospital,
        c1.calculated_field_2,
        c1.calculated_hours_3,
        c1.call_planned,
        c1.call_submission_day,
        c1.case_coverage,
        c1.day_of_week,
        c1.md_minutes,
        c1.md_in_or_ot,
        c1.md_d_call_type,
        c1.md_hours,
        c1.call_discussion_record_type_source_id,
        --c1.record_type_source_id, --call_discussion_record_type_source_id
        c1.comments,
        c1.discussion_topics,
        c1.discussion_type,
        c1.call_discussion_type,
        c1.effectiveness,
        c1.follow_up_activity,
        c1.outcomes,
        c1.follow_up_additional_info,
        c1.follow_up_date,
        c1.materials_used,
        c1.call_detail_name,
        c1.call_discussion_name,
        ------ new columns ends
        current_timestamp() inserted_date,
        current_timestamp() updated_date
    FROM main,
        child_rec,
        call c1
    WHERE main.call_source_id = c1.call_source_id
        AND main.country_code = c1.country_code
        AND main.main_call_source_id = child_rec.parent_call_source_id(+)
        AND main.country_code = child_rec.country_code(+)
    ),
main2
AS (
    SELECT c1.call_source_id,
        c1.parent_call_source_id,
        c1.call_name parent_call_name,
        c1.attendees,
        c1.attendee_type,
        C1.CALL_NAME,
        c1.is_parent_call,
        hco.hco_key,
        c1.country_code,
        c1.call_source_id main_call_source_id
    FROM itg_call c1,
        dim_hco hco
    WHERE (
            c1.parent_call_source_id IS NULL
            OR c1.parent_call_source_id = ''
            )
        AND c1.account_source_id = hco_source_id
        AND c1.country_code = hco.country_code
        AND c1.call_type = 'Group Detail with Sample'
        AND c1.is_deleted = '0'
    
    UNION ALL
    
    SELECT c2.call_source_id,
        c2.parent_call_source_id,
        c1.call_name parent_call_name,
        c2.attendees,
        c2.attendee_type,
        c2.call_name,
        c2.is_parent_call,
        hco.hco_key,
        c1.country_code,
        c1.call_source_id main_call_source_id
    FROM itg_call c1,
        itg_call c2,
        dim_hco hco,
        dim_hco hco1
    WHERE c1.call_type = 'Group Detail with Sample'
        AND c1.call_type = c2.call_type
        AND (
            c1.parent_call_source_id IS NULL
            OR c1.parent_call_source_id = ''
            )
        AND c2.parent_call_source_id = c1.call_source_id
        AND c2.account_source_id = hco.hco_source_id
        AND c2.country_code = hco.country_code
        AND c1.account_source_id = hco1.hco_source_id
        AND c1.country_code = hco1.country_code
        AND c2.is_deleted = '0'
    ),
child_rec2
AS (
    SELECT child.call_source_id,
        child.parent_call_source_id,
        nvl(hcp_key, 'Not Applicable') hcp_key,
        child.country_code
    FROM itg_call child,
        dim_hcp hcp
    WHERE child.account_source_id = hcp.hcp_source_id
        AND child.country_code = hcp.country_code
        AND child.call_type = 'Group Detail with Sample'
        AND parent_call_source_id IS NOT NULL
        AND child.is_deleted = '0'
    ),
cte2
AS (
    SELECT DISTINCT c1.country_code,
        nvl(child_rec2.hcp_key, 'Not Applicable') hcp_key,
        nvl(main2.hco_key, 'Not Applicable') hco_key,
        c1.employee_key,
        c1.profile_key,
        c1.organization_key,
        c1.date_key
        --, to_char(C1.CALL_DATE,'yyyymmdd') date_key
        ,
        CASE 
            WHEN c1.country_code = 'PH'
                THEN convert_timezone('PHT', C1.CALL_DATE_TIME)
            WHEN c1.country_code = 'SG'
                THEN convert_timezone('SGT', C1.CALL_DATE_TIME)
            WHEN c1.country_code = 'ID'
                THEN convert_timezone('UTC+07', C1.CALL_DATE_TIME)
            WHEN c1.country_code = 'MY'
                THEN convert_timezone('MYT', C1.CALL_DATE_TIME)
            WHEN c1.country_code = 'TH'
                THEN convert_timezone('ICT', C1.CALL_DATE_TIME)
            WHEN c1.country_code = 'VN'
                THEN convert_timezone('ICT', C1.CALL_DATE_TIME)
            WHEN c1.country_code = 'NP'
                THEN convert_timezone('NPT', C1.CALL_DATE_TIME)
            END CALL_DATE_TIME,
        CASE 
            WHEN c1.country_code = 'PH'
                THEN convert_timezone('PHT', C1.LAST_MODIFIED_DATE)
            WHEN c1.country_code = 'SG'
                THEN convert_timezone('SGT', C1.LAST_MODIFIED_DATE)
            WHEN c1.country_code = 'ID'
                THEN convert_timezone('UTC+07', C1.LAST_MODIFIED_DATE)
            WHEN c1.country_code = 'MY'
                THEN convert_timezone('MYT', C1.LAST_MODIFIED_DATE)
            WHEN c1.country_code = 'TH'
                THEN convert_timezone('ICT', C1.LAST_MODIFIED_DATE)
            WHEN c1.country_code = 'VN'
                THEN convert_timezone('ICT', C1.LAST_MODIFIED_DATE)
            WHEN c1.country_code = 'NP'
                THEN convert_timezone('NPT', C1.LAST_MODIFIED_DATE)
            END LAST_MODIFIED_DATE,
        CASE 
            WHEN c1.country_code = 'PH'
                THEN convert_timezone('PHT', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            WHEN c1.country_code = 'SG'
                THEN convert_timezone('SGT', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            WHEN c1.country_code = 'ID'
                THEN convert_timezone('UTC+07', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            WHEN c1.country_code = 'MY'
                THEN convert_timezone('MYT', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            WHEN c1.country_code = 'TH'
                THEN convert_timezone('ICT', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            WHEN c1.country_code = 'VN'
                THEN convert_timezone('ICT', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            WHEN c1.country_code = 'NP'
                THEN convert_timezone('NPT', C1.MOBILE_LAST_MODIFIED_DATE_TIME)
            END MOBILE_LAST_MODIFIED_DATE_TIME,
        C1.CALL_DATE_TIME utc_call_date_time,
        C1.LAST_MODIFIED_DATE utc_call_entry_time,
        substring(CALL_STATUS_TYPE, 1, length(call_status_type) - 4) call_status_type,
        'F2F' CALL_CHANNEL_TYP,
        C1.CALL_TYPE,
        C1.SEA_CALL_TYPE,
        C1.CALL_DURATION,
        C1.CALL_CLM
        --,case when ( main.parent_call_source_id is null and nvl(call_detail_priority,1) = 1 ) then 1 else 0 end is_parent_call
        ,
        row_number() OVER (
            PARTITION BY main2.parent_call_name,
            main2.country_code ORDER BY main2.parent_call_source_id nulls first,
                nvl(hcp_key, 'Not Applicable'),
                nvl(hco_key, 'Not Applicable'),
                call_detail_priority
            ) is_parent_call_base,
        main2.attendees,
        substring(main2.attendee_type, 1, length(main2.attendee_type) - 4) attendee_type,
        replace(c1.call_comments, chr(10), '') call_comments,
        c1.next_call_notes,
        C1.pre_call_notes,
        replace(c1.manager_call_comment, chr(10), '') manager_call_comment,
        C1.record_type_name,
        C1.CALL_NAME,
        main2.parent_call_name,
        C1.SUBMITTED_BY_MOBILE,
        C1.call_source_id,
        main2.PARENT_CALL_SOURCE_ID,
        nvl(product_indication_key, 'Not Applicable') AS product_indication_key,
        call_detail_priority,
        CASE 
            WHEN call_detail_modify_date IS NOT NULL
                THEN 'Detail'
            ELSE NULL
            END CALL_DETAIL_TYPE
        --, call_detail_duration
        ,
        call_detail_source_id,
        call_discussion_source_id,
        C1.LAST_MODIFIED_DATE AS LASTMODIFIEDDATE,
        C1.LAST_MODIFIED_BY_ID,
        call_detail_modify_date,
        call_detail_modify_id,
        call_discussion_modify_date,
        call_discussion_modify_id
        --- new columns starts 
        ,
        c1.is_deleted,
        c1.last_activity_date,
        c1.sample_card,
        c1.account_plan,
        c1.mobile_id,
        c1.significant_event,
        c1.location,
        c1.signature_date,
        c1.signature,
        c1.territory,
        c1.detailed_products,
        c1.user_name,
        c1.medical_event,
        c1.is_sampled_call,
        c1.presentations,
        c1.product_priority_1,
        c1.product_priority_2,
        c1.product_priority_3,
        c1.product_priority_4,
        c1.product_priority_5,
        c1.attendee_list,
        c1.msl_interaction_notes,
        c1.sea_call_type,
        c1.interaction_mode,
        c1.hcp_kol_initiated,
        c1.msl_interaction_type,
        c1.call_objective,
        c1.submission_delay,
        c1.region,
        c1.md_hsp_admin,
        c1.hsp_minutes,
        c1.ortho_on_call_case,
        c1.ortho_volunteer_case,
        c1.md_calc1,
        c1.md_calculate_non_case_time,
        c1.md_calculated_hours_field,
        c1.md_casedeployment,
        c1.md_case_coverage_12_hours,
        c1.md_product_discussion,
        c1.md_concurrent_call,
        c1.courtesy_call,
        c1.md_in_service,
        c1.md_kol_course_discussion,
        c1.kol_minutes,
        c1.other_activities_time_12_hours,
        c1.other_in_field_activities,
        c1.md_overseas_workshop_visit,
        c1.md_ra_activities2,
        c1.sales_activity,
        c1.sales_time_12_hours,
        c1.time_spent,
        c1.time_spent_on_other_activities_simp,
        c1.time_spent_on_sales_activity,
        c1.md_time_spent_on_a_call,
        c1.md_case_type,
        c1.md_sets_activities,
        c1.md_time_spent_on_case,
        c1.time_spent_on_other_activities,
        c1.time_spent_per_call,
        c1.md_case_conducted_in_hospital,
        c1.calculated_field_2,
        c1.calculated_hours_3,
        c1.call_planned,
        c1.call_submission_day,
        c1.case_coverage,
        c1.day_of_week,
        c1.md_minutes,
        c1.md_in_or_ot,
        c1.md_d_call_type,
        c1.md_hours,
        c1.call_discussion_record_type_source_id,
        --c1.record_type_source_id, --call_discussion_record_type_source_id
        c1.comments,
        c1.discussion_topics,
        c1.discussion_type,
        c1.call_discussion_type,
        c1.effectiveness,
        c1.follow_up_activity,
        c1.outcomes,
        c1.follow_up_additional_info,
        c1.follow_up_date,
        c1.materials_used,
        c1.call_detail_name,
        c1.call_discussion_name,
        ------ new columns ends
        current_timestamp(),
        current_timestamp()
    FROM main2,
        child_rec2,
        call c1
    WHERE main2.call_source_id = c1.call_source_id
        AND main2.country_code = c1.country_code
        AND main2.main_call_source_id = child_rec2.parent_call_source_id(+)
        AND main2.country_code = child_rec2.country_code(+)
    ),
result
AS (
    SELECT *
    FROM cte1
    
    UNION ALL
    
    SELECT *
    FROM cte2
    )
,

final as (
    select
    *
    from result
)

select * from final
