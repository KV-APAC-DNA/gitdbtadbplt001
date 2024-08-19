{{
    config
    (
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "
                    {% if is_incremental() %}
                    DELETE
                    FROM {{this}}
                    WHERE (Id) IN (
                        SELECT Id
                        FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_coaching_report') }} STG
                        WHERE STG.Id = Id
                        );
                    {% endif %}
                    "
    )
}}

WITH sdl_hcp_osea_coaching_report
AS (
  SELECT *
  FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_coaching_report') }}
  ),
itg_recordtype
AS (
  SELECT *
  FROM dev_dna_core.hcposeitg_integration.itg_recordtype
  ),
trns
AS (
  SELECT jj_core_country_code__c AS country_key,
    ID,
    ownerid,
    (
      CASE 
        WHEN UPPER(isdeleted) = 'TRUE'
          THEN 1
        WHEN UPPER(isdeleted) IS NULL
          THEN 0
        WHEN UPPER(isdeleted) = ' '
          THEN 0
        ELSE 0
        END
      ) AS isdeleted,
    name,
    RECORDTYPEID,
    CreatedDate,
    createdbyid,
    LastModifiedDate,
    LASTMODIFIEDBYID,
    NULL AS systemmodstamp,
    NULL AS lastactivitydate,
    NULL AS mayedit,
    Mobile_ID_vod__c,
    Manager_vod__c,
    Employee_vod__c,
    Review_Date__c,
    review_period__c,
    Status__c,
    overall_rating__c,
    jj_core_country_code__c,
    jj_core_lock__c,
    jj_core_no_of_visits__c,
    (
      CASE 
        WHEN UPPER(JJ_Employee_Review_and_Acknowledged__c) = 'TRUE'
          THEN 1
        WHEN UPPER(JJ_Employee_Review_and_Acknowledged__c) IS NULL
          THEN 0
        WHEN UPPER(JJ_Employee_Review_and_Acknowledged__c) = ' '
          THEN 0
        ELSE 0
        END
      ) AS JJ_Employee_Review_and_Acknowledged__c,
    jj_employee_comments__c AS JJ_Employee_Comments__c,
    JJ_SIMP_Manager_Comments_Long__c,
    JJ_SIMP_Objectives__c,
    JJ_SIMP_Rep_Comments_Long__c,
    JJ_SIMP_SG_Overall_Rating__c,
    jj_simp_long_comments__c,
    Knowledge_Strategy_Overall_Rating__c,
    Selling_Skills_Overall_Rating__c,
    JJ_MY_Call_type__c,
    JJ_MY_Location__c,
    JJ_ID_Overall_Rating__c,
    JJ_VN_MD_Overall_Rating_Med__c,
    JJ_Agreed_Next_Steps__c,
    JJ_Coaching_for_Field_Visits__c,
    JJ_Customer_Interactions__c,
    JJ_Date_of_Review_Concluded__c,
    JJ_General_Observations_and_Comments__c,
    (
      CASE 
        WHEN UPPER(JJ_Manager_Feedback_Completed__c) = 'TRUE'
          THEN 1
        WHEN UPPER(JJ_Manager_Feedback_Completed__c) IS NULL
          THEN 0
        WHEN UPPER(JJ_Manager_Feedback_Completed__c) = ' '
          THEN 0
        ELSE 0
        END
      ) AS jj_manager_feedback_completed,
    JJ_Number_of_Coaching_Days__c,
    JJ_Second_Line_Manager__c,
    JJ_Submission_to_Date__c,
    RelatedCoachingReport__c,
    sysdate() AS updated_date,
    sysdate() AS inserted_date
  FROM sdl_hcp_osea_coaching_report ocr
  LEFT OUTER JOIN itg_recordtype ir ON ocr.RECORDTYPEID = ir.record_type_source_id
    AND ocr.jj_core_country_code__c = ir.country_code
  WHERE record_type_name NOT IN ('Coaching Report - LOCKED MSL', 'Coaching Report MSL', 'Coaching Report MSL Manager Approved', 'HK REP Coaching Report')
  ),
final
AS (
  SELECT country_key::VARCHAR(18) AS country_key,
    id::VARCHAR(18) AS id,
    ownerid::VARCHAR(18) AS ownerid,
    isdeleted::VARCHAR(5) AS isdeleted,
    name::VARCHAR(80) AS name,
    recordtypeid::VARCHAR(18) AS recordtypeid,
    createddate::timestamp_ntz(9) AS createddate,
    createdbyid::VARCHAR(18) AS createdbyid,
    lastmodifieddate::timestamp_ntz(9) AS lastmodifieddate,
    lastmodifiedbyid::VARCHAR(18) AS lastmodifiedbyid,
    systemmodstamp::timestamp_ntz(9) AS systemmodstamp,
    lastactivitydate::timestamp_ntz(9) AS lastactivitydate,
    mayedit::VARCHAR(20) AS mayedit,
    Mobile_ID_vod__c::VARCHAR(1000) AS mobile_id,
    Manager_vod__c::VARCHAR(18) AS manager,
    Employee_vod__c::VARCHAR(18) AS employee,
    Review_Date__c::DATE AS review_date,
    review_period__c::VARCHAR(20) AS review_period,
    Status__c::VARCHAR(255) AS STATUS,
    overall_rating__c::VARCHAR(100) AS overall_rating,
    jj_core_country_code__c::VARCHAR(10) AS jj_core_country_code,
    jj_core_lock__c::VARCHAR(20) AS jj_core_lock,
    jj_core_no_of_visits__c::VARCHAR(10) AS jj_core_no_of_visits,
    jj_employee_review_and_acknowledged__c::VARCHAR(5) AS jj_employee_review_and_acknowledged,
    jj_employee_comments__c::VARCHAR(2000) AS jj_employee_comments,
    JJ_SIMP_Manager_Comments_Long__c::VARCHAR(65535) AS jj_simp_manager_comments,
    jj_simp_objectives__c::VARCHAR(65535) AS jj_simp_objectives,
    jj_simp_rep_comments_long__c::VARCHAR(65535) AS jj_simp_rep_comments_long,
    jj_simp_sg_overall_rating__c::number(18, 1) AS jj_simp_sg_overall_rating,
    jj_simp_long_comments__c::VARCHAR(5000) AS jj_simp_long_comments,
    knowledge_strategy_overall_rating__c::number(18, 1) AS knowledge_strategy_overall_rating,
    selling_skills_overall_rating__c::number(18, 1) AS selling_skills_overall_rating,
    jj_my_call_type__c::VARCHAR(255) AS jj_my_call_type,
    jj_my_location__c::VARCHAR(255) AS jj_my_location,
    jj_id_overall_rating__c::number(18, 1) AS jj_id_overall_rating,
    jj_vn_md_overall_rating_med__c::number(18, 1) AS jj_vn_md_overall_rating_med,
    jj_agreed_next_steps__c::VARCHAR(2000) AS jj_agreed_next_steps,
    jj_coaching_for_field_visits__c::VARCHAR(5) AS jj_coaching_for_field_visits,
    jj_customer_interactions__c::VARCHAR(2000) AS jj_customer_interactions,
    jj_date_of_review_concluded__c::DATE AS jj_date_of_review_concluded,
    jj_general_observations_and_comments__c::VARCHAR(2000) AS jj_general_observations_and_comments,
    jj_manager_feedback_completed::VARCHAR(5) AS jj_manager_feedback_completed,
    jj_number_of_coaching_days__c::VARCHAR(255) AS jj_number_of_coaching_days,
    jj_second_line_manager__c::VARCHAR(18) AS jj_second_line_manager,
    jj_submission_to_date__c::number(18, 1) AS jj_submission_to_date,
    relatedcoachingreport__c::VARCHAR(255) AS relatedcoachingreport,
    updated_date::timestamp_ntz(9) AS updated_date,
    inserted_date::timestamp_ntz(9) AS inserted_date
  FROM trns
  )
SELECT *
FROM final
