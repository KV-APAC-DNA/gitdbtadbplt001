WITH itg_coaching_report
AS (
  SELECT *
  FROM {{ ref('hcposeitg_integration__itg_coaching_report') }}
  ),
itg_lookup_retention_period
AS (
  SELECT *
  FROM {{ source('hcposeitg_integration', 'itg_lookup_retention_period') }}
  ),
dim_country
AS (
  SELECT *
  FROM {{ ref('hcposeedw_integration__dim_country') }}
  ),
dim_date
AS (
  SELECT *
  FROM {{ ref('hcposeedw_integration__dim_date') }}
  ),
dim_employee
AS (
  SELECT *
  FROM {{ ref('hcposeedw_integration__dim_employee') }}
  ),
scr
AS (
  SELECT country_key,
    ID AS Coaching_Report_Source_ID,
    SUBSTRING(DECODE(Review_Date, NULL, createddate, Review_Date), 1, 10) DATE,
    ownerid,
    isdeleted,
    name,
    RECORDTYPEID,
    CreatedDate,
    createdbyid,
    LastModifiedDate,
    LASTMODIFIEDBYID,
    systemmodstamp,
    lastactivitydate,
    mayedit,
    Mobile_ID,
    Manager,
    Employee,
    Review_Date,
    review_period,
    STATUS,
    overall_rating,
    jj_core_country_code,
    (
      CASE 
        WHEN UPPER(jj_core_lock) = 'TRUE'
          THEN '1'
        ELSE '0'
        END
      ) AS jj_core_lock,
    (
      CASE 
        WHEN UPPER(jj_core_no_of_visits) IS NULL
          THEN '0'
        WHEN UPPER(jj_core_no_of_visits) = ' '
          THEN '0'
        ELSE jj_core_no_of_visits
        END
      ) AS jj_core_no_of_visits,
    JJ_Employee_Review_and_Acknowledged AS Employee_Review_and_Acknowledged,
    JJ_Employee_Comments AS Employee_Comments,
    JJ_SIMP_Manager_Comments AS SIMP_Manager_Comments,
    JJ_SIMP_Objectives AS SIMP_Objectives,
    JJ_SIMP_Rep_Comments_Long AS SIMP_Rep_Comments_Long,
    JJ_SIMP_SG_Overall_Rating AS SIMP_SG_Overall_Rating,
    jj_simp_long_comments AS simp_long_comments,
    Knowledge_Strategy_Overall_Rating,
    Selling_Skills_Overall_Rating,
    JJ_MY_Call_type AS MY_Call_type,
    JJ_MY_Location AS MY_Location,
    JJ_ID_Overall_Rating AS ID_Overall_Rating,
    JJ_VN_MD_Overall_Rating_Med AS VN_MD_Overall_Rating_Med,
    JJ_Agreed_Next_Steps AS Agreed_Next_Steps,
    JJ_Coaching_for_Field_Visits AS Coaching_for_Field_Visits,
    JJ_Customer_Interactions AS Customer_Interactions,
    JJ_Date_of_Review_Concluded AS Date_of_Review_Concluded,
    JJ_General_Observations_and_Comments AS General_Observations_and_Comments,
    JJ_Manager_Feedback_Completed AS Manager_Feedback_Completed,
    JJ_Number_of_Coaching_Days AS Number_of_Coaching_Days,
    JJ_Second_Line_Manager AS Second_Line_Manager,
    JJ_Submission_to_Date AS Submission_to_Date,
    RelatedCoachingReport
  FROM itg_coaching_report
  WHERE isdeleted = '0'
  ),
trns
AS (
  SELECT DC.country_key,
    DE.employee_key,
    DM.employee_key AS Manager_Employee_Key,
    DD.date_key AS Coaching_Report_Date_Key,
    SCR.Coaching_Report_Source_ID,
    SCR.ownerid,
    SCR.isdeleted,
    SCR.name,
    SCR.RECORDTYPEID,
    SCR.CreatedDate,
    SCR.createdbyid,
    SCR.LastModifiedDate,
    SCR.LASTMODIFIEDBYID,
    SCR.systemmodstamp,
    SCR.lastactivitydate,
    SCR.mayedit,
    SCR.Mobile_ID,
    SCR.Manager,
    SCR.Employee,
    SCR.Review_Date,
    SCR.review_period,
    SCR.STATUS,
    SCR.overall_rating,
    SCR.jj_core_country_code,
    SCR.jj_core_lock,
    SCR.jj_core_no_of_visits,
    SCR.Employee_Review_and_Acknowledged,
    SCR.Employee_Comments,
    SCR.SIMP_Manager_Comments,
    SCR.SIMP_Objectives,
    SCR.SIMP_Rep_Comments_Long,
    SCR.SIMP_SG_Overall_Rating,
    SCR.simp_long_comments,
    SCR.Knowledge_Strategy_Overall_Rating,
    SCR.Selling_Skills_Overall_Rating,
    SCR.MY_Call_type,
    SCR.MY_Location,
    SCR.ID_Overall_Rating,
    SCR.VN_MD_Overall_Rating_Med,
    SCR.Agreed_Next_Steps,
    SCR.Coaching_for_Field_Visits,
    SCR.Customer_Interactions,
    SCR.Date_of_Review_Concluded,
    SCR.General_Observations_and_Comments,
    SCR.Manager_Feedback_Completed,
    SCR.Number_of_Coaching_Days,
    SCR.Second_Line_Manager,
    SCR.Submission_to_Date,
    SCR.RelatedCoachingReport,
    current_timestamp() AS inserted_date
  FROM SCR
  LEFT OUTER JOIN dim_country DC ON SCR.jj_core_country_code = DC.country_code
  INNER JOIN dim_date DD ON SUBSTRING(SCR.DATE, 1, 4) || substring(SCR.DATE, 6, 2) || substring(SCR.DATE, 9, 2) = CAST(DD.date_key AS TEXT)
  LEFT OUTER JOIN dim_employee DE ON SCR.employee = DE.employee_source_id
    AND DE.country_Code = SCR.jj_core_country_code
  LEFT OUTER JOIN dim_employee DM ON SCR.manager = DM.employee_source_id
    AND DM.country_Code = SCR.jj_core_country_code
  WHERE SCR.Review_Date IS NOT NULL
    AND SCR.DATE > (
      SELECT DATE_TRUNC('year', DATEADD(year, -retention_years, CURRENT_DATE()))
      FROM itg_lookup_retention_period
      WHERE UPPER(table_name) = 'FACT_COACHING_REPORT'
      )
  ),
final
AS (
  SELECT country_key::VARCHAR(8) AS country_key,
    employee_key::VARCHAR(32) AS employee_key,
    manager_employee_key::VARCHAR(32) AS manager_employee_key,
    coaching_report_date_key::number(38, 0) AS coaching_report_date_key,
    coaching_report_source_id::VARCHAR(100) AS coaching_report_source_id,
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
    mobile_id::VARCHAR(1000) AS mobile_id,
    manager::VARCHAR(18) AS manager,
    employee::VARCHAR(18) AS employee,
    review_date::DATE AS review_date,
    review_period::VARCHAR(20) AS review_period,
    STATUS::VARCHAR(255) AS STATUS,
    overall_rating::VARCHAR(100) AS overall_rating,
    jj_core_country_code::VARCHAR(10) AS jj_core_country_code,
    jj_core_lock::VARCHAR(20) AS jj_core_lock,
    jj_core_no_of_visits::VARCHAR(10) AS jj_core_no_of_visits,
    employee_review_and_acknowledged::VARCHAR(5) AS employee_review_and_acknowledged,
    employee_comments::VARCHAR(2000) AS employee_comments,
    simp_manager_comments::VARCHAR(65535) AS simp_manager_comments,
    simp_objectives::VARCHAR(65535) AS simp_objectives,
    simp_rep_comments_long::VARCHAR(65535) AS simp_rep_comments_long,
    simp_sg_overall_rating::number(18, 1) AS simp_sg_overall_rating,
    simp_long_comments::VARCHAR(5000) AS simp_long_comments,
    knowledge_strategy_overall_rating::number(18, 1) AS knowledge_strategy_overall_rating,
    selling_skills_overall_rating::number(18, 1) AS selling_skills_overall_rating,
    my_call_type::VARCHAR(255) AS my_call_type,
    my_location::VARCHAR(255) AS my_location,
    id_overall_rating::number(18, 1) AS id_overall_rating,
    vn_md_overall_rating_med::number(18, 1) AS vn_md_overall_rating_med,
    agreed_next_steps::VARCHAR(2000) AS agreed_next_steps,
    coaching_for_field_visits::VARCHAR(5) AS coaching_for_field_visits,
    customer_interactions::VARCHAR(2000) AS customer_interactions,
    date_of_review_concluded::DATE AS date_of_review_concluded,
    general_observations_and_comments::VARCHAR(2000) AS general_observations_and_comments,
    manager_feedback_completed::VARCHAR(5) AS manager_feedback_completed,
    number_of_coaching_days::VARCHAR(255) AS number_of_coaching_days,
    second_line_manager::VARCHAR(18) AS second_line_manager,
    submission_to_date::number(18, 1) AS submission_to_date,
    relatedcoachingreport::VARCHAR(255) AS relatedcoachingreport,
    current_timestamp()::timestamp_ntz(9) AS updated_date,
    inserted_date::timestamp_ntz(9) AS inserted_date
    from trns
  )
SELECT *
FROM final