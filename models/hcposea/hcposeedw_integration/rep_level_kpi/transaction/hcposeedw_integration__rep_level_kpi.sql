with dim_date as (
select * from {{ ref('hcposeedw_integration__dim_date') }}
),
holiday_list as (
select * from {{ ref('hcposeedw_integration__holiday_list') }}
),
vw_employee_hier as (
select * from {{ ref('hcposeedw_integration__vw_employee_hier') }}
),
dim_employee_iconnect as (
select * from {{ ref('hcposeedw_integration__dim_employee_iconnect') }}
),
wrk_call_detail as (
select * from {{ ref('hcposeedw_integration__wrk_call_detail') }}
), 
wrk_coaching_detail as (
select * from {{ ref('hcposeedw_integration__wrk_coaching_detail') }}
),
wrk_cycle_plan as (
select * from {{ ref('hcposeedw_integration__wrk_cycle_plan') }}
),
transformed as
(
(SELECT wrk_ds.country,
       NULL::CHARACTER VARYING AS jnj_date_year,
       NULL::CHARACTER VARYING AS jnj_date_month,
       NULL::CHARACTER VARYING AS jnj_date_quarter,
       DAT.date_year::CHARACTER VARYING AS date_year,
       DAT.date_month::CHARACTER VARYING AS date_month,
       DAT.date_quarter::CHARACTER VARYING AS date_quarter,
       NULL::CHARACTER VARYING AS my_date_year,
       NULL::CHARACTER VARYING AS my_date_month,
       NULL::CHARACTER VARYING AS my_date_quarter,
       hier.sector,
       hier.l2_username AS l3_username,
       hier.l2_manager_name AS l3_manager_name,
       hier.l3_wwid AS l2_wwid,
       hier.l3_username AS l2_username,
       hier.l3_manager_name AS l2_manager_name,
       hier.l4_wwid AS l1_wwid,
       hier.l4_username AS l1_username,
       hier.l4_manager_name AS l1_manager_name,
       hier.l1_username AS sales_rep_ntid,
       dimemp.organization_l1_name,
       dimemp.organization_l2_name,
       dimemp.organization_l3_name,
       REPLACE(dimemp.organization_l4_name,'_',' ') organization_l4_name,
       dimemp.organization_l5_name,
       CASE
         WHEN hier.employee_name IS NULL THEN dimemp.employee_name
         ELSE hier.employee_name
       END AS sales_rep,
       fact_call.working_days,
       fact_call.total_calls,
       fact_call.total_cnt_call_delay,
       fact_call.total_call_edetailing,
       fact_call.call_total_active_user,
       fact_call.total_calls_with_product,
       fact_call.total_sbmtd_calls_key_message,
       fact_call.total_key_message,
       fact_call.total_call_classification_a,
       fact_call.total_call_classification_b,
       fact_call.total_call_classification_c,
       fact_call.total_call_classification_d,
       fact_call.total_call_classification_u,
       fact_call.total_call_classification_z,
       fact_call.total_call_classification_no_product,
       fact_call.total_detailing,
       fact_coach.team as coaching_team,
       fact_coach.Coaching_status,
       fact_coach.total_coaching_report,
       fact_coach.total_coaching_visit,
       fact_coach.Coaching_manager,
       fact_coach.sales_rep as coaching_sales_rep,
       fact_cycle_target.planned_calls,
       fact_cycle_target.attainment,
       fact_cycle_target.actual_calls,
       fact_cycle_target.cpa_100/100 AS cpa_100,
       fact_cycle_target.target_cpa_status,
       fact_cycle_product.product_cpa_status,
       fact_cycle_product.planned_call_detail_count,
       fact_cycle_product.cycle_plan_detail_attainment,
       fact_cycle_product.actual_call_detail_count,
       fact_cycle_product.cfa_100,
       fact_cycle_product.cfa_33,
       fact_cycle_product.cfa_66
FROM (SELECT *
      FROM dim_date
      WHERE (DATE_YEAR BETWEEN (DATE_PART(YEAR,to_date(current_timestamp())) -2) AND DATE_PART(YEAR,to_date(current_timestamp())))) DAT
  INNER JOIN (SELECT 'SG' AS country,
                     ds.date_year,
                     ds.date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'SG')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       date_year,
                       date_month
              UNION ALL
              SELECT 'MY' AS country,
                     ds.date_year,
                     ds.date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'MY')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       date_year,
                       date_month
              UNION ALL
              SELECT 'VN' AS country,
                     ds.date_year,
                     ds.date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'VN')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       date_year,
                       date_month
              UNION ALL
              SELECT 'TH' AS country,
                     ds.date_year,
                     ds.date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'TH')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       date_year,
                       date_month
              UNION ALL
              SELECT 'PH' AS country,
                     ds.date_year,
                     ds.date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'PH')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       date_year,
                       date_month
              UNION ALL
              SELECT 'ID' AS country,
                     ds.date_year,
                     ds.date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'ID')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       date_year,
                       date_month) WRK_DS
          ON DAT.DATE_YEAR = WRK_DS.DATE_YEAR
         AND DAT.date_month = WRK_DS.date_month
  INNER JOIN vw_employee_hier hier ON wrk_ds.country::TEXT = hier.country_code::TEXT
  INNER JOIN dim_employee_iconnect dimemp
          ON hier.employee_key::TEXT = dimemp.employee_key::TEXT
         AND wrk_ds.country = dimemp.country_code::TEXT
  LEFT JOIN (SELECT country,
                    date_year,
                    date_month,
                    date_quarter,
                    sector,
                    MAX(l3_username) AS l3_username,
                    MAX(l3_manager_name) AS l3_manager_name,
                    MAX(l2_wwid) AS l2_wwid,
                    MAX(l2_username) AS l2_username,
                    MAX(l2_manager_name) AS l2_manager_name,
                    MAX(l1_wwid) AS l1_wwid,
                    MAX(l1_username) AS l1_username,
                    MAX(l1_manager_name) AS l1_manager_name,
                    MAX(sales_rep_ntid) AS sales_rep_ntid,
                    MAX(organization_l1_name) AS organization_l1_name,
                    MAX(organization_l2_name) AS organization_l2_name,
                    MAX(organization_l3_name) AS team,
                    MAX(organization_l4_name) AS organization_l4_name,
                    MAX(organization_l5_name) AS organization_l5_name,
                    sales_rep,
                    working_days,
                    total_calls AS total_calls,
                    total_cnt_call_delay,
                    total_call_edetailing,
                    total_active AS call_total_active_user,
                    detailed_products AS total_calls_with_product,
                    total_sbmtd_calls_key_message,
                    total_key_message,
                    SUM(total_call_classification_a) AS total_call_classification_a,
                    SUM(total_call_classification_b) AS total_call_classification_b,
                    SUM(total_call_classification_c) AS total_call_classification_c,
                    SUM(total_call_classification_d) AS total_call_classification_d,
                    SUM(total_call_classification_u) AS total_call_classification_u,
                    SUM(total_call_classification_z) AS total_call_classification_z,
                    SUM(total_call_classification_no_product) AS total_call_classification_no_product,
                    total_detailing
             FROM wrk_call_detail
             WHERE date_year IS NOT NULL
             GROUP BY country,
                      date_year,
                      date_month,
                      date_quarter,
                      sector,
                      sales_rep,
                      working_days,
                      total_calls,
                      total_cnt_call_delay,
                      total_call_edetailing,
                      total_active,
                      detailed_products,
                      total_sbmtd_calls_key_message,
                      total_key_message,
                      total_detailing) fact_call
         ON fact_call.date_year = DAT.date_year
        AND fact_call.date_month::TEXT = DAT.date_month::TEXT
        AND fact_call.date_quarter::TEXT = DAT.date_quarter::TEXT
        AND fact_call.country::TEXT = WRK_DS.country::TEXT
        AND COALESCE (fact_call.sector,'0'::CHARACTER VARYING)::TEXT = COALESCE (hier.sector,'0'::CHARACTER VARYING)::TEXT
        AND fact_call.sales_rep_ntid::TEXT = hier.l1_username::TEXT
        AND COALESCE (fact_call.l3_manager_name,'#'::CHARACTER VARYING)::TEXT = COALESCE (hier.l2_manager_name::TEXT,'#'::CHARACTER VARYING)::TEXT
  LEFT JOIN (SELECT country,
                    date_year,
                    date_month,
                    date_quarter,
                    sector,
                    MAX(l3_username) AS l3_username,
                    MAX(l3_manager_name) AS l3_manager_name,
                    MAX(l2_wwid) AS l2_wwid,
                    MAX(l2_username) AS l2_username,
                    MAX(l2_manager_name) AS l2_manager_name,
                    MAX(l1_wwid) AS l1_wwid,
                    MAX(l1_username) AS l1_username,
                    MAX(l1_manager_name) AS l1_manager_name,
                    MAX(sales_rep_ntid) AS sales_rep_ntid,
                    MAX(organization_l1_name) AS organization_l1_name,
                    MAX(organization_l2_name) AS organization_l2_name,
                    MAX(organization_l3_name) AS team,
                    MAX(organization_l4_name) AS organization_l4_name,
                    MAX(organization_l5_name) AS organization_l5_name,
                    sales_rep,
                    Coaching_status,
                    total_coaching_report,
                    total_coaching_visit,
                    Coaching_manager
             FROM wrk_coaching_detail
             WHERE date_year IS NOT NULL
             GROUP BY country,
                      date_year,
                      date_month,
                      date_quarter,
                      sector,
                      sales_rep,
                      Coaching_status,
                      total_coaching_report,
                      total_coaching_visit,
                      Coaching_manager) fact_coach
         ON fact_coach.date_year = DAT.date_year
        AND fact_coach.date_month::TEXT = DAT.date_month::TEXT
        AND fact_coach.date_quarter::TEXT = DAT.date_quarter::TEXT
        AND fact_coach.country::TEXT = WRK_DS.country::TEXT
        AND COALESCE (fact_coach.sector,'0'::CHARACTER VARYING)::TEXT = COALESCE (hier.sector,'0'::CHARACTER VARYING)::TEXT
        AND ((COALESCE (fact_coach.l2_manager_name,'#'::CHARACTER VARYING)::TEXT = COALESCE (hier.l2_manager_name::TEXT,'#'::CHARACTER VARYING)::TEXT AND COALESCE (fact_coach.l2_manager_name,'#'::CHARACTER VARYING)::TEXT <> COALESCE (hier.employee_name::TEXT,'#'::CHARACTER VARYING)::TEXT)
		OR (COALESCE (fact_coach.l2_manager_name,'#'::CHARACTER VARYING)::TEXT <> COALESCE (hier.l2_manager_name::TEXT,'#'::CHARACTER VARYING)::TEXT AND COALESCE (fact_coach.l2_manager_name,'#'::CHARACTER VARYING)::TEXT = COALESCE (hier.employee_name::TEXT,'#'::CHARACTER VARYING)::TEXT))
  LEFT JOIN (SELECT country,
                    date_year,
                    date_month,
                    date_quarter,
                    sector,
                    MAX(l3_username) AS l3_username,
                    MAX(l3_manager_name) AS l3_manager_name,
                    MAX(l2_wwid) AS l2_wwid,
                    MAX(l2_username) AS l2_username,
                    MAX(l2_manager_name) AS l2_manager_name,
                    MAX(l1_wwid) AS l1_wwid,
                    MAX(l1_username) AS l1_username,
                    MAX(l1_manager_name) AS l1_manager_name,
                    MAX(sales_rep_ntid) AS sales_rep_ntid,
                    MAX(organization_l1_name) AS organization_l1_name,
                    MAX(organization_l2_name) AS organization_l2_name,
                    MAX(organization_l3_name) AS team,
                    MAX(organization_l4_name) AS organization_l4_name,
                    MAX(organization_l5_name) AS organization_l5_name,
                    sales_rep,
                    cpa_status AS target_cpa_status,
                    SUM(planned_calls) AS planned_calls,
                    SUM(attainment) AS attainment,
                    SUM(actual_calls) AS actual_calls,
                    SUM(cpa_100) AS cpa_100
             FROM wrk_cycle_plan
             WHERE cycle_plan_type = 'TARGET'
             AND   date_year IS NOT NULL
             GROUP BY country,
                      date_year,
                      date_month,
                      date_quarter,
                      sector,
                      sales_rep,
                      cpa_status) fact_cycle_target
         ON fact_cycle_target.date_year = DAT.date_year
        AND fact_cycle_target.date_month::TEXT = DAT.date_month::TEXT
        AND fact_cycle_target.date_quarter::TEXT = DAT.date_quarter::TEXT
        AND fact_cycle_target.country::TEXT = WRK_DS.country::TEXT
        AND COALESCE (fact_cycle_target.sector,'0'::CHARACTER VARYING)::TEXT = COALESCE (hier.sector,'0'::CHARACTER VARYING)::TEXT
        AND fact_cycle_target.sales_rep_ntid::TEXT = hier.l1_username::TEXT
        AND COALESCE (fact_cycle_target.l3_manager_name,'#'::CHARACTER VARYING)::TEXT = COALESCE (hier.l2_manager_name::TEXT,'#'::CHARACTER VARYING)::TEXT
  LEFT JOIN (SELECT country,
                    date_year,
                    date_month,
                    date_quarter,
                    sector,
                    MAX(l3_username) AS l3_username,
                    MAX(l3_manager_name) AS l3_manager_name,
                    MAX(l2_wwid) AS l2_wwid,
                    MAX(l2_username) AS l2_username,
                    MAX(l2_manager_name) AS l2_manager_name,
                    MAX(l1_wwid) AS l1_wwid,
                    MAX(l1_username) AS l1_username,
                    MAX(l1_manager_name) AS l1_manager_name,
                    MAX(sales_rep_ntid) AS sales_rep_ntid,
                    MAX(organization_l1_name) AS organization_l1_name,
                    MAX(organization_l2_name) AS organization_l2_name,
                    MAX(organization_l3_name) AS team,
                    MAX(organization_l4_name) AS organization_l4_name,
                    MAX(organization_l5_name) AS organization_l5_name,
                    sales_rep,
                    cpa_status AS product_cpa_status,
                    product,
                    SUM(planned_call_detail_count) AS planned_call_detail_count,
                    SUM(cycle_plan_detail_attainment) AS cycle_plan_detail_attainment,
                    SUM(actual_call_detail_count) AS actual_call_detail_count,
                    SUM(cfa_100) AS cfa_100,
                    SUM(cfa_33) AS cfa_33,
                    SUM(cfa_66) AS cfa_66
             FROM wrk_cycle_plan
             WHERE cycle_plan_type = 'PRODUCT'
             AND   date_year IS NOT NULL
             GROUP BY country,
                      date_year,
                      date_month,
                      date_quarter,
                      sector,
                      sales_rep,
                      cpa_status,
                      product) fact_cycle_product
         ON fact_cycle_product.date_year = fact_cycle_target.date_year
        AND fact_cycle_product.date_month::TEXT = fact_cycle_target.date_month::TEXT
        AND fact_cycle_product.date_quarter::TEXT = fact_cycle_target.date_quarter::TEXT
        AND fact_cycle_product.country::TEXT = fact_cycle_target.country::TEXT
        AND COALESCE (fact_cycle_product.sector,'0'::CHARACTER VARYING)::TEXT = COALESCE (fact_cycle_target.sector,'0'::CHARACTER VARYING)::TEXT
        AND fact_cycle_product.sales_rep_ntid::TEXT = fact_cycle_target.sales_rep_ntid::TEXT
)
UNION ALL
(SELECT  wrk_ds.country,
       NULL::CHARACTER VARYING AS jnj_date_year,
       NULL::CHARACTER VARYING AS jnj_date_month,
       NULL::CHARACTER VARYING AS jnj_date_quarter,
       NULL::CHARACTER VARYING AS date_year,
       NULL::CHARACTER VARYING AS date_month,
       NULL::CHARACTER VARYING AS date_quarter,
       dat.my_date_year::CHARACTER VARYING AS my_date_year,
       trim(dat.my_date_month::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS my_date_month,
       dat.my_date_quarter::CHARACTER VARYING AS my_date_quarter,
       hier.sector,
       hier.l2_username AS l3_username,
       hier.l2_manager_name AS l3_manager_name,
       hier.l3_wwid AS l2_wwid,
       hier.l3_username AS l2_username,
       hier.l3_manager_name AS l2_manager_name,
       hier.l4_wwid AS l1_wwid,
       hier.l4_username AS l1_username,
       hier.l4_manager_name AS l1_manager_name,
       hier.l1_username AS sales_rep_ntid,
       dimemp.organization_l1_name,
       dimemp.organization_l2_name,
       dimemp.organization_l3_name,
       replace(dimemp.organization_l4_name,'_',' ') organization_l4_name,
       dimemp.organization_l5_name,
       CASE
         WHEN hier.employee_name IS NULL THEN dimemp.employee_name
         ELSE hier.employee_name
       END AS sales_rep,
       fact_call.working_days,
       fact_call.total_calls,
       fact_call.total_cnt_call_delay,
       fact_call.total_call_edetailing,
       fact_call.call_total_active_user,
       fact_call.total_calls_with_product,
       fact_call.total_sbmtd_calls_key_message,
       fact_call.total_key_message,
       fact_call.total_call_classification_a,
       fact_call.total_call_classification_b,
       fact_call.total_call_classification_c,
       fact_call.total_call_classification_d,
       fact_call.total_call_classification_u,
       fact_call.total_call_classification_z,
       fact_call.total_call_classification_no_product,
       fact_call.total_detailing,
       fact_coach.team as coaching_team,
	     fact_coach.Coaching_status,
       fact_coach.total_coaching_report,
       fact_coach.total_coaching_visit,       
       fact_coach.Coaching_manager,
	     fact_coach.sales_rep as coaching_sales_rep,
       fact_cycle_target.planned_calls,
       fact_cycle_target.attainment,
       fact_cycle_target.actual_calls,
       fact_cycle_target.cpa_100/100 AS cpa_100,
       fact_cycle_target.target_cpa_status,
       fact_cycle_product.product_cpa_status,
       fact_cycle_product.planned_call_detail_count,
       fact_cycle_product.cycle_plan_detail_attainment,
       fact_cycle_product.actual_call_detail_count,
       fact_cycle_product.cfa_100,
       fact_cycle_product.cfa_33,
       fact_cycle_product.cfa_66
FROM (SELECT * FROM dim_date WHERE (MY_DATE_YEAR BETWEEN (DATE_PART(YEAR,to_date(current_timestamp())) -2) AND DATE_PART(YEAR,to_date(current_timestamp())))) DAT
  INNER JOIN (SELECT 'SG' AS country,
                     ds.my_date_year,
                     ds.my_date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'SG')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       my_date_year,
                       my_date_month
              UNION ALL
              SELECT 'MY' AS country,
                     ds.my_date_year,
                     ds.my_date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'MY')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       my_date_year,
                       my_date_month
              UNION ALL
              SELECT 'VN' AS country,
                     ds.my_date_year,
                     ds.my_date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'VN')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       my_date_year,
                       my_date_month
              UNION ALL
              SELECT 'TH' AS country,
                     ds.my_date_year,
                     ds.my_date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'TH')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       my_date_year,
                       my_date_month
              UNION ALL
              SELECT 'PH' AS country,
                     ds.my_date_year,
                     ds.my_date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'PH')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       my_date_year,
                       my_date_month
              UNION ALL
              SELECT 'ID' AS country,
                     ds.my_date_year,
                     ds.my_date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'ID')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       my_date_year,
                       my_date_month) WRK_DS
          ON DAT.my_DATE_YEAR = WRK_DS.my_DATE_YEAR
         AND DAT.my_date_month = WRK_DS.my_date_month
  INNER JOIN vw_employee_hier hier ON wrk_ds.country::TEXT = hier.country_code::TEXT
  INNER JOIN dim_employee_iconnect dimemp
         ON hier.employee_key::TEXT = dimemp.employee_key::TEXT
        AND wrk_ds.country = dimemp.country_code::TEXT
  LEFT JOIN (SELECT country,
                    my_date_year,
                    my_date_month,
                    my_date_quarter,
                    sector,
                    MAX(l3_username) AS l3_username,
                    MAX(l3_manager_name) AS l3_manager_name,
                    MAX(l2_wwid) AS l2_wwid,
                    MAX(l2_username) AS l2_username,
                    MAX(l2_manager_name) AS l2_manager_name,
                    MAX(l1_wwid) AS l1_wwid,
                    MAX(l1_username) AS l1_username,
                    MAX(l1_manager_name) AS l1_manager_name,
                    MAX(sales_rep_ntid) AS sales_rep_ntid,
                    MAX(organization_l1_name) AS organization_l1_name,
                    MAX(organization_l2_name) AS organization_l2_name,
                    MAX(organization_l3_name) AS team,
                    MAX(organization_l4_name) AS organization_l4_name,
                    MAX(organization_l5_name) AS organization_l5_name,
                    sales_rep,
                    working_days,
                    total_calls AS total_calls,
                    total_cnt_call_delay,
                    total_call_edetailing,
                    total_active AS call_total_active_user,
                    detailed_products AS total_calls_with_product,
                    total_sbmtd_calls_key_message,
                    total_key_message,
                    SUM(total_call_classification_a) AS total_call_classification_a,
                    SUM(total_call_classification_b) AS total_call_classification_b,
                    SUM(total_call_classification_c) AS total_call_classification_c,
                    SUM(total_call_classification_d) AS total_call_classification_d,
                    SUM(total_call_classification_u) AS total_call_classification_u,
                    SUM(total_call_classification_z) AS total_call_classification_z,
                    SUM(total_call_classification_no_product) AS total_call_classification_no_product,
                    total_detailing
             FROM wrk_call_detail
             WHERE my_date_year IS NOT NULL
             GROUP BY country,
                      my_date_year,
                      my_date_month,
                      my_date_quarter,
                      sector,
                      sales_rep,
                      working_days,
                      total_calls,
                      total_cnt_call_delay,
                      total_call_edetailing,
                      total_active,
                      detailed_products,
                      total_sbmtd_calls_key_message,
                      total_key_message,
                      total_detailing) fact_call
         ON fact_call.my_date_year = DAT.my_date_year
        AND fact_call.my_date_month::TEXT = DAT.my_date_month::TEXT
        AND fact_call.my_date_quarter::TEXT = DAT.my_date_quarter::TEXT
        AND fact_call.country::TEXT = WRK_DS.country::TEXT
        AND COALESCE (fact_call.sector,'0'::CHARACTER VARYING)::TEXT = COALESCE (hier.sector,'0'::CHARACTER VARYING)::TEXT
        AND fact_call.sales_rep_ntid::TEXT = hier.l1_username::TEXT
        AND COALESCE (fact_call.l3_manager_name,'#'::CHARACTER VARYING)::TEXT = COALESCE (hier.l2_manager_name::TEXT,'#'::CHARACTER VARYING)::TEXT
  LEFT JOIN (SELECT country,
                    my_date_year,
                    my_date_month,
                    my_date_quarter,
                    sector,
                    MAX(l3_username) AS l3_username,
                    MAX(l3_manager_name) AS l3_manager_name,
                    MAX(l2_wwid) AS l2_wwid,
                    MAX(l2_username) AS l2_username,
                    MAX(l2_manager_name) AS l2_manager_name,
                    MAX(l1_wwid) AS l1_wwid,
                    MAX(l1_username) AS l1_username,
                    MAX(l1_manager_name) AS l1_manager_name,
                    MAX(sales_rep_ntid) AS sales_rep_ntid,
                    MAX(organization_l1_name) AS organization_l1_name,
                    MAX(organization_l2_name) AS organization_l2_name,
                    MAX(organization_l3_name) AS team,
                    MAX(organization_l4_name) AS organization_l4_name,
                    MAX(organization_l5_name) AS organization_l5_name,
                    sales_rep,
                    Coaching_status,
                    total_coaching_report,
                    total_coaching_visit,
                    Coaching_manager
             FROM wrk_coaching_detail
             WHERE my_date_year IS NOT NULL
             GROUP BY country,
                      my_date_year,
                      my_date_month,
                      my_date_quarter,
                      sector,
                      sales_rep,
                      Coaching_status,
                      total_coaching_report,
                      total_coaching_visit,
                      Coaching_manager) fact_coach
         ON fact_coach.my_date_year = DAT.my_date_year
        AND fact_coach.my_date_month::TEXT = DAT.my_date_month::TEXT
        AND fact_coach.my_date_quarter::TEXT = DAT.my_date_quarter::TEXT
        AND fact_coach.country::TEXT = WRK_DS.country::TEXT
        AND COALESCE (fact_coach.sector,'0'::CHARACTER VARYING)::TEXT = COALESCE (hier.sector,'0'::CHARACTER VARYING)::TEXT
        AND ((COALESCE (fact_coach.l2_manager_name,'#'::CHARACTER VARYING)::TEXT = COALESCE (hier.l2_manager_name::TEXT,'#'::CHARACTER VARYING)::TEXT AND COALESCE (fact_coach.l2_manager_name,'#'::CHARACTER VARYING)::TEXT <> COALESCE (hier.employee_name::TEXT,'#'::CHARACTER VARYING)::TEXT)
		OR (COALESCE (fact_coach.l2_manager_name,'#'::CHARACTER VARYING)::TEXT <> COALESCE (hier.l2_manager_name::TEXT,'#'::CHARACTER VARYING)::TEXT AND COALESCE (fact_coach.l2_manager_name,'#'::CHARACTER VARYING)::TEXT = COALESCE (hier.employee_name::TEXT,'#'::CHARACTER VARYING)::TEXT))
		
  LEFT JOIN (SELECT country,
                    my_date_year,
                    my_date_month,
                    my_date_quarter,
                    sector,
                    MAX(l3_username) AS l3_username,
                    MAX(l3_manager_name) AS l3_manager_name,
                    MAX(l2_wwid) AS l2_wwid,
                    MAX(l2_username) AS l2_username,
                    MAX(l2_manager_name) AS l2_manager_name,
                    MAX(l1_wwid) AS l1_wwid,
                    MAX(l1_username) AS l1_username,
                    MAX(l1_manager_name) AS l1_manager_name,
                    MAX(sales_rep_ntid) AS sales_rep_ntid,
                    MAX(organization_l1_name) AS organization_l1_name,
                    MAX(organization_l2_name) AS organization_l2_name,
                    MAX(organization_l3_name) AS team,
                    MAX(organization_l4_name) AS organization_l4_name,
                    MAX(organization_l5_name) AS organization_l5_name,
                    sales_rep,
                    cpa_status AS target_cpa_status,
                    SUM(planned_calls) AS planned_calls,
                    SUM(attainment) AS attainment,
                    SUM(actual_calls) AS actual_calls,
                    SUM(cpa_100) AS cpa_100
             FROM wrk_cycle_plan
             WHERE cycle_plan_type = 'TARGET'
             AND   my_date_year IS NOT NULL
             GROUP BY country,
                      my_date_year,
                      my_date_month,
                      my_date_quarter,
                      sector,
                      sales_rep,
                      cpa_status) fact_cycle_target
         ON fact_cycle_target.my_date_year = DAT.my_date_year
        AND fact_cycle_target.my_date_month::TEXT = DAT.my_date_month::TEXT
        AND fact_cycle_target.my_date_quarter::TEXT = DAT.my_date_quarter::TEXT
        AND fact_cycle_target.country::TEXT = WRK_DS.country::TEXT
        AND COALESCE (fact_cycle_target.sector,'0'::CHARACTER VARYING)::TEXT = COALESCE (hier.sector,'0'::CHARACTER VARYING)::TEXT
        AND fact_cycle_target.sales_rep_ntid::TEXT = hier.l1_username::TEXT
        AND COALESCE (fact_cycle_target.l3_manager_name,'#'::CHARACTER VARYING)::TEXT = COALESCE (hier.l2_manager_name::TEXT,'#'::CHARACTER VARYING)::TEXT
  LEFT JOIN (SELECT country,
                    my_date_year,
                    my_date_month,
                    my_date_quarter,
                    sector,
                    MAX(l3_username) AS l3_username,
                    MAX(l3_manager_name) AS l3_manager_name,
                    MAX(l2_wwid) AS l2_wwid,
                    MAX(l2_username) AS l2_username,
                    MAX(l2_manager_name) AS l2_manager_name,
                    MAX(l1_wwid) AS l1_wwid,
                    MAX(l1_username) AS l1_username,
                    MAX(l1_manager_name) AS l1_manager_name,
                    MAX(sales_rep_ntid) AS sales_rep_ntid,
                    MAX(organization_l1_name) AS organization_l1_name,
                    MAX(organization_l2_name) AS organization_l2_name,
                    MAX(organization_l3_name) AS team,
                    MAX(organization_l4_name) AS organization_l4_name,
                    MAX(organization_l5_name) AS organization_l5_name,
                    sales_rep,
                    cpa_status AS product_cpa_status,
                    product,
                    SUM(planned_call_detail_count) AS planned_call_detail_count,
                    SUM(cycle_plan_detail_attainment) AS cycle_plan_detail_attainment,
                    SUM(actual_call_detail_count) AS actual_call_detail_count,
                    SUM(cfa_100) AS cfa_100,
                    SUM(cfa_33) AS cfa_33,
                    SUM(cfa_66) AS cfa_66
             FROM wrk_cycle_plan
             WHERE cycle_plan_type = 'PRODUCT'
             AND   my_date_year IS NOT NULL
             GROUP BY country,
                      my_date_year,
                      my_date_month,
                      my_date_quarter,
                      sector,
                      sales_rep,
                      cpa_status,
                      product) fact_cycle_product
         ON fact_cycle_product.my_date_year = fact_cycle_target.my_date_year
        AND fact_cycle_product.my_date_month::TEXT = fact_cycle_target.my_date_month::TEXT
        AND fact_cycle_product.my_date_quarter::TEXT = fact_cycle_target.my_date_quarter::TEXT
        AND fact_cycle_product.country::TEXT = fact_cycle_target.country::TEXT
        AND COALESCE (fact_cycle_product.sector,'0'::CHARACTER VARYING)::TEXT = COALESCE (fact_cycle_target.sector,'0'::CHARACTER VARYING)::TEXT
        AND fact_cycle_product.sales_rep_ntid::TEXT = fact_cycle_target.sales_rep_ntid::TEXT
)
UNION ALL
(SELECT  wrk_ds.country,
       DAT.jnj_date_year::CHARACTER VARYING AS jnj_date_year,
       DAT.jnj_date_month::CHARACTER VARYING AS jnj_date_month,
       DAT.jnj_date_quarter::CHARACTER VARYING AS jnj_date_quarter,
       NULL::CHARACTER VARYING AS date_year,
       NULL::CHARACTER VARYING AS date_month,
       NULL::CHARACTER VARYING AS date_quarter,
       NULL::CHARACTER VARYING AS my_date_year,
       NULL::CHARACTER VARYING AS my_date_month,
       NULL::CHARACTER VARYING AS my_date_quarter,
       hier.sector,
       hier.l2_username AS l3_username,
       hier.l2_manager_name AS l3_manager_name,
       hier.l3_wwid AS l2_wwid,
       hier.l3_username AS l2_username,
       hier.l3_manager_name AS l2_manager_name,
       hier.l4_wwid AS l1_wwid,
       hier.l4_username AS l1_username,
       hier.l4_manager_name AS l1_manager_name,
       hier.l1_username AS sales_rep_ntid,
       dimemp.organization_l1_name,
       dimemp.organization_l2_name,
       dimemp.organization_l3_name,
       replace(dimemp.organization_l4_name,'_',' ') organization_l4_name,
       dimemp.organization_l5_name,
       CASE
         WHEN hier.employee_name IS NULL THEN dimemp.employee_name
         ELSE hier.employee_name
       END AS sales_rep,
       fact_call.working_days,
       fact_call.total_calls,
       fact_call.total_cnt_call_delay,
       fact_call.total_call_edetailing,
       fact_call.call_total_active_user,
       fact_call.total_calls_with_product,
       fact_call.total_sbmtd_calls_key_message,
       fact_call.total_key_message,
       fact_call.total_call_classification_a,
       fact_call.total_call_classification_b,
       fact_call.total_call_classification_c,
       fact_call.total_call_classification_d,
       fact_call.total_call_classification_u,
       fact_call.total_call_classification_z,
       fact_call.total_call_classification_no_product,
       fact_call.total_detailing,
       fact_coach.team as coaching_team,
	     fact_coach.Coaching_status,
       fact_coach.total_coaching_report,
       fact_coach.total_coaching_visit,
       fact_coach.Coaching_manager,
	     fact_coach.sales_rep as coaching_sales_rep,
       fact_cycle_target.planned_calls,
       fact_cycle_target.attainment,
       fact_cycle_target.actual_calls,
       fact_cycle_target.cpa_100/100 AS cpa_100,
       fact_cycle_target.target_cpa_status,
       fact_cycle_product.product_cpa_status,
       fact_cycle_product.planned_call_detail_count,
       fact_cycle_product.cycle_plan_detail_attainment,
       fact_cycle_product.actual_call_detail_count,
       fact_cycle_product.cfa_100,
       fact_cycle_product.cfa_33,
       fact_cycle_product.cfa_66
FROM (SELECT * FROM dim_date WHERE (JNJ_DATE_YEAR BETWEEN (DATE_PART(YEAR,to_date(current_timestamp())) -3) AND DATE_PART(YEAR,to_date(current_timestamp())))) DAT
  INNER JOIN (SELECT 'SG' AS country,
                     ds.jnj_date_year,
                     ds.jnj_date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'SG')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       jnj_date_year,
                       jnj_date_month
              UNION ALL
              SELECT 'MY' AS country,
                     ds.jnj_date_year,
                     ds.jnj_date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'MY')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       jnj_date_year,
                       jnj_date_month
              UNION ALL
              SELECT 'VN' AS country,
                     ds.jnj_date_year,
                     ds.jnj_date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'VN')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       jnj_date_year,
                       jnj_date_month
              UNION ALL
              SELECT 'TH' AS country,
                     ds.jnj_date_year,
                     ds.jnj_date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'TH')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       jnj_date_year,
                       jnj_date_month
              UNION ALL
              SELECT 'PH' AS country,
                     ds.jnj_date_year,
                     ds.jnj_date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'PH')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       jnj_date_year,
                       jnj_date_month
              UNION ALL
              SELECT 'ID' AS country,
                     ds.jnj_date_year,
                     ds.jnj_date_month,
                     COUNT(*) AS working_days
              FROM dim_date ds
              WHERE date_key NOT IN (SELECT DISTINCT holiday_key
                                     FROM holiday_list
                                     WHERE country = 'ID')
              AND   date_dayofweek NOT IN ('Saturday','Sunday')
              AND   date_key < TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY country,
                       jnj_date_year,
                       jnj_date_month) WRK_DS
          ON DAT.jnj_DATE_YEAR = WRK_DS.jnj_DATE_YEAR
         AND DAT.jnj_date_month = WRK_DS.jnj_date_month
  INNER JOIN vw_employee_hier hier ON wrk_ds.country::TEXT = hier.country_code::TEXT
  INNER JOIN dim_employee_iconnect dimemp
         ON hier.employee_key::TEXT = dimemp.employee_key::TEXT
        AND wrk_ds.country = dimemp.country_code::TEXT
  LEFT JOIN (SELECT country,
                    jnj_date_year,
                    jnj_date_month,
                    jnj_date_quarter,
                    sector,
                    MAX(l3_username) AS l3_username,
                    MAX(l3_manager_name) AS l3_manager_name,
                    MAX(l2_wwid) AS l2_wwid,
                    MAX(l2_username) AS l2_username,
                    MAX(l2_manager_name) AS l2_manager_name,
                    MAX(l1_wwid) AS l1_wwid,
                    MAX(l1_username) AS l1_username,
                    MAX(l1_manager_name) AS l1_manager_name,
                    MAX(sales_rep_ntid) AS sales_rep_ntid,
                    MAX(organization_l1_name) AS organization_l1_name,
                    MAX(organization_l2_name) AS organization_l2_name,
                    MAX(organization_l3_name) AS team,
                    MAX(organization_l4_name) AS organization_l4_name,
                    MAX(organization_l5_name) AS organization_l5_name,
                    sales_rep,
                    working_days,
                    total_calls AS total_calls,
                    total_cnt_call_delay,
                    total_call_edetailing,
                    total_active AS call_total_active_user,
                    detailed_products AS total_calls_with_product,
                    total_sbmtd_calls_key_message,
                    total_key_message,
                    SUM(total_call_classification_a) AS total_call_classification_a,
                    SUM(total_call_classification_b) AS total_call_classification_b,
                    SUM(total_call_classification_c) AS total_call_classification_c,
                    SUM(total_call_classification_d) AS total_call_classification_d,
                    SUM(total_call_classification_u) AS total_call_classification_u,
                    SUM(total_call_classification_z) AS total_call_classification_z,
                    SUM(total_call_classification_no_product) AS total_call_classification_no_product,
                    total_detailing
             FROM wrk_call_detail
             WHERE jnj_date_year IS NOT NULL
             GROUP BY country,
                      jnj_date_year,
                      jnj_date_month,
                      jnj_date_quarter,
                      sector,
                      sales_rep,
                      working_days,
                      total_calls,
                      total_cnt_call_delay,
                      total_call_edetailing,
                      total_active,
                      detailed_products,
                      total_sbmtd_calls_key_message,
                      total_key_message,
                      total_detailing) fact_call
         ON fact_call.jnj_date_year = DAT.jnj_date_year
        AND fact_call.jnj_date_month::TEXT = DAT.jnj_date_month::TEXT
        AND fact_call.jnj_date_quarter::TEXT = DAT.jnj_date_quarter::TEXT
        AND fact_call.country::TEXT = WRK_DS.country::TEXT
        AND COALESCE (fact_call.sector,'0'::CHARACTER VARYING)::TEXT = COALESCE (hier.sector,'0'::CHARACTER VARYING)::TEXT
        AND fact_call.sales_rep_ntid::TEXT = hier.l1_username::TEXT
        AND COALESCE (fact_call.l3_manager_name,'#'::CHARACTER VARYING)::TEXT = COALESCE (hier.l2_manager_name::TEXT,'#'::CHARACTER VARYING)::TEXT
  LEFT JOIN (SELECT country,
                    jnj_date_year,
                    jnj_date_month,
                    jnj_date_quarter,
                    sector,
                    MAX(l3_username) AS l3_username,
                    MAX(l3_manager_name) AS l3_manager_name,
                    MAX(l2_wwid) AS l2_wwid,
                    MAX(l2_username) AS l2_username,
                    MAX(l2_manager_name) AS l2_manager_name,
                    MAX(l1_wwid) AS l1_wwid,
                    MAX(l1_username) AS l1_username,
                    MAX(l1_manager_name) AS l1_manager_name,
                    MAX(sales_rep_ntid) AS sales_rep_ntid,
                    MAX(organization_l1_name) AS organization_l1_name,
                    MAX(organization_l2_name) AS organization_l2_name,
                    MAX(organization_l3_name) AS team,
                    MAX(organization_l4_name) AS organization_l4_name,
                    MAX(organization_l5_name) AS organization_l5_name,
                    sales_rep,
                    Coaching_status,
                    total_coaching_report,
                    total_coaching_visit,
                    Coaching_manager
             FROM wrk_coaching_detail
             WHERE jnj_date_year IS NOT NULL
             GROUP BY country,
                      jnj_date_year,
                      jnj_date_month,
                      jnj_date_quarter,
                      sector,
                      sales_rep,
                      Coaching_status,
                      total_coaching_report,
                      total_coaching_visit,
                      Coaching_manager) fact_coach
         ON fact_coach.jnj_date_year = DAT.jnj_date_year
        AND fact_coach.jnj_date_month::TEXT = DAT.jnj_date_month::TEXT
        AND fact_coach.jnj_date_quarter::TEXT = DAT.jnj_date_quarter::TEXT
        AND fact_coach.country::TEXT = WRK_DS.country::TEXT
        AND COALESCE (fact_coach.sector,'0'::CHARACTER VARYING)::TEXT = COALESCE (hier.sector,'0'::CHARACTER VARYING)::TEXT
        AND ((COALESCE (fact_coach.l2_manager_name,'#'::CHARACTER VARYING)::TEXT = COALESCE (hier.l2_manager_name::TEXT,'#'::CHARACTER VARYING)::TEXT AND COALESCE (fact_coach.l2_manager_name,'#'::CHARACTER VARYING)::TEXT <> COALESCE (hier.employee_name::TEXT,'#'::CHARACTER VARYING)::TEXT)
		OR (COALESCE (fact_coach.l2_manager_name,'#'::CHARACTER VARYING)::TEXT <> COALESCE (hier.l2_manager_name::TEXT,'#'::CHARACTER VARYING)::TEXT AND COALESCE (fact_coach.l2_manager_name,'#'::CHARACTER VARYING)::TEXT = COALESCE (hier.employee_name::TEXT,'#'::CHARACTER VARYING)::TEXT))
  LEFT JOIN (SELECT country,
                    jnj_date_year,
                    jnj_date_month,
                    jnj_date_quarter,
                    sector,
                    MAX(l3_username) AS l3_username,
                    MAX(l3_manager_name) AS l3_manager_name,
                    MAX(l2_wwid) AS l2_wwid,
                    MAX(l2_username) AS l2_username,
                    MAX(l2_manager_name) AS l2_manager_name,
                    MAX(l1_wwid) AS l1_wwid,
                    MAX(l1_username) AS l1_username,
                    MAX(l1_manager_name) AS l1_manager_name,
                    MAX(sales_rep_ntid) AS sales_rep_ntid,
                    MAX(organization_l1_name) AS organization_l1_name,
                    MAX(organization_l2_name) AS organization_l2_name,
                    MAX(organization_l3_name) AS team,
                    MAX(organization_l4_name) AS organization_l4_name,
                    MAX(organization_l5_name) AS organization_l5_name,
                    sales_rep,
                    cpa_status AS target_cpa_status,
                    SUM(planned_calls) AS planned_calls,
                    SUM(attainment) AS attainment,
                    SUM(actual_calls) AS actual_calls,
                    SUM(cpa_100) AS cpa_100
             FROM wrk_cycle_plan
             WHERE cycle_plan_type = 'TARGET'
             AND   jnj_date_year IS NOT NULL
             GROUP BY country,
                      jnj_date_year,
                      jnj_date_month,
                      jnj_date_quarter,
                      sector,
                      sales_rep,
                      cpa_status) fact_cycle_target
         ON fact_cycle_target.jnj_date_year = DAT.jnj_date_year
        AND fact_cycle_target.jnj_date_month::TEXT = DAT.jnj_date_month::TEXT
        AND fact_cycle_target.jnj_date_quarter::TEXT = DAT.jnj_date_quarter::TEXT
        AND fact_cycle_target.country::TEXT = WRK_DS.country::TEXT
        AND COALESCE (fact_cycle_target.sector,'0'::CHARACTER VARYING)::TEXT = COALESCE (hier.sector,'0'::CHARACTER VARYING)::TEXT
        AND fact_cycle_target.sales_rep_ntid::TEXT = hier.l1_username::TEXT
        AND COALESCE (fact_cycle_target.l3_manager_name,'#'::CHARACTER VARYING)::TEXT = COALESCE (hier.l2_manager_name::TEXT,'#'::CHARACTER VARYING)::TEXT
  LEFT JOIN (SELECT country,
                    jnj_date_year,
                    jnj_date_month,
                    jnj_date_quarter,
                    sector,
                    MAX(l3_username) AS l3_username,
                    MAX(l3_manager_name) AS l3_manager_name,
                    MAX(l2_wwid) AS l2_wwid,
                    MAX(l2_username) AS l2_username,
                    MAX(l2_manager_name) AS l2_manager_name,
                    MAX(l1_wwid) AS l1_wwid,
                    MAX(l1_username) AS l1_username,
                    MAX(l1_manager_name) AS l1_manager_name,
                    MAX(sales_rep_ntid) AS sales_rep_ntid,
                    MAX(organization_l1_name) AS organization_l1_name,
                    MAX(organization_l2_name) AS organization_l2_name,
                    MAX(organization_l3_name) AS team,
                    MAX(organization_l4_name) AS organization_l4_name,
                    MAX(organization_l5_name) AS organization_l5_name,
                    sales_rep,
                    cpa_status AS product_cpa_status,
                    product,
                    SUM(planned_call_detail_count) AS planned_call_detail_count,
                    SUM(cycle_plan_detail_attainment) AS cycle_plan_detail_attainment,
                    SUM(actual_call_detail_count) AS actual_call_detail_count,
                    SUM(cfa_100) AS cfa_100,
                    SUM(cfa_33) AS cfa_33,
                    SUM(cfa_66) AS cfa_66
             FROM wrk_cycle_plan
             WHERE cycle_plan_type = 'PRODUCT'
             AND   jnj_date_year IS NOT NULL
             GROUP BY country,
                      jnj_date_year,
                      jnj_date_month,
                      jnj_date_quarter,
                      sector,
                      sales_rep,
                      cpa_status,
                      product) fact_cycle_product
         ON fact_cycle_product.jnj_date_year = fact_cycle_target.jnj_date_year
        AND fact_cycle_product.jnj_date_month::TEXT = fact_cycle_target.jnj_date_month::TEXT
        AND fact_cycle_product.jnj_date_quarter::TEXT = fact_cycle_target.jnj_date_quarter::TEXT
        AND fact_cycle_product.country::TEXT = fact_cycle_target.country::TEXT
        AND COALESCE (fact_cycle_product.sector,'0'::CHARACTER VARYING)::TEXT = COALESCE (fact_cycle_target.sector,'0'::CHARACTER VARYING)::TEXT
        AND fact_cycle_product.sales_rep_ntid::TEXT = fact_cycle_target.sales_rep_ntid::TEXT      
)),
final as (
select
country::varchar(20) as country,
jnj_date_year::varchar(11) as jnj_date_year,
jnj_date_month::varchar(5) as jnj_date_month,
jnj_date_quarter::varchar(5) as jnj_date_quarter,
date_year::varchar(11) as date_year,
date_month::varchar(3) as date_month,
date_quarter::varchar(2) as date_quarter,
my_date_year::varchar(11) as my_date_year,
my_date_month::varchar(3) as my_date_month,
my_date_quarter::varchar(2) as my_date_quarter,
sector::varchar(256) as sector,
l3_username::varchar(80) as l3_username,
l3_manager_name::varchar(121) as l3_manager_name,
l2_wwid::varchar(20) as l2_wwid,
l2_username::varchar(80) as l2_username,
l2_manager_name::varchar(121) as l2_manager_name,
l1_wwid::varchar(20) as l1_wwid,
l1_username::varchar(80) as l1_username,
l1_manager_name::varchar(121) as l1_manager_name,
sales_rep_ntid::varchar(80) as sales_rep_ntid,
organization_l1_name::varchar(80) as organization_l1_name,
organization_l2_name::varchar(80) as organization_l2_name,
organization_l3_name::varchar(80) as organization_l3_name,
organization_l4_name::varchar(80) as organization_l4_name,
organization_l5_name::varchar(80) as organization_l5_name,
sales_rep::varchar(121) as sales_rep,
working_days::number(18,0) as working_days,
total_calls::number(38,0) as total_calls,
total_cnt_call_delay::number(38,1) as total_cnt_call_delay,
total_call_edetailing::number(38,0) as total_call_edetailing,
call_total_active_user::number(38,0) as call_total_active_user,
total_calls_with_product::number(38,0) as total_calls_with_product,
total_sbmtd_calls_key_message::number(38,0) as total_sbmtd_calls_key_message,
total_key_message::number(38,0) as total_key_message,
total_call_classification_a::number(38,0) as total_call_classification_a,
total_call_classification_b::number(38,0) as total_call_classification_b,
total_call_classification_c::number(38,0) as total_call_classification_c,
total_call_classification_d::number(38,0) as total_call_classification_d,
total_call_classification_u::number(38,0) as total_call_classification_u,
total_call_classification_z::number(38,0) as total_call_classification_z,
total_call_classification_no_product::number(38,0) as total_call_classification_no_product,
total_detailing::number(38,0) as total_detailing,
coaching_team::varchar(80) as coaching_team,
coaching_status::varchar(255) as coaching_status,
total_coaching_report::number(38,0) as total_coaching_report,
total_coaching_visit::number(38,0) as total_coaching_visit,
coaching_manager::varchar(255) as coaching_manager,
coaching_sales_rep::varchar(255) as coaching_sales_rep,
planned_calls::number(38,0) as planned_calls,
attainment::number(38,0) as attainment,
actual_calls::number(38,0) as actual_calls,
cpa_100::number(38,1) as cpa_100,
target_cpa_status::varchar(18) as target_cpa_status,
product_cpa_status::varchar(18) as product_cpa_status,
planned_call_detail_count::number(38,0) as planned_call_detail_count,
cycle_plan_detail_attainment::number(38,0) as cycle_plan_detail_attainment,
actual_call_detail_count::number(38,0) as actual_call_detail_count,
cfa_100::number(38,0) as cfa_100,
cfa_33::number(38,0) as cfa_33,
cfa_66::number(38,0) as cfa_66
from transformed
)
select * from final