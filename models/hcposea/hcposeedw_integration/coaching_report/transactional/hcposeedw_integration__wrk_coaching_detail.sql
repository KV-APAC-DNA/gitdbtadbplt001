with fact_coaching_report as (
    select * from {{ ref('hcposeedw_integration__fact_coaching_report') }}
),

dim_date as (
    select * from {{ ref('hcposeedw_integration__dim_date') }}
),

dim_employee_iconnect as (
    select * from {{ ref('hcposeedw_integration__dim_employee_iconnect') }}
),

vw_employee_hier as (
    select * from {{ ref('hcposeedw_integration__vw_employee_hier') }}
),


cte1 as (
    SELECT DISTINCT NULL AS jnj_date_year,
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
       src1.l3_wwid::CHARACTER VARYING AS l3_wwid,
       src1.l3_username::CHARACTER VARYING AS l3_username,
       src1.l3_manager_name,
       src1.l2_wwid::CHARACTER VARYING AS l2_wwid,
       src1.l2_username::CHARACTER VARYING AS l2_username,
       src1.l2_manager_name::CHARACTER VARYING AS l2_manager_name,
       src1.l1_wwid::CHARACTER VARYING AS l1_wwid,
       src1.l1_username::CHARACTER VARYING AS l1_username,
       src1.l1_manager_name::CHARACTER VARYING AS l1_manager_name,
       src1.organization_l1_name::CHARACTER VARYING AS organization_l1_name,
       src1.organization_l2_name::CHARACTER VARYING AS organization_l2_name,
       src1.organization_l3_name,
       src1.organization_l4_name::CHARACTER VARYING AS organization_l4_name,
       src1.organization_l5_name::CHARACTER VARYING AS organization_l5_name,
       src1.sales_rep_ntid::CHARACTER VARYING AS sales_rep_ntid,
       src1.sales_rep,
       src1.STATUS AS Coaching_status,
       src1.l2_manager_name AS Coaching_manager,
       src1.total_coaching_report,
       src1.total_coaching_visit
FROM (
       SELECT fact.country_key AS country,
              fact.employee_key,
              fact.manager_employee_key,
              fact.STATUS,
              dimdate.date_year,
              dimdate.date_month,
              dimdate.date_quarter,
              hier.sector,
              "max" (hier.l2_wwid::TEXT) AS l3_wwid,
              "max" (hier.l2_username) AS l3_username,
              hier.l2_manager_name AS l3_manager_name,
              "max" (hier.l3_wwid::TEXT) AS l2_wwid,
              "max" (hier.l3_username) AS l2_username,
              employee1.employee_name AS l2_manager_name,
              "max" (hier.l4_wwid::TEXT) AS l1_wwid,
              "max" (hier.l4_username) AS l1_username,
              "max" (hier.l4_manager_name::TEXT) AS l1_manager_name,
              "max" (employee1.organization_l1_name::TEXT) AS organization_l1_name,
              "max" (employee1.organization_l2_name::TEXT) AS organization_l2_name,
              employee1.organization_l3_name,
              "max" (employee1.organization_l4_name::TEXT) AS organization_l4_name,
              "max" (employee1.organization_l5_name::TEXT) AS organization_l5_name,
              dimemp.employee_name AS sales_rep,
              hier.l1_username AS sales_rep_ntid,
              COUNT(DISTINCT coaching_report_source_id) AS total_coaching_report,
              SUM(jj_core_no_of_visits) AS total_coaching_visit
       FROM fact_coaching_report fact
       LEFT JOIN dim_date dimdate ON fact.coaching_report_date_key::NUMERIC::NUMERIC(18, 0) = dimdate.date_key::NUMERIC::NUMERIC(18, 0)
       LEFT JOIN DIM_EMPLOYEE_ICONNECT employee1 ON fact.manager_employee_key::TEXT = employee1.employee_key::TEXT
       LEFT JOIN vw_employee_hier hier ON fact.manager_employee_key::TEXT = hier.employee_key::TEXT
              AND fact.country_key::TEXT = hier.country_code::TEXT
       LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
              AND fact.country_key::TEXT = dimemp.country_code::TEXT
       WHERE fact.isdeleted::TEXT = '0'::CHARACTER VARYING::TEXT
       GROUP BY fact.country_key,
              fact.employee_key,
              fact.manager_employee_key,
              fact.STATUS,
              dimdate.date_year,
              dimdate.date_month,
              dimdate.date_quarter,
              hier.sector,
              hier.l2_manager_name,
              employee1.employee_name,
              employee1.organization_l3_name,
              dimemp.employee_name,
              hier.l1_username
) src1
),

cte2 as (
    SELECT DISTINCT src1.jnj_date_year::CHARACTER VARYING AS jnj_date_year,
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
       src1.l3_wwid::CHARACTER VARYING AS l3_wwid,
       src1.l3_username::CHARACTER VARYING AS l3_username,
       src1.l3_manager_name,
       src1.l2_wwid::CHARACTER VARYING AS l2_wwid,
       src1.l2_username::CHARACTER VARYING AS l2_username,
       src1.l2_manager_name::CHARACTER VARYING AS l2_manager_name,
       src1.l1_wwid::CHARACTER VARYING AS l1_wwid,
       src1.l1_username::CHARACTER VARYING AS l1_username,
       src1.l1_manager_name::CHARACTER VARYING AS l1_manager_name,
       src1.organization_l1_name::CHARACTER VARYING AS organization_l1_name,
       src1.organization_l2_name::CHARACTER VARYING AS organization_l2_name,
       src1.organization_l3_name,
       src1.organization_l4_name::CHARACTER VARYING AS organization_l4_name,
       src1.organization_l5_name::CHARACTER VARYING AS organization_l5_name,
       src1.sales_rep_ntid::CHARACTER VARYING AS sales_rep_ntid,
       src1.sales_rep,
       src1.STATUS AS Coaching_status,
       src1.l2_manager_name AS Coaching_manager,
       src1.total_coaching_report,
       src1.total_coaching_visit
FROM (
       SELECT fact.country_key AS country,
              fact.employee_key,
              fact.manager_employee_key,
              fact.STATUS,
              dimdate.jnj_date_year,
              dimdate.jnj_date_month,
              dimdate.jnj_date_quarter,
              hier.sector,
              "max" (hier.l2_wwid::TEXT) AS l3_wwid,
              "max" (hier.l2_username) AS l3_username,
              hier.l2_manager_name AS l3_manager_name,
              "max" (hier.l3_wwid::TEXT) AS l2_wwid,
              "max" (hier.l3_username) AS l2_username,
              employee1.employee_name AS l2_manager_name,
              "max" (hier.l4_wwid::TEXT) AS l1_wwid,
              "max" (hier.l4_username) AS l1_username,
              "max" (hier.l4_manager_name::TEXT) AS l1_manager_name,
              "max" (employee1.organization_l1_name::TEXT) AS organization_l1_name,
              "max" (employee1.organization_l2_name::TEXT) AS organization_l2_name,
              employee1.organization_l3_name,
              "max" (employee1.organization_l4_name::TEXT) AS organization_l4_name,
              "max" (employee1.organization_l5_name::TEXT) AS organization_l5_name,
              dimemp.employee_name AS sales_rep,
              hier.l1_username AS sales_rep_ntid,
              COUNT(DISTINCT coaching_report_source_id) AS total_coaching_report,
              SUM(jj_core_no_of_visits) AS total_coaching_visit
       FROM fact_coaching_report fact
       LEFT JOIN dim_date dimdate ON fact.coaching_report_date_key::NUMERIC::NUMERIC(18, 0) = dimdate.date_key::NUMERIC::NUMERIC(18, 0)
       LEFT JOIN DIM_EMPLOYEE_ICONNECT employee1 ON fact.manager_employee_key::TEXT = employee1.employee_key::TEXT
       LEFT JOIN vw_employee_hier hier ON fact.manager_employee_key::TEXT = hier.employee_key::TEXT
              AND fact.country_key::TEXT = hier.country_code::TEXT
       LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
              AND fact.country_key::TEXT = dimemp.country_code::TEXT
       WHERE fact.isdeleted::TEXT = '0'::CHARACTER VARYING::TEXT
       GROUP BY fact.country_key,
              fact.employee_key,
              fact.manager_employee_key,
              fact.STATUS,
              dimdate.jnj_date_year,
              dimdate.jnj_date_month,
              dimdate.jnj_date_quarter,
              hier.sector,
              hier.l2_manager_name,
              employee1.employee_name,
              employee1.organization_l3_name,
              dimemp.employee_name,
              hier.l1_username
) src1
),

cte3 as (
    SELECT DISTINCT NULL AS jnj_date_year,
       NULL AS jnj_date_month,
       NULL AS jnj_date_quarter,
       NULL AS date_year,
       NULL AS date_month,
       NULL AS date_quarter,
       src1.my_date_year::CHARACTER VARYING AS my_date_year,
       src1.my_date_month::CHARACTER VARYING AS my_date_month,
       src1.my_date_quarter::CHARACTER VARYING AS my_date_quarter,
       src1.country,
       src1.sector,
       src1.l3_wwid::CHARACTER VARYING AS l3_wwid,
       src1.l3_username::CHARACTER VARYING AS l3_username,
       src1.l3_manager_name,
       src1.l2_wwid::CHARACTER VARYING AS l2_wwid,
       src1.l2_username::CHARACTER VARYING AS l2_username,
       src1.l2_manager_name::CHARACTER VARYING AS l2_manager_name,
       src1.l1_wwid::CHARACTER VARYING AS l1_wwid,
       src1.l1_username::CHARACTER VARYING AS l1_username,
       src1.l1_manager_name::CHARACTER VARYING AS l1_manager_name,
       src1.organization_l1_name::CHARACTER VARYING AS organization_l1_name,
       src1.organization_l2_name::CHARACTER VARYING AS organization_l2_name,
       src1.organization_l3_name,
       src1.organization_l4_name::CHARACTER VARYING AS organization_l4_name,
       src1.organization_l5_name::CHARACTER VARYING AS organization_l5_name,
       src1.sales_rep_ntid::CHARACTER VARYING AS sales_rep_ntid,
       src1.sales_rep,
       src1.STATUS AS Coaching_status,
       src1.l2_manager_name AS Coaching_manager,
       src1.total_coaching_report,
       src1.total_coaching_visit
FROM (
       SELECT fact.country_key AS country,
              fact.employee_key,
              fact.manager_employee_key,
              fact.STATUS,
              dimdate.my_date_year,
              dimdate.my_date_month,
              dimdate.my_date_quarter,
              hier.sector,
              "max" (hier.l2_wwid::TEXT) AS l3_wwid,
              "max" (hier.l2_username) AS l3_username,
              hier.l2_manager_name AS l3_manager_name,
              "max" (hier.l3_wwid::TEXT) AS l2_wwid,
              "max" (hier.l3_username) AS l2_username,
              employee1.employee_name AS l2_manager_name,
              "max" (hier.l4_wwid::TEXT) AS l1_wwid,
              "max" (hier.l4_username) AS l1_username,
              "max" (hier.l4_manager_name::TEXT) AS l1_manager_name,
              "max" (employee1.organization_l1_name::TEXT) AS organization_l1_name,
              "max" (employee1.organization_l2_name::TEXT) AS organization_l2_name,
              employee1.organization_l3_name,
              "max" (employee1.organization_l4_name::TEXT) AS organization_l4_name,
              "max" (employee1.organization_l5_name::TEXT) AS organization_l5_name,
              dimemp.employee_name AS sales_rep,
              hier.l1_username AS sales_rep_ntid,
              COUNT(DISTINCT coaching_report_source_id) AS total_coaching_report,
              SUM(jj_core_no_of_visits) AS total_coaching_visit
       FROM fact_coaching_report fact
       LEFT JOIN dim_date dimdate ON fact.coaching_report_date_key::NUMERIC::NUMERIC(18, 0) = dimdate.date_key::NUMERIC::NUMERIC(18, 0)
       LEFT JOIN DIM_EMPLOYEE_ICONNECT employee1 ON fact.manager_employee_key::TEXT = employee1.employee_key::TEXT
       LEFT JOIN vw_employee_hier hier ON fact.manager_employee_key::TEXT = hier.employee_key::TEXT
              AND fact.country_key::TEXT = hier.country_code::TEXT
       LEFT JOIN DIM_EMPLOYEE_ICONNECT dimemp ON fact.employee_key::TEXT = dimemp.employee_key::TEXT
              AND fact.country_key::TEXT = dimemp.country_code::TEXT
       WHERE fact.isdeleted::TEXT = '0'::CHARACTER VARYING::TEXT
       GROUP BY fact.country_key,
              fact.employee_key,
              fact.manager_employee_key,
              fact.STATUS,
              dimdate.my_date_year,
              dimdate.my_date_month,
              dimdate.my_date_quarter,
              hier.sector,
              hier.l2_manager_name,
              employee1.employee_name,
              employee1.organization_l3_name,
              dimemp.employee_name,
              hier.l1_username
) src1
),

result as (
    select * from cte1
    union all
    select * from cte2
    union all
    select * from cte3
),

final as (
    select 
        jnj_date_year::varchar(11) as jnj_date_year,
        jnj_date_month::varchar(11) as jnj_date_month,
        jnj_date_quarter::varchar(11) as jnj_date_quarter,
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
        organization_l1_name::varchar(80) as organization_l1_name,
        organization_l2_name::varchar(80) as organization_l2_name,
        organization_l3_name::varchar(80) as organization_l3_name,
        organization_l4_name::varchar(80) as organization_l4_name,
        organization_l5_name::varchar(80) as organization_l5_name,
        sales_rep_ntid::varchar(80) as sales_rep_ntid,
        sales_rep::varchar(121) as sales_rep,
        coaching_status::varchar(255) as coaching_status,
        coaching_manager::varchar(121) as coaching_manager,
        total_coaching_report::number(38,0) as total_coaching_report,
        total_coaching_visit::number(38,0) as total_coaching_visit
    from result
)

select * from final