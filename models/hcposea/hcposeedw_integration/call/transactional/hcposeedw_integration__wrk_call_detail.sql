WITH fact_call_detail
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__fact_call_detail') }}
    ),
dim_date
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__dim_date') }}
    ),
vw_employee_hier
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__vw_employee_hier') }}
    ),
dim_employee_iconnect
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__dim_employee_iconnect') }}
    ),
dim_hco
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__dim_hco') }}
    ),
dim_hcp
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__dim_hcp') }}
    ),
dim_date
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__dim_date') }}
    ),
holiday_list
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__holiday_list') }}
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
fact_timeoff_territory
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__fact_timeoff_territory') }}
    ),
fact_call_key_message
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__fact_call_key_message') }}
    ),
itg_call_detail
AS (
    SELECT *
    FROM {{ ref('hcposeitg_integration__itg_call_detail') }}
    ),
itg_call
AS (
    SELECT *
    FROM {{ ref('hcposeitg_integration__itg_call') }}
    ),
itg_product_metrics
AS (
    SELECT *
    FROM {{ ref('hcposeitg_integration__itg_product_metrics') }}
    ),
dim_product_indication
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__dim_product_indication') }}
    ),
T1
AS (
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
        src1.email_id,
        src1.working_days,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.hcp_speciality,
        src1.business_account_id,
        src1.business_account,
        src1.total_calls,
        fct7.total_cnt_call_delay,
        fct1.total_cnt_clm_flg AS total_call_edetailing,
        CASE 
            WHEN ((src1.date_year || '-'::CHARACTER VARYING::TEXT) || src1.date_month::TEXT) = (
                    (
                        date_part (
                            year,
                            sysdate()
                            )::CHARACTER VARYING(5)::TEXT || '-'::CHARACTER VARYING::TEXT
                        ) || date_part (
                        month,
                        sysdate()
                        )::CHARACTER VARYING(5)::TEXT
                    )
                THEN emp1.current_mnth_emp_cnt
            ELSE actve_usr.total_active
            END AS total_active,
        fct4.total_call_product AS detailed_products,
        fct3.total_sbmtd_calls_key_message,
        fct5.total_key_message,
        fct2.total_call_classification,
        fct2.classification_type,
        CASE 
            WHEN fct2.classification_type = 'A'
                THEN (fct2.total_call_classification)
            END total_call_classification_a,
        CASE 
            WHEN fct2.classification_type = 'B'
                THEN (fct2.total_call_classification)
            END total_call_classification_b,
        CASE 
            WHEN fct2.classification_type = 'C'
                THEN (fct2.total_call_classification)
            END total_call_classification_c,
        CASE 
            WHEN fct2.classification_type = 'D'
                THEN (fct2.total_call_classification)
            END total_call_classification_d,
        CASE 
            WHEN fct2.classification_type = 'U'
                THEN (fct2.total_call_classification)
            END total_call_classification_u,
        CASE 
            WHEN fct2.classification_type = 'Z'
                THEN (fct2.total_call_classification)
            END total_call_classification_z,
        CASE 
            WHEN fct2.classification_type IS NULL
                THEN (fct2.total_call_classification)
            WHEN fct2.classification_type IN ('', ' ')
                THEN (fct2.total_call_classification)
            END total_call_classification_no_product,
        fct6.total_detailing
    FROM (
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
            fact.email_id,
            fact.working_days,
            fact.total_calls,
            fact.sales_rep_ntid,
            fact.employee_key,
            fact.hcp_name,
            fact.hcp_source_id,
            fact.speciality_1_type_english AS hcp_speciality,
            fact.hco_name AS business_account,
            fact.hco_source_id AS business_account_id
        FROM (
            SELECT dimdate.date_year,
                dimdate.date_month,
                dimdate.date_quarter,
                fact.country_key AS country,
                hier.sector,
                "max" (hier.l2_wwid::TEXT) AS l3_wwid,
                "max" (hier.l2_username) AS l3_username,
                hier.l2_manager_name AS l3_manager_name,
                "max" (hier.l3_wwid::TEXT) AS l2_wwid,
                "max" (hier.l3_username) AS l2_username,
                "max" (hier.l3_manager_name::TEXT) AS l2_manager_name,
                "max" (hier.l4_wwid::TEXT) AS l1_wwid,
                "max" (hier.l4_username) AS l1_username,
                "max" (hier.l4_manager_name::TEXT) AS l1_manager_name,
                "max" (employee.organization_l1_name::TEXT) AS organization_l1_name,
                "max" (employee.organization_l2_name::TEXT) AS organization_l2_name,
                employee.organization_l3_name,
                "max" (employee.organization_l4_name::TEXT) AS organization_l4_name,
                "max" (employee.organization_l5_name::TEXT) AS organization_l5_name,
                hier.employee_name AS sales_rep,
                employee.email_id,
                "max" (ds.working_days::NUMERIC::NUMERIC(18, 0)) AS working_days,
                COUNT(*) AS total_calls,
                fact.employee_key,
                hier.l1_username AS sales_rep_ntid,
                dim_hcp.hcp_name,
                dim_hcp.hcp_source_id,
                dim_hcp.speciality_1_type_english,
                dim_hco.hco_name,
                dim_hco.hco_source_id
            FROM (
                SELECT country_key,
                    hcp_key,
                    hco_key,
                    employee_key,
                    profile_key,
                    organization_key,
                    call_date_key,
                    parent_call_name
                FROM fact_call_detail
                WHERE call_status_type::TEXT = 'Submitted'::TEXT
                    AND country_key::TEXT <> 'ZZ'::TEXT
                GROUP BY country_key,
                    hcp_key,
                    hco_key,
                    employee_key,
                    profile_key,
                    organization_key,
                    call_date_key,
                    attendee_type,
                    call_clm_flag,
                    parent_call_name
                ) fact
            JOIN dim_date dimdate ON fact.call_date_key = dimdate.date_key
                AND DATE_YEAR BETWEEN (DATE_PART(year,sysdate()) - 2)
                AND DATE_PART(year,sysdate())
            JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
                AND fact.country_key::TEXT = hier.country_code::TEXT
            LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fact.employee_key::TEXT = employee.employee_key::TEXT
            JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
            JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
            LEFT JOIN (
                SELECT 'SG' AS country,
                    ds.date_year,
                    ds.date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'SG'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    date_year,
                    date_month
                
                UNION ALL
                
                SELECT 'MY' AS country,
                    ds.date_year,
                    ds.date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'MY'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    date_year,
                    date_month
                
                UNION ALL
                
                SELECT 'VN' AS country,
                    ds.date_year,
                    ds.date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'VN'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    date_year,
                    date_month
                
                UNION ALL
                
                SELECT 'TH' AS country,
                    ds.date_year,
                    ds.date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'TH'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    date_year,
                    date_month
                
                UNION ALL
                
                SELECT 'PH' AS country,
                    ds.date_year,
                    ds.date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'PH'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    date_year,
                    date_month
                
                UNION ALL
                
                SELECT 'ID' AS country,
                    ds.date_year,
                    ds.date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'ID'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    date_year,
                    date_month
                ) ds ON dimdate.date_year::TEXT = ds.date_year::TEXT
                AND dimdate.date_month::TEXT = ds.date_month
                AND fact.country_key::TEXT = ds.country::TEXT
            GROUP BY dimdate.date_year,
                dimdate.date_month,
                dimdate.date_quarter,
                fact.country_key,
                hier.employee_name,
                employee.email_id,
                fact.employee_key,
                dim_hcp.hcp_name,
                dim_hcp.hcp_source_id,
                dim_hcp.speciality_1_type_english,
                dim_hco.hco_name,
                dim_hco.hco_source_id,
                hier.l1_username,
                hier.sector,
                hier.l2_manager_name,
                employee.organization_l3_name
            ) fact
        ) src1
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
        ) actve_usr ON src1.date_year = actve_usr.year
        AND src1.date_month::TEXT = actve_usr.month::TEXT
        AND src1.country::TEXT = actve_usr.country_code::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(actve_usr.sector, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(1) AS current_mnth_emp_cnt,
            DIM_EMPLOYEE_ICONNECT.country_code,
            sect.sector
        FROM DIM_EMPLOYEE_ICONNECT
        LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = DIM_EMPLOYEE_ICONNECT.company_name::TEXT
            AND sect.country::TEXT = DIM_EMPLOYEE_ICONNECT.country_code::TEXT
        WHERE DIM_EMPLOYEE_ICONNECT.active_flag = 1::NUMERIC::NUMERIC(18, 0)
            AND (
                DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_Sales'::TEXT
                OR DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_MY_Sales'::TEXT
                )
        GROUP BY DIM_EMPLOYEE_ICONNECT.country_code,
            sect.sector
        ) emp1 ON src1.country::TEXT = emp1.country_code::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(emp1.sector, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(1) AS total_cnt_clm_flg,
            fct.employee_key,
            dimdate.date_year AS yr,
            dimdate.date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        WHERE fct.call_clm_flag = 1
            AND fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
        GROUP BY fct.employee_key,
            dimdate.date_year,
            dimdate.date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct1 ON src1.employee_key::TEXT = fct1.employee_key::TEXT
        AND src1.date_year = fct1.yr
        AND src1.date_month::TEXT = fct1.mnth::TEXT
        AND fct1.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct1.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct1.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(fct.*) AS total_call_classification,
            fct.employee_key,
            dimdate.date_year AS yr,
            dimdate.date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name,
            PRDM.CLASSIFICATION_TYPE
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        LEFT JOIN DIM_HCP HCP ON HCP.HCP_KEY = fct.HCP_KEY
        LEFT JOIN itg_PRODUCT_METRICS PRDM ON HCP.HCP_SOURCE_ID = PRDM.ACCOUNT_SOURCE_ID
        LEFT JOIN DIM_PRODUCT_INDICATION PROD ON PROD.PRODUCT_SOURCE_ID = PRDM.PRODUCT_SOURCE_ID
            AND PROD.PRODUCT_INDICATION_KEY = fct.PRODUCT_INDICATION_KEY
        WHERE PRDM.CLASSIFICATION_TYPE IS NOT NULL
            OR PRDM.CLASSIFICATION_TYPE NOT IN ('', ' ')
        GROUP BY fct.employee_key,
            dimdate.date_year,
            dimdate.date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name,
            PRDM.CLASSIFICATION_TYPE
        ) fct2 ON src1.employee_key::TEXT = fct2.employee_key::TEXT
        AND src1.date_year = fct2.yr
        AND src1.date_month::TEXT = fct2.mnth::TEXT
        AND fct2.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct2.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct2.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct2.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT SUM(COALESCE(terr.sea_frml_hours_on, 0::NUMERIC::NUMERIC(18, 0))) / 8::NUMERIC::NUMERIC(18, 0) AS cnt_total_time_on,
            terr.employee_key,
            terr.country_key,
            date_dim.date_year AS yr,
            date_dim.date_month AS mnth
        FROM fact_timeoff_territory terr
        JOIN dim_date date_dim ON terr.start_date_key::TEXT = date_dim.date_key::TEXT
        WHERE terr.sea_time_on_time_off::TEXT = 'Time On'::TEXT
        GROUP BY terr.employee_key,
            terr.country_key,
            date_dim.date_year,
            date_dim.date_month
        ) terr ON terr.employee_key::TEXT = src1.employee_key::TEXT
        AND terr.country_key::TEXT = src1.country::TEXT
        AND src1.date_year = terr.yr
        AND src1.date_month::TEXT = terr.mnth::TEXT
    LEFT JOIN (
        SELECT SUM(COALESCE(terr.total_time_off, 0::NUMERIC::NUMERIC(18, 0))) / 8::NUMERIC::NUMERIC(18, 0) AS cnt_total_time_off,
            terr.employee_key,
            terr.country_key,
            date_dim.date_year AS yr,
            date_dim.date_month AS mnth
        FROM fact_timeoff_territory terr
        JOIN dim_date date_dim ON terr.start_date_key::TEXT = date_dim.date_key::TEXT
        WHERE terr.sea_time_on_time_off::TEXT = 'Time Off'::TEXT
        GROUP BY terr.employee_key,
            terr.country_key,
            date_dim.date_year,
            date_dim.date_month
        ) terr1 ON terr1.employee_key::TEXT = src1.employee_key::TEXT
        AND terr1.country_key::TEXT = src1.country::TEXT
        AND src1.date_year = terr1.yr
        AND src1.date_month::TEXT = terr1.mnth::TEXT
    LEFT JOIN (
        SELECT COUNT(DISTINCT fct.call_name) AS total_sbmtd_calls_key_message,
            fct.employee_key,
            dimdate.date_year AS yr,
            dimdate.date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN fact_call_key_message fckm ON fct.call_source_id::TEXT = fckm.call_source_id::TEXT
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        WHERE fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
        GROUP BY fct.employee_key,
            dimdate.date_year,
            dimdate.date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct3 ON src1.employee_key::TEXT = fct3.employee_key::TEXT
        AND src1.date_year = fct3.yr
        AND src1.date_month::TEXT = fct3.mnth::TEXT
        AND fct3.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct3.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct3.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct3.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(1) AS total_call_product,
            fct.employee_key,
            dimdate.date_year AS yr,
            dimdate.date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        WHERE fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
            AND (
                fct.detailed_products IS NOT NULL
                OR fct.detailed_products::TEXT <> ''::TEXT
                AND fct.detailed_products::TEXT <> ' '::TEXT
                )
        GROUP BY fct.employee_key,
            dimdate.date_year,
            dimdate.date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct4 ON src1.employee_key::TEXT = fct4.employee_key::TEXT
        AND src1.date_year = fct4.yr
        AND src1.date_month::TEXT = fct4.mnth::TEXT
        AND fct4.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct4.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct4.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct4.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(*) AS total_key_message,
            fckm.employee_key,
            dimdate.date_year AS yr,
            dimdate.date_month AS mnth,
            fckm.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN fact_call_key_message fckm ON fct.call_source_id::TEXT = fckm.call_source_id::TEXT
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        WHERE fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
        GROUP BY fckm.employee_key,
            dimdate.date_year,
            dimdate.date_month,
            fckm.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct5 ON src1.employee_key::TEXT = fct5.employee_key::TEXT
        AND src1.date_year = fct5.yr
        AND src1.date_month::TEXT = fct5.mnth::TEXT
        AND fct5.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct5.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct5.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct5.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(fct_detl.call_detail_source_id) AS total_detailing,
            employee.employee_key,
            dimdate.date_year AS yr,
            dimdate.date_month AS mnth,
            fct.country_code AS country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM itg_call_detail fct_detl
        JOIN itg_call fct ON fct_detl.call_source_id = fct.call_source_id
            AND fct.is_parent_call = 1
            AND fct.is_deleted = 0
            AND fct.CALL_STATUS_TYPE = 'Submitted_vod'
        JOIN dim_date dimdate ON TO_CHAR(fct.CALL_DATE, 'yyyymmdd') = dimdate.date_key
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.owner_source_id::TEXT = employee.employee_source_id::TEXT
        JOIN vw_employee_hier hier ON employee.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_code::TEXT = hier.country_code::TEXT
        WHERE fct_detl.is_parent_call = 1
            AND fct_detl.is_deleted = 0
        GROUP BY employee.employee_key,
            dimdate.date_year,
            dimdate.date_month,
            fct.country_code,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct6 ON src1.employee_key::TEXT = fct6.employee_key::TEXT
        AND src1.date_year = fct6.yr
        AND src1.date_month::TEXT = fct6.mnth::TEXT
        AND fct6.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct6.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct6.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct6.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT SUM(fct.call_submission_day) AS total_cnt_call_delay,
            employee.employee_key,
            dimdate.date_year AS yr,
            dimdate.date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        WHERE fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
            AND fct.country_key::TEXT <> 'ZZ'::TEXT
        GROUP BY employee.employee_key,
            dimdate.date_year,
            dimdate.date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct7 ON src1.employee_key::TEXT = fct7.employee_key::TEXT
        AND src1.date_year = fct7.yr
        AND src1.date_month::TEXT = fct7.mnth::TEXT
        AND fct7.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct7.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct7.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct7.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    ),
T2
AS (
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
        src1.email_id,
        src1.working_days,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.hcp_speciality,
        src1.business_account_id,
        src1.business_account,
        src1.total_calls,
        fct7.total_cnt_call_delay,
        fct1.total_cnt_clm_flg AS total_call_edetailing,
        CASE 
            WHEN ((src1.jnj_date_year || '-'::CHARACTER VARYING::TEXT) || src1.jnj_date_month::TEXT) = (
                    (
                        date_part (
                            year,
                            sysdate()
                            )::CHARACTER VARYING(5)::TEXT || '-'::CHARACTER VARYING::TEXT
                        ) || date_part (
                        month,
                        sysdate()
                        )::CHARACTER VARYING(5)::TEXT
                    )
                THEN emp1.current_mnth_emp_cnt
            ELSE actve_usr.total_active
            END AS total_active,
        fct4.total_call_product AS detailed_products,
        fct3.total_sbmtd_calls_key_message,
        fct5.total_key_message,
        fct2.total_call_classification,
        fct2.classification_type,
        CASE 
            WHEN fct2.classification_type = 'A'
                THEN (fct2.total_call_classification)
            END total_call_classification_a,
        CASE 
            WHEN fct2.classification_type = 'B'
                THEN (fct2.total_call_classification)
            END total_call_classification_b,
        CASE 
            WHEN fct2.classification_type = 'C'
                THEN (fct2.total_call_classification)
            END total_call_classification_c,
        CASE 
            WHEN fct2.classification_type = 'D'
                THEN (fct2.total_call_classification)
            END total_call_classification_d,
        CASE 
            WHEN fct2.classification_type = 'U'
                THEN (fct2.total_call_classification)
            END total_call_classification_u,
        CASE 
            WHEN fct2.classification_type = 'Z'
                THEN (fct2.total_call_classification)
            END total_call_classification_z,
        CASE 
            WHEN fct2.classification_type IS NULL
                THEN (fct2.total_call_classification)
            WHEN fct2.classification_type IN ('', ' ')
                THEN (fct2.total_call_classification)
            END total_call_classification_no_product,
        fct6.total_detailing
    FROM (
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
            fact.email_id,
            fact.working_days,
            fact.total_calls,
            fact.sales_rep_ntid,
            fact.employee_key,
            fact.hcp_name,
            fact.hcp_source_id,
            fact.speciality_1_type_english AS hcp_speciality,
            fact.hco_name AS business_account,
            fact.hco_source_id AS business_account_id
        FROM (
            SELECT dimdate.jnj_date_year,
                dimdate.jnj_date_month,
                dimdate.jnj_date_quarter,
                fact.country_key AS country,
                hier.sector,
                "max" (hier.l2_wwid::TEXT) AS l3_wwid,
                "max" (hier.l2_username) AS l3_username,
                hier.l2_manager_name AS l3_manager_name,
                "max" (hier.l3_wwid::TEXT) AS l2_wwid,
                "max" (hier.l3_username) AS l2_username,
                "max" (hier.l3_manager_name::TEXT) AS l2_manager_name,
                "max" (hier.l4_wwid::TEXT) AS l1_wwid,
                "max" (hier.l4_username) AS l1_username,
                "max" (hier.l4_manager_name::TEXT) AS l1_manager_name,
                "max" (employee.organization_l1_name::TEXT) AS organization_l1_name,
                "max" (employee.organization_l2_name::TEXT) AS organization_l2_name,
                employee.organization_l3_name,
                "max" (employee.organization_l4_name::TEXT) AS organization_l4_name,
                "max" (employee.organization_l5_name::TEXT) AS organization_l5_name,
                hier.employee_name AS sales_rep,
                employee.email_id,
                "max" (ds.working_days::NUMERIC::NUMERIC(18, 0)) AS working_days,
                COUNT(*) AS total_calls,
                fact.employee_key,
                hier.l1_username AS sales_rep_ntid,
                dim_hcp.hcp_name,
                dim_hcp.hcp_source_id,
                dim_hcp.speciality_1_type_english,
                dim_hco.hco_name,
                dim_hco.hco_source_id
            FROM (
                SELECT country_key,
                    hcp_key,
                    hco_key,
                    employee_key,
                    profile_key,
                    organization_key,
                    call_date_key,
                    parent_call_name
                FROM fact_call_detail
                WHERE call_status_type::TEXT = 'Submitted'::TEXT
                    AND country_key::TEXT <> 'ZZ'::TEXT
                GROUP BY country_key,
                    hcp_key,
                    hco_key,
                    employee_key,
                    profile_key,
                    organization_key,
                    call_date_key,
                    attendee_type,
                    call_clm_flag,
                    parent_call_name
                ) fact
            JOIN dim_date dimdate ON fact.call_date_key = dimdate.date_key
                AND jnj_date_year BETWEEN (DATE_PART(year,sysdate()) - 2)
                AND DATE_PART(year,sysdate())
            JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
                AND fact.country_key::TEXT = hier.country_code::TEXT
            LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fact.employee_key::TEXT = employee.employee_key::TEXT
            JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
            JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
            LEFT JOIN (
                SELECT 'SG' AS country,
                    ds.jnj_date_year,
                    ds.jnj_date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'SG'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    jnj_date_year,
                    jnj_date_month
                
                UNION ALL
                
                SELECT 'MY' AS country,
                    ds.jnj_date_year,
                    ds.jnj_date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'MY'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    jnj_date_year,
                    jnj_date_month
                
                UNION ALL
                
                SELECT 'VN' AS country,
                    ds.jnj_date_year,
                    ds.jnj_date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'VN'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    jnj_date_year,
                    jnj_date_month
                
                UNION ALL
                
                SELECT 'TH' AS country,
                    ds.jnj_date_year,
                    ds.jnj_date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'TH'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    jnj_date_year,
                    jnj_date_month
                
                UNION ALL
                
                SELECT 'PH' AS country,
                    ds.jnj_date_year,
                    ds.jnj_date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'PH'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    jnj_date_year,
                    jnj_date_month
                
                UNION ALL
                
                SELECT 'ID' AS country,
                    ds.jnj_date_year,
                    ds.jnj_date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'ID'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    jnj_date_year,
                    jnj_date_month
                ) ds ON dimdate.jnj_date_year::TEXT = ds.jnj_date_year::TEXT
                AND dimdate.jnj_date_month::TEXT = ds.jnj_date_month
                AND fact.country_key::TEXT = ds.country::TEXT
            GROUP BY dimdate.jnj_date_year,
                dimdate.jnj_date_month,
                dimdate.jnj_date_quarter,
                fact.country_key,
                hier.employee_name,
                employee.email_id,
                fact.employee_key,
                dim_hcp.hcp_name,
                dim_hcp.hcp_source_id,
                dim_hcp.speciality_1_type_english,
                dim_hco.hco_name,
                dim_hco.hco_source_id,
                hier.l1_username,
                hier.sector,
                hier.l2_manager_name,
                employee.organization_l3_name
            ) fact
        ) src1
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
        ) actve_usr ON src1.jnj_date_year = actve_usr.year
        AND src1.jnj_date_month::TEXT = actve_usr.month::TEXT
        AND src1.country::TEXT = actve_usr.country_code::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(actve_usr.sector, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(1) AS current_mnth_emp_cnt,
            DIM_EMPLOYEE_ICONNECT.country_code,
            sect.sector
        FROM DIM_EMPLOYEE_ICONNECT
        LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = DIM_EMPLOYEE_ICONNECT.company_name::TEXT
            AND sect.country::TEXT = DIM_EMPLOYEE_ICONNECT.country_code::TEXT
        WHERE DIM_EMPLOYEE_ICONNECT.active_flag = 1::NUMERIC::NUMERIC(18, 0)
            AND (
                DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_Sales'::TEXT
                OR DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_MY_Sales'::TEXT
                )
        GROUP BY DIM_EMPLOYEE_ICONNECT.country_code,
            sect.sector
        ) emp1 ON src1.country::TEXT = emp1.country_code::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(emp1.sector, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(1) AS total_cnt_clm_flg,
            fct.employee_key,
            dimdate.jnj_date_year AS yr,
            dimdate.jnj_date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        WHERE fct.call_clm_flag = 1
            AND fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
        GROUP BY fct.employee_key,
            dimdate.jnj_date_year,
            dimdate.jnj_date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct1 ON src1.employee_key::TEXT = fct1.employee_key::TEXT
        AND src1.jnj_date_year = fct1.yr
        AND src1.jnj_date_month::TEXT = fct1.mnth::TEXT
        AND fct1.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct1.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct1.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(DISTINCT FCT.CALL_NAME) AS total_call_classification,
            fct.employee_key,
            dimdate.jnj_date_year AS yr,
            dimdate.jnj_date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name,
            PRDM.CLASSIFICATION_TYPE
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        LEFT JOIN DIM_HCP HCP ON HCP.HCP_KEY = fct.HCP_KEY
        LEFT JOIN itg_PRODUCT_METRICS PRDM ON HCP.HCP_SOURCE_ID = PRDM.ACCOUNT_SOURCE_ID
        LEFT JOIN DIM_PRODUCT_INDICATION PROD ON PROD.PRODUCT_SOURCE_ID = PRDM.PRODUCT_SOURCE_ID
            AND PROD.PRODUCT_INDICATION_KEY = fct.PRODUCT_INDICATION_KEY
        WHERE PRDM.CLASSIFICATION_TYPE IS NOT NULL
            OR PRDM.CLASSIFICATION_TYPE NOT IN ('', ' ')
        GROUP BY fct.employee_key,
            dimdate.jnj_date_year,
            dimdate.jnj_date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name,
            PRDM.CLASSIFICATION_TYPE
        ) fct2 ON src1.employee_key::TEXT = fct2.employee_key::TEXT
        AND src1.jnj_date_year = fct2.yr
        AND src1.jnj_date_month::TEXT = fct2.mnth::TEXT
        AND fct2.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct2.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct2.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct2.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT SUM(COALESCE(terr.sea_frml_hours_on, 0::NUMERIC::NUMERIC(18, 0))) / 8::NUMERIC::NUMERIC(18, 0) AS cnt_total_time_on,
            terr.employee_key,
            terr.country_key,
            date_dim.jnj_date_year AS yr,
            date_dim.jnj_date_month AS mnth
        FROM fact_timeoff_territory terr
        JOIN dim_date date_dim ON terr.start_date_key::TEXT = date_dim.date_key::TEXT
        WHERE terr.sea_time_on_time_off::TEXT = 'Time On'::TEXT
        GROUP BY terr.employee_key,
            terr.country_key,
            date_dim.jnj_date_year,
            date_dim.jnj_date_month
        ) terr ON terr.employee_key::TEXT = src1.employee_key::TEXT
        AND terr.country_key::TEXT = src1.country::TEXT
        AND src1.jnj_date_year = terr.yr
        AND src1.jnj_date_month::TEXT = terr.mnth::TEXT
    LEFT JOIN (
        SELECT SUM(COALESCE(terr.total_time_off, 0::NUMERIC::NUMERIC(18, 0))) / 8::NUMERIC::NUMERIC(18, 0) AS cnt_total_time_off,
            terr.employee_key,
            terr.country_key,
            date_dim.jnj_date_year AS yr,
            date_dim.jnj_date_month AS mnth
        FROM fact_timeoff_territory terr
        JOIN dim_date date_dim ON terr.start_date_key::TEXT = date_dim.date_key::TEXT
        WHERE terr.sea_time_on_time_off::TEXT = 'Time Off'::TEXT
        GROUP BY terr.employee_key,
            terr.country_key,
            date_dim.jnj_date_year,
            date_dim.jnj_date_month
        ) terr1 ON terr1.employee_key::TEXT = src1.employee_key::TEXT
        AND terr1.country_key::TEXT = src1.country::TEXT
        AND src1.jnj_date_year = terr1.yr
        AND src1.jnj_date_month::TEXT = terr1.mnth::TEXT
    LEFT JOIN (
        SELECT COUNT(DISTINCT fct.call_name) AS total_sbmtd_calls_key_message,
            fct.employee_key,
            dimdate.jnj_date_year AS yr,
            dimdate.jnj_date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN fact_call_key_message fckm ON fct.call_source_id::TEXT = fckm.call_source_id::TEXT
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        WHERE fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
        GROUP BY fct.employee_key,
            dimdate.jnj_date_year,
            dimdate.jnj_date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct3 ON src1.employee_key::TEXT = fct3.employee_key::TEXT
        AND src1.jnj_date_year = fct3.yr
        AND src1.jnj_date_month::TEXT = fct3.mnth::TEXT
        AND fct3.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct3.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct3.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct3.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(1) AS total_call_product,
            fct.employee_key,
            dimdate.jnj_date_year AS yr,
            dimdate.jnj_date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        WHERE fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
            AND (
                fct.detailed_products IS NOT NULL
                OR fct.detailed_products::TEXT <> ''::TEXT
                AND fct.detailed_products::TEXT <> ' '::TEXT
                )
        GROUP BY fct.employee_key,
            dimdate.jnj_date_year,
            dimdate.jnj_date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct4 ON src1.employee_key::TEXT = fct4.employee_key::TEXT
        AND src1.jnj_date_year = fct4.yr
        AND src1.jnj_date_month::TEXT = fct4.mnth::TEXT
        AND fct4.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct4.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct4.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct4.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(*) AS total_key_message,
            fckm.employee_key,
            dimdate.jnj_date_year AS yr,
            dimdate.jnj_date_month AS mnth,
            fckm.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN fact_call_key_message fckm ON fct.call_source_id::TEXT = fckm.call_source_id::TEXT
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        WHERE fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
        GROUP BY fckm.employee_key,
            dimdate.jnj_date_year,
            dimdate.jnj_date_month,
            fckm.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct5 ON rtrim(src1.employee_key)::TEXT = rtrim(fct5.employee_key)::TEXT
        AND src1.jnj_date_year = fct5.yr
        AND src1.jnj_date_month::TEXT = fct5.mnth::TEXT
        AND fct5.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct5.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct5.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct5.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(fct_detl.call_detail_source_id) AS total_detailing,
            employee.employee_key,
            dimdate.jnj_date_year AS yr,
            dimdate.jnj_date_month AS mnth,
            fct.country_code AS country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM itg_call_detail fct_detl
        JOIN itg_call fct ON fct_detl.call_source_id = fct.call_source_id
            AND fct.is_parent_call = 1
            AND fct.is_deleted = 0
            AND fct.CALL_STATUS_TYPE = 'Submitted_vod'
        JOIN dim_date dimdate ON TO_CHAR(fct.CALL_DATE, 'yyyymmdd') = dimdate.date_key
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.owner_source_id::TEXT = employee.employee_source_id::TEXT
        JOIN vw_employee_hier hier ON employee.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_code::TEXT = hier.country_code::TEXT
        WHERE fct_detl.is_parent_call = 1
            AND fct_detl.is_deleted = 0
        GROUP BY employee.employee_key,
            dimdate.jnj_date_year,
            dimdate.jnj_date_month,
            fct.country_code,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct6 ON src1.employee_key::TEXT = fct6.employee_key::TEXT
        AND src1.jnj_date_year = fct6.yr
        AND src1.jnj_date_month::TEXT = fct6.mnth::TEXT
        AND fct6.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct6.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct6.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct6.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT SUM(fct.call_submission_day) AS total_cnt_call_delay,
            employee.employee_key,
            dimdate.jnj_date_year AS yr,
            dimdate.jnj_date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        WHERE fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
            AND fct.country_key::TEXT <> 'ZZ'::TEXT
        GROUP BY employee.employee_key,
            dimdate.jnj_date_year,
            dimdate.jnj_date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct7 ON src1.employee_key::TEXT = fct7.employee_key::TEXT
        AND src1.jnj_date_year = fct7.yr
        AND src1.jnj_date_month::TEXT = fct7.mnth::TEXT
        AND fct7.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct7.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct7.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct7.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    ),
T3
AS (
    SELECT DISTINCT NULL AS jnj_date_year,
        NULL AS jnj_date_month,
        NULL AS jnj_date_quarter,
        NULL AS date_year,
        NULL AS date_month,
        NULL AS date_quarter,
        src1.my_date_year::CHARACTER VARYING AS my_year,
        trim(src1.my_date_month)::CHARACTER VARYING AS my_month,
        src1.my_date_quarter::CHARACTER VARYING AS my_quarter,
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
        src1.email_id,
        src1.working_days,
        src1.hcp_name,
        src1.hcp_source_id,
        src1.hcp_speciality,
        src1.business_account_id,
        src1.business_account,
        src1.total_calls,
        fct7.total_cnt_call_delay,
        fct1.total_cnt_clm_flg AS total_call_edetailing,
        CASE 
            WHEN ((src1.my_date_year || '-'::CHARACTER VARYING::TEXT) || src1.my_date_month::TEXT) = (
                    (
                        date_part (
                            year,
                            sysdate()
                            )::CHARACTER VARYING(5)::TEXT || '-'::CHARACTER VARYING::TEXT
                        ) || date_part (
                        month,
                        sysdate()
                        )::CHARACTER VARYING(5)::TEXT
                    )
                THEN emp1.current_mnth_emp_cnt
            ELSE actve_usr.total_active
            END AS total_active,
        fct4.total_call_product AS detailed_products,
        fct3.total_sbmtd_calls_key_message,
        fct5.total_key_message,
        fct2.total_call_classification,
        fct2.classification_type,
        CASE 
            WHEN fct2.classification_type = 'A'
                THEN (fct2.total_call_classification)
            END total_call_classification_a,
        CASE 
            WHEN fct2.classification_type = 'B'
                THEN (fct2.total_call_classification)
            END total_call_classification_b,
        CASE 
            WHEN fct2.classification_type = 'C'
                THEN (fct2.total_call_classification)
            END total_call_classification_c,
        CASE 
            WHEN fct2.classification_type = 'D'
                THEN (fct2.total_call_classification)
            END total_call_classification_d,
        CASE 
            WHEN fct2.classification_type = 'U'
                THEN (fct2.total_call_classification)
            END total_call_classification_u,
        CASE 
            WHEN fct2.classification_type = 'Z'
                THEN (fct2.total_call_classification)
            END total_call_classification_z,
        CASE 
            WHEN fct2.classification_type IS NULL
                THEN (fct2.total_call_classification)
            WHEN fct2.classification_type IN ('', ' ')
                THEN (fct2.total_call_classification)
            END total_call_classification_no_product,
        fct6.total_detailing
    FROM (
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
            fact.email_id,
            fact.working_days,
            fact.total_calls,
            fact.sales_rep_ntid,
            fact.employee_key,
            fact.hcp_name,
            fact.hcp_source_id,
            fact.speciality_1_type_english AS hcp_speciality,
            fact.hco_name AS business_account,
            fact.hco_source_id AS business_account_id
        FROM (
            SELECT dimdate.my_date_year,
                dimdate.my_date_month,
                dimdate.my_date_quarter,
                fact.country_key AS country,
                hier.sector,
                "max" (hier.l2_wwid::TEXT) AS l3_wwid,
                "max" (hier.l2_username) AS l3_username,
                hier.l2_manager_name AS l3_manager_name,
                "max" (hier.l3_wwid::TEXT) AS l2_wwid,
                "max" (hier.l3_username) AS l2_username,
                "max" (hier.l3_manager_name::TEXT) AS l2_manager_name,
                "max" (hier.l4_wwid::TEXT) AS l1_wwid,
                "max" (hier.l4_username) AS l1_username,
                "max" (hier.l4_manager_name::TEXT) AS l1_manager_name,
                "max" (employee.organization_l1_name::TEXT) AS organization_l1_name,
                "max" (employee.organization_l2_name::TEXT) AS organization_l2_name,
                employee.organization_l3_name,
                "max" (employee.organization_l4_name::TEXT) AS organization_l4_name,
                "max" (employee.organization_l5_name::TEXT) AS organization_l5_name,
                hier.employee_name AS sales_rep,
                employee.email_id,
                "max" (ds.working_days::NUMERIC::NUMERIC(18, 0)) AS working_days,
                COUNT(*) AS total_calls,
                fact.employee_key,
                hier.l1_username AS sales_rep_ntid,
                dim_hcp.hcp_name,
                dim_hcp.hcp_source_id,
                dim_hcp.speciality_1_type_english,
                dim_hco.hco_name,
                dim_hco.hco_source_id
            FROM (
                SELECT country_key,
                    hcp_key,
                    hco_key,
                    employee_key,
                    profile_key,
                    organization_key,
                    call_date_key,
                    parent_call_name
                FROM fact_call_detail
                WHERE call_status_type::TEXT = 'Submitted'::TEXT
                    AND country_key::TEXT <> 'ZZ'::TEXT
                GROUP BY country_key,
                    hcp_key,
                    hco_key,
                    employee_key,
                    profile_key,
                    organization_key,
                    call_date_key,
                    attendee_type,
                    call_clm_flag,
                    parent_call_name
                ) fact
            JOIN dim_date dimdate ON fact.call_date_key = dimdate.date_key
                AND my_date_year BETWEEN (DATE_PART(year,sysdate()) - 2)
                AND DATE_PART(year,sysdate())
            JOIN vw_employee_hier hier ON fact.employee_key::TEXT = hier.employee_key::TEXT
                AND fact.country_key::TEXT = hier.country_code::TEXT
            LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fact.employee_key::TEXT = employee.employee_key::TEXT
            JOIN dim_hco dim_hco ON fact.hco_key::TEXT = dim_hco.hco_key::TEXT
            JOIN dim_hcp dim_hcp ON fact.hcp_key::TEXT = dim_hcp.hcp_key::TEXT
            LEFT JOIN (
                SELECT 'SG' AS country,
                    ds.my_date_year,
                    ds.my_date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'SG'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    my_date_year,
                    my_date_month
                
                UNION ALL
                
                SELECT 'MY' AS country,
                    ds.my_date_year,
                    ds.my_date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'MY'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    my_date_year,
                    my_date_month
                
                UNION ALL
                
                SELECT 'VN' AS country,
                    ds.my_date_year,
                    ds.my_date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'VN'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    my_date_year,
                    my_date_month
                
                UNION ALL
                
                SELECT 'TH' AS country,
                    ds.my_date_year,
                    ds.my_date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'TH'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    my_date_year,
                    my_date_month
                
                UNION ALL
                
                SELECT 'PH' AS country,
                    ds.my_date_year,
                    ds.my_date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'PH'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    my_date_year,
                    my_date_month
                
                UNION ALL
                
                SELECT 'ID' AS country,
                    ds.my_date_year,
                    ds.my_date_month,
                    COUNT(*) AS working_days
                FROM dim_date ds
                WHERE date_key NOT IN (
                        SELECT DISTINCT holiday_key
                        FROM holiday_list
                        WHERE country = 'ID'
                        )
                    AND rtrim(date_dayofweek) NOT IN ('Saturday', 'Sunday')
                    AND date_key < TO_CHAR(sysdate(), 'YYYYMMDD')
                GROUP BY country,
                    my_date_year,
                    my_date_month
                ) ds ON dimdate.my_date_year::TEXT = ds.my_date_year::TEXT
                AND dimdate.my_date_month::TEXT = ds.my_date_month
                AND fact.country_key::TEXT = ds.country::TEXT
            GROUP BY dimdate.my_date_year,
                dimdate.my_date_month,
                dimdate.my_date_quarter,
                fact.country_key,
                hier.employee_name,
                employee.email_id,
                fact.employee_key,
                dim_hcp.hcp_name,
                dim_hcp.hcp_source_id,
                dim_hcp.speciality_1_type_english,
                dim_hco.hco_name,
                dim_hco.hco_source_id,
                hier.l1_username,
                hier.sector,
                hier.l2_manager_name,
                employee.organization_l3_name
            ) fact
        ) src1
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
        ) actve_usr ON src1.my_date_year = actve_usr.year
        AND src1.my_date_month::TEXT = actve_usr.month::TEXT
        AND src1.country::TEXT = actve_usr.country_code::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(actve_usr.sector, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(1) AS current_mnth_emp_cnt,
            DIM_EMPLOYEE_ICONNECT.country_code,
            sect.sector
        FROM DIM_EMPLOYEE_ICONNECT
        LEFT JOIN edw_isight_sector_mapping sect ON sect.company::TEXT = DIM_EMPLOYEE_ICONNECT.company_name::TEXT
            AND sect.country::TEXT = DIM_EMPLOYEE_ICONNECT.country_code::TEXT
        WHERE DIM_EMPLOYEE_ICONNECT.active_flag = 1::NUMERIC::NUMERIC(18, 0)
            AND (
                DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_Sales'::TEXT
                OR DIM_EMPLOYEE_ICONNECT.profile_name::TEXT = 'AP_Core_MY_Sales'::TEXT
                )
        GROUP BY DIM_EMPLOYEE_ICONNECT.country_code,
            sect.sector
        ) emp1 ON src1.country::TEXT = emp1.country_code::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(emp1.sector, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(1) AS total_cnt_clm_flg,
            fct.employee_key,
            dimdate.my_date_year AS yr,
            dimdate.my_date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        WHERE fct.call_clm_flag = 1
            AND fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
        GROUP BY fct.employee_key,
            dimdate.my_date_year,
            dimdate.my_date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct1 ON src1.employee_key::TEXT = fct1.employee_key::TEXT
        AND src1.my_date_year = fct1.yr
        AND src1.my_date_month::TEXT = fct1.mnth::TEXT
        AND fct1.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct1.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct1.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(DISTINCT FCT.CALL_NAME) AS total_call_classification,
            fct.employee_key,
            dimdate.my_date_year AS yr,
            dimdate.my_date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name,
            PRDM.CLASSIFICATION_TYPE
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        LEFT JOIN DIM_HCP HCP ON HCP.HCP_KEY = fct.HCP_KEY
        LEFT JOIN DIM_HCO HCO ON HCO.HCO_KEY = fct.HCO_KEY
        LEFT JOIN itg_PRODUCT_METRICS PRDM ON HCP.HCP_SOURCE_ID = PRDM.ACCOUNT_SOURCE_ID
        LEFT JOIN DIM_PRODUCT_INDICATION PROD ON PROD.PRODUCT_SOURCE_ID = PRDM.PRODUCT_SOURCE_ID
            AND PROD.PRODUCT_INDICATION_KEY = fct.PRODUCT_INDICATION_KEY
        WHERE PRDM.CLASSIFICATION_TYPE IS NOT NULL
            OR PRDM.CLASSIFICATION_TYPE NOT IN ('', ' ')
        GROUP BY fct.employee_key,
            dimdate.my_date_year,
            dimdate.my_date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name,
            PRDM.CLASSIFICATION_TYPE
        ) fct2 ON src1.employee_key::TEXT = fct2.employee_key::TEXT
        AND src1.my_date_year = fct2.yr
        AND src1.my_date_month::TEXT = fct2.mnth::TEXT
        AND fct2.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct2.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct2.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct2.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT SUM(COALESCE(terr.sea_frml_hours_on, 0::NUMERIC::NUMERIC(18, 0))) / 8::NUMERIC::NUMERIC(18, 0) AS cnt_total_time_on,
            terr.employee_key,
            terr.country_key,
            date_dim.my_date_year AS yr,
            date_dim.my_date_month AS mnth
        FROM fact_timeoff_territory terr
        JOIN dim_date date_dim ON terr.start_date_key::TEXT = date_dim.date_key::TEXT
        WHERE terr.sea_time_on_time_off::TEXT = 'Time On'::TEXT
        GROUP BY terr.employee_key,
            terr.country_key,
            date_dim.my_date_year,
            date_dim.my_date_month
        ) terr ON terr.employee_key::TEXT = src1.employee_key::TEXT
        AND terr.country_key::TEXT = src1.country::TEXT
        AND src1.my_date_year = terr.yr
        AND src1.my_date_month::TEXT = terr.mnth::TEXT
    LEFT JOIN (
        SELECT SUM(COALESCE(terr.total_time_off, 0::NUMERIC::NUMERIC(18, 0))) / 8::NUMERIC::NUMERIC(18, 0) AS cnt_total_time_off,
            terr.employee_key,
            terr.country_key,
            date_dim.my_date_year AS yr,
            date_dim.my_date_month AS mnth
        FROM fact_timeoff_territory terr
        JOIN dim_date date_dim ON terr.start_date_key::TEXT = date_dim.date_key::TEXT
        WHERE terr.sea_time_on_time_off::TEXT = 'Time Off'::TEXT
        GROUP BY terr.employee_key,
            terr.country_key,
            date_dim.my_date_year,
            date_dim.my_date_month
        ) terr1 ON terr1.employee_key::TEXT = src1.employee_key::TEXT
        AND terr1.country_key::TEXT = src1.country::TEXT
        AND src1.my_date_year = terr1.yr
        AND src1.my_date_month::TEXT = terr1.mnth::TEXT
    LEFT JOIN (
        SELECT COUNT(DISTINCT fct.call_name) AS total_sbmtd_calls_key_message,
            fct.employee_key,
            dimdate.my_date_year AS yr,
            dimdate.my_date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN fact_call_key_message fckm ON fct.call_source_id::TEXT = fckm.call_source_id::TEXT
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        WHERE fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
        GROUP BY fct.employee_key,
            dimdate.my_date_year,
            dimdate.my_date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct3 ON src1.employee_key::TEXT = fct3.employee_key::TEXT
        AND src1.my_date_year = fct3.yr
        AND src1.my_date_month::TEXT = fct3.mnth::TEXT
        AND fct3.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct3.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct3.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct3.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(1) AS total_call_product,
            fct.employee_key,
            dimdate.my_date_year AS yr,
            dimdate.my_date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        WHERE fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
            AND (
                fct.detailed_products IS NOT NULL
                OR fct.detailed_products::TEXT <> ''::TEXT
                AND fct.detailed_products::TEXT <> ' '::TEXT
                )
        GROUP BY fct.employee_key,
            dimdate.my_date_year,
            dimdate.my_date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct4 ON src1.employee_key::TEXT = fct4.employee_key::TEXT
        AND src1.my_date_year = fct4.yr
        AND src1.my_date_month::TEXT = fct4.mnth::TEXT
        AND fct4.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct4.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct4.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct4.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(*) AS total_key_message,
            fckm.employee_key,
            dimdate.my_date_year AS yr,
            dimdate.my_date_month AS mnth,
            fckm.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        JOIN fact_call_key_message fckm ON fct.call_source_id::TEXT = fckm.call_source_id::TEXT
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        WHERE fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
        GROUP BY fckm.employee_key,
            dimdate.my_date_year,
            dimdate.my_date_month,
            fckm.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct5 ON src1.employee_key::TEXT = fct5.employee_key::TEXT
        AND src1.my_date_year = fct5.yr
        AND src1.my_date_month::TEXT = fct5.mnth::TEXT
        AND fct5.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct5.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct5.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct5.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT COUNT(fct_detl.call_detail_source_id) AS total_detailing,
            employee.employee_key,
            dimdate.my_date_year AS yr,
            dimdate.my_date_month AS mnth,
            fct.country_code AS country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM itg_call_detail fct_detl
        JOIN itg_call fct ON fct_detl.call_source_id = fct.call_source_id
            AND fct.is_parent_call = 1
            AND fct.is_deleted = 0
            AND fct.CALL_STATUS_TYPE = 'Submitted_vod'
        JOIN dim_date dimdate ON TO_CHAR(fct.CALL_DATE, 'yyyymmdd') = dimdate.date_key
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.owner_source_id::TEXT = employee.employee_source_id::TEXT
        JOIN vw_employee_hier hier ON employee.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_code::TEXT = hier.country_code::TEXT
        WHERE fct_detl.is_parent_call = 1
            AND fct_detl.is_deleted = 0
        GROUP BY employee.employee_key,
            dimdate.my_date_year,
            dimdate.my_date_month,
            fct.country_code,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct6 ON src1.employee_key::TEXT = fct6.employee_key::TEXT
        AND src1.my_date_year = fct6.yr
        AND src1.my_date_month::TEXT = fct6.mnth::TEXT
        AND fct6.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct6.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct6.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct6.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
    LEFT JOIN (
        SELECT SUM(fct.call_submission_day) AS total_cnt_call_delay,
            employee.employee_key,
            dimdate.my_date_year AS yr,
            dimdate.my_date_month AS mnth,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        FROM fact_call_detail fct
        JOIN dim_date dimdate ON fct.call_date_key = dimdate.date_key
        LEFT JOIN DIM_EMPLOYEE_ICONNECT employee ON fct.employee_key::TEXT = employee.employee_key::TEXT
        JOIN vw_employee_hier hier ON fct.employee_key::TEXT = hier.employee_key::TEXT
            AND fct.country_key::TEXT = hier.country_code::TEXT
        WHERE fct.parent_call_flag = 1
            AND fct.call_status_type::TEXT = 'Submitted'::TEXT
            AND fct.country_key::TEXT <> 'ZZ'::TEXT
        GROUP BY employee.employee_key,
            dimdate.my_date_year,
            dimdate.my_date_month,
            fct.country_key,
            hier.l2_manager_name,
            hier.sector,
            employee.organization_l3_name
        ) fct7 ON src1.employee_key::TEXT = fct7.employee_key::TEXT
        AND src1.my_date_year = fct7.yr
        AND src1.my_date_month::TEXT = fct7.mnth::TEXT
        AND fct7.country_key::TEXT = src1.country::TEXT
        AND COALESCE(src1.sector, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct7.sector, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.organization_l3_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct7.organization_l3_name, '#'::CHARACTER VARYING)::TEXT
        AND COALESCE(src1.l3_manager_name, '#'::CHARACTER VARYING)::TEXT = COALESCE(fct7.l2_manager_name, '#'::CHARACTER VARYING)::TEXT
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
        COUNTRY::VARCHAR(18) AS COUNTRY,
        SECTOR::VARCHAR(256) AS SECTOR,
        L3_WWID::VARCHAR(20) AS L3_WWID,
        L3_USERNAME::VARCHAR(80) AS L3_USERNAME,
        L3_MANAGER_NAME::VARCHAR(121) AS L3_MANAGER_NAME,
        L2_WWID::VARCHAR(20) AS L2_WWID,
        L2_USERNAME::VARCHAR(80) AS L2_USERNAME,
        L2_MANAGER_NAME::VARCHAR(121) AS L2_MANAGER_NAME,
        L1_WWID::VARCHAR(20) AS L1_WWID,
        L1_USERNAME::VARCHAR(80) AS L1_USERNAME,
        L1_MANAGER_NAME::VARCHAR(121) AS L1_MANAGER_NAME,
        ORGANIZATION_L1_NAME::VARCHAR(80) AS ORGANIZATION_L1_NAME,
        ORGANIZATION_L2_NAME::VARCHAR(80) AS ORGANIZATION_L2_NAME,
        ORGANIZATION_L3_NAME::VARCHAR(80) AS ORGANIZATION_L3_NAME,
        ORGANIZATION_L4_NAME::VARCHAR(80) AS ORGANIZATION_L4_NAME,
        ORGANIZATION_L5_NAME::VARCHAR(80) AS ORGANIZATION_L5_NAME,
        SALES_REP_NTID::VARCHAR(80) AS SALES_REP_NTID,
        SALES_REP::VARCHAR(121) AS SALES_REP,
        EMAIL_ID::VARCHAR(128) AS EMAIL_ID,
        WORKING_DAYS::NUMBER(18,0) AS WORKING_DAYS,
        HCP_NAME::VARCHAR(255) AS HCP_NAME,
        HCP_SOURCE_ID::VARCHAR(18) AS HCP_SOURCE_ID,
        HCP_SPECIALITY::VARCHAR(255) AS HCP_SPECIALITY,
        BUSINESS_ACCOUNT_ID::VARCHAR(18) AS BUSINESS_ACCOUNT_ID,
        BUSINESS_ACCOUNT::VARCHAR(1300) AS BUSINESS_ACCOUNT,
        TOTAL_CALLS::NUMBER(38,0) AS TOTAL_CALLS,
        TOTAL_CNT_CALL_DELAY::NUMBER(38,1) AS TOTAL_CNT_CALL_DELAY,
        TOTAL_CALL_EDETAILING::NUMBER(38,0) AS TOTAL_CALL_EDETAILING,
        TOTAL_ACTIVE::NUMBER(38,0) AS TOTAL_ACTIVE,
        DETAILED_PRODUCTS::NUMBER(38,0) AS DETAILED_PRODUCTS,
        TOTAL_SBMTD_CALLS_KEY_MESSAGE::NUMBER(38,0) AS TOTAL_SBMTD_CALLS_KEY_MESSAGE,
        TOTAL_KEY_MESSAGE::NUMBER(38,0) AS TOTAL_KEY_MESSAGE,
        TOTAL_CALL_CLASSIFICATION::NUMBER(38,0) AS TOTAL_CALL_CLASSIFICATION,
        CLASSIFICATION_TYPE::VARCHAR(255) AS CLASSIFICATION_TYPE,
        TOTAL_CALL_CLASSIFICATION_A::NUMBER(38,0) AS TOTAL_CALL_CLASSIFICATION_A,
        TOTAL_CALL_CLASSIFICATION_B::NUMBER(38,0) AS TOTAL_CALL_CLASSIFICATION_B,
        TOTAL_CALL_CLASSIFICATION_C::NUMBER(38,0) AS TOTAL_CALL_CLASSIFICATION_C,
        TOTAL_CALL_CLASSIFICATION_D::NUMBER(38,0) AS TOTAL_CALL_CLASSIFICATION_D,
        TOTAL_CALL_CLASSIFICATION_U::NUMBER(38,0) AS TOTAL_CALL_CLASSIFICATION_U,
        TOTAL_CALL_CLASSIFICATION_Z::NUMBER(38,0) AS TOTAL_CALL_CLASSIFICATION_Z,
        TOTAL_CALL_CLASSIFICATION_NO_PRODUCT::NUMBER(38,0) AS TOTAL_CALL_CLASSIFICATION_NO_PRODUCT,
        TOTAL_DETAILING::NUMBER(38,0) AS TOTAL_DETAILING
    FROM UNION_OF
    )
SELECT *
FROM FINAL
