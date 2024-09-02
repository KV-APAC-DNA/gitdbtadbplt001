with fact_timeoff_territory as (
    select * from {{ ref('hcposeedw_integration__fact_timeoff_territory') }}
),

vw_employee_hier as (
    select * from {{ ref('hcposeedw_integration__vw_employee_hier') }}
),

dim_employee as (
    select * from {{ ref('hcposeedw_integration__dim_employee') }}
),

dim_date as (
    select * from {{ ref('hcposeedw_integration__dim_date') }}
),

holiday_list as (
    select * from {{ ref('hcposeedw_integration__holiday_list') }}
),

cte1 as (
SELECT src1.country,
            src1.time_on_off,
            src1.DATE,
            src1."year",
            src1."month",
            src1.quarter,
            NULL AS jnj_year,
            NULL AS jnj_month,
            NULL AS jnj_quarter,
            NULL AS my_year,
            NULL AS my_month,
            NULL AS my_quarter,
            CASE 
                WHEN ((src1.time_on_off)::TEXT = ('Time Off'::CHARACTER VARYING)::TEXT)
                    THEN CASE 
                            WHEN (src1.reason_type IS NULL)
                                THEN src1.sm_reason
                            WHEN (src1.sm_reason IS NULL)
                                THEN src1.reason_type
                            ELSE NULL::CHARACTER VARYING
                            END
                WHEN ((src1.time_on_off)::TEXT = ('Time On'::CHARACTER VARYING)::TEXT)
                    THEN CASE 
                            WHEN (
                                    (src1.sm_reason IS NULL)
                                    AND (src1.time_on IS NOT NULL)
                                    )
                                THEN src1.time_on
                            WHEN (
                                    (src1.time_on IS NULL)
                                    AND (src1.sm_reason IS NOT NULL)
                                    )
                                THEN src1.sm_reason
                            WHEN (
                                    (src1.sm_reason IS NULL)
                                    AND (src1.time_on IS NULL)
                                    )
                                THEN src1.reason_type
                            ELSE NULL::CHARACTER VARYING
                            END
                ELSE NULL::CHARACTER VARYING
                END AS reason,
            src1.hours_off,
            src1.hours_on,
            src1.working_days,
            src1.duration,
            src1.l3_wwid,
            (src1.l3_username)::CHARACTER VARYING AS l3_username,
            src1.l3_manager_name,
            src1.l2_wwid,
            (src1.l2_username)::CHARACTER VARYING AS l2_username,
            src1.l2_manager_name,
            src1.l1_wwid,
            (src1.l1_username)::CHARACTER VARYING AS l1_username,
            src1.l1_manager_name,
            (src1.sales_rep_ntid)::CHARACTER VARYING AS sales_rep_ntid,
            src1.sales_rep,
            src1.organization_l1_name,
            src1.organization_l2_name,
            src1.organization_l3_name,
            src1.organization_l4_name,
            src1.organization_l5_name,
            src1.flag
        FROM (
            SELECT fact.country_key AS country,
                CASE 
                    WHEN (
                            (fact.sea_time_on_time_off IS NULL)
                            AND (fact.sea_frml_hours_on > ((0)::NUMERIC)::NUMERIC(18, 0))
                            )
                        THEN 'Time On'::CHARACTER VARYING
                    ELSE fact.sea_time_on_time_off
                    END AS time_on_off,
                    to_date(to_char(date_dim.date_key),'YYYYMMDD') as date,
                -- ((date_dim.date_key)::CHARACTER VARYING)::DATE AS DATE,
                date_dim.date_year AS "year",
                date_dim.date_month AS "month",
                date_dim.date_quarter AS quarter,
                fact.total_time_off AS hours_off,
                fact.sea_frml_hours_on AS hours_on,
                ds.working_days AS working_days,
                CASE 
                    WHEN ((fact.sea_time_on_time_off)::TEXT = ('Time Off'::CHARACTER VARYING)::TEXT)
                        THEN (COALESCE(fact.total_time_off, ((0)::NUMERIC)::NUMERIC(18, 0)) / ((8)::NUMERIC)::NUMERIC(18, 0))
                    WHEN (
                            ((fact.sea_time_on_time_off)::TEXT = ('Time On'::CHARACTER VARYING)::TEXT)
                            OR (fact.sea_frml_hours_on > ((0)::NUMERIC)::NUMERIC(18, 0))
                            )
                        THEN (COALESCE(fact.sea_frml_hours_on, ((0)::NUMERIC)::NUMERIC(18, 0)) / ((8)::NUMERIC)::NUMERIC(18, 0))
                    ELSE (NULL::NUMERIC)::NUMERIC(18, 0)
                    END AS duration,
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
                    WHEN ((hier.employee_name)::TEXT = (''::CHARACTER VARYING)::TEXT)
                        THEN dimemp.employee_name
                    ELSE hier.employee_name
                    END AS sales_rep,
                dimemp.organization_l1_name,
                dimemp.organization_l2_name,
                dimemp.organization_l3_name,
                dimemp.organization_l4_name,
                dimemp.organization_l5_name,
                fact.sm_reason,
                fact.time_on,
                fact.reason_type,
                count(DISTINCT date_dim1.date_month) AS flag,
                fact.time_type
            FROM (
                (
                    (
                        (
                            (
                                fact_timeoff_territory fact JOIN vw_employee_hier hier ON (
                                        (
                                            ((fact.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                            AND ((fact.country_key)::TEXT = (hier.country_code)::TEXT)
                                            )
                                        )
                                ) LEFT JOIN dim_employee dimemp ON (((fact.employee_key)::TEXT = (dimemp.employee_key)::TEXT))
                            ) LEFT JOIN dim_date date_dim ON (((fact.start_date_key)::TEXT = ((date_dim.date_key)::CHARACTER VARYING)::TEXT))
                        ) LEFT JOIN dim_date date_dim1 ON (
                            (
                                (((date_dim1.date_key)::CHARACTER VARYING)::TEXT >= (fact.start_date_key)::TEXT)
                                AND (((date_dim1.date_key)::CHARACTER VARYING)::TEXT <= (fact.end_date_key)::TEXT)
                                )
                            )
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
                                            AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                                            AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                                        AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                                    AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                                AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                            AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
                            )
                    GROUP BY 1,
                        ds.date_year,
                        ds.date_month
                    ) ds ON (
                        (
                            (
                                (((date_dim.date_year)::CHARACTER VARYING)::TEXT = ((ds.date_year)::CHARACTER VARYING)::TEXT)
                                AND ((date_dim.date_month)::TEXT = (ds.date_month)::TEXT)
                                )
                            AND ((fact.country_key)::TEXT = (ds.country)::TEXT)
                            )
                        )
                )
            WHERE (
                    ((fact.country_key)::TEXT <> ('ZZ'::CHARACTER VARYING)::TEXT)
                    AND ((fact.start_date_key)::TEXT < to_char((current_date()::CHARACTER VARYING)::TIMESTAMP without TIME zone, ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                    )
            GROUP BY fact.country_key,
                date_dim.date_key,
                date_dim.date_year,
                date_dim.date_month,
                date_dim.date_quarter,
                fact.total_time_off,
                fact.sea_frml_hours_on,
                ds.working_days,
                fact.sea_time_on_time_off,
                hier.l2_wwid,
                hier.l2_username,
                hier.l2_manager_name,
                hier.l3_wwid,
                hier.l3_username,
                hier.l3_manager_name,
                hier.l4_wwid,
                hier.l4_username,
                hier.l4_manager_name,
                hier.l1_username,
                hier.employee_name,
                dimemp.employee_name,
                dimemp.organization_l1_name,
                dimemp.organization_l2_name,
                dimemp.organization_l3_name,
                dimemp.organization_l4_name,
                dimemp.organization_l5_name,
                fact.sm_reason,
                fact.time_on,
                fact.reason_type,
                fact.time_type
            ) src1
),

cte2 as (
    SELECT src1.country,
            src1.time_on_off,
            src1.DATE,
            NULL AS "year",
            NULL AS "month",
            NULL AS quarter,
            src1.jnj_date_year AS jnj_year,
            src1.jnj_date_month AS jnj_month,
            src1.jnj_date_quarter AS jnj_quarter,
            NULL AS my_year,
            NULL AS my_month,
            NULL AS my_quarter,
            CASE 
                WHEN ((src1.time_on_off)::TEXT = ('Time Off'::CHARACTER VARYING)::TEXT)
                    THEN CASE 
                            WHEN (src1.reason_type IS NULL)
                                THEN src1.sm_reason
                            WHEN (src1.sm_reason IS NULL)
                                THEN src1.reason_type
                            ELSE NULL::CHARACTER VARYING
                            END
                WHEN ((src1.time_on_off)::TEXT = ('Time On'::CHARACTER VARYING)::TEXT)
                    THEN CASE 
                            WHEN (
                                    (src1.sm_reason IS NULL)
                                    AND (src1.time_on IS NOT NULL)
                                    )
                                THEN src1.time_on
                            WHEN (
                                    (src1.time_on IS NULL)
                                    AND (src1.sm_reason IS NOT NULL)
                                    )
                                THEN src1.sm_reason
                            WHEN (
                                    (src1.sm_reason IS NULL)
                                    AND (src1.time_on IS NULL)
                                    )
                                THEN src1.reason_type
                            ELSE NULL::CHARACTER VARYING
                            END
                ELSE NULL::CHARACTER VARYING
                END AS reason,
            src1.hours_off,
            src1.hours_on,
            src1.working_days,
            src1.duration,
            src1.l3_wwid,
            (src1.l3_username)::CHARACTER VARYING AS l3_username,
            src1.l3_manager_name,
            src1.l2_wwid,
            (src1.l2_username)::CHARACTER VARYING AS l2_username,
            src1.l2_manager_name,
            src1.l1_wwid,
            (src1.l1_username)::CHARACTER VARYING AS l1_username,
            src1.l1_manager_name,
            (src1.sales_rep_ntid)::CHARACTER VARYING AS sales_rep_ntid,
            src1.sales_rep,
            src1.organization_l1_name,
            src1.organization_l2_name,
            src1.organization_l3_name,
            src1.organization_l4_name,
            src1.organization_l5_name,
            src1.flag
        FROM (
            SELECT fact.country_key AS country,
                CASE 
                    WHEN (
                            (fact.sea_time_on_time_off IS NULL)
                            AND (fact.sea_frml_hours_on > ((0)::NUMERIC)::NUMERIC(18, 0))
                            )
                        THEN 'Time On'::CHARACTER VARYING
                    ELSE fact.sea_time_on_time_off
                    END AS time_on_off,
                    to_date(to_char(date_dim.date_key),'YYYYMMDD') as date,
                -- ((date_dim.date_key)::CHARACTER VARYING)::DATE AS DATE,
                date_dim.jnj_date_year,
                date_dim.jnj_date_month,
                date_dim.jnj_date_quarter,
                fact.total_time_off AS hours_off,
                fact.sea_frml_hours_on AS hours_on,
                ds.working_days AS working_days,
                CASE 
                    WHEN ((fact.sea_time_on_time_off)::TEXT = ('Time Off'::CHARACTER VARYING)::TEXT)
                        THEN (COALESCE(fact.total_time_off, ((0)::NUMERIC)::NUMERIC(18, 0)) / ((8)::NUMERIC)::NUMERIC(18, 0))
                    WHEN (
                            ((fact.sea_time_on_time_off)::TEXT = ('Time On'::CHARACTER VARYING)::TEXT)
                            OR (fact.sea_frml_hours_on > ((0)::NUMERIC)::NUMERIC(18, 0))
                            )
                        THEN (COALESCE(fact.sea_frml_hours_on, ((0)::NUMERIC)::NUMERIC(18, 0)) / ((8)::NUMERIC)::NUMERIC(18, 0))
                    ELSE (NULL::NUMERIC)::NUMERIC(18, 0)
                    END AS duration,
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
                    WHEN ((hier.employee_name)::TEXT = (''::CHARACTER VARYING)::TEXT)
                        THEN dimemp.employee_name
                    ELSE hier.employee_name
                    END AS sales_rep,
                dimemp.organization_l1_name,
                dimemp.organization_l2_name,
                dimemp.organization_l3_name,
                dimemp.organization_l4_name,
                dimemp.organization_l5_name,
                fact.sm_reason,
                fact.time_on,
                fact.reason_type,
                count(DISTINCT date_dim1.jnj_date_month) AS flag,
                fact.time_type
            FROM (
                (
                    (
                        (
                            (
                                fact_timeoff_territory fact JOIN vw_employee_hier hier ON (
                                        (
                                            ((fact.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                            AND ((fact.country_key)::TEXT = (hier.country_code)::TEXT)
                                            )
                                        )
                                ) LEFT JOIN dim_employee dimemp ON (((fact.employee_key)::TEXT = (dimemp.employee_key)::TEXT))
                            ) LEFT JOIN dim_date date_dim ON (((fact.start_date_key)::TEXT = ((date_dim.date_key)::CHARACTER VARYING)::TEXT))
                        ) LEFT JOIN dim_date date_dim1 ON (
                            (
                                (((date_dim1.date_key)::CHARACTER VARYING)::TEXT >= (fact.start_date_key)::TEXT)
                                AND (((date_dim1.date_key)::CHARACTER VARYING)::TEXT <= (fact.end_date_key)::TEXT)
                                )
                            )
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
                                            AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                                            AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                                        AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                                    AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                                AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
                                )
                        GROUP BY 1,
                            ds.jnj_date_year,
                            ds.jnj_date_month
                        )
                    
                    UNION ALL
                    
                    SELECT 'ID'::CHARACTER VARYING AS country,
                        ds.jnj_date_year,
                        ds.jnj_date_month,
                        count(*) AS count
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
                            AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
                            )
                    GROUP BY 1,
                        ds.jnj_date_year,
                        ds.jnj_date_month
                    ) ds ON (
                        (
                            (
                                (((date_dim.jnj_date_year)::CHARACTER VARYING)::TEXT = ((ds.jnj_date_year)::CHARACTER VARYING)::TEXT)
                                AND ((date_dim.jnj_date_month)::TEXT = (ds.jnj_date_month)::TEXT)
                                )
                            AND ((fact.country_key)::TEXT = (ds.country)::TEXT)
                            )
                        )
                )
            WHERE (
                    ((fact.country_key)::TEXT <> ('ZZ'::CHARACTER VARYING)::TEXT)
                    AND ((fact.start_date_key)::TEXT < to_char((current_date()::CHARACTER VARYING)::TIMESTAMP without TIME zone, ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
                    )
            GROUP BY fact.country_key,
                date_dim.date_key,
                date_dim.jnj_date_year,
                date_dim.jnj_date_month,
                date_dim.jnj_date_quarter,
                fact.total_time_off,
                fact.sea_frml_hours_on,
                ds.working_days,
                fact.sea_time_on_time_off,
                hier.l2_wwid,
                hier.l2_username,
                hier.l2_manager_name,
                hier.l3_wwid,
                hier.l3_username,
                hier.l3_manager_name,
                hier.l4_wwid,
                hier.l4_username,
                hier.l4_manager_name,
                hier.l1_username,
                hier.employee_name,
                dimemp.employee_name,
                dimemp.organization_l1_name,
                dimemp.organization_l2_name,
                dimemp.organization_l3_name,
                dimemp.organization_l4_name,
                dimemp.organization_l5_name,
                fact.sm_reason,
                fact.time_on,
                fact.reason_type,
                fact.time_type
            ) src1
),

table1 as (
    select * from cte1
    union all
    select * from cte2
),

cte3 as (
    SELECT src1.country,
    src1.time_on_off,
    src1.DATE,
    NULL AS "year",
    NULL AS "month",
    NULL AS quarter,
    NULL AS jnj_year,
    NULL AS jnj_month,
    NULL AS jnj_quarter,
    src1.my_date_year AS my_year,
    src1.my_date_month AS my_month,
    src1.my_date_quarter AS my_quarter,
    CASE 
        WHEN ((src1.time_on_off)::TEXT = ('Time Off'::CHARACTER VARYING)::TEXT)
            THEN CASE 
                    WHEN (src1.reason_type IS NULL)
                        THEN src1.sm_reason
                    WHEN (src1.sm_reason IS NULL)
                        THEN src1.reason_type
                    ELSE NULL::CHARACTER VARYING
                    END
        WHEN ((src1.time_on_off)::TEXT = ('Time On'::CHARACTER VARYING)::TEXT)
            THEN CASE 
                    WHEN (
                            (src1.sm_reason IS NULL)
                            AND (src1.time_on IS NOT NULL)
                            )
                        THEN src1.time_on
                    WHEN (
                            (src1.time_on IS NULL)
                            AND (src1.sm_reason IS NOT NULL)
                            )
                        THEN src1.sm_reason
                    WHEN (
                            (src1.sm_reason IS NULL)
                            AND (src1.time_on IS NULL)
                            )
                        THEN src1.reason_type
                    ELSE NULL::CHARACTER VARYING
                    END
        ELSE NULL::CHARACTER VARYING
        END AS reason,
    src1.hours_off,
    src1.hours_on,
    src1.working_days,
    src1.duration,
    src1.l3_wwid,
    (src1.l3_username)::CHARACTER VARYING AS l3_username,
    src1.l3_manager_name,
    src1.l2_wwid,
    (src1.l2_username)::CHARACTER VARYING AS l2_username,
    src1.l2_manager_name,
    src1.l1_wwid,
    (src1.l1_username)::CHARACTER VARYING AS l1_username,
    src1.l1_manager_name,
    (src1.sales_rep_ntid)::CHARACTER VARYING AS sales_rep_ntid,
    src1.sales_rep,
    src1.organization_l1_name,
    src1.organization_l2_name,
    src1.organization_l3_name,
    src1.organization_l4_name,
    src1.organization_l5_name,
    src1.flag
FROM (
    SELECT fact.country_key AS country,
        CASE 
            WHEN (
                    (fact.sea_time_on_time_off IS NULL)
                    AND (fact.sea_frml_hours_on > ((0)::NUMERIC)::NUMERIC(18, 0))
                    )
                THEN 'Time On'::CHARACTER VARYING
            ELSE fact.sea_time_on_time_off
            END AS time_on_off,
            to_date(to_char(date_dim.date_key),'YYYYMMDD') as date,
        -- ((date_dim.date_key)::CHARACTER VARYING)::DATE AS DATE,
        date_dim.my_date_year,
        date_dim.my_date_month,
        date_dim.my_date_quarter,
        fact.total_time_off AS hours_off,
        fact.sea_frml_hours_on AS hours_on,
        ds.working_days AS working_days,
        CASE 
            WHEN ((fact.sea_time_on_time_off)::TEXT = ('Time Off'::CHARACTER VARYING)::TEXT)
                THEN (COALESCE(fact.total_time_off, ((0)::NUMERIC)::NUMERIC(18, 0)) / ((8)::NUMERIC)::NUMERIC(18, 0))
            WHEN (
                    ((fact.sea_time_on_time_off)::TEXT = ('Time On'::CHARACTER VARYING)::TEXT)
                    OR (fact.sea_frml_hours_on > ((0)::NUMERIC)::NUMERIC(18, 0))
                    )
                THEN (COALESCE(fact.sea_frml_hours_on, ((0)::NUMERIC)::NUMERIC(18, 0)) / ((8)::NUMERIC)::NUMERIC(18, 0))
            ELSE (NULL::NUMERIC)::NUMERIC(18, 0)
            END AS duration,
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
            WHEN ((hier.employee_name)::TEXT = (''::CHARACTER VARYING)::TEXT)
                THEN dimemp.employee_name
            ELSE hier.employee_name
            END AS sales_rep,
        dimemp.organization_l1_name,
        dimemp.organization_l2_name,
        dimemp.organization_l3_name,
        dimemp.organization_l4_name,
        dimemp.organization_l5_name,
        fact.sm_reason,
        fact.time_on,
        fact.reason_type,
        count(DISTINCT date_dim1.my_date_month) AS flag,
        fact.time_type
    FROM (
        (
            (
                (
                    (
                        fact_timeoff_territory fact JOIN vw_employee_hier hier ON (
                                (
                                    ((fact.employee_key)::TEXT = (hier.employee_key)::TEXT)
                                    AND ((fact.country_key)::TEXT = (hier.country_code)::TEXT)
                                    )
                                )
                        ) LEFT JOIN dim_employee dimemp ON (((fact.employee_key)::TEXT = (dimemp.employee_key)::TEXT))
                    ) LEFT JOIN dim_date date_dim ON (((fact.start_date_key)::TEXT = ((date_dim.date_key)::CHARACTER VARYING)::TEXT))
                ) LEFT JOIN dim_date date_dim1 ON (
                    (
                        (((date_dim1.date_key)::CHARACTER VARYING)::TEXT >= (fact.start_date_key)::TEXT)
                        AND (((date_dim1.date_key)::CHARACTER VARYING)::TEXT <= (fact.end_date_key)::TEXT)
                        )
                    )
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
                                    AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                                    AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                                AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                            AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                        AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
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
                    AND (to_date(to_char(ds.date_key),'YYYYMMDD') < current_date())
                    )
            GROUP BY 1,
                ds.my_date_year,
                ds.my_date_month
            ) ds ON (
                (
                    (
                        (((date_dim.my_date_year)::CHARACTER VARYING)::TEXT = ((ds.my_date_year)::CHARACTER VARYING)::TEXT)
                        AND (((date_dim.my_date_month)::CHARACTER VARYING)::TEXT = ((ds.my_date_month)::CHARACTER VARYING)::TEXT)
                        )
                    AND ((fact.country_key)::TEXT = (ds.country)::TEXT)
                    )
                )
        )
    WHERE (
            ((fact.country_key)::TEXT <> ('ZZ'::CHARACTER VARYING)::TEXT)
            AND ((fact.start_date_key)::TEXT < to_char((current_date()::CHARACTER VARYING)::TIMESTAMP without TIME zone, ('YYYYMMDD'::CHARACTER VARYING)::TEXT))
            )
    GROUP BY fact.country_key,
        date_dim.date_key,
        date_dim.my_date_year,
        date_dim.my_date_month,
        date_dim.my_date_quarter,
        fact.total_time_off,
        fact.sea_frml_hours_on,
        ds.working_days,
        fact.sea_time_on_time_off,
        hier.l2_wwid,
        hier.l2_username,
        hier.l2_manager_name,
        hier.l3_wwid,
        hier.l3_username,
        hier.l3_manager_name,
        hier.l4_wwid,
        hier.l4_username,
        hier.l4_manager_name,
        hier.l1_username,
        hier.employee_name,
        dimemp.employee_name,
        dimemp.organization_l1_name,
        dimemp.organization_l2_name,
        dimemp.organization_l3_name,
        dimemp.organization_l4_name,
        dimemp.organization_l5_name,
        fact.sm_reason,
        fact.time_on,
        fact.reason_type,
        fact.time_type
    ) src1
),

result as (
    select * from table1
    union all
    select * from cte3
)

select * from result


