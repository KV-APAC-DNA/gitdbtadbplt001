WITH dim_employee_iconnect
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__dim_employee_iconnect') }}
    ),
edw_isight_sector_mapping
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__edw_isight_sector_mapping') }}
    ),
derived_table1
AS (
    SELECT l1.country_code,
        l1.employee_key,
        l1.employee_source_id,
        l1.employee_wwid AS l1_wwid,
        l1.employee_name,
        l1.username,
        l1.company_name,
        l1.division_name,
        l1.department_name,
        l1.active_flag,
        l1.profile_group_ap,
        split_part((l1.username)::TEXT, ('@'::CHARACTER VARYING)::TEXT, 1) AS l1_username,
        l2.employee_wwid AS l2_wwid,
        l2.employee_name AS l2_manager_name,
        split_part((l2.username)::TEXT, ('@'::CHARACTER VARYING)::TEXT, 1) AS l2_username,
        l3.employee_wwid AS l3_wwid,
        l3.employee_name AS l3_manager_name,
        split_part((l3.username)::TEXT, ('@'::CHARACTER VARYING)::TEXT, 1) AS l3_username,
        l4.employee_wwid AS l4_wwid,
        l4.employee_name AS l4_manager_name,
        split_part((l4.username)::TEXT, ('@'::CHARACTER VARYING)::TEXT, 1) AS l4_username,
        dstat.sector,
        l1.city,
        l1.first_name,
        l1.language_local_key,
        l1.last_name,
        l1.manager_source_id,
        l1.postal_code,
        l1.STATE,
        l1.user_type,
        l1.email_id,
        l1.user_license,
        l1.region,
        l1.phone,
        l1.profile_name,
        l1.title,
        l1.user_role_source_id,
        l1.last_login_date,
        l1.modified_dt,
        l1.veeva_user_type,
        l1.veeva_country_code,
        l1.shc_user_franchise
    FROM dim_employee_iconnect l1
    LEFT JOIN dim_employee_iconnect l2 ON (
            (
                (
                    ((l1.manager_wwid)::TEXT = (l2.employee_wwid)::TEXT)
                    AND (l2.employee_wwid IS NOT NULL)
                    )
                AND ((l2.employee_wwid)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                )
            )
    LEFT JOIN dim_employee_iconnect l3 ON (
            (
                (
                    ((l2.manager_wwid)::TEXT = (l3.employee_wwid)::TEXT)
                    AND (l3.employee_wwid IS NOT NULL)
                    )
                AND ((l3.employee_wwid)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                )
            )
    LEFT JOIN dim_employee_iconnect l4 ON (
            (
                (
                    ((l3.manager_wwid)::TEXT = (l4.employee_wwid)::TEXT)
                    AND (l4.employee_wwid IS NOT NULL)
                    )
                AND ((l4.employee_wwid)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                )
            )
    LEFT JOIN dim_employee_iconnect l5 ON (
            (
                (
                    ((l4.manager_wwid)::TEXT = (l5.employee_wwid)::TEXT)
                    AND (l5.employee_wwid IS NOT NULL)
                    )
                AND ((l5.employee_wwid)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                )
            )
    LEFT JOIN edw_isight_sector_mapping dstat ON (
            (
                ((dstat.country)::TEXT = (l1.country_code)::TEXT)
                AND ((dstat.company)::TEXT = (l1.company_name)::TEXT)
                )
            )
    ),
final
AS (
    SELECT DISTINCT derived_table1.country_code,
        derived_table1.employee_key,
        derived_table1.employee_source_id,
        derived_table1.l1_wwid,
        derived_table1.l1_username,
        derived_table1.employee_name,
        derived_table1.username,
        derived_table1.company_name,
        derived_table1.division_name,
        derived_table1.department_name,
        derived_table1.active_flag,
        derived_table1.profile_group_ap,
        derived_table1.l2_wwid,
        derived_table1.l2_username,
        derived_table1.l2_manager_name,
        derived_table1.l3_wwid,
        derived_table1.l3_username,
        derived_table1.l3_manager_name,
        derived_table1.l4_wwid,
        derived_table1.l4_username,
        derived_table1.l4_manager_name,
        derived_table1.sector,
        derived_table1.city,
        derived_table1.first_name,
        derived_table1.language_local_key,
        derived_table1.last_name,
        derived_table1.manager_source_id,
        derived_table1.postal_code,
        derived_table1.STATE,
        derived_table1.user_type,
        derived_table1.email_id,
        derived_table1.user_license,
        derived_table1.region,
        derived_table1.phone,
        derived_table1.profile_name,
        derived_table1.title,
        derived_table1.user_role_source_id,
        derived_table1.last_login_date,
        derived_table1.modified_dt,
        derived_table1.veeva_user_type,
        derived_table1.veeva_country_code,
        derived_table1.shc_user_franchise
    FROM derived_table1
    )
SELECT *
FROM final
