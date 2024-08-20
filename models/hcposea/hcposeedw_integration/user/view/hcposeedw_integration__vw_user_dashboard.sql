WITH edw_isight_sector_mapping
AS (
    SELECT *
    FROM hcposeedw_integration.edw_isight_sector_mapping
    ),
edw_isight_licenses
AS (
    SELECT *
    FROM hcposeedw_integration.edw_isight_licenses
    ),
edw_isight_dim_employee_snapshot_xref
AS (
    SELECT *
    FROM hcposeedw_integration.edw_isight_dim_employee_snapshot_xref
    ),
dim_employee_iconnect
AS (
    SELECT *
    FROM hcposeedw_integration.dim_employee_iconnect
    ),
T1
AS (
    SELECT src2.year,
        src2.month,
        src2.country,
        src2.sector,
        src2.usr_name,
        src2.last30_cnt,
        src2.total_sales_manager,
        src2.total_msl,
        src2.total_marketing,
        src2.total_others,
        lcns.qty AS license_qty,
        src2.wwid,
        src2.manager,
        src2.profile_category,
        src2.role,
        src2.company_name,
        src2.last_login_date,
        src2.active_flag,
        src2.license_type,
        src2.last30_flag_sales_rep,
        src2.last30_flag_sales_manager,
        src2.last30_flag_total_msl,
        src2.last30_flag_total_marketing,
        src2.last30_flag_total_others,
        src2.total_sales_rep,
        LOGIN.lgn AS monthly_active_login,
        src2.profile_name
    FROM (
        (
            (
                SELECT src1.year,
                    src1.month,
                    src1.country,
                    src1.sector,
                    src1.usr_name,
                    src1.last30_cnt,
                    salesman.total_sales_manager,
                    msl.total_msl,
                    marketing.total_marketing,
                    oth.total_others,
                    src1.wwid,
                    src1.manager,
                    src1.profile_category,
                    src1.role,
                    src1.company_name,
                    src1.last_login_date,
                    src1.active_flag,
                    src1.license_type,
                    0 AS last30_flag_sales_rep,
                    0 AS last30_flag_sales_manager,
                    0 AS last30_flag_total_msl,
                    0 AS last30_flag_total_marketing,
                    0 AS last30_flag_total_others,
                    salesrep.total_sales_rep,
                    src1.profile_name
                FROM (
                    (
                        (
                            (
                                (
                                    (
                                        SELECT xref.year,
                                            xref.month,
                                            xref.country,
                                            sect.sector,
                                            xref.employee_name AS usr_name,
                                            0 AS last30_cnt,
                                            xref.employee_wwid AS wwid,
                                            xref.manager_name AS manager,
                                            CASE 
                                                WHEN (
                                                        ((xref.profile_name)::TEXT = ('AP_Core_Sales'::CHARACTER VARYING)::TEXT)
                                                        OR ((xref.profile_name)::TEXT = ('AP_Core_MY_Sales'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    THEN 'sales_rep'::CHARACTER VARYING
                                                WHEN (
                                                        ((xref.profile_name)::TEXT = ('AP_Core_MSL'::CHARACTER VARYING)::TEXT)
                                                        OR ((xref.profile_name)::TEXT = ('AP_Core_MY_MSL'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    THEN 'total_msl'::CHARACTER VARYING
                                                WHEN (
                                                        ((xref.profile_name)::TEXT = ('AP_Core_SalesManager'::CHARACTER VARYING)::TEXT)
                                                        OR ((xref.profile_name)::TEXT = ('AP_Core_MY_SalesManager'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    THEN 'total_sales_manager'::CHARACTER VARYING
                                                WHEN (
                                                        ((xref.profile_name)::TEXT = ('AP_Core_ProductManager'::CHARACTER VARYING)::TEXT)
                                                        OR ((xref.profile_name)::TEXT = ('AP_MY_ProductManager'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    THEN 'total_marketing'::CHARACTER VARYING
                                                WHEN (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        (
                                                                            (
                                                                                ((xref.profile_name)::TEXT <> ('AP_Core_ProductManager'::CHARACTER VARYING)::TEXT)
                                                                                AND ((xref.profile_name)::TEXT <> ('AP_MY_ProductManager'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            AND ((xref.profile_name)::TEXT <> ('AP_Core_MSL'::CHARACTER VARYING)::TEXT)
                                                                            )
                                                                        AND ((xref.profile_name)::TEXT <> ('AP_Core_MY_MSL'::CHARACTER VARYING)::TEXT)
                                                                        )
                                                                    AND ((xref.profile_name)::TEXT <> ('AP_Core_SalesManager'::CHARACTER VARYING)::TEXT)
                                                                    )
                                                                AND ((xref.profile_name)::TEXT <> ('AP_Core_MY_SalesManager'::CHARACTER VARYING)::TEXT)
                                                                )
                                                            AND ((xref.profile_name)::TEXT <> ('AP_Core_Sales'::CHARACTER VARYING)::TEXT)
                                                            )
                                                        AND ((xref.profile_name)::TEXT <> ('AP_Core_MY_Sales'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    THEN 'total_others'::CHARACTER VARYING
                                                ELSE 'others'::CHARACTER VARYING
                                                END AS profile_category,
                                            xref.my_organization_name AS role,
                                            xref.company_name,
                                            xref.last_login_date,
                                            xref.active_flag,
                                            xref.user_license AS license_type,
                                            xref.profile_name
                                        FROM (
                                            (
                                                SELECT xref.year,
                                                    xref.month,
                                                    xref.employee_key,
                                                    xref.country_code AS country,
                                                    xref.employee_source_id,
                                                    xref.modified_dt,
                                                    xref.modified_id,
                                                    xref.employee_name,
                                                    xref.employee_wwid,
                                                    xref.mobile_phone,
                                                    xref.email_id,
                                                    xref.username,
                                                    xref.nickname,
                                                    xref.local_employee_number,
                                                    xref.profile_id,
                                                    xref.profile_name,
                                                    xref.function_name,
                                                    xref.employee_profile_id,
                                                    xref.company_name,
                                                    xref.division_name,
                                                    xref.department_name,
                                                    xref.country_name,
                                                    xref.address,
                                                    xref.alias,
                                                    xref.timezonesidkey,
                                                    xref.user_role_source_id,
                                                    xref.receives_info_emails,
                                                    xref.federation_identifier,
                                                    xref.last_ipad_sync,
                                                    xref.user_license,
                                                    xref.title,
                                                    xref.phone,
                                                    xref.last_login_date,
                                                    xref.region,
                                                    xref.profile_group_ap,
                                                    xref.manager_name,
                                                    xref.manager_wwid,
                                                    xref.active_flag,
                                                    xref.my_organization_code,
                                                    xref.my_organization_name,
                                                    xref.organization_l1_code,
                                                    xref.organization_l1_name,
                                                    xref.organization_l2_code,
                                                    xref.organization_l2_name,
                                                    xref.organization_l3_code,
                                                    xref.organization_l3_name,
                                                    xref.organization_l4_code,
                                                    xref.organization_l4_name,
                                                    xref.organization_l5_code,
                                                    xref.organization_l5_name,
                                                    xref.organization_l6_code,
                                                    xref.organization_l6_name,
                                                    xref.organization_l7_code,
                                                    xref.organization_l7_name,
                                                    xref.organization_l8_code,
                                                    xref.organization_l8_name,
                                                    xref.organization_l9_code,
                                                    xref.organization_l9_name,
                                                    xref.common_organization_l1_code,
                                                    xref.common_organization_l1_name,
                                                    xref.common_organization_l2_code,
                                                    xref.common_organization_l2_name,
                                                    xref.common_organization_l3_code,
                                                    xref.common_organization_l3_name,
                                                    xref.inserted_date,
                                                    xref.updated_date,
                                                    CASE 
                                                        WHEN (
                                                                
                                                                    datediff(day,sysdate(),xref.last_login_date)
                                                                        <= (30)::DOUBLE PRECISION
                                                                )
                                                            THEN 1
                                                        ELSE 0
                                                        END AS last30_flag
                                                FROM edw_isight_dim_employee_snapshot_xref xref
                                                WHERE (
                                                        (xref.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                                                        AND ((xref.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                                                        )
                                                ) xref LEFT JOIN edw_isight_sector_mapping sect ON (
                                                    (
                                                        ((sect.company)::TEXT = (xref.company_name)::TEXT)
                                                        AND ((sect.country)::TEXT = (xref.country)::TEXT)
                                                        )
                                                    )
                                            )
                                        GROUP BY xref.year,
                                            xref.month,
                                            xref.country,
                                            sect.sector,
                                            xref.employee_name,
                                            xref.employee_wwid,
                                            xref.manager_name,
                                            xref.profile_name,
                                            xref.my_organization_name,
                                            xref.company_name,
                                            xref.last_login_date,
                                            xref.active_flag,
                                            xref.user_license
                                        ) src1 LEFT JOIN (
                                        SELECT count(xref.employee_name) AS total_sales_rep,
                                            xref.month,
                                            xref.year,
                                            xref.country_code AS country,
                                            sect.sector
                                        FROM (
                                            edw_isight_dim_employee_snapshot_xref xref LEFT JOIN edw_isight_sector_mapping sect ON (
                                                    (
                                                        ((sect.company)::TEXT = (xref.company_name)::TEXT)
                                                        AND ((sect.country)::TEXT = (xref.country_code)::TEXT)
                                                        )
                                                    )
                                            )
                                        WHERE (
                                                (
                                                    ((xref.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                                                    AND (xref.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                                                    )
                                                AND (
                                                    ((xref.profile_name)::TEXT = ('AP_Core_Sales'::CHARACTER VARYING)::TEXT)
                                                    OR ((xref.profile_name)::TEXT = ('AP_Core_MY_Sales'::CHARACTER VARYING)::TEXT)
                                                    )
                                                )
                                        GROUP BY xref.month,
                                            xref.year,
                                            xref.country_code,
                                            sect.sector
                                        ) salesrep ON (
                                            (
                                                (
                                                    (
                                                        (src1.year = salesrep.year)
                                                        AND (src1.month = salesrep.month)
                                                        )
                                                    AND ((src1.country)::TEXT = (salesrep.country)::TEXT)
                                                    )
                                                AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(salesrep.sector, '#'::CHARACTER VARYING))::TEXT)
                                                )
                                            )
                                    ) LEFT JOIN (
                                    SELECT count(xref.employee_name) AS total_sales_manager,
                                        xref.month,
                                        xref.year,
                                        xref.country_code AS country,
                                        sect.sector
                                    FROM (
                                        edw_isight_dim_employee_snapshot_xref xref LEFT JOIN edw_isight_sector_mapping sect ON (
                                                (
                                                    ((sect.company)::TEXT = (xref.company_name)::TEXT)
                                                    AND ((sect.country)::TEXT = (xref.country_code)::TEXT)
                                                    )
                                                )
                                        )
                                    WHERE (
                                            (
                                                ((xref.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                                                AND (xref.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                                                )
                                            AND (
                                                ((xref.profile_name)::TEXT = ('AP_Core_SalesManager'::CHARACTER VARYING)::TEXT)
                                                OR ((xref.profile_name)::TEXT = ('AP_Core_MY_SalesManager'::CHARACTER VARYING)::TEXT)
                                                )
                                            )
                                    GROUP BY xref.month,
                                        xref.year,
                                        xref.country_code,
                                        sect.sector
                                    ) salesman ON (
                                        (
                                            (
                                                (
                                                    (src1.year = salesman.year)
                                                    AND (src1.month = salesman.month)
                                                    )
                                                AND ((src1.country)::TEXT = (salesman.country)::TEXT)
                                                )
                                            AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(salesman.sector, '#'::CHARACTER VARYING))::TEXT)
                                            )
                                        )
                                ) LEFT JOIN (
                                SELECT count(xref.employee_name) AS total_msl,
                                    xref.month,
                                    xref.year,
                                    xref.country_code AS country,
                                    sect.sector
                                FROM (
                                    edw_isight_dim_employee_snapshot_xref xref LEFT JOIN edw_isight_sector_mapping sect ON (
                                            (
                                                ((sect.company)::TEXT = (xref.company_name)::TEXT)
                                                AND ((sect.country)::TEXT = (xref.country_code)::TEXT)
                                                )
                                            )
                                    )
                                WHERE (
                                        (
                                            ((xref.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                                            AND (xref.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                                            )
                                        AND (
                                            ((xref.profile_name)::TEXT = ('AP_Core_MSL'::CHARACTER VARYING)::TEXT)
                                            OR ((xref.profile_name)::TEXT = ('AP_Core_MY_MSL'::CHARACTER VARYING)::TEXT)
                                            )
                                        )
                                GROUP BY xref.month,
                                    xref.year,
                                    xref.country_code,
                                    sect.sector
                                ) msl ON (
                                    (
                                        (
                                            (
                                                (src1.year = msl.year)
                                                AND (src1.month = msl.month)
                                                )
                                            AND ((src1.country)::TEXT = (msl.country)::TEXT)
                                            )
                                        AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(msl.sector, '#'::CHARACTER VARYING))::TEXT)
                                        )
                                    )
                            ) LEFT JOIN (
                            SELECT count(xref.employee_name) AS total_marketing,
                                xref.month,
                                xref.year,
                                xref.country_code AS country,
                                sect.sector
                            FROM (
                                edw_isight_dim_employee_snapshot_xref xref LEFT JOIN edw_isight_sector_mapping sect ON (
                                        (
                                            ((sect.company)::TEXT = (xref.company_name)::TEXT)
                                            AND ((sect.country)::TEXT = (xref.country_code)::TEXT)
                                            )
                                        )
                                )
                            WHERE (
                                    (
                                        ((xref.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                                        AND (xref.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                                        )
                                    AND (
                                        ((xref.profile_name)::TEXT = ('AP_Core_ProductManager'::CHARACTER VARYING)::TEXT)
                                        OR ((xref.profile_name)::TEXT = ('AP_MY_ProductManager'::CHARACTER VARYING)::TEXT)
                                        )
                                    )
                            GROUP BY xref.month,
                                xref.year,
                                xref.country_code,
                                sect.sector
                            ) marketing ON (
                                (
                                    (
                                        (
                                            (src1.year = marketing.year)
                                            AND (src1.month = marketing.month)
                                            )
                                        AND ((src1.country)::TEXT = (marketing.country)::TEXT)
                                        )
                                    AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(marketing.sector, '#'::CHARACTER VARYING))::TEXT)
                                    )
                                )
                        ) LEFT JOIN (
                        SELECT count(xref.employee_name) AS total_others,
                            xref.month,
                            xref.year,
                            xref.country_code AS country,
                            sect.sector
                        FROM (
                            edw_isight_dim_employee_snapshot_xref xref LEFT JOIN edw_isight_sector_mapping sect ON (
                                    (
                                        ((sect.company)::TEXT = (xref.company_name)::TEXT)
                                        AND ((sect.country)::TEXT = (xref.country_code)::TEXT)
                                        )
                                    )
                            )
                        WHERE (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                ((xref.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                                                                AND (xref.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                                                                )
                                                            AND ((xref.profile_name)::TEXT <> ('AP_Core_ProductManager'::CHARACTER VARYING)::TEXT)
                                                            )
                                                        AND ((xref.profile_name)::TEXT <> ('AP_MY_ProductManager'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    AND ((xref.profile_name)::TEXT <> ('AP_Core_MSL'::CHARACTER VARYING)::TEXT)
                                                    )
                                                AND ((xref.profile_name)::TEXT <> ('AP_Core_MY_MSL'::CHARACTER VARYING)::TEXT)
                                                )
                                            AND ((xref.profile_name)::TEXT <> ('AP_Core_SalesManager'::CHARACTER VARYING)::TEXT)
                                            )
                                        AND ((xref.profile_name)::TEXT <> ('AP_Core_MY_SalesManager'::CHARACTER VARYING)::TEXT)
                                        )
                                    AND ((xref.profile_name)::TEXT <> ('AP_Core_Sales'::CHARACTER VARYING)::TEXT)
                                    )
                                AND ((xref.profile_name)::TEXT <> ('AP_Core_MY_Sales'::CHARACTER VARYING)::TEXT)
                                )
                        GROUP BY xref.month,
                            xref.year,
                            xref.country_code,
                            sect.sector
                        ) oth ON (
                            (
                                (
                                    (
                                        (src1.year = oth.year)
                                        AND (src1.month = oth.month)
                                        )
                                    AND ((src1.country)::TEXT = (oth.country)::TEXT)
                                    )
                                AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(oth.sector, '#'::CHARACTER VARYING))::TEXT)
                                )
                            )
                    )
                ) src2 LEFT JOIN (
                SELECT sum(edw_isight_licenses.qty) AS qty,
                    edw_isight_licenses.year,
                    edw_isight_licenses.country,
                    edw_isight_licenses.sector
                FROM edw_isight_licenses
                GROUP BY edw_isight_licenses.year,
                    edw_isight_licenses.country,
                    edw_isight_licenses.sector
                ) lcns ON (
                    (
                        (
                            (lcns.year = ((src2.year)::NUMERIC)::NUMERIC(18, 0))
                            AND ((lcns.country)::TEXT = (src2.country)::TEXT)
                            )
                        AND ((lcns.sector)::TEXT = (src2.sector)::TEXT)
                        )
                    )
            ) LEFT JOIN (
            SELECT count(DISTINCT xref.employee_name) AS lgn,
                xref1.year,
                xref1.month,
                xref1.country_code,
                sect.sector
            FROM (
                (
                    edw_isight_dim_employee_snapshot_xref xref JOIN edw_isight_dim_employee_snapshot_xref xref1 ON (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    date_part (
                                                        year,
                                                        xref.last_login_date
                                                        ) = xref1.year
                                                    )
                                                AND (
                                                    date_part (
                                                        month,
                                                        xref.last_login_date
                                                        ) = xref1.month
                                                    )
                                                )
                                            AND (xref.year = xref1.year)
                                            )
                                        AND (xref.month = xref1.month)
                                        )
                                    AND ((xref.country_code)::TEXT = (xref1.country_code)::TEXT)
                                    )
                                AND ((xref.employee_key)::TEXT = (xref1.employee_key)::TEXT)
                                )
                            )
                    ) LEFT JOIN edw_isight_sector_mapping sect ON (
                        (
                            ((sect.company)::TEXT = (xref1.company_name)::TEXT)
                            AND ((sect.country)::TEXT = (xref1.country_code)::TEXT)
                            )
                        )
                )
            WHERE (
                    ((xref.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                    AND (xref.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                    )
            GROUP BY xref1.year,
                xref1.month,
                xref1.country_code,
                sect.sector
            ) LOGIN ON (
                (
                    (
                        (
                            (((LOGIN.year)::NUMERIC)::NUMERIC(18, 0) = ((src2.year)::NUMERIC)::NUMERIC(18, 0))
                            AND ((LOGIN.country_code)::TEXT = (src2.country)::TEXT)
                            )
                        AND ((LOGIN.sector)::TEXT = (src2.sector)::TEXT)
                        )
                    AND (LOGIN.month = src2.month)
                    )
                )
        )
    ),
T2
AS (
    SELECT src2.year,
        src2.month,
        src2.country,
        src2.sector,
        src2.usr_name,
        src2.last30_cnt,
        src2.total_sales_manager,
        src2.total_msl,
        src2.total_marketing,
        src2.total_others,
        lcns.qty AS license_qty,
        src2.wwid,
        src2.manager,
        src2.profile_category,
        src2.role,
        src2.company_name,
        src2.last_login_date,
        src2.active_flag,
        src2.license_type,
        src2.last30_flag_sales_rep,
        src2.last30_flag_sales_manager,
        src2.last30_flag_total_msl,
        src2.last30_flag_total_marketing,
        src2.last30_flag_total_others,
        src2.total_sales_rep,
        LOGIN.lgn AS monthly_active_login,
        src2.profile_name
    FROM (
        (
            (
                SELECT date_part (
                        year,
                        sysdate()
                        ) AS year,
                    date_part (
                        month,
                        sysdate()
                        ) AS month,
                    src1.country,
                    src1.sector,
                    src1.usr_name,
                    src1.last30_cnt,
                    salesman.total_sales_manager,
                    msl.total_msl,
                    marketing.total_marketing,
                    oth.total_others,
                    src1.wwid,
                    src1.manager,
                    src1.profile_category,
                    src1.role,
                    src1.company_name,
                    src1.last_login_date,
                    src1.active_flag,
                    src1.license_type,
                    salesrep.last30_flag_sales_rep,
                    salesman.last30_flag_sales_manager,
                    msl.last30_flag_total_msl,
                    marketing.last30_flag_total_marketing,
                    oth.last30_flag_total_others,
                    salesrep.total_sales_rep,
                    src1.profile_name
                FROM (
                    (
                        (
                            (
                                (
                                    (
                                        SELECT xref.country,
                                            sect.sector,
                                            xref.employee_name AS usr_name,
                                            sum(xref.last30_flag) AS last30_cnt,
                                            xref.employee_wwid AS wwid,
                                            xref.manager_name AS manager,
                                            CASE 
                                                WHEN (
                                                        ((xref.profile_name)::TEXT = ('AP_Core_Sales'::CHARACTER VARYING)::TEXT)
                                                        OR ((xref.profile_name)::TEXT = ('AP_Core_MY_Sales'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    THEN 'sales_rep'::CHARACTER VARYING
                                                WHEN (
                                                        ((xref.profile_name)::TEXT = ('AP_Core_MSL'::CHARACTER VARYING)::TEXT)
                                                        OR ((xref.profile_name)::TEXT = ('AP_Core_MY_MSL'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    THEN 'total_msl'::CHARACTER VARYING
                                                WHEN (
                                                        ((xref.profile_name)::TEXT = ('AP_Core_SalesManager'::CHARACTER VARYING)::TEXT)
                                                        OR ((xref.profile_name)::TEXT = ('AP_Core_MY_SalesManager'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    THEN 'total_sales_manager'::CHARACTER VARYING
                                                WHEN (
                                                        ((xref.profile_name)::TEXT = ('AP_Core_ProductManager'::CHARACTER VARYING)::TEXT)
                                                        OR ((xref.profile_name)::TEXT = ('AP_MY_ProductManager'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    THEN 'total_marketing'::CHARACTER VARYING
                                                WHEN (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        (
                                                                            (
                                                                                ((xref.profile_name)::TEXT <> ('AP_Core_ProductManager'::CHARACTER VARYING)::TEXT)
                                                                                AND ((xref.profile_name)::TEXT <> ('AP_MY_ProductManager'::CHARACTER VARYING)::TEXT)
                                                                                )
                                                                            AND ((xref.profile_name)::TEXT <> ('AP_Core_MSL'::CHARACTER VARYING)::TEXT)
                                                                            )
                                                                        AND ((xref.profile_name)::TEXT <> ('AP_Core_MY_MSL'::CHARACTER VARYING)::TEXT)
                                                                        )
                                                                    AND ((xref.profile_name)::TEXT <> ('AP_Core_SalesManager'::CHARACTER VARYING)::TEXT)
                                                                    )
                                                                AND ((xref.profile_name)::TEXT <> ('AP_Core_MY_SalesManager'::CHARACTER VARYING)::TEXT)
                                                                )
                                                            AND ((xref.profile_name)::TEXT <> ('AP_Core_Sales'::CHARACTER VARYING)::TEXT)
                                                            )
                                                        AND ((xref.profile_name)::TEXT <> ('AP_Core_MY_Sales'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    THEN 'total_others'::CHARACTER VARYING
                                                ELSE 'others'::CHARACTER VARYING
                                                END AS profile_category,
                                            xref.my_organization_name AS role,
                                            xref.company_name,
                                            xref.last_login_date,
                                            xref.active_flag,
                                            xref.user_license AS license_type,
                                            xref.profile_name
                                        FROM (
                                            (
                                                SELECT xref.employee_key,
                                                    xref.country_code AS country,
                                                    xref.employee_source_id,
                                                    xref.modified_dt,
                                                    xref.modified_id,
                                                    xref.employee_name,
                                                    xref.employee_wwid,
                                                    xref.mobile_phone,
                                                    xref.email_id,
                                                    xref.username,
                                                    xref.nickname,
                                                    xref.local_employee_number,
                                                    xref.profile_id,
                                                    xref.profile_name,
                                                    xref.function_name,
                                                    xref.employee_profile_id,
                                                    xref.company_name,
                                                    xref.division_name,
                                                    xref.department_name,
                                                    xref.country_name,
                                                    xref.address,
                                                    xref.alias,
                                                    xref.timezonesidkey,
                                                    xref.user_role_source_id,
                                                    xref.receives_info_emails,
                                                    xref.federation_identifier,
                                                    xref.last_ipad_sync,
                                                    xref.user_license,
                                                    xref.title,
                                                    xref.phone,
                                                    xref.last_login_date,
                                                    xref.region,
                                                    xref.profile_group_ap,
                                                    xref.manager_name,
                                                    xref.manager_wwid,
                                                    xref.active_flag,
                                                    xref.my_organization_code,
                                                    xref.my_organization_name,
                                                    xref.organization_l1_code,
                                                    xref.organization_l1_name,
                                                    xref.organization_l2_code,
                                                    xref.organization_l2_name,
                                                    xref.organization_l3_code,
                                                    xref.organization_l3_name,
                                                    xref.organization_l4_code,
                                                    xref.organization_l4_name,
                                                    xref.organization_l5_code,
                                                    xref.organization_l5_name,
                                                    xref.organization_l6_code,
                                                    xref.organization_l6_name,
                                                    xref.organization_l7_code,
                                                    xref.organization_l7_name,
                                                    xref.organization_l8_code,
                                                    xref.organization_l8_name,
                                                    xref.organization_l9_code,
                                                    xref.organization_l9_name,
                                                    xref.common_organization_l1_code,
                                                    xref.common_organization_l1_name,
                                                    xref.common_organization_l2_code,
                                                    xref.common_organization_l2_name,
                                                    xref.common_organization_l3_code,
                                                    xref.common_organization_l3_name,
                                                    xref.inserted_date,
                                                    xref.updated_date,
                                                    CASE 
                                                        WHEN (
                                                                
                                                                    datediff(day,sysdate(),xref.last_login_date)
                                                                     <= (30)::DOUBLE PRECISION
                                                                )
                                                            THEN 1
                                                        ELSE 0
                                                        END AS last30_flag
                                                FROM dim_employee_iconnect xref
                                                WHERE (
                                                        (
                                                            ((xref.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                                                            AND (xref.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                                                            )
                                                        AND (
                                                            "substring" (
                                                                (xref.modified_dt)::TEXT,
                                                                1,
                                                                10
                                                                ) >= '2022-12-01'::TEXT
                                                            )
                                                        )
                                                ) xref LEFT JOIN edw_isight_sector_mapping sect ON (
                                                    (
                                                        ((sect.company)::TEXT = (xref.company_name)::TEXT)
                                                        AND ((sect.country)::TEXT = (xref.country)::TEXT)
                                                        )
                                                    )
                                            )
                                        GROUP BY xref.country,
                                            sect.sector,
                                            xref.employee_name,
                                            xref.employee_wwid,
                                            xref.manager_name,
                                            xref.profile_name,
                                            xref.my_organization_name,
                                            xref.company_name,
                                            xref.last_login_date,
                                            xref.active_flag,
                                            xref.user_license
                                        ) src1 LEFT JOIN (
                                        SELECT count(xref.employee_name) AS total_sales_rep,
                                            xref.country_code AS country,
                                            sect.sector,
                                            sum(CASE 
                                                    WHEN (
                                                            
                                                                datediff(day,sysdate(),xref.last_login_date)
                                                                 <= (30)::DOUBLE PRECISION
                                                            )
                                                        THEN 1
                                                    ELSE 0
                                                    END) AS last30_flag_sales_rep
                                        FROM (
                                            dim_employee_iconnect xref LEFT JOIN edw_isight_sector_mapping sect ON (
                                                    (
                                                        ((sect.company)::TEXT = (xref.company_name)::TEXT)
                                                        AND ((sect.country)::TEXT = (xref.country_code)::TEXT)
                                                        )
                                                    )
                                            )
                                        WHERE (
                                                (
                                                    ((xref.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                                                    AND (xref.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                                                    )
                                                AND (
                                                    ((xref.profile_name)::TEXT = ('AP_Core_Sales'::CHARACTER VARYING)::TEXT)
                                                    OR ((xref.profile_name)::TEXT = ('AP_Core_MY_Sales'::CHARACTER VARYING)::TEXT)
                                                    )
                                                )
                                        GROUP BY xref.country_code,
                                            sect.sector
                                        ) salesrep ON (
                                            (
                                                ((src1.country)::TEXT = (salesrep.country)::TEXT)
                                                AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(salesrep.sector, '#'::CHARACTER VARYING))::TEXT)
                                                )
                                            )
                                    ) LEFT JOIN (
                                    SELECT count(xref.employee_name) AS total_sales_manager,
                                        xref.country_code AS country,
                                        sect.sector,
                                        sum(CASE 
                                                WHEN (
                                                        
                                                            datediff(day,sysdate(),xref.last_login_date)
                                                             <= (30)::DOUBLE PRECISION
                                                        )
                                                    THEN 1
                                                ELSE 0
                                                END) AS last30_flag_sales_manager
                                    FROM (
                                        dim_employee_iconnect xref LEFT JOIN edw_isight_sector_mapping sect ON (
                                                (
                                                    ((sect.company)::TEXT = (xref.company_name)::TEXT)
                                                    AND ((sect.country)::TEXT = (xref.country_code)::TEXT)
                                                    )
                                                )
                                        )
                                    WHERE (
                                            (
                                                ((xref.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                                                AND (xref.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                                                )
                                            AND (
                                                ((xref.profile_name)::TEXT = ('AP_Core_SalesManager'::CHARACTER VARYING)::TEXT)
                                                OR ((xref.profile_name)::TEXT = ('AP_Core_MY_SalesManager'::CHARACTER VARYING)::TEXT)
                                                )
                                            )
                                    GROUP BY xref.country_code,
                                        sect.sector
                                    ) salesman ON (
                                        (
                                            ((src1.country)::TEXT = (salesman.country)::TEXT)
                                            AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(salesman.sector, '#'::CHARACTER VARYING))::TEXT)
                                            )
                                        )
                                ) LEFT JOIN (
                                SELECT count(xref.employee_name) AS total_msl,
                                    xref.country_code AS country,
                                    sect.sector,
                                    sum(CASE 
                                            WHEN (
                                                    
                                                        datediff(day,sysdate(),xref.last_login_date)
                                                         <= (30)::DOUBLE PRECISION
                                                    )
                                                THEN 1
                                            ELSE 0
                                            END) AS last30_flag_total_msl
                                FROM (
                                    dim_employee_iconnect xref LEFT JOIN edw_isight_sector_mapping sect ON (
                                            (
                                                ((sect.company)::TEXT = (xref.company_name)::TEXT)
                                                AND ((sect.country)::TEXT = (xref.country_code)::TEXT)
                                                )
                                            )
                                    )
                                WHERE (
                                        (
                                            ((xref.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                                            AND (xref.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                                            )
                                        AND (
                                            ((xref.profile_name)::TEXT = ('AP_Core_MSL'::CHARACTER VARYING)::TEXT)
                                            OR ((xref.profile_name)::TEXT = ('AP_Core_MY_MSL'::CHARACTER VARYING)::TEXT)
                                            )
                                        )
                                GROUP BY xref.country_code,
                                    sect.sector
                                ) msl ON (
                                    (
                                        ((src1.country)::TEXT = (msl.country)::TEXT)
                                        AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(msl.sector, '#'::CHARACTER VARYING))::TEXT)
                                        )
                                    )
                            ) LEFT JOIN (
                            SELECT count(xref.employee_name) AS total_marketing,
                                xref.country_code AS country,
                                sect.sector,
                                sum(CASE 
                                        WHEN (
                                                
                                                    datediff(day,sysdate(),xref.last_login_date)
                                                     <= (30)::DOUBLE PRECISION
                                                )
                                            THEN 1
                                        ELSE 0
                                        END) AS last30_flag_total_marketing
                            FROM (
                                dim_employee_iconnect xref LEFT JOIN edw_isight_sector_mapping sect ON (
                                        (
                                            ((sect.company)::TEXT = (xref.company_name)::TEXT)
                                            AND ((sect.country)::TEXT = (xref.country_code)::TEXT)
                                            )
                                        )
                                )
                            WHERE (
                                    (
                                        ((xref.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                                        AND (xref.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                                        )
                                    AND (
                                        ((xref.profile_name)::TEXT = ('AP_Core_ProductManager'::CHARACTER VARYING)::TEXT)
                                        OR ((xref.profile_name)::TEXT = ('AP_MY_ProductManager'::CHARACTER VARYING)::TEXT)
                                        )
                                    )
                            GROUP BY xref.country_code,
                                sect.sector
                            ) marketing ON (
                                (
                                    ((src1.country)::TEXT = (marketing.country)::TEXT)
                                    AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(marketing.sector, '#'::CHARACTER VARYING))::TEXT)
                                    )
                                )
                        ) LEFT JOIN (
                        SELECT count(xref.employee_name) AS total_others,
                            xref.country_code AS country,
                            sect.sector,
                            sum(CASE 
                                    WHEN (
                                            
                                                datediff(day,sysdate(),xref.last_login_date)
                                                 <= (30)::DOUBLE PRECISION
                                            )
                                        THEN 1
                                    ELSE 0
                                    END) AS last30_flag_total_others
                        FROM (
                            dim_employee_iconnect xref LEFT JOIN edw_isight_sector_mapping sect ON (
                                    (
                                        ((sect.company)::TEXT = (xref.company_name)::TEXT)
                                        AND ((sect.country)::TEXT = (xref.country_code)::TEXT)
                                        )
                                    )
                            )
                        WHERE (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                ((xref.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                                                                AND (xref.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                                                                )
                                                            AND ((xref.profile_name)::TEXT <> ('AP_Core_ProductManager'::CHARACTER VARYING)::TEXT)
                                                            )
                                                        AND ((xref.profile_name)::TEXT <> ('AP_MY_ProductManager'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    AND ((xref.profile_name)::TEXT <> ('AP_Core_MSL'::CHARACTER VARYING)::TEXT)
                                                    )
                                                AND ((xref.profile_name)::TEXT <> ('AP_Core_MY_MSL'::CHARACTER VARYING)::TEXT)
                                                )
                                            AND ((xref.profile_name)::TEXT <> ('AP_Core_SalesManager'::CHARACTER VARYING)::TEXT)
                                            )
                                        AND ((xref.profile_name)::TEXT <> ('AP_Core_MY_SalesManager'::CHARACTER VARYING)::TEXT)
                                        )
                                    AND ((xref.profile_name)::TEXT <> ('AP_Core_Sales'::CHARACTER VARYING)::TEXT)
                                    )
                                AND ((xref.profile_name)::TEXT <> ('AP_Core_MY_Sales'::CHARACTER VARYING)::TEXT)
                                )
                        GROUP BY xref.country_code,
                            sect.sector
                        ) oth ON (
                            (
                                ((src1.country)::TEXT = (oth.country)::TEXT)
                                AND ((COALESCE(src1.sector, '#'::CHARACTER VARYING))::TEXT = (COALESCE(oth.sector, '#'::CHARACTER VARYING))::TEXT)
                                )
                            )
                    )
                ) src2 LEFT JOIN (
                SELECT sum(edw_isight_licenses.qty) AS qty,
                    edw_isight_licenses.year,
                    edw_isight_licenses.country,
                    edw_isight_licenses.sector
                FROM edw_isight_licenses
                GROUP BY edw_isight_licenses.year,
                    edw_isight_licenses.country,
                    edw_isight_licenses.sector
                ) lcns ON (
                    (
                        (
                            (lcns.year = ((src2.year)::NUMERIC)::NUMERIC(18, 0))
                            AND ((lcns.country)::TEXT = (src2.country)::TEXT)
                            )
                        AND ((lcns.sector)::TEXT = (src2.sector)::TEXT)
                        )
                    )
            ) LEFT JOIN (
            SELECT count(dim_employee_iconnect.employee_name) AS lgn,
                date_part (
                    year,
                    sysdate()
                    ) AS year,
                date_part (
                    month,
                    sysdate()
                    ) AS month,
                dim_employee_iconnect.country_code,
                sect.sector
            FROM (
                dim_employee_iconnect LEFT JOIN edw_isight_sector_mapping sect ON (
                        (
                            ((sect.company)::TEXT = (dim_employee_iconnect.company_name)::TEXT)
                            AND ((sect.country)::TEXT = (dim_employee_iconnect.country_code)::TEXT)
                            )
                        )
                )
            WHERE (
                    (
                        (
                            (
                                date_part (
                                    year,
                                    dim_employee_iconnect.last_login_date
                                    ) = date_part (
                                    year,
                                    sysdate()
                                    )
                                )
                            AND (
                                date_part (
                                    month,
                                    dim_employee_iconnect.last_login_date
                                    ) = date_part (
                                    month,
                                    sysdate()
                                    )
                                )
                            )
                        AND ((dim_employee_iconnect.profile_name)::TEXT ! LIKE ('%Chatter%'::CHARACTER VARYING)::TEXT)
                        )
                    AND (dim_employee_iconnect.active_flag = ((1)::NUMERIC)::NUMERIC(18, 0))
                    )
            GROUP BY dim_employee_iconnect.country_code,
                sect.sector
            ) LOGIN ON (
                (
                    (
                        (
                            (((LOGIN.year)::NUMERIC)::NUMERIC(18, 0) = ((src2.year)::NUMERIC)::NUMERIC(18, 0))
                            AND ((LOGIN.country_code)::TEXT = (src2.country)::TEXT)
                            )
                        AND ((LOGIN.sector)::TEXT = (src2.sector)::TEXT)
                        )
                    AND (LOGIN.month = src2.month)
                    )
                )
        )
    ),
UNION_OF
AS (
    SELECT *
    FROM T1
    
    UNION ALL
    
    SELECT *
    FROM T2
    ),
FINAL
AS (
    SELECT src3.year,
        src3.month,
        src3.country,
        src3.sector,
        src3.usr_name,
        src3.last30_cnt,
        src3.total_sales_manager,
        src3.total_msl,
        src3.total_marketing,
        src3.total_others,
        src3.license_qty,
        src3.wwid,
        src3.manager,
        src3.profile_category,
        src3.role,
        src3.company_name,
        src3.last_login_date,
        src3.active_flag,
        src3.license_type,
        src3.last30_flag_sales_rep,
        src3.last30_flag_sales_manager,
        src3.last30_flag_total_msl,
        src3.last30_flag_total_marketing,
        src3.last30_flag_total_others,
        src3.total_sales_rep,
        src3.monthly_active_login,
        src3.profile_name
    FROM UNION_OF src3
    WHERE ((src3.country)::TEXT <> ('ZZ'::CHARACTER VARYING)::TEXT)
    GROUP BY src3.year,
        src3.month,
        src3.country,
        src3.sector,
        src3.usr_name,
        src3.last30_cnt,
        src3.total_sales_manager,
        src3.total_msl,
        src3.total_marketing,
        src3.total_others,
        src3.license_qty,
        src3.wwid,
        src3.manager,
        src3.profile_category,
        src3.role,
        src3.company_name,
        src3.last_login_date,
        src3.active_flag,
        src3.license_type,
        src3.last30_flag_sales_rep,
        src3.last30_flag_sales_manager,
        src3.last30_flag_total_msl,
        src3.last30_flag_total_marketing,
        src3.last30_flag_total_others,
        src3.total_sales_rep,
        src3.monthly_active_login,
        src3.profile_name
    )
SELECT *
FROM FINAL
