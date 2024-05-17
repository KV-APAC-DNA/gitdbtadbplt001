with v_rpt_pos_offtake_wkly_nonkorea as (
    select * from {{ ref('ntaedw_integration__v_rpt_pos_offtake_wkly_nonkorea') }}
),
itg_sls_grp_to_customer_mapping as (
    select * from snapntaitg_integration.itg_sls_grp_to_customer_mapping
),
itg_mds_tw_customer_sales_rep_mapping as (
    select * from {{ ref('ntaitg_integration__itg_mds_tw_customer_sales_rep_mapping') }}
),
v_rpt_dly_plan_ims_enrich as(
    select * from v_rpt_dly_plan_ims_enrich

),
edw_vw_promo_calendar as (
    select * from {{ ref('ntaedw_integration__edw_vw_promo_calendar') }}
),
itg_tsi_target_data as (
    select * from {{ ref('ntaitg_integration__itg_tsi_target_data') }}
),
vw_tw_forecast as (
    select * from {{ ref('ntaedw_integration__vw_tw_forecast') }}
),
itg_mds_tw_incentive_schemes as (
    select * from {{ ref('ntaitg_integration__itg_mds_tw_incentive_schemes') }}
),
itg_mds_tw_sales_representative as (
    select * from {{ ref('ntaitg_integration__itg_mds_tw_sales_representative') }}
),
monthly_offtake as 
(
    SELECT 
        'Offtake_M' as source_type,
        main.cntry_cd,
        main.crncy_cd,
        main.to_crncy,
        main.psr_code,
        main.psr_name,
        main.year,
        NULL AS qrtr,
        main.mnth_id,
        e.report_to,
        e.reportto_name,
        e.reverse,
        cast(AVG(main.offtake_actual) as numeric(38, 4)) AS monthly_actual,
        cast(AVG(main.offtake_target) as numeric(38, 4)) AS monthly_target,
        AVG(main.offtake_achievement) AS monthly_achievement,
        AVG(
            CASE
                WHEN c.incentive_type = 'YM'
                AND e.reverse = 'True' THEN c.nts_si
                ELSE c.offtake_si
            END
        ) AS monthly_incentive_amount,
        0 AS quarterly_actual,
        0 AS quarterly_target,
        0 AS quarterly_achievement,
        0 AS quarterly_incentive_amount
    FROM 
        (
            SELECT 
                'Offtake_M' AS source_type,
                cntry_cd,
                crncy_cd,
                to_crncy,
                psr_code,
                psr_name,
                YEAR,
                MONTH AS mnth_id,
                SUM(offtake_actual) offtake_actual,
                SUM(offtake_target) offtake_target,
                cast(
                    (
                        SUM(offtake_actual) / NULLIF(SUM(offtake_target), 0)
                    ) * 100 as decimal(10, 2)
                ) as offtake_achievement
            FROM 
                (
                    (
                        SELECT 
                            'TW' AS cntry_cd,
                            Q.crncy_cd,
                            Q.to_crncy,
                            Q.customer,
                            c.psr_code,
                            c.psr_name,
                            Q.month,
                            Q.year,
                            SUM(offtake_actual) offtake_actual,
                            0 AS offtake_target
                        FROM 
                            (
                                SELECT 'TW' AS cntry_cd,
                                    a.crncy_cd,
                                    a.to_crncy,
                                    a.sls_grp,
                                    CAST(
                                        SUBSTRING(fisc_per, 1, 4) || SUBSTRING(fisc_per, 6, 7) AS INTEGER
                                    ) AS MONTH,
                                    CAST(SUBSTRING(fisc_per, 1, 4) AS INTEGER) AS YEAR,
                                    CASE
                                        WHEN b.sls_grp IN ('Cosmed 康是美', 'Poya 寶雅', 'Watsons 屈臣氏') THEN strategy_customer_hierachy_name
                                    END AS customer,
                                    SUM(sls_amt) offtake_actual,
                                    0 AS offtake_target
                                FROM v_rpt_pos_offtake_wkly_nonkorea a
                                    LEFT JOIN itg_sls_grp_to_customer_mapping b ON a.sls_grp = b.sls_grp
                                WHERE ctry_nm = 'Taiwan'
                                    AND b.sls_grp IN ('Cosmed 康是美', 'Poya 寶雅', 'Watsons 屈臣氏')
                                    AND to_crncy = 'TWD'
                                GROUP BY a.crncy_cd,
                                    a.to_crncy,
                                    a.sls_grp,
                                    MONTH,
                                    YEAR,
                                    customer
                                UNION ALL
                                SELECT 'TW' AS cntry_cd,
                                    a.crncy_cd,
                                    a.to_crncy,
                                    a.sls_grp,
                                    CAST(
                                        SUBSTRING(univ_per, 1, 4) || SUBSTRING(univ_per, 6, 7) AS INTEGER
                                    ) AS MONTH,
                                    CAST(SUBSTRING(univ_per, 1, 4) AS INTEGER) AS YEAR,
                                    CASE
                                        WHEN b.sls_grp IN ('Carrefour 家樂福', 'PX 全聯', 'EC', 'RT-Mart 大潤發') THEN strategy_customer_hierachy_name
                                    END AS customer,
                                    SUM(sls_amt) offtake_actual,
                                    0 AS offtake_target
                                FROM v_rpt_pos_offtake_wkly_nonkorea a
                                    LEFT JOIN itg_sls_grp_to_customer_mapping b ON a.sls_grp = b.sls_grp
                                WHERE ctry_nm = 'Taiwan'
                                    AND b.sls_grp IN ('Carrefour 家樂福', 'PX 全聯', 'EC', 'RT-Mart 大潤發')
                                    AND to_crncy = 'TWD'
                                GROUP BY a.crncy_cd,
                                    a.to_crncy,
                                    a.sls_grp,
                                    MONTH,
                                    YEAR,
                                    customer
                            ) Q
                            LEFT JOIN 
                            (
                                SELECT DISTINCT customer_code,
                                    customer_name,
                                    psr_code,
                                    psr_name
                                FROM itg_mds_tw_customer_sales_rep_mapping
                            ) c ON Q.customer = c.customer_name
                        GROUP BY Q.crncy_cd,
                            Q.to_crncy,
                            Q.customer,
                            c.psr_code,
                            c.psr_name,
                            Q.month,
                            Q.year
                    )
                    UNION ALL
                    (
                        SELECT ctry_cd,
                            crncy_cd,
                            to_crncy,
                            strategy_customer_hierachy_name,
                            c.psr_code,
                            c.psr_name,
                            MONTH,
                            YEAR,
                            SUM(offtake_actual) offtake_actual,
                            0 AS offtake_target
                        FROM 
                            (
                                SELECT DISTINCT customer_code,
                                    customer_name,
                                    -- SUBSTRING(customers_name,1,6) AS customer_s_code,
                                    psr_code,
                                    psr_name
                                FROM itg_mds_tw_customer_sales_rep_mapping
                            ) c
                            LEFT JOIN 
                            (
                                SELECT 'Distributor' AS sls_grp,
                                    a.ctry_cd,
                                    'TWD' AS crncy_cd,
                                    a.to_crncy,
                                    CAST(cal.mnth_id AS INTEGER) AS MONTH,
                                    CAST(SUBSTRING(ims_txn_dt, 1, 4) AS INTEGER) AS YEAR,
                                    CASE
                                        WHEN dstr_cd IN ('120812', '123291')
                                        AND dstr_nm IN ('Arich-DS', 'Arich-GPHP') THEN 'Arich (TW-D)'
                                        WHEN dstr_cd IN ('122296', '136454')
                                        AND dstr_nm IN ('Cheng Der Sin', 'Cheng Der Sin (S)') THEN 'Cheng Der Sin (TW-D)'
                                        ELSE target.customer_name
                                    END AS strategy_customer_hierachy_name,
                                    SUM((sls_qty - rtrn_qty) *(sell_in_price_manual)) AS offtake_actual,
                                    0 AS offtake_target
                                FROM v_rpt_dly_plan_ims_enrich a
                                    LEFT JOIN (
                                        SELECT DISTINCT YEAR,
                                            mnth_no,
                                            mnth_id,
                                            qrtr
                                        FROM edw_vw_promo_calendar
                                    ) cal ON SUBSTRING (ims_txn_dt, 1, 4) || DATE_PART (MONTH, ims_txn_dt) = cal.year || cal.mnth_no
                                    LEFT JOIN (
                                        select distinct regexp_replace(code, '[^[:digit:]]', ' ') dist_code,
                                            customer_name
                                        from (
                                                select customer_sname as code,
                                                    customer_name
                                                from itg_tsi_target_data
                                            )
                                    ) target ON a.dstr_cd = target.dist_code
                                WHERE ctry_cd = 'TW'
                                    AND to_crncy = 'TWD'
                                GROUP BY a.ctry_cd,
                                    a.to_crncy,
                                    MONTH,
                                    SUBSTRING(ims_txn_dt, 1, 4),
                                    strategy_customer_hierachy_name
                            ) dist ON dist.strategy_customer_hierachy_name = c.customer_name
                        GROUP BY ctry_cd,
                            crncy_cd,
                            to_crncy,
                            strategy_customer_hierachy_name,
                            c.psr_code,
                            c.psr_name,
                            MONTH,
                            YEAR
                    )
                    UNION ALL
                    SELECT a.cntry_cd,
                        a.crncy_cd,
                        a.to_crncy,
                        a.strategy_customer_hierachy_name,
                        b.psr_code,
                        b.psr_name,
                        CAST(cal.mnth_id AS INTEGER) AS MONTH,
                        forecast_for_year,
                        SUM(gts_act) AS offtake_actual,
                        0 AS offtake_target
                    FROM vw_tw_forecast a
                        LEFT JOIN (
                            SELECT DISTINCT YEAR,
                                mnth_no,
                                mnth_id,
                                qrtr
                            FROM edw_vw_promo_calendar
                        ) cal ON forecast_for_year || forecast_for_mnth = cal.year || cal.mnth_no
                        LEFT JOIN itg_mds_tw_customer_sales_rep_mapping b ON a.strategy_customer_hierachy_name = b.customer_name
                    WHERE subsource_type = 'SAPBW_ACTUAL'
                        AND to_crncy = 'TWD'
                        AND strategy_customer_hierachy_name = 'Costco (Asia/Pacific)'
                    GROUP BY a.cntry_cd,
                        a.crncy_cd,
                        a.to_crncy,
                        a.strategy_customer_hierachy_name,
                        b.psr_code,
                        b.psr_name,
                        cal.mnth_id,
                        forecast_for_year
                    UNION ALL
                    (
                        SELECT 'TW' AS cntry_cd,
                            'TWD' AS crncy_cd,
                            'TWD' AS to_crncy,
                            a.customer_name,
                            b.psr_code,
                            b.psr_name,
                            CAST(year_month AS INTEGER) AS MONTH,
                            CAST(SUBSTRING(year_month, 1, 4) AS INTEGER) AS YEAR,
                            0 AS offtake_actual,
                            sum(a."offtake/sellout") as offtake_target
                        FROM itg_tsi_target_data a
                            LEFT JOIN (
                                SELECT DISTINCT YEAR,
                                    mnth_no,
                                    mnth_id,
                                    qrtr
                                FROM edw_vw_promo_calendar
                            ) cal ON a.year_month = cal.mnth_id
                            LEFT JOIN itg_mds_tw_customer_sales_rep_mapping b ON a.customer_name = b.customer_name
                        GROUP BY a.customer_name,
                            b.psr_code,
                            b.psr_name,
                            year_month,
                            SUBSTRING(year_month, 1, 4)
                    )
                )
            GROUP BY source_type,
                cntry_cd,
                crncy_cd,
                to_crncy,
                psr_code,
                psr_name,
                YEAR,
                mnth_id
        ) main
        LEFT JOIN (
            SELECT incentive_type,
                begin * 100 AS begin,
                end * 100 AS end,
                nts_si,
                offtake_si
            FROM itg_mds_tw_incentive_schemes
            WHERE incentive_type = 'YM'
        ) c ON main.offtake_achievement >= c.begin
        AND main.offtake_achievement <= c.end
        LEFT JOIN (
            SELECT *
            FROM itg_mds_tw_sales_representative
        ) e ON main.psr_code = e.psr_code
        LEFT JOIN (
            SELECT *
            FROM itg_mds_tw_customer_sales_rep_mapping
        ) f ON main.psr_code = f.psr_code
    GROUP BY source_type,
        main.cntry_cd,
        main.crncy_cd,
        main.to_crncy,
        main.psr_code,
        main.psr_name,
        main.year,
        main.mnth_id,
        e.report_to,
        e.reportto_name,
        e.reverse
),
quarterly_offtake as 
(
    SELECT 'Offtake_Q' AS source_type,
        main.cntry_cd,
        main.crncy_cd,
        main.to_crncy,
        main.psr_code,
        main.psr_name,
        main.year,
        main.qrtr,
        cast(NULL AS integer) as mnth_id,
        e.report_to,
        e.reportto_name,
        e.reverse,
        0 AS monthly_actual,
        0 AS monthly_target,
        0 AS monthly_achievement,
        0 AS monthly_incentive_amount,
        cast(AVG(main.offtake_actual) as numeric(38, 4)) AS quarterly_actual,
        cast(AVG(main.offtake_target) as numeric(38, 4)) AS quarterly_target,
        AVG(main.offtake_achievement) AS quarterly_achievement,
        coalesce(
            AVG(
                CASE
                    WHEN c.incentive_type = 'YQ'
                    AND e.reverse = 'True' THEN c.nts_si
                    WHEN d.incentive_type = 'YQ_EC_Bonus'
                    AND f.ec_code = 'Y' THEN d.offtake_si
                    ELSE c.offtake_si
                END
            ),
            0
        ) AS quarterly_incentive_amount
    FROM 
        (
            SELECT 'Offtake_Q' AS source_type,
                cntry_cd,
                crncy_cd,
                to_crncy,
                psr_code,
                psr_name,
                YEAR,
                -- month as mnth_id,
                qrtr,
                SUM(offtake_actual) offtake_actual,
                SUM(offtake_target) offtake_target,
                cast(
                    (
                        SUM(offtake_actual) / NULLIF(SUM(offtake_target), 0)
                    ) * 100 as decimal(10, 2)
                ) as offtake_achievement
            FROM (
                    (
                        SELECT 'TW' AS cntry_cd,
                            Q.crncy_cd,
                            Q.to_crncy,
                            Q.customer,
                            c.psr_code,
                            c.psr_name,
                            Q.month,
                            Q.year,
                            qrtr,
                            SUM(offtake_actual) offtake_actual,
                            0 AS offtake_target
                        FROM 
                            (
                                SELECT 'TW' AS cntry_cd,
                                    a.crncy_cd,
                                    a.to_crncy,
                                    a.sls_grp,
                                    CAST(cal.mnth_id AS INTEGER) AS month,
                                    CAST(SUBSTRING(a.fisc_per, 1, 4) AS INTEGER) AS YEAR,
                                    cal.qrtr,
                                    CASE
                                        WHEN b.sls_grp IN ('Cosmed 康是美', 'Poya 寶雅', 'Watsons 屈臣氏') THEN strategy_customer_hierachy_name
                                    END AS customer,
                                    SUM(sls_amt) offtake_actual,
                                    0 AS offtake_target
                                FROM v_rpt_pos_offtake_wkly_nonkorea a
                                    LEFT JOIN (
                                        select distinct year,
                                            mnth_no,
                                            mnth_id,
                                            qrtr
                                        from edw_vw_promo_calendar
                                    ) cal on SUBSTRING(a.fisc_per, 1, 4) || SUBSTRING(a.fisc_per, 6, 7) = cal.mnth_id
                                    LEFT JOIN itg_sls_grp_to_customer_mapping b ON a.sls_grp = b.sls_grp
                                WHERE ctry_nm = 'Taiwan' --       AND   fisc_per = '2023003'
                                    AND b.sls_grp IN ('Cosmed 康是美', 'Poya 寶雅', 'Watsons 屈臣氏')
                                    AND to_crncy = 'TWD'
                                GROUP BY a.crncy_cd,
                                    a.to_crncy,
                                    a.sls_grp,
                                    cal.mnth_id,
                                    SUBSTRING(a.fisc_per, 1, 4),
                                    cal.qrtr,
                                    customer
                                UNION ALL
                                SELECT 'TW' AS cntry_cd,
                                    a.crncy_cd,
                                    a.to_crncy,
                                    a.sls_grp,
                                    cast(cal.MNTH_ID as integer) as MONTH,
                                    CAST(SUBSTRING(a.univ_per, 1, 4) AS INTEGER) AS YEAR,
                                    cal.qrtr,
                                    CASE
                                        WHEN b.sls_grp IN ('Carrefour 家樂福', 'PX 全聯', 'EC', 'RT-Mart 大潤發') THEN strategy_customer_hierachy_name
                                    END AS customer,
                                    SUM(sls_amt) offtake_actual,
                                    0 AS offtake_target
                                FROM v_rpt_pos_offtake_wkly_nonkorea a
                                    LEFT JOIN itg_sls_grp_to_customer_mapping b ON a.sls_grp = b.sls_grp
                                    LEFT JOIN (
                                        select distinct year,
                                            mnth_no,
                                            mnth_id,
                                            qrtr
                                        from edw_vw_promo_calendar
                                    ) cal on SUBSTRING(a.univ_per, 1, 4) || SUBSTRING(a.univ_per, 6, 7) = cal.mnth_id
                                WHERE ctry_nm = 'Taiwan'
                                    AND b.sls_grp IN ('Carrefour 家樂福', 'PX 全聯', 'EC', 'RT-Mart 大潤發')
                                    AND to_crncy = 'TWD'
                                GROUP BY a.crncy_cd,
                                    a.to_crncy,
                                    a.sls_grp,
                                    cal.mnth_id,
                                    SUBSTRING(a.univ_per, 1, 4),
                                    cal.qrtr,
                                    customer
                            ) Q
                            LEFT JOIN (
                                SELECT DISTINCT customer_code,
                                    customer_name,
                                    psr_code,
                                    psr_name
                                FROM itg_mds_tw_customer_sales_rep_mapping
                            ) c ON Q.customer = c.customer_name
                        GROUP BY Q.crncy_cd,
                            Q.to_crncy,
                            Q.customer,
                            c.psr_code,
                            c.psr_name,
                            Q.month,
                            Q.year,
                            Q.qrtr
                    )
                    UNION ALL
                    (
                        SELECT ctry_cd,
                            crncy_cd,
                            to_crncy,
                            strategy_customer_hierachy_name,
                            c.psr_code,
                            c.psr_name,
                            MONTH,
                            YEAR,
                            qrtr,
                            SUM(offtake_actual) offtake_actual,
                            0 AS offtake_target
                        FROM (
                                SELECT DISTINCT customer_code,
                                    customer_name,
                                    psr_code,
                                    psr_name
                                FROM itg_mds_tw_customer_sales_rep_mapping
                            ) c
                            LEFT JOIN (
                                SELECT 'Distributor' AS sls_grp,
                                    a.ctry_cd,
                                    'TWD' AS crncy_cd,
                                    a.to_crncy,
                                    CAST(cal.mnth_id AS INTEGER) AS MONTH,
                                    CAST(SUBSTRING(ims_txn_dt, 1, 4) AS INTEGER) AS YEAR,
                                    cal.qrtr,
                                    CASE
                                        WHEN dstr_cd IN ('120812', '123291')
                                        AND dstr_nm IN ('Arich-DS', 'Arich-GPHP') THEN 'Arich (TW-D)'
                                        WHEN dstr_cd IN ('122296', '136454')
                                        AND dstr_nm IN ('Cheng Der Sin', 'Cheng Der Sin (S)') THEN 'Cheng Der Sin (TW-D)'
                                        ELSE target.customer_name
                                    END AS strategy_customer_hierachy_name,
                                    SUM((sls_qty - rtrn_qty) *(sell_in_price_manual)) AS offtake_actual,
                                    0 AS offtake_target
                                FROM v_rpt_dly_plan_ims_enrich a
                                    LEFT JOIN (
                                        SELECT DISTINCT YEAR,
                                            mnth_no,
                                            mnth_id,
                                            qrtr
                                        FROM edw_vw_promo_calendar
                                    ) cal ON SUBSTRING (ims_txn_dt, 1, 4) || DATE_PART (MONTH, ims_txn_dt) = cal.year || cal.mnth_no
                                    LEFT JOIN (
                                        SELECT DISTINCT SUBSTRING(customer_sname, 1, 6) AS customer_sname,
                                            customer_name
                                        FROM itg_tsi_target_data
                                    ) target ON a.dstr_cd = target.customer_sname
                                WHERE ctry_cd = 'TW'
                                    AND to_crncy = 'TWD'
                                GROUP BY a.ctry_cd,
                                    a.to_crncy,
                                    MONTH,
                                    cal.qrtr,
                                    SUBSTRING(ims_txn_dt, 1, 4),
                                    strategy_customer_hierachy_name
                            ) dist ON dist.strategy_customer_hierachy_name = c.customer_name
                        GROUP BY ctry_cd,
                            crncy_cd,
                            to_crncy,
                            strategy_customer_hierachy_name,
                            c.psr_code,
                            c.psr_name,
                            MONTH,
                            YEAR,
                            qrtr
                    )
                    UNION ALL
                    SELECT a.cntry_cd,
                        a.crncy_cd,
                        a.to_crncy,
                        a.strategy_customer_hierachy_name,
                        b.psr_code,
                        b.psr_name,
                        CAST(cal.mnth_id AS INTEGER) AS MONTH,
                        forecast_for_year,
                        cal.qrtr,
                        SUM(gts_act) AS offtake_actual,
                        0 AS offtake_target
                    FROM vw_tw_forecast a
                        LEFT JOIN (
                            select distinct year,
                                mnth_no,
                                mnth_id,
                                qrtr
                            from edw_vw_promo_calendar
                        ) cal on forecast_for_year || forecast_for_mnth = cal.year || cal.mnth_no
                        LEFT JOIN itg_mds_tw_customer_sales_rep_mapping b ON a.strategy_customer_hierachy_name = b.customer_name
                    WHERE subsource_type = 'SAPBW_ACTUAL'
                        AND to_crncy = 'TWD'
                        AND strategy_customer_hierachy_name = 'Costco (Asia/Pacific)'
                    GROUP BY a.cntry_cd,
                        a.crncy_cd,
                        a.to_crncy,
                        a.strategy_customer_hierachy_name,
                        b.psr_code,
                        b.psr_name,
                        cal.mnth_id,
                        forecast_for_year,
                        cal.qrtr
                    UNION ALL
                    (
                        SELECT 'TW' AS cntry_cd,
                            'TWD' AS crncy_cd,
                            'TWD' AS to_crncy,
                            a.customer_name,
                            b.psr_code,
                            b.psr_name,
                            CAST(year_month AS INTEGER) AS MONTH,
                            CAST(SUBSTRING(year_month, 1, 4) AS INTEGER) AS YEAR,
                            cal.qrtr,
                            0 AS offtake_actual,
                            sum(a."offtake/sellout") as offtake_target
                        FROM itg_tsi_target_data a
                            LEFT JOIN (
                                select distinct year,
                                    mnth_no,
                                    mnth_id,
                                    qrtr
                                from edw_vw_promo_calendar
                            ) cal on a.year_month = cal.mnth_id
                            LEFT JOIN itg_mds_tw_customer_sales_rep_mapping b ON a.customer_name = b.customer_name --  WHERE year_month = '202303'
                        GROUP BY a.customer_name,
                            b.psr_code,
                            b.psr_name,
                            year_month,
                            SUBSTRING(year_month, 1, 4),
                            cal.qrtr
                    )
                )
            GROUP BY source_type,
                cntry_cd,
                crncy_cd,
                to_crncy,
                psr_code,
                psr_name,
                YEAR,
                -- mnth_id,
                qrtr
        ) main
        LEFT JOIN (
            SELECT incentive_type,
                begin * 100 AS begin,
                end * 100 AS end,
                nts_si,
                offtake_si
            FROM itg_mds_tw_incentive_schemes
            WHERE incentive_type = 'YQ'
        ) c ON main.offtake_achievement >= c.begin
        AND main.offtake_achievement <= c.end
        LEFT JOIN (
            SELECT incentive_type,
                begin * 100 AS begin,
                end * 100 AS end,
                nts_si,
                offtake_si
            FROM itg_mds_tw_incentive_schemes
            WHERE incentive_type = 'YQ_EC_Bonus'
        ) d ON main.offtake_achievement >= d.begin
        AND main.offtake_achievement <= d.end
        LEFT JOIN (
            SELECT *
            FROM itg_mds_tw_sales_representative
        ) e ON main.psr_code = e.psr_code
        LEFT JOIN (
            SELECT *
            FROM itg_mds_tw_customer_sales_rep_mapping
        ) f ON main.psr_code = f.psr_code
    GROUP BY source_type,
        main.cntry_cd,
        main.crncy_cd,
        main.to_crncy,
        main.psr_code,
        main.psr_name,
        main.year,
        -- main.mnth_id
        main.qrtr,
        e.report_to,
        e.reportto_name,
        e.reverse
),
ec_customer_offtake_data as 
(
    SELECT 'Offtake_Q_EC' AS source_type,
        main.cntry_cd,
        main.crncy_cd,
        main.to_crncy,
        main.psr_code,
        main.psr_name,
        main.year,
        main.qrtr,
        cast(NULL AS integer) as mnth_id,
        e.report_to,
        e.reportto_name,
        e.reverse,
        0 AS monthly_actual,
        0 AS monthly_target,
        0 AS monthly_achievement,
        0 AS monthly_incentive_amount,
        cast(AVG(main.offtake_actual) as numeric(38, 4)) AS quarterly_actual,
        cast(AVG(main.offtake_target) as numeric(38, 4)) AS quarterly_target,
        AVG(main.offtake_achievement) AS quarterly_achievement,
        coalesce(
            AVG(
                CASE
                    WHEN d.incentive_type = 'YQ_EC'
                    AND f.ec_code = 'Y' THEN d.offtake_si
                    ELSE 0
                END
            ),
            0
        ) AS quarterly_incentive_amount
    FROM (
            SELECT 'Offtake_Q_EC' AS source_type,
                cntry_cd,
                crncy_cd,
                to_crncy,
                psr_code,
                psr_name,
                YEAR,
                -- month as mnth_id,
                qrtr,
                SUM(offtake_actual) offtake_actual,
                SUM(offtake_target) offtake_target,
                cast(
                    (
                        SUM(offtake_actual) / NULLIF(SUM(offtake_target), 0)
                    ) * 100 as decimal(10, 2)
                ) as offtake_achievement
            FROM (
                    (
                        SELECT 'TW' AS cntry_cd,
                            Q.crncy_cd,
                            Q.to_crncy,
                            Q.customer,
                            c.psr_code,
                            c.psr_name,
                            Q.month,
                            Q.year,
                            qrtr,
                            SUM(offtake_actual) offtake_actual,
                            0 AS offtake_target
                        FROM (
                                SELECT 'TW' AS cntry_cd,
                                    a.crncy_cd,
                                    a.to_crncy,
                                    a.sls_grp,
                                    CAST(cal.mnth_id AS INTEGER) AS month,
                                    CAST(SUBSTRING(a.fisc_per, 1, 4) AS INTEGER) AS YEAR,
                                    cal.qrtr,
                                    CASE
                                        WHEN b.sls_grp IN ('Cosmed 康是美', 'Poya 寶雅', 'Watsons 屈臣氏') THEN strategy_customer_hierachy_name
                                    END AS customer,
                                    SUM(sls_amt) offtake_actual,
                                    0 AS offtake_target
                                FROM v_rpt_pos_offtake_wkly_nonkorea a
                                    LEFT JOIN (
                                        select distinct year,
                                            mnth_no,
                                            mnth_id,
                                            qrtr
                                        from edw_vw_promo_calendar
                                    ) cal on SUBSTRING(a.fisc_per, 1, 4) || SUBSTRING(a.fisc_per, 6, 7) = cal.mnth_id
                                    LEFT JOIN itg_sls_grp_to_customer_mapping b ON a.sls_grp = b.sls_grp
                                WHERE ctry_nm = 'Taiwan'
                                    AND b.sls_grp IN ('Cosmed 康是美', 'Poya 寶雅', 'Watsons 屈臣氏')
                                    AND to_crncy = 'TWD'
                                GROUP BY a.crncy_cd,
                                    a.to_crncy,
                                    a.sls_grp,
                                    cal.mnth_id,
                                    SUBSTRING(a.fisc_per, 1, 4),
                                    cal.qrtr,
                                    customer
                                UNION ALL
                                SELECT 'TW' AS cntry_cd,
                                    a.crncy_cd,
                                    a.to_crncy,
                                    a.sls_grp,
                                    cast(cal.MNTH_ID as integer) as MONTH,
                                    CAST(SUBSTRING(a.univ_per, 1, 4) AS INTEGER) AS YEAR,
                                    cal.qrtr,
                                    CASE
                                        WHEN b.sls_grp IN ('Carrefour 家樂福', 'PX 全聯', 'EC', 'RT-Mart 大潤發') THEN strategy_customer_hierachy_name
                                    END AS customer,
                                    SUM(sls_amt) offtake_actual,
                                    0 AS offtake_target
                                FROM v_rpt_pos_offtake_wkly_nonkorea a
                                    LEFT JOIN itg_sls_grp_to_customer_mapping b ON a.sls_grp = b.sls_grp
                                    LEFT JOIN (
                                        select distinct year,
                                            mnth_no,
                                            mnth_id,
                                            qrtr
                                        from edw_vw_promo_calendar
                                    ) cal on SUBSTRING(a.univ_per, 1, 4) || SUBSTRING(a.univ_per, 6, 7) = cal.mnth_id
                                WHERE ctry_nm = 'Taiwan'
                                    AND b.sls_grp IN ('Carrefour 家樂福', 'PX 全聯', 'EC', 'RT-Mart 大潤發')
                                    AND to_crncy = 'TWD'
                                GROUP BY a.crncy_cd,
                                    a.to_crncy,
                                    a.sls_grp,
                                    cal.mnth_id,
                                    SUBSTRING(a.univ_per, 1, 4),
                                    cal.qrtr,
                                    customer
                            ) Q
                            LEFT JOIN (
                                SELECT DISTINCT customer_code,
                                    customer_name,
                                    psr_code,
                                    psr_name
                                FROM itg_mds_tw_customer_sales_rep_mapping
                            ) c ON Q.customer = c.customer_name
                        GROUP BY Q.crncy_cd,
                            Q.to_crncy,
                            Q.customer,
                            c.psr_code,
                            c.psr_name,
                            Q.month,
                            Q.year,
                            Q.qrtr
                    )
                    UNION ALL
                    (
                        SELECT ctry_cd,
                            crncy_cd,
                            to_crncy,
                            strategy_customer_hierachy_name,
                            c.psr_code,
                            c.psr_name,
                            MONTH,
                            YEAR,
                            qrtr,
                            SUM(offtake_actual) offtake_actual,
                            0 AS offtake_target
                        FROM (
                                SELECT DISTINCT customer_code,
                                    customer_name,
                                    psr_code,
                                    psr_name
                                FROM itg_mds_tw_customer_sales_rep_mapping
                            ) c
                            LEFT JOIN (
                                SELECT 'Distributor' AS sls_grp,
                                    a.ctry_cd,
                                    'TWD' AS crncy_cd,
                                    a.to_crncy,
                                    CAST(cal.mnth_id AS INTEGER) AS MONTH,
                                    CAST(SUBSTRING(ims_txn_dt, 1, 4) AS INTEGER) AS YEAR,
                                    cal.qrtr,
                                    CASE
                                        WHEN dstr_cd IN ('120812', '123291')
                                        AND dstr_nm IN ('Arich-DS', 'Arich-GPHP') THEN 'Arich (TW-D)'
                                        WHEN dstr_cd IN ('122296', '136454')
                                        AND dstr_nm IN ('Cheng Der Sin', 'Cheng Der Sin (S)') THEN 'Cheng Der Sin (TW-D)'
                                        ELSE target.customer_name
                                    END AS strategy_customer_hierachy_name,
                                    SUM((sls_qty - rtrn_qty) *(sell_in_price_manual)) AS offtake_actual,
                                    0 AS offtake_target
                                FROM v_rpt_dly_plan_ims_enrich a
                                    LEFT JOIN (
                                        SELECT DISTINCT YEAR,
                                            mnth_no,
                                            mnth_id,
                                            qrtr
                                        FROM edw_vw_promo_calendar
                                    ) cal ON SUBSTRING (ims_txn_dt, 1, 4) || DATE_PART (MONTH, ims_txn_dt) = cal.year || cal.mnth_no
                                    LEFT JOIN (
                                        SELECT DISTINCT SUBSTRING(customer_sname, 1, 6) AS customer_sname,
                                            customer_name
                                        FROM itg_tsi_target_data
                                    ) target ON a.dstr_cd = target.customer_sname
                                WHERE ctry_cd = 'TW'
                                    AND to_crncy = 'TWD'
                                GROUP BY a.ctry_cd,
                                    a.to_crncy,
                                    MONTH,
                                    cal.qrtr,
                                    SUBSTRING(ims_txn_dt, 1, 4),
                                    strategy_customer_hierachy_name
                            ) dist ON dist.strategy_customer_hierachy_name = c.customer_name
                        GROUP BY ctry_cd,
                            crncy_cd,
                            to_crncy,
                            strategy_customer_hierachy_name,
                            c.psr_code,
                            c.psr_name,
                            MONTH,
                            YEAR,
                            qrtr
                    )
                    UNION ALL
                    SELECT a.cntry_cd,
                        a.crncy_cd,
                        a.to_crncy,
                        a.strategy_customer_hierachy_name,
                        b.psr_code,
                        b.psr_name,
                        CAST(cal.mnth_id AS INTEGER) AS MONTH,
                        forecast_for_year,
                        cal.qrtr,
                        SUM(gts_act) AS offtake_actual,
                        0 AS offtake_target
                    FROM vw_tw_forecast a
                        LEFT JOIN (
                            select distinct year,
                                mnth_no,
                                mnth_id,
                                qrtr
                            from edw_vw_promo_calendar
                        ) cal on forecast_for_year || forecast_for_mnth = cal.year || cal.mnth_no
                        LEFT JOIN itg_mds_tw_customer_sales_rep_mapping b ON a.strategy_customer_hierachy_name = b.customer_name
                    WHERE subsource_type = 'SAPBW_ACTUAL'
                        AND to_crncy = 'TWD'
                        AND strategy_customer_hierachy_name = 'Costco (Asia/Pacific)'
                    GROUP BY a.cntry_cd,
                        a.crncy_cd,
                        a.to_crncy,
                        a.strategy_customer_hierachy_name,
                        b.psr_code,
                        b.psr_name,
                        cal.mnth_id,
                        forecast_for_year,
                        cal.qrtr
                    UNION ALL
                    (
                        SELECT 'TW' AS cntry_cd,
                            'TWD' AS crncy_cd,
                            'TWD' AS to_crncy,
                            a.customer_name,
                            b.psr_code,
                            b.psr_name,
                            CAST(year_month AS INTEGER) AS MONTH,
                            CAST(SUBSTRING(year_month, 1, 4) AS INTEGER) AS YEAR,
                            cal.qrtr,
                            0 AS offtake_actual,
                            sum(a."offtake/sellout") as offtake_target
                        FROM itg_tsi_target_data a
                            LEFT JOIN (
                                select distinct year,
                                    mnth_no,
                                    mnth_id,
                                    qrtr
                                from edw_vw_promo_calendar
                            ) cal on a.year_month = cal.mnth_id
                            LEFT JOIN itg_mds_tw_customer_sales_rep_mapping b ON a.customer_name = b.customer_name
                        GROUP BY a.customer_name,
                            b.psr_code,
                            b.psr_name,
                            year_month,
                            SUBSTRING(year_month, 1, 4),
                            cal.qrtr
                    )
                )
            GROUP BY source_type,
                cntry_cd,
                crncy_cd,
                to_crncy,
                psr_code,
                psr_name,
                YEAR,
                -- mnth_id,
                qrtr
        ) main
        LEFT JOIN (
            SELECT incentive_type,
                begin * 100 AS begin,
                end * 100 AS end,
                nts_si,
                offtake_si
            FROM itg_mds_tw_incentive_schemes
            WHERE incentive_type = 'YQ_EC'
        ) d ON main.offtake_achievement >= d.begin
        AND main.offtake_achievement <= d.end
        LEFT JOIN (
            SELECT *
            FROM itg_mds_tw_sales_representative
        ) e ON main.psr_code = e.psr_code
        LEFT JOIN (
            SELECT *
            FROM itg_mds_tw_customer_sales_rep_mapping
        ) f ON main.psr_code = f.psr_code
    where f.ec_code = 'Y'
    GROUP BY source_type,
        main.cntry_cd,
        main.crncy_cd,
        main.to_crncy,
        main.psr_code,
        main.psr_name,
        main.year,
        -- main.mnth_id
        main.qrtr,
        e.report_to,
        e.reportto_name,
        e.reverse
),
final as 
(
    select * from
    (
        select * from monthly_offtake
        union all
        select * from quarterly_offtake
        union all
        select * from ec_customer_offtake_data
    )
    where year >= (DATE_PART(YEAR, current_date) -2)
)
select * from final