with 
vw_tw_forecast as
(
    select * from {{ ref('ntaedw_integration__vw_tw_forecast') }}
),
edw_vw_promo_calendar as
(
    select * from {{ ref('ntaedw_integration__edw_vw_promo_calendar') }}
),
itg_tsi_target_data as
(
    select * from {{ ref('ntaitg_integration__itg_tsi_target_data') }}
),
itg_mds_tw_incentive_schemes as
(
    select * from {{ ref('ntaitg_integration__itg_mds_tw_incentive_schemes') }}
),
itg_mds_tw_sales_representative as
(
    select * from {{ source('ntaitg_integration', 'itg_mds_tw_sales_representative') }}
), --as a source
itg_mds_tw_customer_sales_rep_mapping as 
(
    select * from {{ source('ntaitg_integration', 'itg_mds_tw_customer_sales_rep_mapping') }}
), --as a source
v_rpt_copa_ciw as
(
    select * from {{ ref('aspedw_integration__v_rpt_copa_ciw') }}
), 
edw_customer_attr_hier_dim as 
(
    select * from {{ source('aspedw_integration', 'edw_customer_attr_hier_dim') }}
), 
trans as
(
    select *
from (
        (
            SELECT 'TP_M' AS source_type,
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
                cast(AVG(main.tp_actual) as numeric(38, 4)) AS monthly_actual,
                cast(AVG(main.tp_target) as numeric(38, 4)) AS monthly_target,
                AVG(main.tp_achievement) AS monthly_achievement,
                AVG(
                    case
                        when c.incentive_type = 'YM_TP'
                        and (main.tp_actual) < (main.tp_target) then (c.tp_si)
                        WHEN (main.tp_actual) > (main.tp_target) then 0
                        else 0
                    end
                ) as monthly_incentive_amount,
                0 AS quarterly_actual,
                0 AS quarterly_target,
                0 AS quarterly_achievement,
                0 AS quarterly_incentive_amount
            from (
                    SELECT 'TP_M' AS source_type,
                        Q.cntry_cd,
                        Q.crncy_cd,
                        Q.to_crncy,
                        Q.psr_code,
                        Q.psr_name,
                        Q.year,
                        NULL AS qrtr,
                        Q.mnth_id,
                        sum(Q.tp_actual) AS tp_actual,
                        sum(Q.tp_target) AS tp_target,
                        cast(
                            (SUM(tp_actual) / NULLIF(SUM(tp_target), 0)) * 100 as decimal(10, 2)
                        ) as tp_achievement
                    from (
                            SELECT cntry_cd,
                                crncy_cd,
                                to_crncy,
                                CAST(forecast_for_year AS INTEGER) AS YEAR,
                                CAST(cal.mnth_id AS INTEGER) AS mnth_id,
                                b.psr_code,
                                b.psr_name,
                                cast(
                                    (
                                        sum(a.gts_act - a.nts_act) / sum(nullif(a.gts_act, 0))
                                    ) * 100 as decimal(10, 2)
                                ) as tp_actual,
                                0 AS tp_target
                            FROM vw_tw_forecast a
                                LEFT JOIN (
                                    SELECT DISTINCT YEAR,
                                        mnth_no,
                                        mnth_id
                                    FROM edw_vw_promo_calendar
                                ) cal ON forecast_for_year || forecast_for_mnth = cal.year || cal.mnth_no
                                LEFT JOIN (
                                    select distinct customer_name,
                                        psr_code,
                                        psr_name
                                    from itg_mds_tw_customer_sales_rep_mapping
                                ) b ON a.strategy_customer_hierachy_name = b.customer_name
                            WHERE subsource_type = 'SAPBW_ACTUAL'
                                AND to_crncy = 'TWD'
                            GROUP BY cntry_cd,
                                crncy_cd,
                                to_crncy,
                                forecast_for_year,
                                cal.mnth_id,
                                b.psr_code,
                                b.psr_name
                            UNION ALL
                            SELECT 'TW' AS cntry_cd,
                                'TWD' AS crncy_cd,
                                'TWD' AS to_crncy,
                                CAST(SUBSTRING(year_month, 1, 4) AS INTEGER) AS YEAR,
                                CAST(year_month AS INTEGER) AS mnth_id,
                                b.psr_code,
                                b.psr_name,
                                0 AS tp_actual,
                                cast(
                                    (sum(pre_sales - NTS) / sum(nullif(pre_sales, 0))) * 100 as decimal(10, 2)
                                ) as tp_target
                            FROM itg_tsi_target_data a
                                LEFT JOIN (
                                    select distinct customer_name,
                                        psr_code,
                                        psr_name
                                    from itg_mds_tw_customer_sales_rep_mapping
                                ) b ON a.customer_name = b.customer_name
                            GROUP BY cntry_cd,
                                crncy_cd,
                                to_crncy,
                                SUBSTRING(year_month, 1, 4),
                                year_month,
                                b.psr_code,
                                b.psr_name
                        ) Q
                    GROUP BY source_type,
                        cntry_cd,
                        crncy_cd,
                        to_crncy,
                        Q.psr_code,
                        Q.psr_name,
                        Q.YEAR,
                        Q.mnth_id
                ) main
                LEFT JOIN (
                    SELECT distinct incentive_type,
                        begin * 100 AS begin,
                        end * 100 AS end,
                        tp_si
                    FROM itg_mds_tw_incentive_schemes
                    WHERE incentive_type = 'YM_TP'
                ) c ON main.tp_achievement >= c.begin
                or main.tp_achievement <= c.end
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
        ) 
        union all
        (
            SELECT 'CIW_Q' AS source_type,
                main.cntry_cd,
                main.obj_crncy_co_obj,
                main.from_crncy,
                main.psr_code,
                main.psr_name,
                main.year,
                main.qrtr,
                NULL AS mnth_id,
                e.report_to,
                e.reportto_name,
                e.reverse,
                0 AS monthly_actual,
                0 AS monthly_target,
                0 AS monthly_achievement,
                0 AS monthly_incentive_amount,
                cast(AVG(main.ciw_actual) as numeric(38, 4)) AS quarterly_actual,
                cast(AVG(main.ciw_target) as numeric(38, 4)) AS quarterly_target,
                AVG(main.ciw_achievement) AS quarterly_achievement,
                AVG(
                    CASE
                        WHEN c.incentive_type = 'YQ_CIW'
                        AND (main.ciw_actual) <(main.ciw_target) THEN (c.ciw_si)
                        WHEN d.incentive_type = 'YQ_EC_Bonus_CIW'
                        AND f.ec_code = 'Y' THEN (d.ciw_si)
                        WHEN (main.ciw_actual) >(main.ciw_target) THEN 0
                        else 0
                    END
                ) AS quarterly_incentive_amount
            FROM (
                    SELECT 'CIW_Q' AS source_type,
                        cntry_cd,
                        from_crncy,
                        obj_crncy_co_obj,
                        psr_code,
                        psr_name,
                        YEAR,
                        qrtr,
                        SUM(ciw_actual) ciw_actual,
                        SUM(ciw_target) ciw_target,
                        cast(
                            (SUM(ciw_actual) / NULLIF(SUM(ciw_target), 0)) * 100 as decimal(10, 2)
                        ) as ciw_achievement
                    FROM (
                            SELECT cntry_cd,
                                from_crncy,
                                obj_crncy_co_obj,
                                psr_code,
                                psr_name,
                                YEAR,
                                qrtr,
                                cast(
                                    (SUM(GTS - NTS) / SUM(NULLIF(GTS, 0))) * 100 as decimal(10, 2)
                                ) AS CIW_actual,
                                0 AS ciw_target
                            FROM (
                                    SELECT 'TW' AS cntry_cd,
                                        from_crncy,
                                        obj_crncy_co_obj,
                                        strategy_customer_hierachy_name,
                                        fisc_yr AS YEAR,
                                        qrtr,
                                        c.psr_code,
                                        c.psr_name,
                                        SUM(GTS) GTS,
                                        SUM(NTS) NTS
                                    FROM (
                                            SELECT 'TW' AS cntry_cd,
                                                from_crncy,
                                                obj_crncy_co_obj,
                                                cust_num,
                                                strategy_customer_hierachy_name,
                                                fisc_yr,
                                                cal.mnth_id,
                                                cal.qrtr,
                                                SUM(
                                                    CASE
                                                        WHEN acct_hier_shrt_desc = 'NTS'
                                                        AND ciw_desc = 'Gross Trade Prod Sales' THEN amt_lcy
                                                    END
                                                ) AS GTS,
                                                SUM(
                                                    CASE
                                                        WHEN acct_hier_shrt_desc = 'NTS' THEN amt_lcy
                                                    END
                                                ) AS NTS
                                            FROM v_rpt_copa_ciw CIW
                                                LEFT JOIN (
                                                    SELECT DISTINCT YEAR,
                                                        mnth_no,
                                                        mnth_id,
                                                        qrtr
                                                    FROM edw_vw_promo_calendar
                                                ) cal ON SUBSTRING (ciw.fisc_day, 1, 4) || DATE_PART (MONTH, ciw.fisc_day) = cal.year || cal.mnth_no
                                                LEFT JOIN (
                                                    SELECT edw_customer_attr_hier_dim.sold_to_party,
                                                        "max"(
                                                            edw_customer_attr_hier_dim.strategy_customer_hierachy_name::TEXT
                                                        ) AS strategy_customer_hierachy_name
                                                    FROM edw_customer_attr_hier_dim
                                                    WHERE edw_customer_attr_hier_dim.cntry::TEXT = 'Taiwan'::CHARACTER VARYING::TEXT
                                                    GROUP BY edw_customer_attr_hier_dim.sold_to_party
                                                    ORDER BY edw_customer_attr_hier_dim.sold_to_party
                                                ) b ON LTRIM (ciw.cust_num, '0') = b.sold_to_party
                                            WHERE ctry_nm = 'Taiwan'
                                                AND from_crncy = 'TWD'
                                            GROUP BY from_crncy,
                                                obj_crncy_co_obj,
                                                cust_num,
                                                strategy_customer_hierachy_name,
                                                fisc_yr,
                                                cal.mnth_id,
                                                cal.qrtr
                                        ) a
                                        LEFT JOIN (
                                            SELECT DISTINCT customer_code,
                                                customer_name,
                                                psr_code,
                                                psr_name
                                            FROM itg_mds_tw_customer_sales_rep_mapping
                                        ) c ON a.strategy_customer_hierachy_name = c.customer_name
                                    GROUP BY cntry_cd,
                                        from_crncy,
                                        obj_crncy_co_obj,
                                        strategy_customer_hierachy_name,
                                        YEAR,
                                        qrtr,
                                        psr_code,
                                        c.psr_name
                                )
                            GROUP BY cntry_cd,
                                from_crncy,
                                obj_crncy_co_obj,
                                psr_code,
                                psr_name,
                                YEAR,
                                qrtr
                            UNION ALL
                            SELECT cntry_cd,
                                crncy_cd,
                                to_crncy,
                                psr_code,
                                psr_name,
                                YEAR,
                                qrtr,
                                0 AS ciw_actual,
                                cast(
                                    (SUM(GTS - NTS) / SUM(NULLIF(GTS, 0))) * 100 as decimal(10, 2)
                                ) AS ciw_target
                            FROM (
                                    SELECT 'TW' AS cntry_cd,
                                        'TWD' AS crncy_cd,
                                        'TWD' AS to_crncy,
                                        a.customer_name,
                                        CAST(SUBSTRING(year_month, 1, 4) AS INTEGER) AS YEAR,
                                        cal.qrtr,
                                        b.psr_code,
                                        b.psr_name,
                                        SUM(GTS) GTS,
                                        SUM(NTS) NTS
                                    FROM itg_tsi_target_data a
                                        LEFT JOIN (
                                            SELECT DISTINCT customer_name,
                                                psr_code,
                                                psr_name
                                            FROM itg_mds_tw_customer_sales_rep_mapping
                                        ) b ON a.customer_name = b.customer_name
                                        LEFT JOIN (
                                            SELECT DISTINCT YEAR,
                                                mnth_no,
                                                mnth_id,
                                                qrtr
                                            FROM edw_vw_promo_calendar
                                        ) cal ON a.year_month = cal.mnth_id
                                    GROUP BY cntry_cd,
                                        crncy_cd,
                                        to_crncy,
                                        a.customer_name,
                                        SUBSTRING(year_month, 1, 4),
                                        --year_month,
                                        cal.qrtr,
                                        b.psr_code,
                                        b.psr_name
                                )
                            GROUP BY cntry_cd,
                                crncy_cd,
                                to_crncy,
                                psr_code,
                                psr_name,
                                YEAR,
                                qrtr
                        )
                    GROUP BY source_type,
                        cntry_cd,
                        from_crncy,
                        obj_crncy_co_obj,
                        psr_code,
                        psr_name,
                        YEAR,
                        qrtr
                ) main
                LEFT JOIN (
                    SELECT distinct incentive_type,
                        begin * 100 AS begin,
                        end * 100 AS end,
                        ciw_si
                    FROM itg_mds_tw_incentive_schemes
                    WHERE incentive_type = 'YQ_CIW'
                ) c ON main.ciw_achievement >= c.begin
                AND main.ciw_achievement <= c.end
                LEFT JOIN (
                    SELECT distinct incentive_type,
                        begin * 100 AS begin,
                        end * 100 AS end,
                        ciw_si
                    FROM itg_mds_tw_incentive_schemes
                    WHERE incentive_type = 'YQ_EC_Bonus_CIW'
                ) d ON main.ciw_achievement >= d.begin
                AND main.ciw_achievement <= d.end
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
                main.obj_crncy_co_obj,
                main.from_crncy,
                main.psr_code,
                main.psr_name,
                main.year,
                main.qrtr,
                e.report_to,
                e.reportto_name,
                e.reverse
        )
        union all
        (
            SELECT 'TP_Q_EC' AS source_type,
                main.cntry_cd,
                main.crncy_cd,
                main.to_crncy,
                main.psr_code,
                main.psr_name,
                main.year,
                main.qrtr,
                NULL AS mnth_id,
                e.report_to,
                e.reportto_name,
                e.reverse,
                0 AS monthly_actual,
                0 AS monthly_target,
                0 AS monthly_achievement,
                0 AS monthly_incentive_amount,
                cast(AVG(main.tp_actual) as numeric(38, 4)) AS quarterly_actual,
                cast(AVG(main.tp_target) as numeric(38, 4)) AS quarterly_target,
                AVG(main.tp_achievement) AS quarterly_achievement,
                AVG(
                    case
                        WHEN d.incentive_type = 'YQ_EC_TP'
                        AND f.ec_code = 'Y' THEN (d.tp_si)
                        WHEN (main.tp_actual) > (main.tp_target) then 0
                        else 0
                    end
                ) as quarterly_incentive_amount
            from(
                    SELECT 'TP_Q_EC' AS source_type,
                        Q.cntry_cd,
                        Q.crncy_cd,
                        Q.to_crncy,
                        Q.psr_code,
                        Q.psr_name,
                        Q.year,
                        Q.qrtr,
                        sum(Q.tp_actual) AS tp_actual,
                        sum(Q.tp_target) AS tp_target,
                        cast(
                            (SUM(tp_actual) / NULLIF(SUM(tp_target), 0)) * 100 as decimal(10, 2)
                        ) as tp_achievement
                    from (
                            select cntry_cd,
                                crncy_cd,
                                to_crncy,
                                YEAR,
                                qrtr,
                                psr_code,
                                psr_name,
                                cast(
                                    (sum(gts_act - nts_act) / sum(nullif(gts_act, 0))) * 100 as decimal(10, 2)
                                ) as tp_actual,
                                0 AS tp_target
                            from (
                                    SELECT cntry_cd,
                                        crncy_cd,
                                        to_crncy,
                                        CAST(forecast_for_year AS INTEGER) AS YEAR,
                                        CAST(cal.mnth_id AS INTEGER) AS mnth_id,
                                        qrtr,
                                        b.psr_code,
                                        b.psr_name,
                                        sum(gts_act) gts_act,
                                        sum(nts_act) nts_act
                                    FROM vw_tw_forecast a
                                        LEFT JOIN (
                                            SELECT DISTINCT YEAR,
                                                mnth_no,
                                                mnth_id,
                                                qrtr
                                            FROM edw_vw_promo_calendar
                                        ) cal ON forecast_for_year || forecast_for_mnth = cal.year || cal.mnth_no
                                        LEFT JOIN (
                                            select distinct customer_name,
                                                psr_code,
                                                psr_name
                                            from itg_mds_tw_customer_sales_rep_mapping
                                        ) b ON a.strategy_customer_hierachy_name = b.customer_name
                                    WHERE subsource_type = 'SAPBW_ACTUAL'
                                        AND to_crncy = 'TWD'
                                    GROUP BY cntry_cd,
                                        crncy_cd,
                                        to_crncy,
                                        forecast_for_year,
                                        qrtr,
                                        cal.mnth_id,
                                        b.psr_code,
                                        b.psr_name
                                )
                            group by cntry_cd,
                                crncy_cd,
                                to_crncy,
                                YEAR,
                                qrtr,
                                psr_code,
                                psr_name
                            UNION ALL
                            SELECT 'TW' AS cntry_cd,
                                'TWD' AS crncy_cd,
                                'TWD' AS to_crncy,
                                CAST(SUBSTRING(year_month, 1, 4) AS INTEGER) AS YEAR,
                                cal.qrtr,
                                b.psr_code,
                                b.psr_name,
                                0 AS tp_actual,
                                cast(
                                    (sum(pre_sales - NTS) / sum(nullif(pre_sales, 0))) * 100 as decimal(10, 2)
                                ) as tp_target
                            FROM itg_tsi_target_data a
                                LEFT JOIN (
                                    select distinct customer_name,
                                        psr_code,
                                        psr_name
                                    from itg_mds_tw_customer_sales_rep_mapping
                                ) b ON a.customer_name = b.customer_name
                                LEFT JOIN (
                                    SELECT DISTINCT YEAR,
                                        mnth_no,
                                        mnth_id,
                                        qrtr
                                    FROM edw_vw_promo_calendar
                                ) cal ON a.year_month = cal.mnth_id
                            GROUP BY cntry_cd,
                                crncy_cd,
                                to_crncy,
                                SUBSTRING(year_month, 1, 4),
                                cal.qrtr,
                                b.psr_code,
                                b.psr_name
                        ) Q
                    GROUP BY Q.cntry_cd,
                        Q.crncy_cd,
                        Q.to_crncy,
                        Q.psr_code,
                        Q.psr_name,
                        Q.YEAR,
                        Q.qrtr
                ) main
                LEFT JOIN (
                    SELECT distinct incentive_type,
                        begin * 100 AS begin,
                        end * 100 AS end,
                        tp_si,
                        ciw_si
                    FROM itg_mds_tw_incentive_schemes
                    WHERE incentive_type = 'YQ_EC_TP'
                ) d ON main.tp_achievement >= d.begin
                AND main.tp_achievement <= d.end
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
                main.qrtr,
                e.report_to,
                e.reportto_name,
                e.reverse
        )
    )
where year >= (DATE_PART(YEAR, current_timestamp()) -2)
),
final as
(
  select 
    source_type::varchar(7) as source_type,
	cntry_cd::varchar(2) as cntry_cd,
	crncy_cd::varchar(5) as crncy_cd,
	to_crncy::varchar(5) as to_crncy,
	psr_code::varchar(100) as psr_code,
	psr_name::varchar(255) as psr_name,
	year::number(18,0) as year,
	qrtr::varchar(14) as qrtr,
	mnth_id::number(18,0) as mnth_id,
	report_to::varchar(500) as report_to,
	reportto_name::varchar(500) as reportto_name,
	reverse::varchar(500) as reverse,
	monthly_actual::number(38,4) as monthly_actual,
	monthly_target::number(38,4) as monthly_target,
	monthly_achievement::number(38,2) as monthly_achievement,
	monthly_incentive_amount::number(38,4) as monthly_incentive_amount,
	quarterly_actual::number(38,4) as quarterly_actual,
	quarterly_target::number(38,4) as quarterly_target,
	quarterly_achievement::number(38,2) as quarterly_achievement,
	quarterly_incentive_amount::number(38,4) as quarterly_incentive_amount,
    from trans
)
select * from final