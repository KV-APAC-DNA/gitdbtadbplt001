with vw_tw_forecast as (
select * from {{ ref('ntaedw_integration__vw_tw_forecast') }}
),
edw_vw_promo_calendar as (
select * from {{ ref('ntaedw_integration__edw_vw_promo_calendar') }}
),
itg_mds_tw_customer_sales_rep_mapping as (
select * from {{ ref('ntaitg_integration__itg_mds_tw_customer_sales_rep_mapping') }}
),
itg_tsi_target_data as (
select * from {{ ref('ntaitg_integration__itg_tsi_target_data') }}
),
itg_mds_tw_incentive_schemes as (
select * from {{ ref('ntaitg_integration__itg_mds_tw_incentive_schemes') }}
),
itg_mds_tw_sales_representative as (
select * from {{ ref('ntaitg_integration__itg_mds_tw_sales_representative') }}
),
itg_mds_tw_customer_sales_rep_mapping as (
select * from {{ ref('ntaitg_integration__itg_mds_tw_customer_sales_rep_mapping') }}
),
final as (
SELECT *
FROM (
       (
              SELECT 'NTS_M' AS source_type,
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
                     cast(AVG(main.nts_actual) AS NUMERIC(38, 4)) AS monthly_actual,
                     cast(AVG(main.nts_target) AS NUMERIC(38, 4)) AS monthly_target,
                     AVG(main.nts_achievement) AS monthly_achievement,
                     AVG(CASE 
                                   WHEN c.incentive_type = 'YM'
                                          AND e.reverse = 'True'
                                          THEN c.offtake_si
                                   ELSE c.nts_si
                                   END) AS monthly_incentive_amount,
                     0 AS quarterly_actual,
                     0 AS quarterly_target,
                     0 AS quarterly_achievement,
                     0 AS quarterly_incentive_amount
              FROM (
                     SELECT 'NTS_M' AS source_type,
                            cntry_cd,
                            crncy_cd,
                            to_crncy,
                            psr_code,
                            psr_name,
                            YEAR,
                            mnth_id,
                            SUM(nts_actual) nts_actual,
                            SUM(nts_target) nts_target,
                            cast((SUM(nts_actual) / NULLIF(SUM(nts_target), 0)) * 100 AS DECIMAL(10, 2)) nts_achievement
                     FROM (
                            SELECT cntry_cd,
                                   crncy_cd,
                                   to_crncy,
                                   strategy_customer_hierachy_name,
                                   CAST(forecast_for_year AS INTEGER) AS YEAR,
                                   CAST(cal.mnth_id AS INTEGER) AS mnth_id,
                                   b.psr_code,
                                   b.psr_name,
                                   SUM(a.nts_act) nts_actual,
                                   0 AS nts_target
                            FROM vw_tw_forecast a
                            LEFT JOIN (
                                   SELECT DISTINCT YEAR,
                                          mnth_no,
                                          mnth_id
                                   FROM edw_vw_promo_calendar
                                   ) cal ON forecast_for_year || forecast_for_mnth = cal.year || cal.mnth_no
                            LEFT JOIN itg_mds_tw_customer_sales_rep_mapping b ON rtrim(a.strategy_customer_hierachy_name) = rtrim(b.customer_name)
                            WHERE subsource_type = 'SAPBW_ACTUAL'
                                   AND to_crncy = 'TWD'
                            GROUP BY cntry_cd,
                                   crncy_cd,
                                   to_crncy,
                                   strategy_customer_hierachy_name,
                                   forecast_for_year,
                                   cal.mnth_id,
                                   b.psr_code,
                                   b.psr_name
                            
                            UNION ALL
                            
                            SELECT 'TW' AS cntry_cd,
                                   'TWD' AS crncy_cd,
                                   'TWD' AS to_crncy,
                                   a.customer_name,
                                   CAST(SUBSTRING(year_month, 1, 4) AS INTEGER) AS YEAR,
                                   CAST(year_month AS INTEGER) AS mnth_id,
                                   b.psr_code,
                                   b.psr_name,
                                   0 AS nts_actual,
                                   SUM(a.nts) nts_target
                            FROM itg_tsi_target_data a
                            LEFT JOIN itg_mds_tw_customer_sales_rep_mapping b ON a.customer_name = b.customer_name
                            GROUP BY cntry_cd,
                                   crncy_cd,
                                   to_crncy,
                                   a.customer_name,
                                   SUBSTRING(year_month, 1, 4),
                                   year_month,
                                   b.psr_code,
                                   b.psr_name
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
                     ) c ON main.nts_achievement >= c.begin
                     AND main.nts_achievement <= c.end
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
       -----------NTS_Quarter---------------------------------------    
       
       UNION ALL
       
       (
              SELECT 'NTS_Q' AS source_type,
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
                     cast(AVG(main.nts_actual) AS NUMERIC(38, 4)) AS quarterly_actual,
                     cast(AVG(main.nts_target) AS NUMERIC(38, 4)) AS quarterly_target,
                     AVG(main.nts_achievement) AS quarterly_achievement,
                     AVG(CASE 
                                   WHEN c.incentive_type = 'YQ'
                                          AND e.reverse = 'True'
                                          THEN c.offtake_si
                                   WHEN d.incentive_type = 'YQ_EC_Bonus'
                                          AND f.ec_code = 'Y'
                                          THEN d.nts_si
                                   ELSE c.nts_si
                                   END) AS quarterly_incentive_amount
              FROM (
                     SELECT 'NTS_Q' AS source_type,
                            cntry_cd,
                            crncy_cd,
                            to_crncy,
                            psr_code,
                            psr_name,
                            YEAR,
                            -- mnth_id,
                            qrtr,
                            SUM(nts_actual) nts_actual,
                            SUM(nts_target) nts_target,
                            cast((SUM(nts_actual) / NULLIF(SUM(nts_target), 0)) * 100 AS DECIMAL(10, 2)) nts_achievement
                     FROM (
                            SELECT cntry_cd,
                                   crncy_cd,
                                   to_crncy,
                                   strategy_customer_hierachy_name,
                                   CAST(forecast_for_year AS INTEGER) AS YEAR,
                                   CAST(cal.mnth_id AS INTEGER) AS mnth_id,
                                   cal.qrtr,
                                   b.psr_code,
                                   b.psr_name,
                                   SUM(a.nts_act) AS nts_actual,
                                   0 AS nts_target
                            FROM vw_tw_forecast a
                            LEFT JOIN (
                                   SELECT DISTINCT YEAR,
                                          mnth_no,
                                          mnth_id,
                                          qrtr
                                   FROM edw_vw_promo_calendar
                                   ) cal ON forecast_for_year || forecast_for_mnth = cal.year || cal.mnth_no
                            LEFT JOIN itg_mds_tw_customer_sales_rep_mapping b ON rtrim(a.strategy_customer_hierachy_name) = rtrim(b.customer_name)
                            WHERE subsource_type = 'SAPBW_ACTUAL'
                                   AND to_crncy = 'TWD'
                            GROUP BY cntry_cd,
                                   crncy_cd,
                                   to_crncy,
                                   strategy_customer_hierachy_name,
                                   forecast_for_year,
                                   cal.mnth_id,
                                   cal.qrtr,
                                   b.psr_code,
                                   b.psr_name
                            
                            UNION ALL
                            
                            SELECT 'TW' AS cntry_cd,
                                   'TWD' AS crncy_cd,
                                   'TWD' AS to_crncy,
                                   a.customer_name,
                                   CAST(SUBSTRING(year_month, 1, 4) AS INTEGER) AS YEAR,
                                   CAST(year_month AS INTEGER) AS mnth_id,
                                   cal.qrtr,
                                   b.psr_code,
                                   b.psr_name,
                                   0 AS nts_actual,
                                   SUM(a.nts) AS nts_target
                            FROM itg_tsi_target_data a
                            LEFT JOIN (
                                   SELECT DISTINCT YEAR,
                                          mnth_no,
                                          mnth_id,
                                          qrtr
                                   FROM edw_vw_promo_calendar
                                   ) cal ON a.year_month = cal.mnth_id
                            LEFT JOIN itg_mds_tw_customer_sales_rep_mapping b ON a.customer_name = b.customer_name
                            GROUP BY cntry_cd,
                                   crncy_cd,
                                   to_crncy,
                                   a.customer_name,
                                   SUBSTRING(year_month, 1, 4),
                                   year_month,
                                   cal.qrtr,
                                   b.psr_code,
                                   b.psr_name
                            )
                     GROUP BY source_type,
                            cntry_cd,
                            crncy_cd,
                            to_crncy,
                            psr_code,
                            psr_name,
                            YEAR,
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
                     ) c ON main.nts_achievement >= c.begin
                     AND main.nts_achievement <= c.end
              LEFT JOIN (
                     SELECT incentive_type,
                            begin * 100 AS begin,
                            end * 100 AS end,
                            nts_si,
                            offtake_si
                     FROM itg_mds_tw_incentive_schemes
                     WHERE incentive_type = 'YQ_EC_Bonus'
                     ) d ON main.nts_achievement >= d.begin
                     AND main.nts_achievement <= d.end
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
                     main.qrtr,
                     e.report_to,
                     e.reportto_name,
                     e.reverse
              )
       -----------------------------EC customer quarter data-------------------------------------------------
       
       UNION ALL
       
       (
              SELECT 'NTS_Q_EC' AS source_type,
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
                     cast(AVG(main.nts_actual) AS NUMERIC(38, 4)) AS quarterly_actual,
                     cast(AVG(main.nts_target) AS NUMERIC(38, 4)) AS quarterly_target,
                     AVG(main.nts_achievement) AS quarterly_achievement,
                     AVG(CASE 
                                   WHEN d.incentive_type = 'YQ_EC'
                                          AND f.ec_code = 'Y'
                                          THEN d.nts_si
                                   ELSE 0
                                   END) AS quarterly_incentive_amount
              FROM (
                     SELECT 'NTS_Q_EC' AS source_type,
                            cntry_cd,
                            crncy_cd,
                            to_crncy,
                            psr_code,
                            psr_name,
                            YEAR,
                            -- mnth_id,
                            qrtr,
                            SUM(nts_actual) nts_actual,
                            SUM(nts_target) nts_target,
                            cast((SUM(nts_actual) / NULLIF(SUM(nts_target), 0)) * 100 AS DECIMAL(10, 2)) nts_achievement
                     FROM (
                            SELECT cntry_cd,
                                   crncy_cd,
                                   to_crncy,
                                   strategy_customer_hierachy_name,
                                   CAST(forecast_for_year AS INTEGER) AS YEAR,
                                   CAST(cal.mnth_id AS INTEGER) AS mnth_id,
                                   cal.qrtr,
                                   b.psr_code,
                                   b.psr_name,
                                   SUM(a.nts_act) AS nts_actual,
                                   0 AS nts_target
                            FROM vw_tw_forecast a
                            LEFT JOIN (
                                   SELECT DISTINCT YEAR,
                                          mnth_no,
                                          mnth_id,
                                          qrtr
                                   FROM edw_vw_promo_calendar
                                   ) cal ON forecast_for_year || forecast_for_mnth = cal.year || cal.mnth_no
                            LEFT JOIN itg_mds_tw_customer_sales_rep_mapping b ON rtrim(a.strategy_customer_hierachy_name) = rtrim(b.customer_name)
                            WHERE subsource_type = 'SAPBW_ACTUAL'
                                   AND to_crncy = 'TWD'
                            GROUP BY cntry_cd,
                                   crncy_cd,
                                   to_crncy,
                                   strategy_customer_hierachy_name,
                                   forecast_for_year,
                                   cal.mnth_id,
                                   cal.qrtr,
                                   b.psr_code,
                                   b.psr_name
                            
                            UNION ALL
                            
                            SELECT 'TW' AS cntry_cd,
                                   'TWD' AS crncy_cd,
                                   'TWD' AS to_crncy,
                                   a.customer_name,
                                   CAST(SUBSTRING(year_month, 1, 4) AS INTEGER) AS YEAR,
                                   CAST(year_month AS INTEGER) AS mnth_id,
                                   cal.qrtr,
                                   b.psr_code,
                                   b.psr_name,
                                   0 AS nts_actual,
                                   SUM(a.nts) AS nts_target
                            FROM itg_tsi_target_data a
                            LEFT JOIN (
                                   SELECT DISTINCT YEAR,
                                          mnth_no,
                                          mnth_id,
                                          qrtr
                                   FROM edw_vw_promo_calendar
                                   ) cal ON a.year_month = cal.mnth_id
                            LEFT JOIN itg_mds_tw_customer_sales_rep_mapping b ON a.customer_name = b.customer_name
                            GROUP BY cntry_cd,
                                   crncy_cd,
                                   to_crncy,
                                   a.customer_name,
                                   SUBSTRING(year_month, 1, 4),
                                   year_month,
                                   cal.qrtr,
                                   b.psr_code,
                                   b.psr_name
                            )
                     GROUP BY source_type,
                            cntry_cd,
                            crncy_cd,
                            to_crncy,
                            psr_code,
                            psr_name,
                            YEAR,
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
                     ) d ON main.nts_achievement >= d.begin
                     AND main.nts_achievement <= d.end
              LEFT JOIN (
                     SELECT *
                     FROM itg_mds_tw_sales_representative
                     ) e ON main.psr_code = e.psr_code
              LEFT JOIN (
                     SELECT *
                     FROM itg_mds_tw_customer_sales_rep_mapping
                     ) f ON main.psr_code = f.psr_code
              WHERE f.ec_code = 'Y'
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
WHERE year >= (DATE_PART(YEAR, current_timestamp()) - 2)
)
select * from final