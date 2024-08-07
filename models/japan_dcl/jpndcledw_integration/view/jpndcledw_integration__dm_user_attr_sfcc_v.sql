WITH threshold_amount_master AS 
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.threshold_amount_master
),

dm_user_attr AS 
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.dm_user_attr
),

kr_new_stage_point AS 
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.kr_new_stage_point
),

dm_user_status AS 
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.dm_user_status
),

final AS 
(
    SELECT 
        dm.webno,
        SUBSTRING(CAST(dm.birthday AS VARCHAR), 1, 4) AS col1,
        SUBSTRING(CAST(dm.birthday AS VARCHAR), 5, 2) AS col2,
        TO_CHAR(dm.l_order_dt, 'YYYYMMDD') AS col3,
        TO_CHAR(dm.i_order_dt, 'YYYYMMDD') AS col4,
        (dm.order_cnt_call_3m + dm.order_cnt_web_3m + dm.order_cnt_store_3m) AS col5,
        (dm.order_cnt_call_6m + dm.order_cnt_web_6m + dm.order_cnt_store_6m) AS col6,
        (dm.order_cnt_call_1y + dm.order_cnt_web_1y + dm.order_cnt_store_1y) AS col7,
        (dm.order_cnt_call_this_year + dm.order_cnt_web_this_year + dm.order_cnt_store_this_year) AS col8,
        CASE 
            WHEN (dm.order_amt_call_1y + dm.order_amt_web_1y + dm.order_amt_store_1y) = 0 THEN NULL
            WHEN (dm.order_amt_call_1y + dm.order_amt_web_1y + dm.order_amt_store_1y) >= threshold.high_threshold_amt THEN 'High'
            WHEN (dm.order_amt_call_1y + dm.order_amt_web_1y + dm.order_amt_store_1y) >= threshold.medium_threshold_amt THEN 'Medium'
            ELSE 'Low'
        END AS col9,
        CASE 
            WHEN (dm.order_amt_call_1y_term_start + dm.order_amt_web_1y_term_start + dm.order_amt_store_1y_term_start) = 0 THEN NULL
            WHEN (dm.order_amt_call_1y_term_start + dm.order_amt_web_1y_term_start + dm.order_amt_store_1y_term_start) >= threshold.high_threshold_amt THEN 'High'
            WHEN (dm.order_amt_call_1y_term_start + dm.order_amt_web_1y_term_start + dm.order_amt_store_1y_term_start) >= threshold.medium_threshold_amt THEN 'Medium'
            ELSE 'Low'
        END AS col10,
        stage.stage_first AS col11,
        dm.subscription_flg AS col12,
        latest_status.STATUS AS col13,
        NULL AS col14,
        NULL AS col15,
        NULL AS col16,
        NULL AS col17,
        NULL AS col18,
        NULL AS col19,
        NULL AS col20,
        NULL AS col21,
        NULL AS col22,
        NULL AS col23,
        NULL AS col24,
        NULL AS col25,
        NULL AS col26,
        NULL AS col27,
        NULL AS col28,
        NULL AS col29,
        NULL AS col30
    FROM dm_user_attr dm
    LEFT JOIN (
        SELECT 
            knsp.kokyano,
            knsp.yyyymm,
            knsp.stage AS stage_first
        FROM kr_new_stage_point knsp
        WHERE TO_CHAR(DATEADD(MONTH, -2, CURRENT_TIMESTAMP), 'YYYYMM') = knsp.yyyymm
    ) stage ON dm.kokyano = stage.kokyano
    LEFT JOIN (
        SELECT 
            us.kokyano,
            us.STATUS
        FROM dm_user_status us
        JOIN (
            SELECT 
                dm_user_status.kokyano,
                MAX(dm_user_status.dt) AS max_dt
            FROM dm_user_status
            WHERE dm_user_status.base = 'order'
            GROUP BY dm_user_status.kokyano
        ) latest_status_dt ON us.kokyano = latest_status_dt.kokyano AND us.dt = latest_status_dt.max_dt AND us.base = 'order'
    ) latest_status ON dm.kokyano = latest_status.kokyano
    CROSS JOIN (
        SELECT 
            threshold_amount_master.year,
            threshold_amount_master.high_threshold_amt,
            threshold_amount_master.medium_threshold_amt
        FROM threshold_amount_master
        WHERE threshold_amount_master.year = COALESCE((
            SELECT b.year
            FROM threshold_amount_master b
            WHERE b.year = TO_CHAR(CURRENT_TIMESTAMP, 'YYYY')
        ), '9999')
    ) threshold
    WHERE dm.webno IS NOT NULL AND dm.taikai_flg <> '1'
)

SELECT *
FROM final