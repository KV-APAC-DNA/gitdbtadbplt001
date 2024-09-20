WITH dm_user_attr_hist AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__dm_user_attr_hist') }}
),

dm_user_attr AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__dm_user_attr') }}
),

final AS (
    SELECT 
        CAST(base.yrs AS VARCHAR) AS yyyy,
        COALESCE(base.kokyano, hist_base.kokyano) AS kokyano,
        COALESCE(base.l_channel, hist_base.l_channel) AS main_channel,
        hist_base.order_amt_1y_hist AS order_amt_term_start,
        base.order_amt_1y_current AS order_amt_term_end,
        prev_base.order_amt_total
    FROM (
        SELECT 
            past_yrs.yrs,
            dm_user_attr_hist.kokyano,
            dm_user_attr_hist.order_amt_call_1y + dm_user_attr_hist.order_amt_web_1y + dm_user_attr_hist.order_amt_store_1y AS order_amt_1y_current,
            dm_user_attr_hist.l_channel
        FROM dm_user_attr_hist
        JOIN (
            SELECT DISTINCT LEFT(dm_user_attr_hist.yymm::TEXT, 4) AS yrs
            FROM dm_user_attr_hist
            WHERE LEFT(dm_user_attr_hist.yymm::TEXT, 4) <> TO_CHAR(CURRENT_TIMESTAMP, 'YYYY')
        ) past_yrs ON dm_user_attr_hist.yymm::TEXT = past_yrs.yrs || '12'
    ) base
    FULL JOIN (
        SELECT 
            past_yrs.yrs,
            dm_user_attr_hist.kokyano,
            dm_user_attr_hist.order_amt_call_1y + dm_user_attr_hist.order_amt_web_1y + dm_user_attr_hist.order_amt_store_1y AS order_amt_1y_hist,
            dm_user_attr_hist.l_channel
        FROM dm_user_attr_hist
        JOIN (
            SELECT DISTINCT LEFT(dm_user_attr_hist.yymm::TEXT, 4) AS yrs
            FROM dm_user_attr_hist
            WHERE LEFT(dm_user_attr_hist.yymm::TEXT, 4) <> TO_CHAR(CURRENT_TIMESTAMP, 'YYYY')
        ) past_yrs ON dm_user_attr_hist.yymm::TEXT = CAST(CAST(past_yrs.yrs AS INT) - 1 AS VARCHAR) || '12'
    ) hist_base ON base.kokyano = hist_base.kokyano AND base.yrs = hist_base.yrs
    LEFT JOIN (
        SELECT 
            past_yrs.yrs,
            dm_user_attr_hist.kokyano,
            dm_user_attr_hist.order_amt_call + dm_user_attr_hist.order_amt_web + dm_user_attr_hist.order_amt_store AS order_amt_total,
            dm_user_attr_hist.l_channel
        FROM dm_user_attr_hist
        JOIN (
            SELECT DISTINCT LEFT(dm_user_attr_hist.yymm::TEXT, 4) AS yrs
            FROM dm_user_attr_hist
            WHERE LEFT(dm_user_attr_hist.yymm::TEXT, 4) <> TO_CHAR(CURRENT_TIMESTAMP, 'YYYY')
        ) past_yrs ON dm_user_attr_hist.yymm::TEXT = CAST(CAST(past_yrs.yrs AS INT) - 1 AS VARCHAR) || '12'
    ) prev_base ON base.kokyano = prev_base.kokyano AND prev_base.yrs = base.yrs
    WHERE hist_base.order_amt_1y_hist > 0 OR base.order_amt_1y_current > 0

    UNION ALL

    SELECT 
        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY') AS yyyy,
        COALESCE(base_1.kokyano, hist_base_1.kokyano) AS kokyano,
        COALESCE(base_1.l_channel, hist_base_1.l_channel) AS main_channel,
        hist_base_1.order_amt_1y_hist AS order_amt_term_start,
        base_1.order_amt_1y_current AS order_amt_term_end,
        prev_base_1.order_amt_total
    FROM (
        SELECT 
            dm_user_attr.kokyano,
            dm_user_attr.order_amt_call_1y + dm_user_attr.order_amt_web_1y + dm_user_attr.order_amt_store_1y AS order_amt_1y_current,
            dm_user_attr.l_channel
        FROM dm_user_attr
    ) base_1
    FULL JOIN (
        SELECT 
            dm_user_attr_hist.yymm,
            dm_user_attr_hist.kokyano,
            dm_user_attr_hist.order_amt_call_1y + dm_user_attr_hist.order_amt_web_1y + dm_user_attr_hist.order_amt_store_1y AS order_amt_1y_hist,
            dm_user_attr_hist.l_channel
        FROM dm_user_attr_hist
        WHERE dm_user_attr_hist.yymm::TEXT = CAST(CAST(TO_CHAR(CURRENT_TIMESTAMP, 'YYYY') AS INT) - 1 AS VARCHAR) || '12'
    ) hist_base_1 ON base_1.kokyano = hist_base_1.kokyano
    LEFT JOIN (
        SELECT 
            dm_user_attr_hist.kokyano,
            dm_user_attr_hist.order_amt_call + dm_user_attr_hist.order_amt_web + dm_user_attr_hist.order_amt_store AS order_amt_total,
            dm_user_attr_hist.l_channel
        FROM dm_user_attr_hist
        WHERE dm_user_attr_hist.yymm::TEXT = CAST(CAST(TO_CHAR(CURRENT_TIMESTAMP, 'YYYY') AS INT) - 1 AS VARCHAR) || '12'
    ) prev_base_1 ON base_1.kokyano = prev_base_1.kokyano
    WHERE hist_base_1.order_amt_1y_hist > 0 OR base_1.order_amt_1y_current > 0
)

SELECT * FROM final
