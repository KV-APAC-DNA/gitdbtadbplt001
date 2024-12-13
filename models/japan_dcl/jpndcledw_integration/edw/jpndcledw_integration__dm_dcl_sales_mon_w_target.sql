{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_SMALL_WH")+ ";",
    )
}}
with d2c_data AS (
    SELECT *
    FROM {{ ref('jpndcledw_access__dm_kesai_mart_dly_general') }}  -- Reference to the previous model
),
date_mapping AS (
    SELECT *
    FROM {{ ref('jpndcledw_access__cld_m') }}  -- Reference to 445 period
),
translated_channels AS (
    SELECT *,
        CASE
            WHEN "channel" = '通販' THEN 'E-Commerce'
            WHEN "channel" = '直営・百貨店' THEN 'Direct Store & Department Store'
            ELSE "channel"  -- Keep original value if no match
        END AS channel_english
    FROM d2c_data
),
d2c_monthly as (
    SELECT 
        LEFT(Y."ymonth_445", 4) || SUBSTR(Y."ymonth_445", 6, 2) AS month_id,
        SUM(K."gts") AS gts_base,
        LAG(SUM(K."gts"), 1, 0) OVER (PARTITION BY K.channel_english ORDER BY month_id) AS gts_mom,
        LAG(SUM(K."gts"), 12, 0) OVER (PARTITION BY K.channel_english ORDER BY month_id) AS gts_yoy,
        SUM(K."nts") AS nts_base,
        LAG(SUM(K."nts"), 1, 0) OVER (PARTITION BY K.channel_english ORDER BY month_id) AS nts_mom,
        LAG(SUM(K."nts"), 12, 0) OVER (PARTITION BY K.channel_english ORDER BY month_id) AS nts_yoy,
        K.channel_english AS channel,
        (UNIFORM(0::FLOAT, 1.5::FLOAT, RANDOM())) * SUM(K."gts") AS gts_target,
        (UNIFORM(0::FLOAT, 1.5::FLOAT, RANDOM())) * SUM(K."nts") AS nts_target
    FROM translated_channels K
    LEFT JOIN date_mapping Y ON K."order_dt" = Y."ymd_dt"
    WHERE 
        K."order_dt" >= to_char(extract(year FROM dateadd(year, -3, current_date))) || '-01-01' 
        AND K.channel_english IN ('Web', 'E-Commerce', 'Direct Store & Department Store')
    GROUP BY month_id, channel
), 
Non_d2c_data as (
    SELECT *
    FROM {{ ref('aspedw_access__v_rpt_copa') }} 
),
Non_d2c_monthly as (
    SELECT extract(year from K."fisc_day") || substr(to_char(K."fisc_day"),6,2) as month_id,
        SUM(K."gts_lcy") AS gts_base, 
        LAG(SUM(K."gts_lcy"), 1, 0) OVER (PARTITION BY K."channel" ORDER BY month_id) AS gts_mom,
        LAG(SUM(K."gts_lcy"), 12, 0) OVER (PARTITION BY K."channel" ORDER BY month_id) AS gts_yoy,
        SUM(K."nts_lcy") AS nts_base,
        LAG(SUM(K."nts_lcy"), 1, 0) OVER (PARTITION BY K."channel" ORDER BY month_id) AS nts_mom,
        LAG(SUM(K."nts_lcy"), 12, 0) OVER (PARTITION BY K."channel" ORDER BY month_id) AS nts_yoy, 
        K."channel" AS channel,
        (UNIFORM(0.5::FLOAT, 1.5::FLOAT, RANDOM())) * SUM(K."gts_lcy") AS gts_target,
        (UNIFORM(0.5::FLOAT, 1.5::FLOAT, RANDOM())) * SUM(K."nts_lcy") AS nts_target
    FROM Non_d2c_data K
    WHERE 
        K."fisc_day" >= to_char(extract(year FROM dateadd(year, -3, current_date))) || '-01-01'
        AND K."channel" IN ('Web', 'E-Commerce', 'Direct Store & Department Store')
    GROUP BY month_id, K."channel"
),
final_data as (
    SELECT * 
    FROM d2c_monthly
    UNION ALL
    SELECT * 
    FROM Non_d2c_monthly
)
SELECT * 
FROM final_data
