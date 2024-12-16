with d2c_data AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}  -- Reference to the previous model
),
date_mapping AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cld_m') }}  -- Reference to 445 period
),
target_sales AS (
    SELECT * 
    FROM {{ ref('jpndcledw_integration__edw_mds_jp_dcl_sales_target') }}
),
d2c_monthly as (
    SELECT 
        LEFT(Y.ymonth_445, 4) || SUBSTR(Y.ymonth_445, 6, 2) AS month_id,
        SUM(K.gts) AS gts_base,
        LAG(SUM(K.gts), 1, 0) OVER (PARTITION BY K.channel ORDER BY month_id) AS gts_mom,
        LAG(SUM(K.gts), 12, 0) OVER (PARTITION BY K.channel ORDER BY month_id) AS gts_yoy,
        SUM(K.nts) AS nts_base,
        LAG(SUM(K.nts), 1, 0) OVER (PARTITION BY K.channel ORDER BY month_id) AS nts_mom,
        LAG(SUM(K.nts), 12, 0) OVER (PARTITION BY K.channel ORDER BY month_id) AS nts_yoy,
        K.channel AS channel
    FROM d2c_data K
    LEFT JOIN date_mapping Y ON K.order_dt = Y.ymd_dt
    WHERE 
        K.order_dt >= to_char(extract(year FROM dateadd(year, -3, current_date))) || '-01-01' 
        AND K.channel IN ('Web', '通販' , '直営・百貨店')
    GROUP BY month_id, channel
), 
Non_d2c_data as (
    SELECT *
    FROM {{ ref('aspedw_integration__v_rpt_copa') }} 
),
Non_d2c_monthly as (
    SELECT extract(year from K.fisc_day) || substr(to_char(K.fisc_day),6,2) as month_id,
        SUM(K.gts_lcy) AS gts_base, 
        LAG(SUM(K.gts_lcy), 1, 0) OVER (PARTITION BY K.channel ORDER BY month_id) AS gts_mom,
        LAG(SUM(K.gts_lcy), 12, 0) OVER (PARTITION BY K.channel ORDER BY month_id) AS gts_yoy,
        SUM(K.nts_lcy) AS nts_base,
        LAG(SUM(K.nts_lcy), 1, 0) OVER (PARTITION BY K.channel ORDER BY month_id) AS nts_mom,
        LAG(SUM(K.nts_lcy), 12, 0) OVER (PARTITION BY K.channel ORDER BY month_id) AS nts_yoy, 
        K.channel AS channel
    FROM Non_d2c_data K
    WHERE 
        K.fisc_day >= to_char(extract(year FROM dateadd(year, -3, current_date))) || '-01-01'
        and k.brand = 'Dr. Ci: Labo' and K.ctry_nm = 'Japan DCL'
        AND K.channel IN ('E-Commerce', 'Wholesaler')
    GROUP BY month_id, K.channel
),
All_DCL_Sales as (
    SELECT * 
    FROM d2c_monthly
    UNION ALL
    SELECT * 
    FROM Non_d2c_monthly
),
final_data AS (
    SELECT A.* exclude channel, 
    case A.channel 
    when '通販' then 'Call' 
    when '直営・百貨店' then 'Store'
    else A.channel
    end channel,
    b.target * 1000000 as target_nts, c.target * 1000000  as target_gts
--, c.target as target_gts 
from All_DCL_Sales A
LEFT JOIN target_sales B on A.month_id = B.MONTH_ID and A.channel = B.channel_code and B.kpi_code = 'NTS'
LEFT JOIN target_sales C on A.month_id = C.MONTH_ID and A.channel = C.channel_code and C.kpi_code = 'GTS'
order by A.month_id desc,  A.channel
)
SELECT *,
current_timestamp()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by ,
current_timestamp()::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
FROM final_data