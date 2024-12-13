with d2c_data AS (
    SELECT *
    FROM {{ ref('jpndcledw_access__dm_kesai_mart_dly_general') }}  -- Reference to the previous model
),
date_mapping AS (
    SELECT *
    FROM {{ ref('jpndcledw_access__cld_m') }}  -- Reference to 445 period
),
net_sales_monthly AS (
SELECT 
    LEFT(Y."ymonth_445", 4) || SUBSTR(Y."ymonth_445", 6, 2) AS month_id, -- Derived column
    K."channel" as channel,
    K."teikikeiyaku" AS teikikeiyaku,
    SUM(K."nts") AS nts_base,
    COUNT(DISTINCT K."saleno") AS total_order,
    COUNT(K."saleno") AS no_orders,
    COUNT(DISTINCT K."kokyano") AS total_customer
FROM d2c_data K
LEFT JOIN date_mapping Y ON K."order_dt" = Y."ymd_dt"
WHERE 
    K."order_dt" >= to_char(extract(year FROM dateadd(year, -3, current_date)))  || '-01-01' 
    AND K."channel" in ('Web', '通販')
    AND K."teikikeiyaku" in ('通常', '定期契約商品')
GROUP BY 
    month_id,
    K."channel",
    K."saleno",
    K."teikikeiyaku"
)
SELECT *,
current_timestamp()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by ,
current_timestamp()::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
FROM net_sales_monthly