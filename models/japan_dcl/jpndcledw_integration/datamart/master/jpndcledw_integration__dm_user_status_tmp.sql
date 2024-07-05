{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
    )
}}
with temp_kesai_016 as(
    select * from {{ ref('jpndcledw_integration__temp_kesai_016') }}
),
cld_m as(
    select * from {{ source('jpndcledw_integration', 'cld_m') }}
),
temp_u_new_016 as(
    select * from {{ ref('jpndcledw_integration__temp_u_new_016') }}
),
u_new_order AS (
    --Order starts
    SELECT DISTINCT 'order' AS base,
        kesai.kokyano,
        kesai.order_dt AS dt,
        'New' AS STATUS
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m a ON kesai.order_dt = a.ymd_dt
    INNER JOIN temp_u_new_016 u_new ON kesai.kokyano = u_new.kokyano
    WHERE a.year = u_new.first_order_year
        AND datediff(day, NVL(prev_order_dt, order_dt), order_dt) < 365
    ),
u_lapsed_F1_order AS (
    SELECT DISTINCT 'order' AS base,
        kesai.kokyano,
        kesai.order_dt AS dt,
        'Lapsed' AS STATUS,
        a.year
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m a ON kesai.order_dt = a.ymd_dt
    WHERE datediff(day, prev_order_dt, order_dt) >= 365
    ),
u_lapsed_F2_order AS (
    SELECT DISTINCT 'order' AS base,
        kesai.kokyano,
        kesai.order_dt AS dt,
        'Lapsed' AS STATUS
    FROM temp_kesai_016 kesai
    WHERE datediff(day, prev_order_dt, order_dt) < 365
        AND EXISTS (
            SELECT 1
            FROM u_lapsed_F1_order
            WHERE u_lapsed_F1_order.kokyano = kesai.kokyano
                AND substring(u_lapsed_F1_order.dt, 1, 4) = substring(kesai.order_dt, 1, 4)
            )
    ),
u_existing_order_purchase AS (
    SELECT DISTINCT 'order' AS base,
        kesai.kokyano,
        kesai.order_dt AS dt,
        'Existing' AS STATUS
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m a ON kesai.order_dt = a.ymd_dt
    WHERE datediff(day, prev_order_dt, order_dt) < 365
        AND NOT EXISTS (
            SELECT 1
            FROM u_lapsed_F2_order
            WHERE u_lapsed_F2_order.kokyano = kesai.kokyano
                AND kesai.order_dt = u_lapsed_F2_order.dt
            )
        AND NOT EXISTS (
            SELECT 1
            FROM u_new_order
            WHERE u_new_order.kokyano = kesai.kokyano
                AND kesai.order_dt = u_new_order.dt
            )
    ),
u_all_order_yr_start AS (
    -- 一旦、kokyanoと1/1でレコードを作る。
    SELECT u_new.kokyano,
        ymd
    FROM (
        SELECT ymd_dt AS ymd
        FROM cld_m
        WHERE month = 1
            AND day = 1
        ) cld
    CROSS JOIN temp_u_new_016 u_new
    ),
u_exists_order_yr_start AS (
    -- 1/1時点でのexistingユーザをすべて算出
    SELECT 'order' AS base,
        kokyano,
        ymd AS dt,
        'Existing' AS STATUS
    FROM u_all_order_yr_start
    WHERE EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            INNER JOIN cld_m a ON kesai.order_dt = a.ymd_dt
            WHERE u_all_order_yr_start.kokyano = kesai.kokyano
                AND kesai.order_dt + interval '365 days' > u_all_order_yr_start.ymd
                AND kesai.order_dt < u_all_order_yr_start.ymd
            )
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_order_yr_start.kokyano
                AND u_all_order_yr_start.ymd = kesai.order_dt
            )
    ),
u_lapsed_order_yr_start AS (
    SELECT 'order' AS base,
        kokyano,
        ymd AS dt,
        'Lapsed' AS STATUS
    FROM u_all_order_yr_start
    WHERE EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            INNER JOIN cld_m a ON kesai.order_dt = a.ymd_dt
            WHERE u_all_order_yr_start.kokyano = kesai.kokyano
                AND kesai.order_dt + interval '365 days' <= u_all_order_yr_start.ymd
                AND kesai.order_dt < u_all_order_yr_start.ymd
            )
        AND NOT EXISTS (
            SELECT 1
            FROM u_exists_order_yr_start
            WHERE u_all_order_yr_start.kokyano = u_exists_order_yr_start.kokyano
                AND u_all_order_yr_start.ymd = u_exists_order_yr_start.dt
            )
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_order_yr_start.kokyano
                AND u_all_order_yr_start.ymd = kesai.order_dt
            )
    ),
u_all_month_end_order AS (
    SELECT u_new.kokyano,
        to_date(ymd) AS ymd
    FROM (
        SELECT y.ymd_dt AS ymd
        FROM cld_m x,
            cld_m y
       WHERE x.ymd_dt = DATEADD(day, 1, y.ymd_dt)
            AND x.month <> y.month
            AND y.ymd_dt <= CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp())
        ) cld
    CROSS JOIN temp_u_new_016 u_new
    ),
    -- cte1 as(
    -- SELECT count(*)
    --         FROM temp_u_new_016 u_new, u_all_month_end_order, cld_m x
    --         WHERE u_new.kokyano = u_all_month_end_order.kokyano
    --             AND u_new.first_order_year = x.year
    --             AND u_all_month_end_order.ymd > u_new.first_order_dt
    --             AND datediff(day, u_new.first_order_dt, u_all_month_end_order.ymd) < 365
    -- ),
    -- cte2 as(
    -- SELECT count(*)
    --         FROM temp_kesai_016 kesai, u_all_month_end_order
    --         WHERE kesai.kokyano = u_all_month_end_order.kokyano
    --             AND u_all_month_end_order.ymd = kesai.order_dt
    -- )select * from cte2;,
    
u_month_end_new_order AS (
    SELECT 'order' AS base,
        u_all_month_end_order.kokyano,
        ymd AS dt,
        'New' AS STATUS
    FROM u_all_month_end_order
    INNER JOIN cld_m x ON u_all_month_end_order.ymd = x.ymd_dt
    join temp_u_new_016 u_new
    on (u_new.kokyano = u_all_month_end_order.kokyano
                AND u_new.first_order_year = x.year
                AND u_all_month_end_order.ymd > u_new.first_order_dt
                AND datediff(day, u_new.first_order_dt, u_all_month_end_order.ymd) < 365)
    join temp_kesai_016 kesai  --Added 07252022 for leap year
     on (kesai.kokyano = u_all_month_end_order.kokyano
                AND u_all_month_end_order.ymd = kesai.order_dt
            )
),
u_month_end_lapsed_order AS (
    SELECT 'order' AS base,
        u_all_month_end_order.kokyano,
        ymd AS dt,
        'Lapsed' AS STATUS
    FROM u_all_month_end_order
    INNER JOIN cld_m x ON u_all_month_end_order.ymd = x.ymd_dt
    INNER JOIN u_lapsed_F1_order ON u_lapsed_F1_order.kokyano = u_all_month_end_order.kokyano
    INNER JOIN cld_m y ON u_lapsed_F1_order.dt = y.ymd_dt
    WHERE x.year = y.year
        AND u_lapsed_F1_order.dt < u_all_month_end_order.ymd
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_month_end_order.kokyano
                AND u_all_month_end_order.ymd = kesai.order_dt
            )
    
    UNION ALL
    
    SELECT 'order' AS base,
        kokyano,
        ymd AS dt,
        'Lapsed' AS STATUS
    FROM (
        SELECT u_all_month_end_order.*,
            max(kesai.order_dt) AS prev_order_dt
        FROM u_all_month_end_order
        INNER JOIN temp_kesai_016 kesai ON kesai.kokyano = u_all_month_end_order.kokyano
        INNER JOIN cld_m a ON kesai.order_dt = a.ymd_dt
        WHERE u_all_month_end_order.ymd >= kesai.order_dt
        GROUP BY 1,
            2
        ) A
    WHERE datediff(day, A.prev_order_dt, A.ymd) > 365
    ),
u_month_end_existing_order AS (
    SELECT 'order' AS base,
        u_all_month_end_order.kokyano,
        ymd AS dt,
        'Existing' AS STATUS
    FROM u_all_month_end_order
    INNER JOIN cld_m x ON u_all_month_end_order.ymd = x.ymd_dt
    LEFT JOIN u_lapsed_F1_order ON u_lapsed_F1_order.kokyano = u_all_month_end_order.kokyano
        AND x.year = u_lapsed_F1_order.year
    WHERE nvl(u_lapsed_F1_order.dt, '31-dec-9999') > u_all_month_end_order.ymd
        AND EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            INNER JOIN cld_m a ON kesai.order_dt = a.ymd_dt
            WHERE u_all_month_end_order.kokyano = kesai.kokyano
                AND kesai.order_dt + interval '365 days' > u_all_month_end_order.ymd
                AND kesai.order_dt < u_all_month_end_order.ymd
            )
        AND NOT EXISTS (
            SELECT 1
            FROM temp_u_new_016 u_new
            WHERE u_new.kokyano = u_all_month_end_order.kokyano
                AND u_new.first_order_year = x.year
            )
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_month_end_order.kokyano
                AND u_all_month_end_order.ymd = kesai.order_dt
            )
    ),
u_last_order_per_year AS (
    SELECT kokyano,
        max(order_dt) AS last_order_dt,
        to_number(substring(kesai.order_dt, 1, 4), '9999') AS order_yr
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m a ON kesai.order_dt = a.ymd_dt
    WHERE to_number(substring(kesai.order_dt, 1, 4), '9999') < extract(year FROM CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp()))
    GROUP BY kokyano,
        to_number(substring(kesai.order_dt, 1, 4), '9999')
    ),
u_lapsed_order_after_1yr AS (
    SELECT 'order' AS base,
        kokyano,
        to_date(last_order_dt + interval '365 days') AS dt,
        'Lapsed' AS STATUS
    FROM u_last_order_per_year
    WHERE NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_last_order_per_year.kokyano
                AND kesai.order_dt <= u_last_order_per_year.last_order_dt + interval '365 days'
                AND to_number(substring(kesai.order_dt, 1, 4), '9999') = u_last_order_per_year.order_yr + 1
            )
        AND NOT EXISTS (
            SELECT 1
            FROM u_month_end_lapsed_order
            WHERE u_month_end_lapsed_order.kokyano = u_last_order_per_year.kokyano
                AND u_month_end_lapsed_order.dt = to_date(u_last_order_per_year.last_order_dt + interval '365 days')
            )
        AND to_date(last_order_dt + interval '365 days') <= CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp())
        AND NOT EXISTS (
            SELECT 1
            FROM temp_u_new_016 u_new
            WHERE u_new.kokyano = u_last_order_per_year.kokyano
                AND u_new.first_order_year = u_last_order_per_year.order_yr
            )
        AND to_date(last_order_dt + interval '365 days') NOT IN (
            SELECT ymd_dt AS ymd
            FROM cld_m
            WHERE month = 1
                AND day = 1
            )
    )--Order ends
,
u_new_ship AS (
    --Ship starts
    SELECT DISTINCT 'ship445' AS base,
        kesai.kokyano,
        kesai.ship_dt AS dt,
        'New' AS STATUS
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m a ON kesai.ship_dt = a.ymd_dt
    INNER JOIN temp_u_new_016 u_new ON kesai.kokyano = u_new.kokyano
    WHERE a.year_445 = u_new.first_ship_year
        AND datediff(day, NVL(prev_ship_dt, ship_dt), ship_dt) < 365
    ),
u_lapsed_F1_ship AS (
    SELECT DISTINCT 'ship445' AS base,
        kesai.kokyano,
        kesai.ship_dt AS dt,
        'Lapsed' AS STATUS,
        cld_m.year_445
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m ON kesai.ship_dt = cld_m.ymd_dt
    WHERE datediff(day, prev_ship_dt, ship_dt) >= 365
    ),
u_lapsed_F2_ship AS (
    SELECT DISTINCT 'ship445' AS base,
        kesai.kokyano,
        kesai.ship_dt AS dt,
        'Lapsed' AS STATUS
    FROM temp_kesai_016 kesai
    WHERE datediff(day, prev_ship_dt, ship_dt) < 365
        AND EXISTS (
            SELECT 1
            FROM u_lapsed_F1_ship
            WHERE u_lapsed_F1_ship.kokyano = kesai.kokyano
                AND u_lapsed_F1_ship.year_445 = kesai.year_445
            )
    ),
u_existing_ship_purchase AS (
    SELECT DISTINCT 'ship445' AS base,
        kesai.kokyano,
        kesai.ship_dt AS dt,
        'Existing' AS STATUS
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m a ON kesai.ship_dt = a.ymd_dt
    WHERE datediff(day, prev_ship_dt, ship_dt) < 365
        AND NOT EXISTS (
            SELECT 1
            FROM u_lapsed_F2_ship
            WHERE u_lapsed_F2_ship.kokyano = kesai.kokyano
                AND kesai.ship_dt = u_lapsed_F2_ship.dt
            )
        AND NOT EXISTS (
            SELECT 1
            FROM u_new_ship
            WHERE u_new_ship.kokyano = kesai.kokyano
                AND kesai.ship_dt = u_new_ship.dt
            )
    ),
u_all_ship_yr_start AS (
    -- 一旦、kokyanoと1/1でレコードを作る。
    SELECT u_new.kokyano,
        ymd
    FROM (
        SELECT X.ymd_dt AS ymd
        FROM cld_m X
        INNER JOIN cld_m Y ON X.ymd_dt = DATEADD(day, 1, Y.ymd_dt)
        WHERE X.year_445 > Y.year_445
        ) cld
    CROSS JOIN temp_u_new_016 u_new
    ),
u_exists_ship_yr_start AS (
    -- 1/1時点でのexistingユーザをすべて算出
    SELECT 'ship445' AS base,
        kokyano,
        ymd AS dt,
        'Existing' AS STATUS
    FROM u_all_ship_yr_start
    WHERE EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            INNER JOIN cld_m a ON kesai.ship_dt = a.ymd_dt
            WHERE u_all_ship_yr_start.kokyano = kesai.kokyano
                AND kesai.ship_dt + interval '365 days' > u_all_ship_yr_start.ymd
                AND kesai.ship_dt < u_all_ship_yr_start.ymd
            )
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_ship_yr_start.kokyano
                AND u_all_ship_yr_start.ymd = kesai.ship_dt
            )
    ),
u_lapsed_ship_yr_start AS (
    SELECT 'ship445' AS base,
        kokyano,
        ymd AS dt,
        'Lapsed' AS STATUS
    FROM u_all_ship_yr_start
    WHERE EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            INNER JOIN cld_m a ON kesai.ship_dt = a.ymd_dt
            WHERE u_all_ship_yr_start.kokyano = kesai.kokyano
                AND kesai.ship_dt + interval '365 days' <= u_all_ship_yr_start.ymd
                AND kesai.ship_dt < u_all_ship_yr_start.ymd
            )
        AND NOT EXISTS (
            SELECT 1
            FROM u_exists_ship_yr_start
            WHERE u_all_ship_yr_start.kokyano = u_exists_ship_yr_start.kokyano
                AND u_all_ship_yr_start.ymd = u_exists_ship_yr_start.dt
            )
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_ship_yr_start.kokyano
                AND u_all_ship_yr_start.ymd = kesai.ship_dt
            )
    ),
u_all_month_end_ship AS (
    SELECT u_new.kokyano,
        to_date(ymd) AS ymd
    FROM (
        SELECT y.ymd_dt AS ymd
        FROM cld_m x,
            cld_m y
        WHERE x.ymd_dt = DATEADD(day, 1, y.ymd_dt)
            AND x.month_445 <> y.month_445
            AND y.ymd_dt <= CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp())
        ) cld
    CROSS JOIN temp_u_new_016 u_new
    ),
u_month_end_new_ship AS (
    SELECT 'ship445' AS base,
        kokyano,
        ymd AS dt,
        'New' AS STATUS
    FROM u_all_month_end_ship
    INNER JOIN cld_m x ON u_all_month_end_ship.ymd = x.ymd_dt
    WHERE EXISTS (
            SELECT 1
            FROM temp_u_new_016 u_new
            WHERE u_new.kokyano = u_all_month_end_ship.kokyano
                AND u_new.first_ship_year = x.year_445
                AND u_all_month_end_ship.ymd > u_new.first_ship_dt
            )
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_month_end_ship.kokyano
                AND u_all_month_end_ship.ymd = kesai.ship_dt
            )
    ),
u_month_end_lapsed_ship AS (
    SELECT 'ship445' AS base,
        u_all_month_end_ship.kokyano,
        ymd AS dt,
        'Lapsed' AS STATUS
    FROM u_all_month_end_ship
    INNER JOIN cld_m x ON u_all_month_end_ship.ymd = x.ymd_dt
    INNER JOIN u_lapsed_F1_ship ON u_lapsed_F1_ship.kokyano = u_all_month_end_ship.kokyano
    INNER JOIN cld_m y ON u_lapsed_F1_ship.dt = y.ymd_dt
    WHERE x.year_445 = y.year_445
        AND u_lapsed_F1_ship.dt < u_all_month_end_ship.ymd
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_month_end_ship.kokyano
                AND u_all_month_end_ship.ymd = kesai.ship_dt
            )
    
    UNION
    
    SELECT 'ship445' AS base,
        kokyano,
        ymd AS dt,
        'Lapsed' AS STATUS
    FROM (
        SELECT u_all_month_end_ship.*,
            max(kesai.ship_dt) AS prev_ship_dt
        FROM u_all_month_end_ship
        INNER JOIN temp_kesai_016 kesai ON kesai.kokyano = u_all_month_end_ship.kokyano
        INNER JOIN cld_m a ON kesai.ship_dt = a.ymd_dt
        WHERE u_all_month_end_ship.ymd >= kesai.ship_dt
        GROUP BY 1,
            2
        ) A
    INNER JOIN cld_m ON A.ymd = cld_m.ymd_dt
    WHERE datediff(day, A.prev_ship_dt, A.ymd) > 365
        AND NOT EXISTS (
            SELECT 1
            FROM temp_u_new_016 u_new
            WHERE u_new.kokyano = A.kokyano
                AND u_new.first_ship_year = cld_m.year_445
            )
    ),
u_month_end_existing_ship AS (
    SELECT 'ship445' AS base,
        u_all_month_end_ship.kokyano,
        ymd AS dt,
        'Existing' AS STATUS
    FROM u_all_month_end_ship
    INNER JOIN cld_m x ON u_all_month_end_ship.ymd = x.ymd_dt
    LEFT JOIN (
        SELECT kokyano,
            min(dt) dt,
            year_445
        FROM u_lapsed_F1_ship
        GROUP BY kokyano,
            year_445
        ) u_lapsed_F1_ship ON u_lapsed_F1_ship.kokyano = u_all_month_end_ship.kokyano
        AND x.year_445 = u_lapsed_F1_ship.year_445
    WHERE nvl(u_lapsed_F1_ship.dt, '31-dec-9999') > u_all_month_end_ship.ymd
        AND EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            INNER JOIN cld_m a ON kesai.ship_dt = a.ymd_dt
            WHERE u_all_month_end_ship.kokyano = kesai.kokyano
                AND kesai.ship_dt + interval '365 days' > u_all_month_end_ship.ymd
                AND kesai.ship_dt < u_all_month_end_ship.ymd
            )
        AND NOT EXISTS (
            SELECT 1
            FROM temp_u_new_016 u_new
            WHERE u_new.kokyano = u_all_month_end_ship.kokyano
                AND u_new.first_ship_year = x.year_445
            )
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_month_end_ship.kokyano
                AND u_all_month_end_ship.ymd = kesai.ship_dt
            )
    ),
u_last_ship_per_year AS (
    SELECT kokyano,
        max(ship_dt) AS last_ship_dt,
        to_number(kesai.year_445, '9999') AS ship_yr
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m a ON kesai.ship_dt = a.ymd_dt
    WHERE to_number(kesai.year_445, '9999') < (
            SELECT to_number(year_445, '9999')
            FROM cld_m
            WHERE ymd_dt = to_date(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp()))
            )
    GROUP BY kokyano,
        to_number(kesai.year_445, '9999')
    ),
u_lapsed_ship_after_1yr AS (
    SELECT 'ship445' AS base,
        kokyano,
        to_date(last_ship_dt + interval '365 days') AS dt,
        'Lapsed' AS STATUS
    FROM u_last_ship_per_year
    WHERE NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_last_ship_per_year.kokyano
                AND kesai.ship_dt <= u_last_ship_per_year.last_ship_dt + interval '365 days'
                AND to_number(kesai.year_445, '9999') = u_last_ship_per_year.ship_yr + 1
            )
        AND NOT EXISTS (
            SELECT 1
            FROM u_month_end_lapsed_ship
            WHERE u_month_end_lapsed_ship.kokyano = u_last_ship_per_year.kokyano
                AND u_month_end_lapsed_ship.dt = to_date(u_last_ship_per_year.last_ship_dt + interval '365 days')
            )
        AND to_date(last_ship_dt + interval '365 days') <= CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp())
        AND NOT EXISTS (
            SELECT 1
            FROM temp_u_new_016 u_new
            WHERE u_new.kokyano = u_last_ship_per_year.kokyano
                AND u_new.first_ship_year = u_last_ship_per_year.ship_yr
            )
        AND to_date(last_ship_dt + interval '365 days') NOT IN (
            SELECT X.ymd_dt AS ymd
            FROM cld_m X
            INNER JOIN cld_m Y ON X.ymd_dt = DATEADD(day, 1, Y.ymd_dt) 
            WHERE X.year_445 > Y.year_445
            )
    ) ----	365 days Logic Addition 09/06/2022 Starts for Order
,
u_new_order_365 AS (
    SELECT DISTINCT 'order_365' AS base,
        kesai.kokyano,
        kesai.order_dt AS dt,
        'New' AS STATUS
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m a ON kesai.order_dt = a.ymd_dt
    INNER JOIN temp_u_new_016 u_new ON kesai.kokyano = u_new.kokyano
    WHERE kesai.order_dt < u_new.first_order_dt + interval '365 days'
    ),
u_lapsed_F1_order_365 AS (
    SELECT DISTINCT 'order_365' AS base,
        kesai.kokyano,
        kesai.order_dt AS dt,
        'Lapsed' AS STATUS
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m a ON kesai.order_dt = a.ymd_dt
    WHERE datediff(day, prev_order_dt, order_dt) >= 365
    ),
u_lapsed_F2_order_365 AS (
    SELECT DISTINCT 'order_365' AS base,
        kesai.kokyano,
        kesai.order_dt AS dt,
        'Lapsed' AS STATUS
    FROM temp_kesai_016 kesai
    INNER JOIN u_lapsed_F1_order_365 ON u_lapsed_F1_order_365.kokyano = kesai.kokyano
    WHERE kesai.order_dt < u_lapsed_F1_order_365.dt + interval '365 days'
        AND kesai.order_dt > u_lapsed_F1_order_365.dt
    ),
u_existing_order_365_purchase AS (
    SELECT DISTINCT 'order_365' AS base,
        kesai.kokyano,
        kesai.order_dt AS dt,
        'Existing' AS STATUS
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m a ON kesai.order_dt = a.ymd_dt
    INNER JOIN temp_u_new_016 u_new ON u_new.kokyano = kesai.kokyano
        AND kesai.order_dt >= u_new.first_order_dt + interval '365 days'
    WHERE datediff(day, prev_order_dt, order_dt) < 365
        AND NOT EXISTS (
            SELECT 1
            FROM u_lapsed_F1_order_365
            WHERE u_lapsed_F1_order_365.kokyano = kesai.kokyano
                AND kesai.order_dt >= u_lapsed_F1_order_365.dt
                AND kesai.order_dt < u_lapsed_F1_order_365.dt + interval '365 days'
            )
    ),
u_lapsed_order_365_after_1yr AS (
    SELECT DISTINCT 'order_365' AS base,
        kesai.kokyano,
        to_date(kesai.order_dt + interval '365 days') AS dt,
        'Lapsed' AS STATUS
    FROM temp_kesai_016 kesai
    WHERE NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai2
            WHERE kesai.kokyano = kesai2.kokyano
                AND kesai2.order_dt > kesai.order_dt
                AND kesai2.order_dt <= to_date(kesai.order_dt + interval '365 days')
            )
        AND /* not exists (select 1 from u_month_end_lapsed_order where u_month_end_lapsed_order.kokyano = u_last_order_per_year.kokyano and 
u_month_end_lapsed_order.dt = to_date(u_last_order_per_year.last_order_dt + interval '365 days'))
and */ to_date(kesai.order_dt + interval '365 days') <= CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp())
        AND datediff(day, kesai.prev_order_dt, kesai.order_dt) < 365
    ),
u_existing_order_365_after_1yr AS (
    SELECT DISTINCT 'order_365' AS base,
        u_new.kokyano,
        to_date(u_new.first_order_dt + interval '365 days') AS dt,
        'Existing' AS STATUS
    FROM temp_u_new_016 u_new
    INNER JOIN u_new_order_365 ON u_new.kokyano = u_new_order_365.kokyano
    WHERE u_new_order_365.dt > u_new.first_order_dt
        AND u_new_order_365.dt < to_date(u_new.first_order_dt + interval '365 days')
        AND /* not exists (select 1 from u_month_end_lapsed_order where u_month_end_lapsed_order.kokyano = u_last_order_per_year.kokyano and 
u_month_end_lapsed_order.dt = to_date(u_last_order_per_year.last_order_dt + interval '365 days'))
and */ to_date(u_new.first_order_dt + interval '365 days') <= CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp())
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_new.kokyano
                AND kesai.order_dt = to_date(u_new.first_order_dt + interval '365 days')
            )
    
    UNION ALL
    
    SELECT DISTINCT 'order_365' AS base,
        u_lapsed_F1_order_365.kokyano,
        to_date(u_lapsed_F1_order_365.dt + interval '365 days') AS dt,
        'Existing' AS STATUS
    FROM u_lapsed_F1_order_365
    INNER JOIN u_lapsed_F2_order_365 ON u_lapsed_F1_order_365.kokyano = u_lapsed_F2_order_365.kokyano
    WHERE u_lapsed_F2_order_365.dt > u_lapsed_F1_order_365.dt
        AND u_lapsed_F2_order_365.dt < to_date(u_lapsed_F1_order_365.dt + interval '365 days')
        AND /* not exists (select 1 from u_month_end_lapsed_order where u_month_end_lapsed_order.kokyano = u_last_order_per_year.kokyano and 
u_month_end_lapsed_order.dt = to_date(u_last_order_per_year.last_order_dt + interval '365 days'))
and */ to_date(u_lapsed_F1_order_365.dt + interval '365 days') <= CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp())
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_lapsed_F1_order_365.kokyano
                AND kesai.order_dt = to_date(u_lapsed_F1_order_365.dt + interval '365 days')
            )
    ),
u_all_month_end_order_365 AS (
    SELECT kokyano,
        ymd
    FROM u_all_month_end_order
    WHERE ymd <= CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp())
        AND DATE_PART('year', ymd) >= DATE_PART('year', TO_DATE(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', CURRENT_TIMESTAMP()))) - 2
    ),
u_month_end_new_order_365 AS (
    SELECT 'order_365' AS base,
        u_new.kokyano,
        ymd AS dt,
        'New' AS STATUS
    FROM u_all_month_end_order_365
    INNER JOIN temp_u_new_016 u_new ON u_new.kokyano = u_all_month_end_order_365.kokyano
    WHERE u_all_month_end_order_365.ymd > u_new.first_order_dt
        AND u_all_month_end_order_365.ymd < u_new.first_order_dt + interval '365 days' --Added 07252022 for leap year 
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_month_end_order_365.kokyano
                AND u_all_month_end_order_365.ymd = kesai.order_dt
            )
    ) ,
u_month_end_lapsed_order_365 AS (
    SELECT 'order_365' AS base,
        u_all_month_end_order_365.kokyano,
        ymd AS dt,
        'Lapsed' AS STATUS
    FROM u_all_month_end_order_365
    INNER JOIN u_lapsed_F1_order_365 ON u_lapsed_F1_order_365.kokyano = u_all_month_end_order_365.kokyano
    WHERE u_lapsed_F1_order_365.dt < u_all_month_end_order_365.ymd
        AND u_lapsed_F1_order_365.dt + interval '365 days' > u_all_month_end_order_365.ymd
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_month_end_order_365.kokyano
                AND u_all_month_end_order_365.ymd = kesai.order_dt
            )
    
    UNION ALL
    
    SELECT 'order_365' AS base,
        kokyano,
        ymd AS dt,
        'Lapsed' AS STATUS
    FROM (
        SELECT u_all_month_end_order_365.kokyano,
            u_all_month_end_order_365.ymd,
            max(kesai.order_dt) AS prev_order_dt
        FROM u_all_month_end_order_365
        INNER JOIN temp_kesai_016 kesai ON kesai.kokyano = u_all_month_end_order_365.kokyano
        INNER JOIN cld_m a ON kesai.order_dt = a.ymd_dt
        WHERE u_all_month_end_order_365.ymd >= kesai.order_dt
        GROUP BY 1,
            2
        ) A
    WHERE datediff(day, A.prev_order_dt, A.ymd) > 365
    ),
u_month_end_existing_order_365 AS (
    SELECT DISTINCT 'order_365' AS base,
        u_all_month_end_order_365.kokyano,
        ymd AS dt,
        'Existing' AS STATUS
    FROM u_all_month_end_order_365
    INNER JOIN temp_u_new_016 u_new ON u_new.kokyano = u_all_month_end_order_365.kokyano
    WHERE u_all_month_end_order_365.ymd > u_new.first_order_dt
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_month_end_order_365.kokyano
                AND u_all_month_end_order_365.ymd = kesai.order_dt
            )
        AND NOT EXISTS (
            SELECT 1
            FROM u_month_end_new_order_365
            WHERE u_month_end_new_order_365.kokyano = u_all_month_end_order_365.kokyano
                AND u_month_end_new_order_365.dt = u_all_month_end_order_365.ymd
            )
        AND NOT EXISTS (
            SELECT 1
            FROM u_month_end_lapsed_order_365
            WHERE u_month_end_lapsed_order_365.kokyano = u_all_month_end_order_365.kokyano
                AND u_month_end_lapsed_order_365.dt = u_all_month_end_order_365.ymd
            )
        AND NOT EXISTS (
            SELECT 1
            FROM u_existing_order_365_after_1yr
            WHERE u_existing_order_365_after_1yr.kokyano = u_all_month_end_order_365.kokyano
                AND u_existing_order_365_after_1yr.dt = u_all_month_end_order_365.ymd
            )
        AND NOT EXISTS (
            SELECT 1
            FROM u_lapsed_order_365_after_1yr
            WHERE u_lapsed_order_365_after_1yr.kokyano = u_all_month_end_order_365.kokyano
                AND u_lapsed_order_365_after_1yr.dt = u_all_month_end_order_365.ymd
            )
    )----	365 days Logic Addition 09/06/2022 Ends for Order
,
u_new_ship445_365 AS (
    ----	365 days Logic Addition 09/06/2022 Starts for Shipping
    SELECT DISTINCT 'ship445_365' AS base,
        kesai.kokyano,
        kesai.ship_dt AS dt,
        'New' AS STATUS
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m a ON kesai.ship_dt = a.ymd_dt
    INNER JOIN temp_u_new_016 u_new ON kesai.kokyano = u_new.kokyano
    WHERE kesai.ship_dt < u_new.first_ship_dt + interval '365 days'
    ),
u_lapsed_F1_ship445_365 AS (
    SELECT DISTINCT 'ship445_365' AS base,
        kesai.kokyano,
        kesai.ship_dt AS dt,
        'Lapsed' AS STATUS
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m a ON kesai.ship_dt = a.ymd_dt
    WHERE datediff(day, prev_ship_dt, ship_dt) >= 365
    ),
u_lapsed_F2_ship445_365 AS (
    SELECT DISTINCT 'ship445_365' AS base,
        kesai.kokyano,
        kesai.ship_dt AS dt,
        'Lapsed' AS STATUS
    FROM temp_kesai_016 kesai
    INNER JOIN u_lapsed_F1_ship445_365 ON u_lapsed_F1_ship445_365.kokyano = kesai.kokyano
    WHERE kesai.ship_dt < u_lapsed_F1_ship445_365.dt + interval '365 days'
        AND kesai.ship_dt > u_lapsed_F1_ship445_365.dt
    ),
u_existing_ship445_365_purchase AS (
    SELECT DISTINCT 'ship445_365' AS base,
        kesai.kokyano,
        kesai.ship_dt AS dt,
        'Existing' AS STATUS
    FROM temp_kesai_016 kesai
    INNER JOIN cld_m a ON kesai.ship_dt = a.ymd_dt
    INNER JOIN temp_u_new_016 u_new ON u_new.kokyano = kesai.kokyano
        AND kesai.ship_dt >= u_new.first_ship_dt + interval '365 days'
    WHERE datediff(day, prev_ship_dt, ship_dt) < 365
        AND NOT EXISTS (
            SELECT 1
            FROM u_lapsed_F1_ship445_365
            WHERE u_lapsed_F1_ship445_365.kokyano = kesai.kokyano
                AND kesai.ship_dt >= u_lapsed_F1_ship445_365.dt
                AND kesai.ship_dt < u_lapsed_F1_ship445_365.dt + interval '365 days'
            )
    ) ,
u_lapsed_ship445_365_after_1yr AS (
    SELECT DISTINCT 'ship445_365' AS base,
        kesai.kokyano,
        to_date(kesai.ship_dt + interval '365 days') AS dt,
        'Lapsed' AS STATUS
    FROM temp_kesai_016 kesai
    WHERE NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai2
            WHERE kesai.kokyano = kesai2.kokyano
                AND kesai2.ship_dt > kesai.ship_dt
                AND kesai2.ship_dt <= to_date(kesai.ship_dt + interval '365 days')
            )
        AND /* not exists (select 1 from u_month_end_lapsed_ship where u_month_end_lapsed_ship.kokyano = u_last_ship_per_year.kokyano and 
u_month_end_lapsed_ship.dt = to_date(u_last_ship_per_year.last_ship_dt + interval '365 days'))
and */ to_date(kesai.ship_dt + interval '365 days') <= CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp())
        AND datediff(day, kesai.prev_ship_dt, kesai.ship_dt) < 365
    ),
u_existing_ship445_365_after_1yr AS (
    SELECT DISTINCT 'ship445_365' AS base,
        u_new.kokyano,
        to_date(u_new.first_ship_dt + interval '365 days') AS dt,
        'Existing' AS STATUS
    FROM temp_u_new_016 u_new
    INNER JOIN u_new_ship445_365 ON u_new.kokyano = u_new_ship445_365.kokyano
    WHERE u_new_ship445_365.dt > u_new.first_ship_dt
        AND u_new_ship445_365.dt < to_date(u_new.first_ship_dt + interval '365 days')
        AND /* not exists (select 1 from u_month_end_lapsed_ship where u_month_end_lapsed_ship.kokyano = u_last_ship_per_year.kokyano and 
u_month_end_lapsed_ship.dt = to_date(u_last_ship_per_year.last_ship_dt + interval '365 days'))
and */ to_date(u_new.first_ship_dt + interval '365 days') <= CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp())
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_new.kokyano
                AND kesai.ship_dt = to_date(u_new.first_ship_dt + interval '365 days')
            )
    
    UNION ALL
    
    SELECT DISTINCT 'ship445_365' AS base,
        u_lapsed_F1_ship445_365.kokyano,
        to_date(u_lapsed_F1_ship445_365.dt + interval '365 days') AS dt,
        'Existing' AS STATUS
    FROM u_lapsed_F1_ship445_365
    INNER JOIN u_lapsed_F2_ship445_365 ON u_lapsed_F1_ship445_365.kokyano = u_lapsed_F2_ship445_365.kokyano
    WHERE u_lapsed_F2_ship445_365.dt > u_lapsed_F1_ship445_365.dt
        AND u_lapsed_F2_ship445_365.dt < to_date(u_lapsed_F1_ship445_365.dt + interval '365 days')
        AND /* not exists (select 1 from u_month_end_lapsed_ship where u_month_end_lapsed_ship.kokyano = u_last_ship_per_year.kokyano and 
u_month_end_lapsed_ship.dt = to_date(u_last_ship_per_year.last_ship_dt + interval '365 days'))
and */ to_date(u_lapsed_F1_ship445_365.dt + interval '365 days') <= CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp())
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_lapsed_F1_ship445_365.kokyano
                AND kesai.ship_dt = to_date(u_lapsed_F1_ship445_365.dt + interval '365 days')
            )
    ),
u_all_month_end_ship445_365 AS (
    SELECT kokyano,
        ymd
    FROM u_all_month_end_ship
    WHERE ymd <= CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp())
        AND DATE_PART('year', ymd) >= DATE_PART('year', TO_DATE(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', CURRENT_TIMESTAMP()))) - 2
    ),
u_month_end_new_ship445_365 AS (
    SELECT 'ship445_365' AS base,
        u_new.kokyano,
        ymd AS dt,
        'New' AS STATUS
    FROM u_all_month_end_ship445_365
    INNER JOIN temp_u_new_016 u_new ON u_new.kokyano = u_all_month_end_ship445_365.kokyano
    WHERE u_all_month_end_ship445_365.ymd > u_new.first_ship_dt
        AND u_all_month_end_ship445_365.ymd < u_new.first_ship_dt + interval '365 days' --Added 07252022 for leap year 
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_month_end_ship445_365.kokyano
                AND u_all_month_end_ship445_365.ymd = kesai.ship_dt
            )
    ),
u_month_end_lapsed_ship445_365 AS (
    SELECT 'ship445_365' AS base,
        u_all_month_end_ship445_365.kokyano,
        ymd AS dt,
        'Lapsed' AS STATUS
    FROM u_all_month_end_ship445_365
    INNER JOIN u_lapsed_F1_ship445_365 ON u_lapsed_F1_ship445_365.kokyano = u_all_month_end_ship445_365.kokyano
    WHERE u_lapsed_F1_ship445_365.dt < u_all_month_end_ship445_365.ymd
        AND u_lapsed_F1_ship445_365.dt + interval '365 days' > u_all_month_end_ship445_365.ymd
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_month_end_ship445_365.kokyano
                AND u_all_month_end_ship445_365.ymd = kesai.ship_dt
            )
    
    UNION ALL
    
    SELECT 'ship445_365' AS base,
        kokyano,
        ymd AS dt,
        'Lapsed' AS STATUS
    FROM (
        SELECT u_all_month_end_ship445_365.kokyano,
            u_all_month_end_ship445_365.ymd,
            max(kesai.ship_dt) AS prev_ship_dt
        FROM u_all_month_end_ship445_365
        INNER JOIN temp_kesai_016 kesai ON kesai.kokyano = u_all_month_end_ship445_365.kokyano
        INNER JOIN cld_m a ON kesai.ship_dt = a.ymd_dt
        WHERE u_all_month_end_ship445_365.ymd >= kesai.ship_dt
        GROUP BY 1,
            2
        ) A
    WHERE datediff(day, A.prev_ship_dt, A.ymd) > 365
    ),
u_month_end_existing_ship445_365 AS (
    SELECT DISTINCT 'ship445_365' AS base,
        u_all_month_end_ship445_365.kokyano,
        ymd AS dt,
        'Existing' AS STATUS
    FROM u_all_month_end_ship445_365
    INNER JOIN temp_u_new_016 u_new ON u_new.kokyano = u_all_month_end_ship445_365.kokyano
    WHERE u_all_month_end_ship445_365.ymd > u_new.first_ship_dt
        AND NOT EXISTS (
            SELECT 1
            FROM u_month_end_new_ship445_365
            WHERE u_month_end_new_ship445_365.kokyano = u_all_month_end_ship445_365.kokyano
                AND u_month_end_new_ship445_365.dt = u_all_month_end_ship445_365.ymd
            )
        AND NOT EXISTS (
            SELECT 1
            FROM u_month_end_lapsed_ship445_365
            WHERE u_month_end_lapsed_ship445_365.kokyano = u_all_month_end_ship445_365.kokyano
                AND u_month_end_lapsed_ship445_365.dt = u_all_month_end_ship445_365.ymd
            )
        AND NOT EXISTS (
            SELECT 1
            FROM temp_kesai_016 kesai
            WHERE kesai.kokyano = u_all_month_end_ship445_365.kokyano
                AND u_all_month_end_ship445_365.ymd = kesai.ship_dt
            )
        AND NOT EXISTS (
            SELECT 1
            FROM u_existing_ship445_365_after_1yr
            WHERE u_existing_ship445_365_after_1yr.kokyano = u_all_month_end_ship445_365.kokyano
                AND u_existing_ship445_365_after_1yr.dt = u_all_month_end_ship445_365.ymd
            )
        AND NOT EXISTS (
            SELECT 1
            FROM u_lapsed_ship445_365_after_1yr
            WHERE u_lapsed_ship445_365_after_1yr.kokyano = u_all_month_end_ship445_365.kokyano
                AND u_lapsed_ship445_365_after_1yr.dt = u_all_month_end_ship445_365.ymd
            )
    ), ----	365 days Logic Addition 09/06/2022 Ends for Shipping
transformed as(
SELECT *
FROM u_new_order

UNION ALL

SELECT *
FROM u_exists_order_yr_start

UNION ALL

SELECT *
FROM u_lapsed_order_yr_start

UNION ALL

SELECT base,kokyano,dt,STATUS
FROM u_lapsed_F1_order

UNION ALL

SELECT *
FROM u_lapsed_F2_order

UNION ALL

SELECT *
FROM u_existing_order_purchase

UNION ALL

SELECT *
FROM u_lapsed_order_after_1yr

UNION ALL

SELECT *
FROM u_new_ship

UNION ALL

SELECT *
FROM u_exists_ship_yr_start

UNION ALL

SELECT *
FROM u_lapsed_ship_yr_start

UNION ALL

SELECT base,
kokyano,
dt,
STATUS
FROM u_lapsed_F1_ship

UNION ALL

SELECT *
FROM u_lapsed_F2_ship

UNION ALL

SELECT *
FROM u_existing_ship_purchase

UNION ALL

SELECT *
FROM u_lapsed_ship_after_1yr

UNION ALL

SELECT *
FROM u_month_end_new_order ------------------

UNION ALL

SELECT *
FROM u_month_end_lapsed_order

UNION ALL

SELECT *
FROM u_month_end_existing_order

UNION ALL

SELECT *
FROM u_month_end_new_ship

UNION ALL

SELECT *
FROM u_month_end_lapsed_ship

UNION ALL

SELECT *
FROM u_month_end_existing_ship

UNION ALL

SELECT *
FROM u_new_order_365

UNION ALL

SELECT *
FROM u_lapsed_F1_order_365

UNION ALL

SELECT *
FROM u_lapsed_F2_order_365

UNION ALL

SELECT *
FROM u_existing_order_365_purchase

UNION ALL

SELECT *
FROM u_lapsed_order_365_after_1yr

UNION ALL

SELECT *
FROM u_existing_order_365_after_1yr

UNION ALL

SELECT *
FROM u_month_end_new_order_365

UNION ALL

SELECT *
FROM u_month_end_lapsed_order_365

UNION ALL

SELECT *
FROM u_month_end_existing_order_365

UNION ALL

SELECT *
FROM u_new_ship445_365

UNION ALL

SELECT *
FROM u_lapsed_F1_ship445_365

UNION ALL

SELECT *
FROM u_lapsed_F2_ship445_365

UNION ALL

SELECT *
FROM u_existing_ship445_365_purchase

UNION ALL

SELECT *
FROM u_lapsed_ship445_365_after_1yr

UNION ALL

SELECT *
FROM u_existing_ship445_365_after_1yr

UNION ALL

SELECT *
FROM u_month_end_new_ship445_365

UNION ALL

SELECT *
FROM u_month_end_lapsed_ship445_365

UNION ALL

SELECT *
FROM u_month_end_existing_ship445_365
),
final as(
    select 
        base::varchar(11) as base,
        kokyano::varchar(60) as kokyano,
        dt::timestamp_ntz(9) as dt,
        status::varchar(8) as status
    from transformed
)
select * from final