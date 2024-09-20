WITH dm_kesai_mart_dly_general
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}
),
    a AS (
    SELECT kokyano,
        count(DISTINCT saleno) AS Order_Cnt_store,
        sum(gts) AS Order_Amt_store,
        sum(ciw_point) AS Point_Usage_Amt_store,
        count(DISTINCT CASE
                WHEN ciw_point > 0
                    THEN saleno
                ELSE NULL
                END) AS Point_Usage_Cnt_store,
        sum(ciw_discount) AS Coupon_Usage_Amt_store,
        count(DISTINCT CASE
                WHEN ciw_discount > 0
                    THEN saleno
                ELSE NULL
                END) AS Coupon_Usage_Cnt_store
    FROM dm_kesai_mart_dly_general
    WHERE order_dt >= '2019-01-01'
        AND channel IN ('直営・百貨店')
        AND (
            juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
            )
        AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
    GROUP BY 1
    ),
b
AS (
    SELECT kokyano,
        count(DISTINCT saleno) AS Order_Cnt_store_2y,
        sum(gts) AS Order_Amt_store_2y,
        sum(ciw_point) AS Point_Usage_Amt_store_2y,
        count(DISTINCT CASE
                WHEN ciw_point > 0
                    THEN saleno
                ELSE NULL
                END) AS Point_Usage_Cnt_store_2y,
        sum(ciw_discount) AS Coupon_Usage_Amt_store_2y,
        count(DISTINCT CASE
                WHEN ciw_discount > 0
                    THEN saleno
                ELSE NULL
                END) AS Coupon_Usage_Cnt_store_2y
    FROM dm_kesai_mart_dly_general
    WHERE order_dt >= add_months( date_trunc('day', current_timestamp())::date,-24)
    --add_months(date_trunc('day', current_timestamp()), - 24)
    --date_trunc('day', sysdate) + interval '2 year ago'
        AND channel IN ('直営・百貨店')
        AND (
            juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
            )
        AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
    GROUP BY 1
    ),
c
AS (
    SELECT kokyano,
        count(DISTINCT saleno) AS Order_Cnt_store_1y,
        sum(gts) AS Order_Amt_store_1y,
        sum(ciw_point) AS Point_Usage_Amt_store_1y,
        count(DISTINCT CASE
                WHEN ciw_point > 0
                    THEN saleno
                ELSE NULL
                END) AS Point_Usage_Cnt_store_1y,
        sum(ciw_discount) AS Coupon_Usage_Amt_store_1y,
        count(DISTINCT CASE
                WHEN ciw_discount > 0
                    THEN saleno
                ELSE NULL
                END) AS Coupon_Usage_Cnt_store_1y
    FROM dm_kesai_mart_dly_general
    WHERE order_dt >= add_months( date_trunc('day', current_timestamp())::date,-12)
    --add_months(date_trunc('day', current_timestamp()), - 12)
    --date_trunc('day', sysdate) + interval '1 year ago'
        AND channel IN ('直営・百貨店')
        AND (
            juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
            )
        AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
    GROUP BY 1
    ),
d
AS (
    SELECT kokyano,
        count(DISTINCT saleno) AS Order_Cnt_store_6m,
        sum(gts) AS Order_Amt_store_6m,
        sum(ciw_point) AS Point_Usage_Amt_store_6m,
        count(DISTINCT CASE
                WHEN ciw_point > 0
                    THEN saleno
                ELSE NULL
                END) AS Point_Usage_Cnt_store_6m,
        sum(ciw_discount) AS Coupon_Usage_Amt_store_6m,
        count(DISTINCT CASE
                WHEN ciw_discount > 0
                    THEN saleno
                ELSE NULL
                END) AS Coupon_Usage_Cnt_store_6m
    FROM dm_kesai_mart_dly_general
    WHERE order_dt >= add_months( date_trunc('day', current_timestamp())::date,-6)
    --add_months(date_trunc('day', current_timestamp()), - 6)
    --date_trunc('day', sysdate) + interval '6 month ago'
        AND channel IN ('直営・百貨店')
        AND (
            juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
            )
        AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
    GROUP BY 1
    ),
e
AS (
    SELECT kokyano,
        count(DISTINCT saleno) AS Order_Cnt_store_3m,
        sum(gts) AS Order_Amt_store_3m,
        sum(ciw_point) AS Point_Usage_Amt_store_3m,
        count(DISTINCT CASE
                WHEN ciw_point > 0
                    THEN saleno
                ELSE NULL
                END) AS Point_Usage_Cnt_store_3m,
        sum(ciw_discount) AS Coupon_Usage_Amt_store_3m,
        count(DISTINCT CASE
                WHEN ciw_discount > 0
                    THEN saleno
                ELSE NULL
                END) AS Coupon_Usage_Cnt_store_3m
    FROM dm_kesai_mart_dly_general
    WHERE order_dt >= add_months( date_trunc('day', current_timestamp())::date,-3)
    --add_months(date_trunc('day', current_timestamp()), - 3)
    --date_trunc('day', sysdate) + interval '3 month ago'
        AND channel IN ('直営・百貨店')
        AND (
            juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
            )
        AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
    GROUP BY 1
    ),
f
AS (
    SELECT kokyano,
        count(DISTINCT saleno) AS Order_Cnt_store_1m,
        sum(gts) AS Order_Amt_store_1m,
        sum(ciw_point) AS Point_Usage_Amt_store_1m,
        count(DISTINCT CASE
                WHEN ciw_point > 0
                    THEN saleno
                ELSE NULL
                END) AS Point_Usage_Cnt_store_1m,
        sum(ciw_discount) AS Coupon_Usage_Amt_store_1m,
        count(DISTINCT CASE
                WHEN ciw_discount > 0
                    THEN saleno
                ELSE NULL
                END) AS Coupon_Usage_Cnt_store_1m
    FROM dm_kesai_mart_dly_general
    WHERE order_dt >= add_months( date_trunc('day', current_timestamp())::date,-1)
    --add_months(date_trunc('day', current_timestamp()), -1)
    --date_trunc('day', sysdate) + interval '1 month ago'
        AND channel IN ('直営・百貨店')
        AND (
            juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
            )
        AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
    GROUP BY 1
    ),
final
AS (
    SELECT DISTINCT dkmd.kokyano::VARCHAR(60) AS Customer_No,
        f.Order_Cnt_store_1m::NUMBER(18,0) AS Order_Cnt_store_1m,
        f.Order_Amt_store_1m::FLOAT AS Order_Amt_store_1m,
        f.Point_Usage_Cnt_store_1m ::NUMBER(18,0) AS Point_Usage_Cnt_store_1m,
        f.Point_Usage_Amt_store_1m::FLOAT AS Point_Usage_Amt_store_1m,
        f.Coupon_Usage_Cnt_store_1m ::NUMBER(18,0) AS Coupon_Usage_Cnt_store_1m,
        f.Coupon_Usage_Amt_store_1m::FLOAT AS Coupon_Usage_Amt_store_1m,
        e.Order_Cnt_store_3m::NUMBER(18,0) AS Order_Cnt_store_3m,
        e.Order_Amt_store_3m::FLOAT AS Order_Amt_store_3m,
        e.Point_Usage_Cnt_store_3m ::NUMBER(18,0) AS Point_Usage_Cnt_store_3m,
        e.Point_Usage_Amt_store_3m::FLOAT AS Point_Usage_Amt_store_3m,
        e.Coupon_Usage_Cnt_store_3m ::NUMBER(18,0) AS Coupon_Usage_Cnt_store_3m,
        e.Coupon_Usage_Amt_store_3m::FLOAT AS Coupon_Usage_Amt_store_3m,
        d.Order_Cnt_store_6m::NUMBER(18,0) AS Order_Cnt_store_6m,
        d.Order_Amt_store_6m::FLOAT AS Order_Amt_store_6m,
        d.Point_Usage_Cnt_store_6m ::NUMBER(18,0) AS Point_Usage_Cnt_store_6m,
        d.Point_Usage_Amt_store_6m::FLOAT AS Point_Usage_Amt_store_6m,
        d.Coupon_Usage_Cnt_store_6m ::NUMBER(18,0) AS Coupon_Usage_Cnt_store_6m,
        d.Coupon_Usage_Amt_store_6m::FLOAT AS Coupon_Usage_Amt_store_6m,
        c.Order_Cnt_store_1y::NUMBER(18,0) AS Order_Cnt_store_1y,
        c.Order_Amt_store_1y::FLOAT AS Order_Amt_store_1y,
        c.Point_Usage_Cnt_store_1y ::NUMBER(18,0) AS Point_Usage_Cnt_store_1y,
        c.Point_Usage_Amt_store_1y::FLOAT AS Point_Usage_Amt_store_1y,
        c.Coupon_Usage_Cnt_store_1y ::NUMBER(18,0) AS Coupon_Usage_Cnt_store_1y,
        c.Coupon_Usage_Amt_store_1y::FLOAT AS Coupon_Usage_Amt_store_1y,
        b.Order_Cnt_store_2y::NUMBER(18,0) AS Order_Cnt_store_2y,
        b.Order_Amt_store_2y::FLOAT AS Order_Amt_store_2y,
        b.Point_Usage_Cnt_store_2y ::NUMBER(18,0) AS Point_Usage_Cnt_store_2y,
        b.Point_Usage_Amt_store_2y::FLOAT AS Point_Usage_Amt_store_2y,
        b.Coupon_Usage_Cnt_store_2y ::NUMBER(18,0) AS Coupon_Usage_Cnt_store_2y,
        b.Coupon_Usage_Amt_store_2y::FLOAT AS Coupon_Usage_Amt_store_2y,
        a.Order_Cnt_store::NUMBER(18,0) AS Order_Cnt_store,
        a.Order_Amt_store::FLOAT AS Order_Amt_store,
        a.Point_Usage_Cnt_store ::NUMBER(18,0) AS Point_Usage_Cnt_store,
        a.Point_Usage_Amt_store::FLOAT AS Point_Usage_Amt_store,
        a.Coupon_Usage_Cnt_store ::NUMBER(18,0) AS Coupon_Usage_Cnt_store,
        a.Coupon_Usage_Amt_store::FLOAT AS Coupon_Usage_Amt_store
    FROM dm_kesai_mart_dly_general dkmd
    JOIN a ON rtrim(dkmd.kokyano) = rtrim(a.kokyano)
    -- 2 years
    LEFT JOIN b ON rtrim(a.kokyano) = rtrim(b.kokyano)
    -- 1 year
    LEFT JOIN c ON rtrim(a.kokyano) = rtrim(c.kokyano)
    -- 6 month
    LEFT JOIN d ON rtrim(a.kokyano) = rtrim(d.kokyano)
    -- 3 month
    LEFT JOIN e ON rtrim(a.kokyano) = rtrim(e.kokyano)
    -- 1 month
    LEFT JOIN f ON rtrim(a.kokyano) = rtrim(f.kokyano)
    WHERE (
            dkmd.juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
            OR dkmd.juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
            OR dkmd.juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
            )
        AND dkmd.meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
    )
SELECT *
FROM
final
