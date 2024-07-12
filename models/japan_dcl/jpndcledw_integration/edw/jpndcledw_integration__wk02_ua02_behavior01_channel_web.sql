WITH dm_kesai_mart_dly_general
AS (
    SELECT *
    FROM snapjpdcledw_integration.dm_kesai_mart_dly_general
    ),
a
AS (
    SELECT kokyano,
        count(DISTINCT saleno) AS Order_Cnt_web,
        sum(gts) AS Order_Amt_web,
        sum(ciw_point) AS Point_Usage_Amt_web,
        count(DISTINCT CASE 
                WHEN ciw_point > 0
                    THEN saleno
                ELSE NULL
                END) AS Point_Usage_Cnt_web,
        sum(ciw_discount) AS Coupon_Usage_Amt_web,
        count(DISTINCT CASE 
                WHEN ciw_discount > 0
                    THEN saleno
                ELSE NULL
                END) AS Coupon_Usage_Cnt_web
    FROM dm_kesai_mart_dly_general
    WHERE order_dt >= '2019-01-01'
        AND channel IN ('Web')
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
        count(DISTINCT saleno) AS Order_Cnt_web_2y,
        sum(gts) AS Order_Amt_web_2y,
        sum(ciw_point) AS Point_Usage_Amt_web_2y,
        count(DISTINCT CASE 
                WHEN ciw_point > 0
                    THEN saleno
                ELSE NULL
                END) AS Point_Usage_Cnt_web_2y,
        sum(ciw_discount) AS Coupon_Usage_Amt_web_2y,
        count(DISTINCT CASE 
                WHEN ciw_discount > 0
                    THEN saleno
                ELSE NULL
                END) AS Coupon_Usage_Cnt_web_2y
    FROM dm_kesai_mart_dly_general
    WHERE order_dt >= add_months(date_trunc('day', '2024-07-01'::DATE), - 24)
    --add_months(date_trunc('day', sysdate()), - 24)
        --date_trunc('day', sysdate) + interval '2 year ago'
        AND channel IN ('Web')
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
        count(DISTINCT saleno) AS Order_Cnt_web_1y,
        sum(gts) AS Order_Amt_web_1y,
        sum(ciw_point) AS Point_Usage_Amt_web_1y,
        count(DISTINCT CASE 
                WHEN ciw_point > 0
                    THEN saleno
                ELSE NULL
                END) AS Point_Usage_Cnt_web_1y,
        sum(ciw_discount) AS Coupon_Usage_Amt_web_1y,
        count(DISTINCT CASE 
                WHEN ciw_discount > 0
                    THEN saleno
                ELSE NULL
                END) AS Coupon_Usage_Cnt_web_1y
    FROM dm_kesai_mart_dly_general
    WHERE order_dt >= add_months(date_trunc('day', '2024-07-01'::DATE), - 12)
    --add_months(date_trunc('day', sysdate()), - 12)
        --date_trunc('day', sysdate) + interval '1 year ago'
        AND channel IN ('Web')
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
        count(DISTINCT saleno) AS Order_Cnt_web_6m,
        sum(gts) AS Order_Amt_web_6m,
        sum(ciw_point) AS Point_Usage_Amt_web_6m,
        count(DISTINCT CASE 
                WHEN ciw_point > 0
                    THEN saleno
                ELSE NULL
                END) AS Point_Usage_Cnt_web_6m,
        sum(ciw_discount) AS Coupon_Usage_Amt_web_6m,
        count(DISTINCT CASE 
                WHEN ciw_discount > 0
                    THEN saleno
                ELSE NULL
                END) AS Coupon_Usage_Cnt_web_6m
    FROM dm_kesai_mart_dly_general
    WHERE order_dt >= add_months(date_trunc('day', '2024-07-01'::DATE), - 6)
    --add_months(date_trunc('day', sysdate()), - 6)
        --date_trunc('day', sysdate) + interval '6 month ago'
        AND channel IN ('Web')
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
        count(DISTINCT saleno) AS Order_Cnt_web_3m,
        sum(gts) AS Order_Amt_web_3m,
        sum(ciw_point) AS Point_Usage_Amt_web_3m,
        count(DISTINCT CASE 
                WHEN ciw_point > 0
                    THEN saleno
                ELSE NULL
                END) AS Point_Usage_Cnt_web_3m,
        sum(ciw_discount) AS Coupon_Usage_Amt_web_3m,
        count(DISTINCT CASE 
                WHEN ciw_discount > 0
                    THEN saleno
                ELSE NULL
                END) AS Coupon_Usage_Cnt_web_3m
    FROM dm_kesai_mart_dly_general
    WHERE order_dt >= add_months(date_trunc('day', '2024-07-01'::DATE), - 3)
    --add_months(date_trunc('day', sysdate()), - 3)
        --date_trunc('day', sysdate) + interval '3 month ago'
        AND channel IN ('Web')
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
        count(DISTINCT saleno) AS Order_Cnt_web_1m,
        sum(gts) AS Order_Amt_web_1m,
        sum(ciw_point) AS Point_Usage_Amt_web_1m,
        count(DISTINCT CASE 
                WHEN ciw_point > 0
                    THEN saleno
                ELSE NULL
                END) AS Point_Usage_Cnt_web_1m,
        sum(ciw_discount) AS Coupon_Usage_Amt_web_1m,
        count(DISTINCT CASE 
                WHEN ciw_discount > 0
                    THEN saleno
                ELSE NULL
                END) AS Coupon_Usage_Cnt_web_1m
    FROM dm_kesai_mart_dly_general
    WHERE order_dt >= add_months(date_trunc('day', '2024-07-01'::DATE), - 1)
        --add_months(date_trunc('day', sysdate()), - 1)
        --date_trunc('day', sysdate) + interval '1 month ago'
        AND channel IN ('Web')
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
        f.Order_Cnt_web_1m::NUMBER(18,0) AS Order_Cnt_web_1m,
        f.Order_Amt_web_1m::FLOAT AS Order_Amt_web_1m,
        f.Point_Usage_Cnt_web_1m::NUMBER(18,0) AS Point_Usage_Cnt_web_1m,
        f.Point_Usage_Amt_web_1m::FLOAT AS Point_Usage_Amt_web_1m,
        f.Coupon_Usage_Cnt_web_1m::NUMBER(18,0) AS Coupon_Usage_Cnt_web_1m,
        f.Coupon_Usage_Amt_web_1m::FLOAT AS Coupon_Usage_Amt_web_1m,
        e.Order_Cnt_web_3m::NUMBER(18,0) AS Order_Cnt_web_3m,
        e.Order_Amt_web_3m::FLOAT AS Order_Amt_web_3m,
        e.Point_Usage_Cnt_web_3m::NUMBER(18,0) AS Point_Usage_Cnt_web_3m,
        e.Point_Usage_Amt_web_3m::FLOAT AS Point_Usage_Amt_web_3m,
        e.Coupon_Usage_Cnt_web_3m::NUMBER(18,0) AS Coupon_Usage_Cnt_web_3m,
        e.Coupon_Usage_Amt_web_3m::FLOAT AS Coupon_Usage_Amt_web_3m,
        d.Order_Cnt_web_6m::NUMBER(18,0) AS Order_Cnt_web_6m,
        d.Order_Amt_web_6m::FLOAT AS Order_Amt_web_6m,
        d.Point_Usage_Cnt_web_6m::NUMBER(18,0) AS Point_Usage_Cnt_web_6m,
        d.Point_Usage_Amt_web_6m::FLOAT AS Point_Usage_Amt_web_6m,
        d.Coupon_Usage_Cnt_web_6m::NUMBER(18,0) AS Coupon_Usage_Cnt_web_6m,
        d.Coupon_Usage_Amt_web_6m::FLOAT AS Coupon_Usage_Amt_web_6m,
        c.Order_Cnt_web_1y::NUMBER(18,0) AS Order_Cnt_web_1y,
        c.Order_Amt_web_1y::FLOAT AS Order_Amt_web_1y,
        c.Point_Usage_Cnt_web_1y::NUMBER(18,0) AS Point_Usage_Cnt_web_1y,
        c.Point_Usage_Amt_web_1y::FLOAT AS Point_Usage_Amt_web_1y,
        c.Coupon_Usage_Cnt_web_1y::NUMBER(18,0) AS Coupon_Usage_Cnt_web_1y,
        c.Coupon_Usage_Amt_web_1y::FLOAT AS Coupon_Usage_Amt_web_1y,
        b.Order_Cnt_web_2y::NUMBER(18,0) AS Order_Cnt_web_2y,
        b.Order_Amt_web_2y::FLOAT AS Order_Amt_web_2y,
        b.Point_Usage_Cnt_web_2y::NUMBER(18,0) AS Point_Usage_Cnt_web_2y,
        b.Point_Usage_Amt_web_2y::FLOAT AS Point_Usage_Amt_web_2y,
        b.Coupon_Usage_Cnt_web_2y::NUMBER(18,0) AS Coupon_Usage_Cnt_web_2y,
        b.Coupon_Usage_Amt_web_2y::FLOAT AS Coupon_Usage_Amt_web_2y,
        a.Order_Cnt_web::NUMBER(18,0) AS Order_Cnt_web,
        a.Order_Amt_web::FLOAT AS Order_Amt_web,
        a.Point_Usage_Cnt_web::NUMBER(18,0) AS Point_Usage_Cnt_web,
        a.Point_Usage_Amt_web::FLOAT AS Point_Usage_Amt_web,
        a.Coupon_Usage_Cnt_web::NUMBER(18,0) AS Coupon_Usage_Cnt_web,
        a.Coupon_Usage_Amt_web::FLOAT AS Coupon_Usage_Amt_web
    FROM dm_kesai_mart_dly_general dkmd
    JOIN a ON dkmd.kokyano = a.kokyano
    -- 2 years
    LEFT JOIN b ON a.kokyano = b.kokyano
    -- 1 year
    LEFT JOIN c ON a.kokyano = c.kokyano
    -- 6 month
    LEFT JOIN d ON a.kokyano = d.kokyano
    -- 3 month
    LEFT JOIN e ON a.kokyano = e.kokyano
    -- 1 month
    LEFT JOIN
     f ON a.kokyano = f.kokyano WHERE (
        dkmd.juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
        OR dkmd.juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
        OR dkmd.juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
        )
    AND dkmd.meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0) 
)
select * from final