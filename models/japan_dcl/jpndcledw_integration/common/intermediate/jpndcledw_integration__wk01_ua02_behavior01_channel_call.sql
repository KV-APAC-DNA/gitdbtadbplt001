with dm_kesai_mart_dly_general as
(
    select * from snapjpdcledw_integration.dm_kesai_mart_dly_general
),
a AS
(
    SELECT kokyano,
            count(DISTINCT saleno) AS Order_Cnt_call,
            sum(gts) AS Order_Amt_call -- GTS
            ,
            sum(ciw_point) AS Point_Usage_Amt_call --CIW_POINT
            ,
            count(DISTINCT CASE 
                    WHEN ciw_point > 0
                        THEN saleno
                    ELSE NULL
                    END) AS Point_Usage_Cnt_call --count (distinct case when ciw_point > 0 then saleno else null end)
                    ,
            sum(ciw_discount) AS Coupon_Usage_Amt_call --CIW_Discount
            ,
            count(DISTINCT CASE 
                    WHEN ciw_discount > 0
                        THEN saleno
                    ELSE NULL
                    END) AS Coupon_Usage_Cnt_call --count(distinct case when ciw_discount > 0 then saleno else null end)
        FROM dm_kesai_mart_dly_general
        WHERE order_dt >= '2019-01-01'
            AND channel IN ('通販')
            AND (
                juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
                OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
                OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
                )
            AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
        GROUP BY 1
),
b as
(
    SELECT kokyano,
            count(DISTINCT saleno) AS Order_Cnt_call_2y,
            sum(gts) AS Order_Amt_call_2y,
            sum(ciw_point) AS Point_Usage_Amt_call_2y,
            count(DISTINCT CASE 
                    WHEN ciw_point > 0
                        THEN saleno
                    ELSE NULL
                    END) AS Point_Usage_Cnt_call_2y,
            sum(ciw_discount) AS Coupon_Usage_Amt_call_2y,
            count(DISTINCT CASE 
                    WHEN ciw_discount > 0
                        THEN saleno
                    ELSE NULL
                    END) AS Coupon_Usage_Cnt_call_2y
        FROM dm_kesai_mart_dly_general
        WHERE order_dt >= add_months( date_trunc('day', '2024-07-01'::date)::date,-24)
        --add_months( date_trunc('day', sysdate())::date,-24) 
        -- + interval '2 year ago'
            AND channel IN ('通販')
            AND (
                juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
                OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
                OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
                )
            AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
        GROUP BY 1
),
c as 
(
    SELECT kokyano,
            count(DISTINCT saleno) AS Order_Cnt_call_1y,
            sum(gts) AS Order_Amt_call_1y,
            sum(ciw_point) AS Point_Usage_Amt_call_1y,
            count(DISTINCT CASE 
                    WHEN ciw_point > 0
                        THEN saleno
                    ELSE NULL
                    END) AS Point_Usage_Cnt_call_1y,
            sum(ciw_discount) AS Coupon_Usage_Amt_call_1y,
            count(DISTINCT CASE 
                    WHEN ciw_discount > 0
                        THEN saleno
                    ELSE NULL
                    END) AS Coupon_Usage_Cnt_call_1y
        FROM dm_kesai_mart_dly_general
        WHERE order_dt >= add_months( date_trunc('day', '2024-07-01'::date)::date,-12)
        --add_months( date_trunc('day', sysdate())::date,-12) 
        --date_trunc('day', sysdate) + interval '1 year ago'
            AND channel IN ('通販')
            AND (
                juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
                OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
                OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
                )
            AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
        GROUP BY 1
), 
d as 
(
    SELECT kokyano,
            count(DISTINCT saleno) AS Order_Cnt_call_6m,
            sum(gts) AS Order_Amt_call_6m,
            sum(ciw_point) AS Point_Usage_Amt_call_6m,
            count(DISTINCT CASE 
                    WHEN ciw_point > 0
                        THEN saleno
                    ELSE NULL
                    END) AS Point_Usage_Cnt_call_6m,
            sum(ciw_discount) AS Coupon_Usage_Amt_call_6m,
            count(DISTINCT CASE 
                    WHEN ciw_discount > 0
                        THEN saleno
                    ELSE NULL
                    END) AS Coupon_Usage_Cnt_call_6m
        FROM dm_kesai_mart_dly_general
        WHERE order_dt >= add_months( date_trunc('day', '2024-07-01'::date)::date,-6)
        --add_months( date_trunc('day', sysdate())::date,-6) 
        --date_trunc('day', sysdate) + interval '6 month ago'
            AND channel IN ('通販')
            AND (
                juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
                OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
                OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
                )
            AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
        GROUP BY 1
), 
e as 
(
    SELECT kokyano,
            count(DISTINCT saleno) AS Order_Cnt_call_3m,
            sum(gts) AS Order_Amt_call_3m,
            sum(ciw_point) AS Point_Usage_Amt_call_3m,
            count(DISTINCT CASE 
                    WHEN ciw_point > 0
                        THEN saleno
                    ELSE NULL
                    END) AS Point_Usage_Cnt_call_3m,
            sum(ciw_discount) AS Coupon_Usage_Amt_call_3m,
            count(DISTINCT CASE 
                    WHEN ciw_discount > 0
                        THEN saleno
                    ELSE NULL
                    END) AS Coupon_Usage_Cnt_call_3m
        FROM dm_kesai_mart_dly_general
        WHERE order_dt >= add_months( date_trunc('day', '2024-07-01'::date)::date,-3)
        --add_months( date_trunc('day', sysdate())::date,-3) 
        --date_trunc('day', sysdate) + interval '3 month ago'
            AND channel IN ('通販')
            AND (
                juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
                OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
                OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
                )
            AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
        GROUP BY 1  
), 
f as 
(
    SELECT kokyano,
            count(DISTINCT saleno) AS Order_Cnt_call_1m,
            sum(gts) AS Order_Amt_call_1m,
            sum(ciw_point) AS Point_Usage_Amt_call_1m,
            count(DISTINCT CASE 
                    WHEN ciw_point > 0
                        THEN saleno
                    ELSE NULL
                    END) AS Point_Usage_Cnt_call_1m,
            sum(ciw_discount) AS Coupon_Usage_Amt_call_1m,
            count(DISTINCT CASE 
                    WHEN ciw_discount > 0
                        THEN saleno
                    ELSE NULL
                    END) AS Coupon_Usage_Cnt_call_1m
        FROM dm_kesai_mart_dly_general
        WHERE order_dt >= add_months( date_trunc('day', '2024-07-01'::date)::date,-1)
        --add_months( date_trunc('day', sysdate())::date,-1) 
        --date_trunc('day', sysdate) + interval '1 month ago'
            AND channel IN ('通販')
            AND (
                juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
                OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
                OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
                )
            AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
        GROUP BY 1
),
final as
(
    SELECT DISTINCT dkmd.kokyano::varchar(60) AS Customer_No,
        f.Order_Cnt_call_1m::NUMBER(18,0) AS Order_Cnt_call_1m,
        f.Order_Amt_call_1m::FLOAT AS Order_Amt_call_1m,
        f.Point_Usage_Cnt_call_1m::NUMBER(18,0) AS Point_Usage_Cnt_call_1m,
        f.Point_Usage_Amt_call_1m::FLOAT AS Point_Usage_Amt_call_1m,
        f.Coupon_Usage_Cnt_call_1m::NUMBER(18,0) AS Coupon_Usage_Cnt_call_1m,
        f.Coupon_Usage_Amt_call_1m::FLOAT AS Coupon_Usage_Amt_call_1m,
        e.Order_Cnt_call_3m::NUMBER(18,0) AS Order_Cnt_call_3m,
        e.Order_Amt_call_3m::FLOAT AS Order_Amt_call_3m,
        e.Point_Usage_Cnt_call_3m::NUMBER(18,0) AS Point_Usage_Cnt_call_3m,
        e.Point_Usage_Amt_call_3m::FLOAT AS Point_Usage_Amt_call_3m,
        e.Coupon_Usage_Cnt_call_3m::NUMBER(18,0) AS Coupon_Usage_Cnt_call_3m,
        e.Coupon_Usage_Amt_call_3m::FLOAT AS Coupon_Usage_Amt_call_3m,
        d.Order_Cnt_call_6m::NUMBER(18,0) AS Order_Cnt_call_6m,
        d.Order_Amt_call_6m::FLOAT AS Order_Amt_call_6m,
        d.Point_Usage_Cnt_call_6m::NUMBER(18,0) AS Point_Usage_Cnt_call_6m,
        d.Point_Usage_Amt_call_6m::FLOAT AS Point_Usage_Amt_call_6m,
        d.Coupon_Usage_Cnt_call_6m::NUMBER(18,0) AS Coupon_Usage_Cnt_call_6m,
        d.Coupon_Usage_Amt_call_6m::FLOAT AS Coupon_Usage_Amt_call_6m,
        c.Order_Cnt_call_1y::NUMBER(18,0) AS Order_Cnt_call_1y,
        c.Order_Amt_call_1y::FLOAT AS Order_Amt_call_1y,
        c.Point_Usage_Cnt_call_1y::NUMBER(18,0) AS Point_Usage_Cnt_call_1y,
        c.Point_Usage_Amt_call_1y::FLOAT AS Point_Usage_Amt_call_1y,
        c.Coupon_Usage_Cnt_call_1y::NUMBER(18,0) AS Coupon_Usage_Cnt_call_1y,
        c.Coupon_Usage_Amt_call_1y::FLOAT AS Coupon_Usage_Amt_call_1y,
        b.Order_Cnt_call_2y::NUMBER(18,0) AS Order_Cnt_call_2y,
        b.Order_Amt_call_2y::FLOAT AS Order_Amt_call_2y,
        b.Point_Usage_Cnt_call_2y::NUMBER(18,0) AS Point_Usage_Cnt_call_2y,
        b.Point_Usage_Amt_call_2y::FLOAT AS Point_Usage_Amt_call_2y,
        b.Coupon_Usage_Cnt_call_2y::NUMBER(18,0) AS Coupon_Usage_Cnt_call_2y,
        b.Coupon_Usage_Amt_call_2y::FLOAT AS Coupon_Usage_Amt_call_2y,
        a.Order_Cnt_call::NUMBER(18,0) AS Order_Cnt_call,
        a.Order_Amt_call::FLOAT AS Order_Amt_call,
        a.Point_Usage_Cnt_call::NUMBER(18,0) AS Point_Usage_Cnt_call,
        a.Point_Usage_Amt_call::FLOAT AS Point_Usage_Amt_call,
        a.Coupon_Usage_Cnt_call::NUMBER(18,0) AS Coupon_Usage_Cnt_call,
        a.Coupon_Usage_Amt_call::FLOAT AS Coupon_Usage_Amt_call
    FROM dm_kesai_mart_dly_general dkmd
    join a ON rtrim(dkmd.kokyano) = rtrim(a.kokyano)
    left join b ON rtrim(a.kokyano) = rtrim(b.kokyano)
    left join c ON rtrim(a.kokyano) = rtrim(c.kokyano)
    left join d ON rtrim(a.kokyano) = rtrim(d.kokyano)
    left join e ON rtrim(a.kokyano) = rtrim(e.kokyano)
    left join f ON rtrim(a.kokyano) = rtrim(f.kokyano)
    WHERE (
            dkmd.juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
            OR dkmd.juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
            OR dkmd.juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
            )
        AND dkmd.meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
)
select * from final 