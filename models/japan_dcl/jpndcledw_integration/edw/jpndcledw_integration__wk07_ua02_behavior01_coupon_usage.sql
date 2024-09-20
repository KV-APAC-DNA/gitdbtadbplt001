with dm_kesai_mart_dly_general
AS
(
    select * from {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}
),
transform as (
    SELECT kokyano,
        cast((
                count(DISTINCT CASE 
                        WHEN ciw_discount > 0
                            THEN saleno
                        ELSE NULL
                        END)
                ) AS float8) AS a --purchase done with coupon
        ,
        cast(count(DISTINCT saleno) AS float8) AS b --total # of purchase.  
        ,
        a / b AS coupon_Usage_ratio,
        cast((sum(ciw_discount)) AS float8) AS c --purchase amount done with coupon
        ,
        CASE 
            WHEN c = '0'
                THEN '0'
            ELSE c / a
            END AS Coupon_Usage_Amt_Average --total used amount of coupon/how many purchase done with coupon
        ,
        min(order_dt) AS I_Coupon_Usage_dt,
        max(order_dt) AS L_Coupon_Usage_dt
    FROM dm_kesai_mart_dly_general
    WHERE order_dt >= '2019-01-01'
        AND channel IN ('通販', 'Web', '直営・百貨店')
        AND (
            juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
            )
        AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
    GROUP BY 1
    ),
    final AS (
        SELECT kokyano::VARCHAR(60) AS customer_no,
            coupon_Usage_Ratio::FLOAT AS coupon_Usage_Ratio,
            Coupon_Usage_Amt_Average::FLOAT AS Coupon_Usage_Amt_Average,
            I_Coupon_Usage_dt::DATE AS I_Coupon_Usage_dt,
            L_Coupon_Usage_dt::DATE AS L_Coupon_Usage_dt
        FROM transform
        )

SELECT *
FROM final
