WITH dm_kesai_mart_dly_general
AS (
    select * from snapjpdcledw_integration.dm_kesai_mart_dly_general
),
    transform AS (
    SELECT DISTINCT kokyano,
        cast((
                count(DISTINCT CASE 
                        WHEN ciw_point > 0
                            THEN saleno
                        ELSE NULL
                        END)
                ) AS float8) AS a --purchase done with point
        ,
        cast(count(DISTINCT saleno) AS float8) AS b --total # of purchase.
        ,
        a / b AS Point_Usage_Ratio
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
final
AS (
    SELECT DISTINCT kokyano::VARCHAR(60) AS customer_no,
        Point_Usage_Ratio::float AS Point_Usage_Ratio
    FROM transform
    )
SELECT *
FROM final