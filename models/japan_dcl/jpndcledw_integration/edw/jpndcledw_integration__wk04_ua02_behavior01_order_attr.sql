WITH dm_kesai_mart_dly_general
AS
(
    select * from {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}
),
a AS
(
    SELECT DISTINCT kokyano,
        min(order_dt) AS i,
        max(order_dt) AS l
    FROM dm_kesai_mart_dly_general
    WHERE channel IN ('通販', 'Web', '直営・百貨店')
        AND (
            juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
            )
        AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
    GROUP BY 1
    ),
final AS
(
SELECT DISTINCT dkmd.kokyano::VARCHAR(60) AS Customer_No,
    count(DISTINCT CASE 
            WHEN channel = '通販'
                AND dkmd.order_dt >= '2019-01-01'
                THEN saleno
            ELSE NULL
            END)::NUMBER(18,0) AS LTV_Order_Cnt_Call,
    count(DISTINCT CASE 
            WHEN channel = 'Web'
                AND dkmd.order_dt >= '2019-01-01'
                THEN saleno
            ELSE NULL
            END)::NUMBER(18,0) AS LTV_Order_Cnt_Web,
    count(DISTINCT CASE 
            WHEN channel = '直営・百貨店'
                AND dkmd.order_dt >= '2019-01-01'
                THEN saleno
            ELSE NULL
            END)::NUMBER(18,0) AS LTV_Order_Cnt_Store,
    a.i::DATE AS I_Order_dt --to align w channel wktable, not use STATUS table and can we use min(order_dt)
    ,
    a.L::DATE AS L_Order_dt
FROM dm_kesai_mart_dly_general dkmd
LEFT JOIN a ON dkmd.kokyano = a.kokyano
WHERE dkmd.channel IN ('通販', 'Web', '直営・百貨店')
    AND dkmd.order_dt >= '2019-01-01'
    AND (
        dkmd.juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
        OR dkmd.juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
        OR dkmd.juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
        )
    AND dkmd.meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
GROUP BY 1,
    5,
    6
)
select * from final
