{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
}}

WITH dm_kesai_mart_dly_general
AS (
    select * from snapjpdcledw_integration.dm_kesai_mart_dly_general
    where channel IN ('通販', 'Web', '直営・百貨店')
        AND order_dt >= '2019-01-01'
        AND (
            juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
            OR juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
            )
        AND meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)--564482819 --37176363
),
edw_mds_jp_dcl_mt_h_product AS 
(
    select * from snapjpdcledw_integration.edw_mds_jp_dcl_mt_h_product --5982
),
happy AS 
(
    SELECT dkmdg_h.kokyano,
        dkmdg_h.saleno,
        dkmdg_h.order_dt
    FROM dm_kesai_mart_dly_general dkmdg_h
    WHERE dkmdg_h.h_o_item_code IN (
            SELECT distinct "ci-code"
            FROM edw_mds_jp_dcl_mt_h_product
            WHERE "happy bag flag" = '1'
            )
        -- AND dkmdg_h.channel IN ('通販', 'Web', '直営・百貨店')
        -- AND dkmdg_h.order_dt >= '2019-01-01'
        -- AND (
        --     dkmdg_h.juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
        --     OR dkmdg_h.juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
        --     OR dkmdg_h.juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
        --     )
        -- AND dkmdg_h.meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
    ),
outlet
AS (
    SELECT dkmdg_o.kokyano,
        dkmdg_o.saleno,
        dkmdg_o.order_dt
    FROM dm_kesai_mart_dly_general dkmdg_o
    WHERE dkmdg_o.h_o_item_code IN (
            SELECT distinct "ci-code"
            FROM edw_mds_jp_dcl_mt_h_product
            WHERE "outlet flag" = '1'
            )
        -- AND dkmdg_o.channel IN ('通販', 'Web', '直営・百貨店')
        -- AND dkmdg_o.order_dt >= '2019-01-01'
        -- AND (
        --     dkmdg_o.juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
        --     OR dkmdg_o.juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
        --     OR dkmdg_o.juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
        --     )
        -- AND dkmdg_o.meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
    ),
final
AS (
    SELECT DISTINCT dkmdg.kokyano,
        count(DISTINCT happy.saleno) AS Lbag_Reaction_Cnt,
        min(happy.order_dt) AS i_Lbag_Order_dt,
        max(happy.order_dt) AS l_Lbag_Order_dt,
        count(DISTINCT outlet.saleno) AS outlet_Reaction_Cnt,
        min(outlet.order_dt) AS i_outlet_Order_dt,
        max(outlet.order_dt) AS l_outlet_Order_dt
    FROM dm_kesai_mart_dly_general dkmdg
    LEFT JOIN happy ON dkmdg.kokyano = happy.kokyano
    LEFT JOIN outlet ON dkmdg.kokyano = outlet.kokyano
    -- WHERE dkmdg.channel IN ('通販', 'Web', '直営・百貨店')
    --     AND dkmdg.order_dt >= '2019-01-01'
    --     AND (
    --         dkmdg.juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
    --         OR dkmdg.juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
    --         OR dkmdg.juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
    --         )
    --     AND dkmdg.meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
    GROUP BY 1
    )
SELECT *
FROM final
