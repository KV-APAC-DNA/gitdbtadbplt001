WITH kesai_m_data_mart_sub_old_dum
AS (
    SELECT *
    FROM snapjpdcledw_integration.kesai_m_data_mart_sub_old_dum
    ),
kesai_m_data_mart_sub_old_n_bn
AS (
    SELECT *
    FROM snapjpdcledw_integration.kesai_m_data_mart_sub_old_n_bn 
    ),
KESAI_M_DATA_MART_SUB_OLD_N as
(
    select * from {{ref('jpndcledw_integration__kesai_m_data_mart_sub_old_n')}}
),
t1
AS (
    SELECT (saleno)::CHARACTER VARYING AS saleno,
        gyono,
        0 AS bun_gyono,
        meisaikbn,
        itemcode,
        itemcode AS bun_itemcode,
        wariritu,
        tanka,
        warimaekomitanka,
        suryo,
        hensu,
        kingaku,
        warimaekomikingaku,
        meisainukikingaku,
        warimaenukikingaku,
        meisaitax,
        kesaiid,
        (saleno_trim)::CHARACTER VARYING AS saleno_trim,
        NULL AS bun_wariritu,
        0 AS bun_tanka,
        0 AS bun_warimaekomitanka,
        0 AS bun_suryo,
        0 AS bun_hensu,
        0 AS bun_kingaku,
        0 AS bun_warimaekomikingaku,
        0 AS bun_meisainukikingaku,
        0 AS bun_warimaenukikingaku,
        0 AS bun_meisaitax,
        CASE 
            WHEN ((meisaikbn)::TEXT = ('商品'::CHARACTER VARYING)::TEXT)
                THEN '1'::CHARACTER VARYING
            WHEN ((meisaikbn)::TEXT = ('特典'::CHARACTER VARYING)::TEXT)
                THEN '1'::CHARACTER VARYING
            WHEN ((meisaikbn)::TEXT = ('送料'::CHARACTER VARYING)::TEXT)
                THEN '1'::CHARACTER VARYING
            ELSE '0'::CHARACTER VARYING
            END AS ci_port_flg
    FROM kesai_m_data_mart_sub_old_n
    ),
t2
AS (
    SELECT saleno,
        gyono,
        bun_gyono,
        meisaikbn,
        itemcode,
        bun_itemcode,
        NULL AS wariritu,
        0 AS tanka,
        0 AS warimaekomitanka,
        0 AS suryo,
        0 AS hensu,
        0 AS kingaku,
        0 AS warimaekomikingaku,
        0 AS meisainukikingaku,
        0 AS warimaenukikingaku,
        0 AS meisaitax,
        kesaiid,
        saleno_trim,
        bun_wariritu,
        bun_tanka,
        bun_warimaekomitanka,
        bun_suryo,
        bun_hensu,
        bun_kingaku,
        bun_warimaekomikingaku,
        bun_meisainukikingaku,
        bun_warimaenukikingaku,
        bun_meisaitax,
        CASE 
            WHEN ((meisaikbn)::TEXT = ('商品'::CHARACTER VARYING)::TEXT)
                THEN '1'::CHARACTER VARYING
            WHEN ((meisaikbn)::TEXT = ('特典'::CHARACTER VARYING)::TEXT)
                THEN '1'::CHARACTER VARYING
            WHEN ((meisaikbn)::TEXT = ('送料'::CHARACTER VARYING)::TEXT)
                THEN '1'::CHARACTER VARYING
            ELSE '0'::CHARACTER VARYING
            END AS ci_port_flg
    FROM kesai_m_data_mart_sub_old_n_bn
    ),
t3
AS (
    SELECT saleno,
        gyono,
        gyono AS bun_gyono,
        meisaikbn,
        itemcode,
        itemcode AS bun_itemcode,
        NULL AS wariritu,
        kingaku AS tanka,
        kingaku AS warimaekomitanka,
        1 AS suryo,
        0 AS hensu,
        kingaku,
        kingaku AS warimaekomikingaku,
        kingaku AS meisainukikingaku,
        kingaku AS warimaenukikingaku,
        meisaitax,
        kesaiid,
        saleno_trim,
        NULL AS bun_wariritu,
        kingaku AS bun_tanka,
        kingaku AS bun_warimaekomitanka,
        1 AS bun_suryo,
        0 AS bun_hensu,
        kingaku AS bun_kingaku,
        kingaku AS bun_warimaekomikingaku,
        kingaku AS bun_meisainukikingaku,
        kingaku AS bun_warimaenukikingaku,
        meisaitax AS bun_meisaitax,
        '1' AS ci_port_flg
    FROM kesai_m_data_mart_sub_old_dum
    WHERE (
            (
                ((maker)::CHARACTER(3) = '1')
                OR ((maker)::CHARACTER(3) = '2')
                )
            AND ((meisaikbn)::TEXT = ('送料'::CHARACTER VARYING)::TEXT)
            )
    ),
final
AS (
    SELECT *
    FROM T1
    
    UNION ALL
    
    SELECT *
    FROM T2
    
    UNION ALL
    
    SELECT *
    FROM T3
    )
SELECT *
FROM final
