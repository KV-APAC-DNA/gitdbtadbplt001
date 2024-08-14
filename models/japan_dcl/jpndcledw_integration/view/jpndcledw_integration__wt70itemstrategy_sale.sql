WITH tm14shkos
AS (
    SELECT *
    FROM {{ source('jpdcledw_integration', 'tm14shkos') }}
    ),
zaiko_shohin_attr
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__zaiko_shohin_attr') }}
    ),
tm67juch_nm
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__tm67juch_nm') }}
    ),
get_ci_next_sale
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__get_ci_next_sale') }}
    ),
cit86osalm_kaigai
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cit86osalm_kaigai') }}
    ),
cit86osalm
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cit86osalm') }}
    ),
cit85osalh_kaigai
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cit85osalh_kaigai') }}
    ),
cit85osalh
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cit85osalh') }}
    ),
cit81salem
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cit81salem') }}
    ),
cit80saleh
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cit80saleh') }}
    ),
cim03item_zaiko
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cim03item_zaiko') }}
    ),
cim02tokui
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cim02tokui') }}
    ),
cim03item_hanbai
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cim03item_hanbai') }}
    ),
"wqtm07属性未設定名称マスタ"
AS (
    SELECT *
    FROM {{ source('jpdcledw_integration', '"WQTM07属性未設定名称マスタ"') }}
    ),
c_tbecclient
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__c_tbecclient') }}
    ),
ct1
AS (
    SELECT h.shukadate AS shukanengetubi,
        CASE 
            WHEN h.smkeiroid = 6
                THEN '02'
            ELSE '01'
            END AS channel,
        h.daihanrobunname AS konyuchubuncode,
        h.torikeikbn,
        'ダミーコード' AS tokuicode,
        m.itemcode_hanbai AS itemcode,
        k.kosecode,
        h.juchkbn,
        SUM(m.suryo * COALESCE(k.suryo, 1)) AS suryo,
        SUM(CASE 
                WHEN LEFT(h.juchkbn, 1) = '9'
                    THEN m.hensu * COALESCE(k.suryo, 1)
                ELSE 0
                END) AS hensu,
        SUM(NEXT.tyouseimaekingaku) AS anbunmaegaku_tyouseimae,
        SUM(NEXT.tyouseimaekingaku * COALESCE(k.koseritsu, 1)) AS meisainukikingaku_tyouseimae,
        SUM(m.meisainukikingaku) AS anbunmaegaku_tyouseigo,
        SUM(m.meisainukikingaku * COALESCE(k.koseritsu, 1)) AS meisainukikingaku_tyouseigo,
        SUM(m.anbunmeisainukikingaku * COALESCE(k.koseritsu, 1)) AS anbunmeisainukikingaku,
        h.henreasoncode,
        h.henreasonname,
        '' AS sokoname
    FROM cit80saleh h
    JOIN cit81salem m ON h.saleno = m.saleno
    LEFT JOIN tm14shkos k ON m.itemcode = k.itemcode
    LEFT JOIN get_ci_next_sale NEXT ON m.saleno = NEXT.saleno
        AND m.juchgyono = NEXT.juchgyono
    WHERE h.cancelflg = 0
        AND h.torikeikbn = '01'
        AND m.itemcode NOT IN ('9990000100', '9990000200')
        AND h.kakokbn = 0
    GROUP BY h.shukadate,
        CASE 
            WHEN h.smkeiroid = 6
                THEN '02'
            ELSE '01'
            END,
        h.daihanrobunname,
        h.torikeikbn,
        m.itemcode_hanbai,
        k.kosecode,
        h.juchkbn,
        h.henreasoncode,
        h.henreasonname
    
    UNION ALL
    
    SELECT h.shukadate AS shukanengetubi,
        CASE 
            WHEN h.torikeikbn <> '02'
                THEN '10'
            ELSE CASE 
                    WHEN h.shokuikibunrui = '1'
                        THEN '05'
                    WHEN h.shokuikibunrui = '2'
                        THEN '06'
                    WHEN h.shokuikibunrui = '3'
                        THEN '07'
                    ELSE CASE 
                            WHEN SUBSTRING(h.tokuicode, 2, 9) <= '000499999'
                                THEN '04'
                            WHEN SUBSTRING(h.tokuicode, 2, 9) BETWEEN '000500001'
                                    AND '000599999'
                                THEN '05'
                            WHEN SUBSTRING(h.tokuicode, 2, 9) BETWEEN '000600000'
                                    AND '000799999'
                                THEN '09'
                            WHEN SUBSTRING(h.tokuicode, 2, 9) BETWEEN '000800001'
                                    AND '000899999'
                                THEN '04'
                            WHEN SUBSTRING(h.tokuicode, 2, 9) >= '000900000'
                                THEN '09'
                            ELSE ''
                            END
                    END
            END AS channel,
        '' AS konyuchubuncode,
        h.torikeikbn,
        h.tokuicode,
        m.itemcode,
        k.kosecode,
        h.juchkbn,
        SUM(m.suryo * COALESCE(k.suryo, 1)) AS suryo,
        SUM(CASE 
                WHEN LEFT(h.juchkbn, 1) = '9'
                    THEN m.hensu * COALESCE(k.suryo, 1)
                ELSE 0
                END) AS hensu,
        0 AS anbunmaegaku_tyouseimae,
        0 AS meisainukikingaku_tyouseimae,
        SUM(m.meisainukikingaku) AS anbunmaegaku_tyouseigo,
        SUM(m.meisainukikingaku * COALESCE(k.koseritsu, 1)) AS meisainukikingaku_tyouseigo,
        SUM(m.anbunmeisainukikingaku * COALESCE(k.koseritsu, 1)) AS anbunmeisainukikingaku,
        h.henreasoncode,
        h.henreasonname,
        t.sokoname
    FROM cit85osalh h
    JOIN cit86osalm m ON h.ourino = m.ourino
    LEFT JOIN tm14shkos k ON m.itemcode = k.itemcode
    LEFT JOIN (
        SELECT tm.c_dstempocode AS sokocode,
            CONCAT (
                tm.c_dstempocode,
                ' : ',
                tm.c_dstemponame,
                ' ',
                tm.c_dstemponame
                ) AS sokoname
        FROM c_tbecclient tm
        ) t ON h.tenpocode = t.sokocode
    WHERE h.kakokbn = 0
    GROUP BY h.shukadate,
        CASE 
            WHEN h.torikeikbn <> '02'
                THEN '10'
            ELSE CASE 
                    WHEN h.shokuikibunrui = '1'
                        THEN '05'
                    WHEN h.shokuikibunrui = '2'
                        THEN '06'
                    WHEN h.shokuikibunrui = '3'
                        THEN '07'
                    ELSE CASE 
                            WHEN SUBSTRING(h.tokuicode, 2, 9) <= '000499999'
                                THEN '04'
                            WHEN SUBSTRING(h.tokuicode, 2, 9) BETWEEN '000500001'
                                    AND '000599999'
                                THEN '05'
                            WHEN SUBSTRING(h.tokuicode, 2, 9) BETWEEN '000600000'
                                    AND '000799999'
                                THEN '09'
                            WHEN SUBSTRING(h.tokuicode, 2, 9) BETWEEN '000800001'
                                    AND '000899999'
                                THEN '04'
                            WHEN SUBSTRING(h.tokuicode, 2, 9) >= '000900000'
                                THEN '09'
                            ELSE ''
                            END
                    END
            END,
        h.torikeikbn,
        h.tokuicode,
        m.itemcode,
        k.kosecode,
        h.juchkbn,
        h.henreasoncode,
        h.henreasonname,
        t.sokoname
    ),
t
AS (
    SELECT *
    FROM ct1
    
    UNION ALL
    
    SELECT h.shukadate AS shukanengetubi,
        '11' AS channel,
        '' AS konyuchubuncode,
        h.torikeikbn,
        h.tokuicode,
        m.itemcode,
        k.kosecode,
        h.juchkbn,
        SUM(m.suryo * COALESCE(k.suryo, 1)) AS suryo,
        SUM(CASE 
                WHEN LEFT(h.juchkbn, 1) = '9'
                    OR LEFT(h.juchkbn, 1) IS NULL
                    THEN m.hensu * COALESCE(k.suryo, 1)
                ELSE 0
                END) AS hensu,
        0 AS anbunmaegaku_tyouseimae,
        0 AS meisainukikingaku_tyouseimae,
        SUM(m.meisainukikingaku) AS anbunmaegaku_tyouseigo,
        SUM(m.meisainukikingaku * COALESCE(k.koseritsu, 1)) AS meisainukikingaku_tyouseigo,
        SUM(m.anbunmeisainukikingaku * COALESCE(k.koseritsu, 1)) AS anbunmeisainukikingaku,
        h.henreasoncode,
        h.henreasonname,
        '' AS sokoname
    FROM cit85osalh_kaigai h
    JOIN cit86osalm_kaigai m ON h.ourino = m.ourino
    LEFT JOIN tm14shkos k ON m.itemcode = k.itemcode
    WHERE h.kakokbn = 0
    GROUP BY h.shukadate,
        h.torikeikbn,
        h.tokuicode,
        m.itemcode,
        k.kosecode,
        h.juchkbn,
        h.henreasoncode,
        h.henreasonname
    ),
final
AS (
    SELECT t.shukanengetubi,
        t.channel,
        t.konyuchubuncode,
        t.torikeikbn,
        CONCAT (
            t.tokuicode,
            NVL2(cim02.tokuiname, ' : ', ''),
            COALESCE(CONCAT (
                    cim02.tokuiname,
                    ' ',
                    cim02.tokuiname_ryaku
                    ), '')
            ) AS tokuicode,
        CONCAT (
            COALESCE(t.itemcode, ''),
            ' : ',
            COALESCE(ia.itemname, ia2.itemname, '')
            ) AS hanbaiitem,
        CONCAT (
            ib.bar_cd2,
            ' : ',
            COALESCE(ib.itemname, ib2.itemname)
            ) AS kouseiitem,
        t.kosecode,
        z.bumon6_add_attr7 AS kuwake1,
        z.bumon6_add_attr8 AS kuwake2,
        t.juchkbn,
        tm67.cname AS juchkbncname,
        t.suryo,
        t.hensu,
        t.suryo + t.hensu AS urisuryo,
        t.anbunmaegaku_tyouseimae,
        t.meisainukikingaku_tyouseimae,
        t.anbunmaegaku_tyouseigo,
        t.meisainukikingaku_tyouseigo,
        t.anbunmeisainukikingaku,
        CASE 
            WHEN t.channel = '01'
                THEN '01:通販'
            WHEN t.channel = '02'
                THEN '02:社内販売'
            WHEN t.channel = '03'
                THEN '03:職域販売'
            WHEN t.channel = '04'
                THEN '04:代理店'
            WHEN t.channel = '05'
                THEN '05:職域（特販）'
            WHEN t.channel = '06'
                THEN '06:職域（代理店）'
            WHEN t.channel = '07'
                THEN '07:職域（販売会)'
            WHEN t.channel = '08'
                THEN '08:専門店'
            WHEN t.channel = '09'
                THEN '09:QVC 他'
            WHEN t.channel = '10'
                THEN '10:百貨店'
            WHEN t.channel = '11'
                THEN '11:海外'
            ELSE ''
            END AS channelname,
        z.bumon6_add_attr9 AS kuwakename1,
        z.bumon6_add_attr10 AS kuwakename2,
        t.henreasoncode,
        CONCAT (
            t.henreasoncode,
            ' : ',
            t.henreasonname
            ) AS henreasonname,
        t.sokoname,
        COALESCE(z.bumon6_kubun2, misettei1."区分名称その他") AS kubun2,
        COALESCE(z.bumon6_syohingun, misettei2."区分名称その他") AS syohingun,
        COALESCE(z.bumon6_jyuutenitem, misettei3."区分名称その他") AS jyuutenitem,
        COALESCE(z.bumon6_brand, misettei4."区分名称その他no") AS brandno,
        COALESCE(z.bumon6_brand, misettei4."区分名称その他") AS brand,
        COALESCE(z.bumon6_category, misettei5."区分名称その他no") AS categoryno,
        COALESCE(z.bumon6_category, misettei5."区分名称その他") AS category,
        COALESCE(z.bumon6_line, misettei6."区分名称その他no") AS LINENO,
        COALESCE(z.bumon6_line, misettei6."区分名称その他") AS line,
        COALESCE(z.bumon6_itemcategory, misettei7."区分名称その他no") AS itemcategoryno,
        COALESCE(z.bumon6_itemcategory, misettei7."区分名称その他") AS itemcategory,
        COALESCE(z.bumon6_kubun2_old, misettei8."区分名称その他") AS kubun2_old,
        z.bumon7_add_attr1,
        z.bumon7_add_attr2,
        z.bumon7_add_attr3,
        z.bumon7_add_attr4,
        z.bumon7_add_attr5,
        z.bumon7_add_attr6,
        z.bumon7_add_attr7,
        z.bumon7_add_attr8,
        z.bumon7_add_attr9,
        z.bumon7_add_attr10,
        z.bumon6_20kisyohingun,
        z.bumon6_20kinaieki1,
        z.bumon6_20kinaieki2,
        z.bumon6_20kinaieki3,
        z.bumon6_zyutensyohinyobi1,
        z.bumon6_zyutensyohinyobi2,
        z.bumon6_zyutensyohinyobi3,
        z.bumon6_zyutensyohinyobi4,
        z.bumon6_zyutensyohinyobi5,
        z.bumon6_okikaename,
        z.bumon6_zukyuyosoku1,
        z.bumon6_zukyuyosoku2,
        z.bumon6_zukyuyosoku3
    FROM t
    LEFT JOIN cim03item_hanbai ia ON t.itemcode = ia.itemcode
    LEFT JOIN cim03item_zaiko ib ON t.kosecode = ib.itemcode
    LEFT JOIN cim03item_zaiko ia2 ON t.itemcode = ia2.itemcode
    LEFT JOIN cim03item_hanbai ib2 ON t.kosecode = ib2.itemcode
    LEFT JOIN zaiko_shohin_attr z ON t.kosecode = z.shohin_code
    LEFT JOIN tm67juch_nm tm67 ON t.juchkbn = tm67.code
    LEFT JOIN cim02tokui cim02 ON t.tokuicode = cim02.tokuicode
    JOIN (
        SELECT "区分名称その他no",
            "区分名称その他"
        FROM "wqtm07属性未設定名称マスタ"
        WHERE "属性区分コード" = '1'
        ) misettei1 ON 1 = 1
    JOIN (
        SELECT "区分名称その他no",
            "区分名称その他"
        FROM "wqtm07属性未設定名称マスタ"
        WHERE "属性区分コード" = '2'
        ) misettei2 ON 1 = 1
    JOIN (
        SELECT "区分名称その他no",
            "区分名称その他"
        FROM "wqtm07属性未設定名称マスタ"
        WHERE "属性区分コード" = '3'
        ) misettei3 ON 1 = 1
    JOIN (
        SELECT "区分名称その他no",
            "区分名称その他"
        FROM "wqtm07属性未設定名称マスタ"
        WHERE "属性区分コード" = '4'
        ) misettei4 ON 1 = 1
    JOIN (
        SELECT "区分名称その他no",
            "区分名称その他"
        FROM "wqtm07属性未設定名称マスタ"
        WHERE "属性区分コード" = '5'
        ) misettei5 ON 1 = 1
    JOIN (
        SELECT "区分名称その他no",
            "区分名称その他"
        FROM "wqtm07属性未設定名称マスタ"
        WHERE "属性区分コード" = '6'
        ) misettei6 ON 1 = 1
    JOIN (
        SELECT "区分名称その他no",
            "区分名称その他"
        FROM "wqtm07属性未設定名称マスタ"
        WHERE "属性区分コード" = '7'
        ) misettei7 ON 1 = 1
    JOIN (
        SELECT "区分名称その他no",
            "区分名称その他"
        FROM "wqtm07属性未設定名称マスタ"
        WHERE "属性区分コード" = '8'
        ) misettei8 ON 1 = 1
    )
SELECT *
FROM final