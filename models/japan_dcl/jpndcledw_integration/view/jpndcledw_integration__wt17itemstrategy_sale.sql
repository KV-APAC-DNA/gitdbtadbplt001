WITH tm14shkos
AS (
    SELECT *
    FROM {{ source('jpdcledw_integration', 'tm14shkos') }}
    ),
cim08shkos_bunkai
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cim08shkos_bunkai') }}
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
syouhincd_henkan
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__syouhincd_henkan') }}
    ),
get_ci_next_sale
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__get_ci_next_sale') }}
    ),
cit86osalm
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cit86osalm') }}
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
cim03item_hanbai
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cim03item_hanbai') }}
    ),
"wqtm07属性未設定名称マスタ"
AS (
    SELECT *
    FROM {{ source('jpdcledw_integration', '"wqtm07属性未設定名称マスタ"') }}
    ),
final
AS (
    SELECT t.shukadate,
        t.channel,
        t.konyuchubuncode,
        (((t.itemcode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (COALESCE(ia.itemname, ia2.itemname))::TEXT) AS hanbaiitem,
        (((t.kosecode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (COALESCE(ib.itemname, ib2.itemname))::TEXT) AS kouseiitem,
        z.bumon6_add_attr7 AS kuwake1,
        z.bumon6_add_attr8 AS kuwake2,
        t.juchkbn,
        tm67.cname AS juchkbncname,
        t.suryo,
        t.hensu,
        (t.suryo + t.hensu) AS urisuryo,
        t.anbunmaegaku_tyouseimae,
        t.meisainukikingaku_tyouseimae,
        t.anbunmaegaku_tyouseigo,
        t.meisainukikingaku_tyouseigo,
        t.anbunmeisainukikingaku,
        CASE 
            WHEN ((t.channel)::TEXT = ('01'::CHARACTER VARYING)::TEXT)
                THEN '01:通販'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('02'::CHARACTER VARYING)::TEXT)
                THEN '02:社内販売'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('03'::CHARACTER VARYING)::TEXT)
                THEN '03:職域販売'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('04'::CHARACTER VARYING)::TEXT)
                THEN '04:代理店'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('05'::CHARACTER VARYING)::TEXT)
                THEN '05:職域（特販）'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('06'::CHARACTER VARYING)::TEXT)
                THEN '06:職域（代理店）'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('07'::CHARACTER VARYING)::TEXT)
                THEN '07:職域（販売会)'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('08'::CHARACTER VARYING)::TEXT)
                THEN '08:専門店'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('09'::CHARACTER VARYING)::TEXT)
                THEN '09:QVC 他'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('10'::CHARACTER VARYING)::TEXT)
                THEN '10:百貨店'::CHARACTER VARYING
            ELSE ''::CHARACTER VARYING
            END AS channelname,
        z.bumon6_add_attr9 AS kuwakename1,
        z.bumon6_add_attr10 AS kuwakename2,
        t.henreasoncode,
        (((t.henreasoncode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (t.henreasonname)::TEXT) AS henreasonname,
        CASE 
            WHEN (z.bumon6_kubun2 IS NULL)
                THEN misettei1."区分名称その他"
            ELSE z.bumon6_kubun2
            END AS kubun2,
        CASE 
            WHEN (z.bumon6_syohingun IS NULL)
                THEN misettei2."区分名称その他"
            ELSE z.bumon6_syohingun
            END AS syohingun,
        CASE 
            WHEN (z.bumon6_jyuutenitem IS NULL)
                THEN misettei3."区分名称その他"
            ELSE z.bumon6_jyuutenitem
            END AS jyuutenitem,
        CASE 
            WHEN (z.bumon6_brand IS NULL)
                THEN misettei4."区分名称その他no"
            ELSE z.bumon6_brandno
            END AS brandno,
        CASE 
            WHEN (z.bumon6_brand IS NULL)
                THEN misettei4."区分名称その他"
            ELSE z.bumon6_brand
            END AS brand,
        CASE 
            WHEN (z.bumon6_category IS NULL)
                THEN misettei5."区分名称その他no"
            ELSE z.bumon6_categoryno
            END AS categoryno,
        CASE 
            WHEN (z.bumon6_category IS NULL)
                THEN misettei5."区分名称その他"
            ELSE z.bumon6_category
            END AS category,
        CASE 
            WHEN (z.bumon6_line IS NULL)
                THEN misettei6."区分名称その他no"
            ELSE z.bumon6_lineno
            END AS LINENO,
        CASE 
            WHEN (z.bumon6_line IS NULL)
                THEN misettei6."区分名称その他"
            ELSE z.bumon6_line
            END AS line,
        CASE 
            WHEN (z.bumon6_itemcategory IS NULL)
                THEN misettei7."区分名称その他no"
            ELSE z.bumon6_itemcategoryno
            END AS itemcategoryno,
        CASE 
            WHEN (z.bumon6_itemcategory IS NULL)
                THEN misettei7."区分名称その他"
            ELSE z.bumon6_itemcategory
            END AS itemcategory,
        CASE 
            WHEN (z.bumon6_kubun2_old IS NULL)
                THEN misettei8."区分名称その他"
            ELSE z.bumon6_kubun2_old
            END AS kubun2_old
    FROM (
        (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        SELECT h.shukadate,
                                                                            CASE 
                                                                                WHEN ((h.kaisha)::TEXT = ('000'::CHARACTER VARYING)::TEXT)
                                                                                    THEN '01'::CHARACTER VARYING
                                                                                WHEN ((h.kaisha)::TEXT = ('001'::CHARACTER VARYING)::TEXT)
                                                                                    THEN '02'::CHARACTER VARYING
                                                                                ELSE '03'::CHARACTER VARYING
                                                                                END AS channel,
                                                                            h.daihanrobunname AS konyuchubuncode,
                                                                            m.itemcode_hanbai AS itemcode,
                                                                            "k".kosecode,
                                                                            h.juchkbn,
                                                                            ((m.suryo * COALESCE(item.bunkai_kossu, ((1)::NUMERIC)::NUMERIC(18, 0))) * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0))) AS suryo,
                                                                            CASE 
                                                                                WHEN (
                                                                                        (
                                                                                            "substring" (
                                                                                                (h.juchkbn)::TEXT,
                                                                                                1,
                                                                                                1
                                                                                                ) = ('9'::CHARACTER VARYING)::TEXT
                                                                                            )
                                                                                        OR (
                                                                                            (
                                                                                                "substring" (
                                                                                                    (h.juchkbn)::TEXT,
                                                                                                    1,
                                                                                                    1
                                                                                                    ) IS NULL
                                                                                                )
                                                                                            AND ('9' IS NULL)
                                                                                            )
                                                                                        )
                                                                                    THEN ((m.hensu * COALESCE(item.bunkai_kossu, ((1)::NUMERIC)::NUMERIC(18, 0))) * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
                                                                                ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                                                                                END AS hensu,
                                                                            0 AS anbunmaegaku_tyouseimae,
                                                                            0 AS meisainukikingaku_tyouseimae,
                                                                            m.meisainukikingaku AS anbunmaegaku_tyouseigo,
                                                                            (((m.meisainukikingaku)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION) AS meisainukikingaku_tyouseigo,
                                                                            (((m.anbunmeisainukikingaku)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION) AS anbunmeisainukikingaku,
                                                                            h.henreasoncode,
                                                                            h.henreasonname
                                                                        FROM (
                                                                            (
                                                                                (
                                                                                    (
                                                                                        cit80saleh h JOIN cit81salem m ON (((h.saleno)::TEXT = (m.saleno)::TEXT))
                                                                                        ) LEFT JOIN cim08shkos_bunkai item ON (((m.itemcode_hanbai)::TEXT = (item.itemcode)::TEXT))
                                                                                    ) LEFT JOIN syouhincd_henkan henkan ON (((item.bunkai_itemcode)::TEXT = (henkan.itemcode)::TEXT))
                                                                                ) LEFT JOIN tm14shkos "k" ON (((COALESCE(henkan.koseiocode, COALESCE(item.bunkai_itemcode, m.itemcode)))::TEXT = ("k".itemcode)::TEXT))
                                                                            )
                                                                        WHERE (
                                                                                (
                                                                                    (
                                                                                        (
                                                                                            (h.cancelflg = ((0)::NUMERIC)::NUMERIC(18, 0))
                                                                                            AND ((h.torikeikbn)::TEXT = ('01'::CHARACTER VARYING)::TEXT)
                                                                                            )
                                                                                        AND ((m.itemcode)::TEXT <> ('9990000100'::CHARACTER VARYING)::TEXT)
                                                                                        )
                                                                                    AND ((m.itemcode)::TEXT <> ('9990000200'::CHARACTER VARYING)::TEXT)
                                                                                    )
                                                                                AND (h.kakokbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                                                                                )
                                                                        
                                                                        UNION ALL
                                                                        
                                                                        SELECT h.shukadate,
                                                                            CASE 
                                                                                WHEN (h.smkeiroid = ((6)::NUMERIC)::NUMERIC(18, 0))
                                                                                    THEN '02'::CHARACTER VARYING
                                                                                ELSE '01'::CHARACTER VARYING
                                                                                END AS channel,
                                                                            h.daihanrobunname AS konyuchubuncode,
                                                                            m.itemcode_hanbai AS itemcode,
                                                                            "k".kosecode,
                                                                            h.juchkbn,
                                                                            (m.suryo * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0))) AS suryo,
                                                                            CASE 
                                                                                WHEN (
                                                                                        (
                                                                                            "substring" (
                                                                                                (h.juchkbn)::TEXT,
                                                                                                1,
                                                                                                1
                                                                                                ) = ('9'::CHARACTER VARYING)::TEXT
                                                                                            )
                                                                                        OR (
                                                                                            (
                                                                                                "substring" (
                                                                                                    (h.juchkbn)::TEXT,
                                                                                                    1,
                                                                                                    1
                                                                                                    ) IS NULL
                                                                                                )
                                                                                            AND ('9' IS NULL)
                                                                                            )
                                                                                        )
                                                                                    THEN (m.hensu * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
                                                                                ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                                                                                END AS hensu,
                                                                            "next".tyouseimaekingaku AS anbunmaegaku_tyouseimae,
                                                                            ("next".tyouseimaekingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS meisainukikingaku_tyouseimae,
                                                                            m.meisainukikingaku AS anbunmaegaku_tyouseigo,
                                                                            (m.meisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS meisainukikingaku_tyouseigo,
                                                                            (m.anbunmeisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS anbunmeisainukikingaku,
                                                                            h.henreasoncode,
                                                                            h.henreasonname
                                                                        FROM (
                                                                            (
                                                                                (
                                                                                    cit80saleh h JOIN cit81salem m ON (((h.saleno)::TEXT = (m.saleno)::TEXT))
                                                                                    ) LEFT JOIN tm14shkos "k" ON (((m.itemcode)::TEXT = ("k".itemcode)::TEXT))
                                                                                ) LEFT JOIN get_ci_next_sale "next" ON (
                                                                                    (
                                                                                        ((m.saleno)::TEXT = ("next".saleno)::TEXT)
                                                                                        AND (m.juchgyono = "next".juchgyono)
                                                                                        )
                                                                                    )
                                                                            )
                                                                        WHERE (
                                                                                (
                                                                                    (
                                                                                        (
                                                                                            (h.cancelflg = ((0)::NUMERIC)::NUMERIC(18, 0))
                                                                                            AND ((h.torikeikbn)::TEXT = ('01'::CHARACTER VARYING)::TEXT)
                                                                                            )
                                                                                        AND ((m.itemcode)::TEXT <> ('9990000100'::CHARACTER VARYING)::TEXT)
                                                                                        )
                                                                                    AND ((m.itemcode)::TEXT <> ('9990000200'::CHARACTER VARYING)::TEXT)
                                                                                    )
                                                                                AND (h.kakokbn = ((0)::NUMERIC)::NUMERIC(18, 0))
                                                                                )
                                                                        )
                                                                    
                                                                    UNION ALL
                                                                    
                                                                    SELECT h.shukadate,
                                                                        CASE 
                                                                            WHEN ((h.torikeikbn)::TEXT <> ('02'::CHARACTER VARYING)::TEXT)
                                                                                THEN '10'::CHARACTER VARYING
                                                                            ELSE CASE 
                                                                                    WHEN ((h.shokuikibunrui)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                                                                                        THEN '05'::CHARACTER VARYING
                                                                                    WHEN ((h.shokuikibunrui)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                                                                                        THEN '06'::CHARACTER VARYING
                                                                                    WHEN ((h.shokuikibunrui)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                                                                                        THEN '07'::CHARACTER VARYING
                                                                                    ELSE CASE 
                                                                                            WHEN (
                                                                                                    "substring" (
                                                                                                        (h.tokuicode)::TEXT,
                                                                                                        2,
                                                                                                        9
                                                                                                        ) <= ('000499999'::CHARACTER VARYING)::TEXT
                                                                                                    )
                                                                                                THEN '04'::CHARACTER VARYING
                                                                                            WHEN (
                                                                                                    (
                                                                                                        "substring" (
                                                                                                            (h.tokuicode)::TEXT,
                                                                                                            2,
                                                                                                            9
                                                                                                            ) >= ('000500001'::CHARACTER VARYING)::TEXT
                                                                                                        )
                                                                                                    AND (
                                                                                                        "substring" (
                                                                                                            (h.tokuicode)::TEXT,
                                                                                                            2,
                                                                                                            9
                                                                                                            ) <= ('000599999'::CHARACTER VARYING)::TEXT
                                                                                                        )
                                                                                                    )
                                                                                                THEN '05'::CHARACTER VARYING
                                                                                            WHEN (
                                                                                                    (
                                                                                                        "substring" (
                                                                                                            (h.tokuicode)::TEXT,
                                                                                                            2,
                                                                                                            9
                                                                                                            ) >= ('000600000'::CHARACTER VARYING)::TEXT
                                                                                                        )
                                                                                                    AND (
                                                                                                        "substring" (
                                                                                                            (h.tokuicode)::TEXT,
                                                                                                            2,
                                                                                                            9
                                                                                                            ) <= ('000799999'::CHARACTER VARYING)::TEXT
                                                                                                        )
                                                                                                    )
                                                                                                THEN '09'::CHARACTER VARYING
                                                                                            WHEN (
                                                                                                    (
                                                                                                        "substring" (
                                                                                                            (h.tokuicode)::TEXT,
                                                                                                            2,
                                                                                                            9
                                                                                                            ) >= ('000800001'::CHARACTER VARYING)::TEXT
                                                                                                        )
                                                                                                    AND (
                                                                                                        "substring" (
                                                                                                            (h.tokuicode)::TEXT,
                                                                                                            2,
                                                                                                            9
                                                                                                            ) <= ('000899999'::CHARACTER VARYING)::TEXT
                                                                                                        )
                                                                                                    )
                                                                                                THEN '04'::CHARACTER VARYING
                                                                                            WHEN (
                                                                                                    "substring" (
                                                                                                        (h.tokuicode)::TEXT,
                                                                                                        2,
                                                                                                        9
                                                                                                        ) >= ('000900000'::CHARACTER VARYING)::TEXT
                                                                                                    )
                                                                                                THEN '09'::CHARACTER VARYING
                                                                                            ELSE ''::CHARACTER VARYING
                                                                                            END
                                                                                    END
                                                                            END AS channel,
                                                                        ''::CHARACTER VARYING AS konyuchubuncode,
                                                                        m.itemcode_hanbai AS itemcode,
                                                                        "k".kosecode,
                                                                        h.juchkbn,
                                                                        ((m.suryo * COALESCE(item.bunkai_kossu, ((1)::NUMERIC)::NUMERIC(18, 0))) * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0))) AS suryo,
                                                                        CASE 
                                                                            WHEN (
                                                                                    (
                                                                                        "substring" (
                                                                                            (h.juchkbn)::TEXT,
                                                                                            1,
                                                                                            1
                                                                                            ) = ('9'::CHARACTER VARYING)::TEXT
                                                                                        )
                                                                                    OR (
                                                                                        (
                                                                                            "substring" (
                                                                                                (h.juchkbn)::TEXT,
                                                                                                1,
                                                                                                1
                                                                                                ) IS NULL
                                                                                            )
                                                                                        AND ('9' IS NULL)
                                                                                        )
                                                                                    )
                                                                                THEN ((m.hensu * COALESCE(item.bunkai_kossu, ((1)::NUMERIC)::NUMERIC(18, 0))) * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
                                                                            ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                                                                            END AS hensu,
                                                                        0 AS anbunmaegaku_tyouseimae,
                                                                        0 AS meisainukikingaku_tyouseimae,
                                                                        m.meisainukikingaku AS anbunmaegaku_tyouseigo,
                                                                        (((m.meisainukikingaku)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION) AS meisainukikingaku_tyouseigo,
                                                                        (((m.anbunmeisainukikingaku)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION) AS anbunmeisainukikingaku,
                                                                        h.henreasoncode,
                                                                        h.henreasonname
                                                                    FROM (
                                                                        (
                                                                            (
                                                                                (
                                                                                    cit85osalh h JOIN cit86osalm m ON (((h.ourino)::TEXT = (m.ourino)::TEXT))
                                                                                    ) LEFT JOIN cim08shkos_bunkai item ON (((m.itemcode_hanbai)::TEXT = (item.itemcode)::TEXT))
                                                                                ) LEFT JOIN syouhincd_henkan henkan ON (((item.bunkai_itemcode)::TEXT = (henkan.itemcode)::TEXT))
                                                                            ) LEFT JOIN tm14shkos "k" ON (((COALESCE(henkan.koseiocode, COALESCE(item.bunkai_itemcode, m.itemcode)))::TEXT = ("k".itemcode)::TEXT))
                                                                        )
                                                                    WHERE (h.kakokbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                                                                    )
                                                                
                                                                UNION ALL
                                                                
                                                                SELECT h.shukadate,
                                                                    CASE 
                                                                        WHEN ((h.torikeikbn)::TEXT <> ('02'::CHARACTER VARYING)::TEXT)
                                                                            THEN '10'::CHARACTER VARYING
                                                                        ELSE CASE 
                                                                                WHEN ((h.shokuikibunrui)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                                                                                    THEN '05'::CHARACTER VARYING
                                                                                WHEN ((h.shokuikibunrui)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                                                                                    THEN '06'::CHARACTER VARYING
                                                                                WHEN ((h.shokuikibunrui)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                                                                                    THEN '07'::CHARACTER VARYING
                                                                                ELSE CASE 
                                                                                        WHEN (
                                                                                                "substring" (
                                                                                                    (h.tokuicode)::TEXT,
                                                                                                    2,
                                                                                                    9
                                                                                                    ) <= ('000499999'::CHARACTER VARYING)::TEXT
                                                                                                )
                                                                                            THEN '04'::CHARACTER VARYING
                                                                                        WHEN (
                                                                                                (
                                                                                                    "substring" (
                                                                                                        (h.tokuicode)::TEXT,
                                                                                                        2,
                                                                                                        9
                                                                                                        ) >= ('000500001'::CHARACTER VARYING)::TEXT
                                                                                                    )
                                                                                                AND (
                                                                                                    "substring" (
                                                                                                        (h.tokuicode)::TEXT,
                                                                                                        2,
                                                                                                        9
                                                                                                        ) <= ('000599999'::CHARACTER VARYING)::TEXT
                                                                                                    )
                                                                                                )
                                                                                            THEN '05'::CHARACTER VARYING
                                                                                        WHEN (
                                                                                                (
                                                                                                    "substring" (
                                                                                                        (h.tokuicode)::TEXT,
                                                                                                        2,
                                                                                                        9
                                                                                                        ) >= ('000600000'::CHARACTER VARYING)::TEXT
                                                                                                    )
                                                                                                AND (
                                                                                                    "substring" (
                                                                                                        (h.tokuicode)::TEXT,
                                                                                                        2,
                                                                                                        9
                                                                                                        ) <= ('000799999'::CHARACTER VARYING)::TEXT
                                                                                                    )
                                                                                                )
                                                                                            THEN '09'::CHARACTER VARYING
                                                                                        WHEN (
                                                                                                (
                                                                                                    "substring" (
                                                                                                        (h.tokuicode)::TEXT,
                                                                                                        2,
                                                                                                        9
                                                                                                        ) >= ('000800001'::CHARACTER VARYING)::TEXT
                                                                                                    )
                                                                                                AND (
                                                                                                    "substring" (
                                                                                                        (h.tokuicode)::TEXT,
                                                                                                        2,
                                                                                                        9
                                                                                                        ) <= ('000899999'::CHARACTER VARYING)::TEXT
                                                                                                    )
                                                                                                )
                                                                                            THEN '04'::CHARACTER VARYING
                                                                                        WHEN (
                                                                                                "substring" (
                                                                                                    (h.tokuicode)::TEXT,
                                                                                                    2,
                                                                                                    9
                                                                                                    ) >= ('000900000'::CHARACTER VARYING)::TEXT
                                                                                                )
                                                                                            THEN '09'::CHARACTER VARYING
                                                                                        ELSE ''::CHARACTER VARYING
                                                                                        END
                                                                                END
                                                                        END AS channel,
                                                                    ''::CHARACTER VARYING AS konyuchubuncode,
                                                                    m.itemcode,
                                                                    "k".kosecode,
                                                                    h.juchkbn,
                                                                    (m.suryo * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0))) AS suryo,
                                                                    CASE 
                                                                        WHEN (
                                                                                (
                                                                                    "substring" (
                                                                                        (h.juchkbn)::TEXT,
                                                                                        1,
                                                                                        1
                                                                                        ) = ('9'::CHARACTER VARYING)::TEXT
                                                                                    )
                                                                                OR (
                                                                                    (
                                                                                        "substring" (
                                                                                            (h.juchkbn)::TEXT,
                                                                                            1,
                                                                                            1
                                                                                            ) IS NULL
                                                                                        )
                                                                                    AND ('9' IS NULL)
                                                                                    )
                                                                                )
                                                                            THEN (m.hensu * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
                                                                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                                                                        END AS hensu,
                                                                    0 AS anbunmaegaku_tyouseimae,
                                                                    0 AS meisainukikingaku_tyouseimae,
                                                                    m.meisainukikingaku AS anbunmaegaku_tyouseigo,
                                                                    (m.meisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS meisainukikingaku_tyouseigo,
                                                                    (m.anbunmeisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS anbunmeisainukikingaku,
                                                                    h.henreasoncode,
                                                                    h.henreasonname
                                                                FROM (
                                                                    (
                                                                        cit85osalh h JOIN cit86osalm m ON (((h.ourino)::TEXT = (m.ourino)::TEXT))
                                                                        ) LEFT JOIN tm14shkos "k" ON (((m.itemcode)::TEXT = ("k".itemcode)::TEXT))
                                                                    )
                                                                WHERE (h.kakokbn = ((0)::NUMERIC)::NUMERIC(18, 0))
                                                                ) t LEFT JOIN cim03item_hanbai ia ON (((t.itemcode)::TEXT = (ia.itemcode)::TEXT))
                                                            ) LEFT JOIN cim03item_zaiko ib ON (((t.kosecode)::TEXT = (ib.itemcode)::TEXT))
                                                        ) LEFT JOIN cim03item_zaiko ia2 ON (((t.itemcode)::TEXT = (ia2.itemcode)::TEXT))
                                                    ) LEFT JOIN cim03item_hanbai ib2 ON (((t.kosecode)::TEXT = (ib2.itemcode)::TEXT))
                                                ) LEFT JOIN zaiko_shohin_attr z ON (((t.kosecode)::TEXT = (z.shohin_code)::TEXT))
                                            ) LEFT JOIN tm67juch_nm tm67 ON (((t.juchkbn)::TEXT = (tm67.code)::TEXT))
                                        ) JOIN (
                                        SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
                                            "wqtm07属性未設定名称マスタ"."区分名称その他"
                                        FROM "wqtm07属性未設定名称マスタ"
                                        WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                                        ) misettei1 ON ((1 = 1))
                                    ) JOIN (
                                    SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
                                        "wqtm07属性未設定名称マスタ"."区分名称その他"
                                    FROM "wqtm07属性未設定名称マスタ"
                                    WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                                    ) misettei2 ON ((1 = 1))
                                ) JOIN (
                                SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
                                    "wqtm07属性未設定名称マスタ"."区分名称その他"
                                FROM "wqtm07属性未設定名称マスタ"
                                WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                                ) misettei3 ON ((1 = 1))
                            ) JOIN (
                            SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
                                "wqtm07属性未設定名称マスタ"."区分名称その他"
                            FROM "wqtm07属性未設定名称マスタ"
                            WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('4'::CHARACTER VARYING)::TEXT)
                            ) misettei4 ON ((1 = 1))
                        ) JOIN (
                        SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
                            "wqtm07属性未設定名称マスタ"."区分名称その他"
                        FROM "wqtm07属性未設定名称マスタ"
                        WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('5'::CHARACTER VARYING)::TEXT)
                        ) misettei5 ON ((1 = 1))
                    ) JOIN (
                    SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
                        "wqtm07属性未設定名称マスタ"."区分名称その他"
                    FROM "wqtm07属性未設定名称マスタ"
                    WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('6'::CHARACTER VARYING)::TEXT)
                    ) misettei6 ON ((1 = 1))
                ) JOIN (
                SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
                    "wqtm07属性未設定名称マスタ"."区分名称その他"
                FROM "wqtm07属性未設定名称マスタ"
                WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('7'::CHARACTER VARYING)::TEXT)
                ) misettei7 ON ((1 = 1))
            ) JOIN (
            SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
                "wqtm07属性未設定名称マスタ"."区分名称その他"
            FROM "wqtm07属性未設定名称マスタ"
            WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('8'::CHARACTER VARYING)::TEXT)
            ) misettei8 ON ((1 = 1))
        )
    )
SELECT *
FROM final