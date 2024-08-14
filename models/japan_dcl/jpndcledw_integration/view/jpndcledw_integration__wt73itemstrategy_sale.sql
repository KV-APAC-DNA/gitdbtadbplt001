WITH cit85osalh
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cit85osalh') }}
    ),
"wqtm07属性未設定名称マスタ" AS 
(
    SELECT * FROM {{ source('jpdcledw_integration', '"WQTM07属性未設定名称マスタ"') }}
),
cit80saleh AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__cit80saleh') }}
),
cit81salem AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__cit81salem') }}
),
cim08shkos_bunkai AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__cim08shkos_bunkai') }}
),
syouhincd_henkan AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__syouhincd_henkan') }}
),
tm14shkos AS 
(
    SELECT * FROM {{ source('jpdcledw_integration', 'tm14shkos') }}
),
get_ci_next_sale AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__get_ci_next_sale') }}
),
cit86osalm AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__cit86osalm') }}
),
c_tbecclient AS 
(
    SELECT * FROM {{ ref('jpndclitg_integration__c_tbecclient') }}
),
cit85osalh_kaigai AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__cit85osalh_kaigai') }}
),
cit86osalm_kaigai AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__cit86osalm_kaigai') }}
),
cim03item_hanbai AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__cim03item_hanbai') }}
),
cim03item_zaiko AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__cim03item_zaiko') }}
),
zaiko_shohin_attr AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__zaiko_shohin_attr') }}
),
tm67juch_nm AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__tm67juch_nm') }}
),
cim02tokui AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__cim02tokui') }}
),
cim24itbun AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__cim24itbun') }}
),
misettei1
AS (
    SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
        "wqtm07属性未設定名称マスタ"."区分名称その他"
    FROM "wqtm07属性未設定名称マスタ"
    WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('1'::CHARACTER VARYING)::TEXT)
    ),
misettei2
AS (
    SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
        "wqtm07属性未設定名称マスタ"."区分名称その他"
    FROM "wqtm07属性未設定名称マスタ"
    WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('2'::CHARACTER VARYING)::TEXT)
    ),
misettei3
AS (
    SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
        "wqtm07属性未設定名称マスタ"."区分名称その他"
    FROM "wqtm07属性未設定名称マスタ"
    WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('3'::CHARACTER VARYING)::TEXT)
    ),
misettei4
AS (
    SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
        "wqtm07属性未設定名称マスタ"."区分名称その他"
    FROM "wqtm07属性未設定名称マスタ"
    WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('4'::CHARACTER VARYING)::TEXT)
    ),
misettei5
AS (
    SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
        "wqtm07属性未設定名称マスタ"."区分名称その他"
    FROM "wqtm07属性未設定名称マスタ"
    WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('5'::CHARACTER VARYING)::TEXT)
    ),
misettei6
AS (
    SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
        "wqtm07属性未設定名称マスタ"."区分名称その他"
    FROM "wqtm07属性未設定名称マスタ"
    WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('6'::CHARACTER VARYING)::TEXT)
    ),
misettei7
AS (
    SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
        "wqtm07属性未設定名称マスタ"."区分名称その他"
    FROM "wqtm07属性未設定名称マスタ"
    WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('7'::CHARACTER VARYING)::TEXT)
    ),
misettei8
AS (
    SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
        "wqtm07属性未設定名称マスタ"."区分名称その他"
    FROM "wqtm07属性未設定名称マスタ"
    WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('8'::CHARACTER VARYING)::TEXT)
    ),
t1
AS (
    SELECT trunc(((h.shukadate)::DOUBLE PRECISION / (100)::DOUBLE PRECISION)) AS shukanengetu,
        CASE 
            WHEN ((h.kaisha)::TEXT = ('000'::CHARACTER VARYING)::TEXT)
                THEN CASE 
                        WHEN ((h.daihanrobunname)::TEXT = ('Web'::CHARACTER VARYING)::TEXT)
                            THEN '121'::CHARACTER VARYING
                        ELSE '111'::CHARACTER VARYING
                        END
            WHEN ((h.kaisha)::TEXT = ('001'::CHARACTER VARYING)::TEXT)
                THEN '112'::CHARACTER VARYING
            ELSE '114'::CHARACTER VARYING
            END AS channel,
        rtrim(h.daihanrobunname) AS konyuchubuncode,
        rtrim(h.torikeikbn) as torikeikbn,
        'ダミーコード'::CHARACTER VARYING AS tokuicode,
        rtrim(m.itemcode_hanbai) AS itemcode,
        rtrim("k".kosecode) as kosecode,
        rtrim(h.juchkbn) as juchkbn,
        sum(((m.suryo * COALESCE(item.bunkai_kossu, ((1)::NUMERIC)::NUMERIC(18, 0))) * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))) AS suryo,
        sum(CASE 
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
                END) AS hensu,
        0 AS anbunmaegaku_tyouseimae,
        0 AS meisainukikingaku_tyouseimae,
        sum(m.meisainukikingaku) AS anbunmaegaku_tyouseigo,
        sum((((m.meisainukikingaku)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION)) AS meisainukikingaku_tyouseigo,
        sum((((m.anbunmeisainukikingaku)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION)) AS anbunmeisainukikingaku,
        rtrim(h.henreasoncode) as henreasoncode,
        rtrim(h.henreasonname) as henreasonname,
        ''::CHARACTER VARYING AS sokoname,
        rtrim(h.bmn_hyouji_cd) as bmn_hyouji_cd,
        rtrim(h.bmn_nms) as bmn_nms
    FROM (
        (
            (
                (
                    cit80saleh h JOIN cit81salem m ON ((rtrim(h.saleno)::TEXT = rtrim(m.saleno)::TEXT))
                    ) LEFT JOIN cim08shkos_bunkai item ON ((rtrim(m.itemcode_hanbai)::TEXT = rtrim(item.itemcode)::TEXT))
                ) LEFT JOIN syouhincd_henkan henkan ON ((rtrim(item.bunkai_itemcode)::TEXT = rtrim(henkan.itemcode)::TEXT))
            ) LEFT JOIN tm14shkos "k" ON (((COALESCE(rtrim(henkan.koseiocode), COALESCE(rtrim(item.bunkai_itemcode), rtrim(m.itemcode)::CHARACTER VARYING(65535))))::TEXT = rtrim("k".itemcode)::TEXT))
        )
    WHERE (
            (
                (
                    (
                        (rtrim(h.cancelflg) = ((0)::NUMERIC)::NUMERIC(18, 0))
                        AND (rtrim(h.torikeikbn)::TEXT = ('01'::CHARACTER VARYING)::TEXT)
                        )
                    AND (rtrim(m.itemcode)::TEXT <> ('9990000100'::CHARACTER VARYING)::TEXT)
                    )
                AND (rtrim(m.itemcode)::TEXT <> ('9990000200'::CHARACTER VARYING)::TEXT)
                )
            AND (rtrim(h.kakokbn) = ((1)::NUMERIC)::NUMERIC(18, 0))
            )
    GROUP BY trunc(((h.shukadate)::DOUBLE PRECISION / (100)::DOUBLE PRECISION)),
        CASE 
            WHEN ((h.kaisha)::TEXT = ('000'::CHARACTER VARYING)::TEXT)
                THEN CASE 
                        WHEN ((h.daihanrobunname)::TEXT = ('Web'::CHARACTER VARYING)::TEXT)
                            THEN '121'::CHARACTER VARYING
                        ELSE '111'::CHARACTER VARYING
                        END
            WHEN ((h.kaisha)::TEXT = ('001'::CHARACTER VARYING)::TEXT)
                THEN '112'::CHARACTER VARYING
            ELSE '114'::CHARACTER VARYING
            END,
        rtrim(h.daihanrobunname),
        rtrim(h.torikeikbn),
        rtrim(m.itemcode_hanbai),
        rtrim("k".kosecode),
        rtrim(h.juchkbn),
        rtrim(h.henreasoncode),
        rtrim(h.henreasonname),
        rtrim(h.bmn_hyouji_cd),
        rtrim(h.bmn_nms)
    ),
t2
AS (
    SELECT trunc(((h.shukadate)::DOUBLE PRECISION / (100)::DOUBLE PRECISION)) AS shukanengetu,
        CASE 
            WHEN (h.smkeiroid = ((5)::NUMERIC)::NUMERIC(18, 0))
                THEN '121'::CHARACTER VARYING
            ELSE CASE 
                    WHEN (h.smkeiroid = ((6)::NUMERIC)::NUMERIC(18, 0))
                        THEN '112'::CHARACTER VARYING
                    WHEN (h.marker = ((4)::NUMERIC)::NUMERIC(18, 0))
                        THEN '511'::CHARACTER VARYING
                    ELSE '111'::CHARACTER VARYING
                    END
            END AS channel,
        h.daihanrobunname AS konyuchubuncode,
        h.torikeikbn,
        'ダミーコード'::CHARACTER VARYING AS tokuicode,
        m.itemcode_hanbai AS itemcode,
        "k".kosecode,
        h.juchkbn,
        sum((m.suryo * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))) AS suryo,
        sum(CASE 
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
                END) AS hensu,
        sum("next".tyouseimaekingaku) AS anbunmaegaku_tyouseimae,
        sum(("next".tyouseimaekingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))) AS meisainukikingaku_tyouseimae,
        sum(m.meisainukikingaku) AS anbunmaegaku_tyouseigo,
        sum((m.meisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))) AS meisainukikingaku_tyouseigo,
        sum((m.anbunmeisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))) AS anbunmeisainukikingaku,
        h.henreasoncode,
        h.henreasonname,
        ''::CHARACTER VARYING AS sokoname,
        h.bmn_hyouji_cd,
        h.bmn_nms
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
    GROUP BY trunc(((h.shukadate)::DOUBLE PRECISION / (100)::DOUBLE PRECISION)),
        CASE 
            WHEN (h.smkeiroid = ((5)::NUMERIC)::NUMERIC(18, 0))
                THEN '121'::CHARACTER VARYING
            ELSE CASE 
                    WHEN (h.smkeiroid = ((6)::NUMERIC)::NUMERIC(18, 0))
                        THEN '112'::CHARACTER VARYING
                    WHEN (h.marker = ((4)::NUMERIC)::NUMERIC(18, 0))
                        THEN '511'::CHARACTER VARYING
                    ELSE '111'::CHARACTER VARYING
                    END
            END,
        h.daihanrobunname,
        h.torikeikbn,
        m.itemcode_hanbai,
        "k".kosecode,
        h.juchkbn,
        h.henreasoncode,
        h.henreasonname,
        h.bmn_hyouji_cd,
        h.bmn_nms
    ),
t3
AS (
    SELECT trunc(((h.shukadate)::DOUBLE PRECISION / (100)::DOUBLE PRECISION)) AS shukanengetu,
        CASE 
            WHEN ((h.torikeikbn)::TEXT = ('02'::CHARACTER VARYING)::TEXT)
                THEN CASE 
                        WHEN ((h.tokuicode)::TEXT = ('QVC0000001'::CHARACTER VARYING)::TEXT)
                            THEN '321'::CHARACTER VARYING
                        WHEN (
                                ((h.tokuicode)::TEXT = ('CC00100001'::CHARACTER VARYING)::TEXT)
                                OR ((h.tokuicode)::TEXT = ('CC00100000'::CHARACTER VARYING)::TEXT)
                                )
                            THEN '111'::CHARACTER VARYING
                        ELSE CASE 
                                WHEN (
                                        (h.fskbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                                        OR (h.fskbn = ((0)::NUMERIC)::NUMERIC(18, 0))
                                        )
                                    THEN '511'::CHARACTER VARYING
                                ELSE CASE 
                                        WHEN ((h.shokuikibunrui)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                                            THEN '312'::CHARACTER VARYING
                                        WHEN ((h.shokuikibunrui)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                                            THEN '313'::CHARACTER VARYING
                                        WHEN ((h.shokuikibunrui)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                                            THEN '314'::CHARACTER VARYING
                                        ELSE CASE 
                                                WHEN (
                                                        "substring" (
                                                            (h.tokuicode)::TEXT,
                                                            2,
                                                            9
                                                            ) <= ('000499999'::CHARACTER VARYING)::TEXT
                                                        )
                                                    THEN '311'::CHARACTER VARYING
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
                                                    THEN '412'::CHARACTER VARYING
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
                                                    THEN '312'::CHARACTER VARYING
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
                                                    THEN '311'::CHARACTER VARYING
                                                WHEN (
                                                        "substring" (
                                                            (h.tokuicode)::TEXT,
                                                            2,
                                                            9
                                                            ) >= ('000900000'::CHARACTER VARYING)::TEXT
                                                        )
                                                    THEN CASE 
                                                            WHEN ((h.tokuicode)::TEXT LIKE ('000099%'::CHARACTER VARYING)::TEXT)
                                                                THEN '411'::CHARACTER VARYING
                                                            WHEN ((h.tokuicode)::TEXT LIKE ('VIP%'::CHARACTER VARYING)::TEXT)
                                                                THEN '113'::CHARACTER VARYING
                                                            WHEN ((h.tokuicode)::TEXT LIKE ('CINEXT%'::CHARACTER VARYING)::TEXT)
                                                                THEN '111'::CHARACTER VARYING
                                                            ELSE '412'::CHARACTER VARYING
                                                            END
                                                ELSE ''::CHARACTER VARYING
                                                END
                                        END
                                END
                        END
            ELSE CASE 
                    WHEN ((h.torikeikbn)::TEXT = ('03'::CHARACTER VARYING)::TEXT)
                        THEN '211'::CHARACTER VARYING
                    WHEN ((h.torikeikbn)::TEXT = ('04'::CHARACTER VARYING)::TEXT)
                        THEN '212'::CHARACTER VARYING
                    WHEN ((h.torikeikbn)::TEXT = ('05'::CHARACTER VARYING)::TEXT)
                        THEN '213'::CHARACTER VARYING
                    WHEN ((h.torikeikbn)::TEXT = ('06'::CHARACTER VARYING)::TEXT)
                        THEN '214'::CHARACTER VARYING
                    ELSE NULL::CHARACTER VARYING
                    END
            END AS channel,
        ''::CHARACTER VARYING AS konyuchubuncode,
        h.torikeikbn,
        h.tokuicode,
        m.itemcode_hanbai AS itemcode,
        "k".kosecode,
        h.juchkbn,
        sum(((m.suryo * COALESCE(item.bunkai_kossu, ((1)::NUMERIC)::NUMERIC(18, 0))) * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))) AS suryo,
        sum(CASE 
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
                END) AS hensu,
        0 AS anbunmaegaku_tyouseimae,
        0 AS meisainukikingaku_tyouseimae,
        sum(m.meisainukikingaku) AS anbunmaegaku_tyouseigo,
        sum((((m.meisainukikingaku)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION)) AS meisainukikingaku_tyouseigo,
        sum((((m.anbunmeisainukikingaku)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION)) AS anbunmeisainukikingaku,
        h.henreasoncode,
        h.henreasonname,
        ''::CHARACTER VARYING AS sokoname,
        h.bmn_hyouji_cd,
        h.bmn_nms
    FROM (
        (
            (
                (
                    cit85osalh h JOIN cit86osalm m ON (((h.ourino)::TEXT = (m.ourino)::TEXT))
                    ) LEFT JOIN cim08shkos_bunkai item ON (((m.itemcode_hanbai)::TEXT = (item.itemcode)::TEXT))
                ) LEFT JOIN syouhincd_henkan henkan ON (((item.bunkai_itemcode)::TEXT = (henkan.itemcode)::TEXT))
            ) LEFT JOIN tm14shkos "k" ON (((COALESCE(henkan.koseiocode, COALESCE(item.bunkai_itemcode, (m.itemcode)::CHARACTER VARYING(65535))))::TEXT = ("k".itemcode)::TEXT))
        )
    WHERE (h.kakokbn = ((1)::NUMERIC)::NUMERIC(18, 0))
    GROUP BY trunc(((h.shukadate)::DOUBLE PRECISION / (100)::DOUBLE PRECISION)),
        CASE 
            WHEN ((h.torikeikbn)::TEXT = ('02'::CHARACTER VARYING)::TEXT)
                THEN CASE 
                        WHEN ((h.tokuicode)::TEXT = ('QVC0000001'::CHARACTER VARYING)::TEXT)
                            THEN '321'::CHARACTER VARYING
                        WHEN (
                                ((h.tokuicode)::TEXT = ('CC00100001'::CHARACTER VARYING)::TEXT)
                                OR ((h.tokuicode)::TEXT = ('CC00100000'::CHARACTER VARYING)::TEXT)
                                )
                            THEN '111'::CHARACTER VARYING
                        ELSE CASE 
                                WHEN (
                                        (h.fskbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                                        OR (h.fskbn = ((0)::NUMERIC)::NUMERIC(18, 0))
                                        )
                                    THEN '511'::CHARACTER VARYING
                                ELSE CASE 
                                        WHEN ((h.shokuikibunrui)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                                            THEN '312'::CHARACTER VARYING
                                        WHEN ((h.shokuikibunrui)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                                            THEN '313'::CHARACTER VARYING
                                        WHEN ((h.shokuikibunrui)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                                            THEN '314'::CHARACTER VARYING
                                        ELSE CASE 
                                                WHEN (
                                                        "substring" (
                                                            (h.tokuicode)::TEXT,
                                                            2,
                                                            9
                                                            ) <= ('000499999'::CHARACTER VARYING)::TEXT
                                                        )
                                                    THEN '311'::CHARACTER VARYING
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
                                                    THEN '412'::CHARACTER VARYING
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
                                                    THEN '312'::CHARACTER VARYING
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
                                                    THEN '311'::CHARACTER VARYING
                                                WHEN (
                                                        "substring" (
                                                            (h.tokuicode)::TEXT,
                                                            2,
                                                            9
                                                            ) >= ('000900000'::CHARACTER VARYING)::TEXT
                                                        )
                                                    THEN CASE 
                                                            WHEN ((h.tokuicode)::TEXT LIKE ('000099%'::CHARACTER VARYING)::TEXT)
                                                                THEN '411'::CHARACTER VARYING
                                                            WHEN ((h.tokuicode)::TEXT LIKE ('VIP%'::CHARACTER VARYING)::TEXT)
                                                                THEN '113'::CHARACTER VARYING
                                                            WHEN ((h.tokuicode)::TEXT LIKE ('CINEXT%'::CHARACTER VARYING)::TEXT)
                                                                THEN '111'::CHARACTER VARYING
                                                            ELSE '412'::CHARACTER VARYING
                                                            END
                                                ELSE ''::CHARACTER VARYING
                                                END
                                        END
                                END
                        END
            ELSE CASE 
                    WHEN ((h.torikeikbn)::TEXT = ('03'::CHARACTER VARYING)::TEXT)
                        THEN '211'::CHARACTER VARYING
                    WHEN ((h.torikeikbn)::TEXT = ('04'::CHARACTER VARYING)::TEXT)
                        THEN '212'::CHARACTER VARYING
                    WHEN ((h.torikeikbn)::TEXT = ('05'::CHARACTER VARYING)::TEXT)
                        THEN '213'::CHARACTER VARYING
                    WHEN ((h.torikeikbn)::TEXT = ('06'::CHARACTER VARYING)::TEXT)
                        THEN '214'::CHARACTER VARYING
                    ELSE NULL::CHARACTER VARYING
                    END
            END,
        h.torikeikbn,
        h.tokuicode,
        m.itemcode_hanbai,
        "k".kosecode,
        h.juchkbn,
        h.henreasoncode,
        h.henreasonname,
        h.bmn_hyouji_cd,
        h.bmn_nms
    ),
t4
AS (
    SELECT trunc(((h.shukadate)::DOUBLE PRECISION / (100)::DOUBLE PRECISION)) AS shukanengetu,
        CASE 
            WHEN ((h.torikeikbn)::TEXT = ('02'::CHARACTER VARYING)::TEXT)
                THEN CASE 
                        WHEN ((h.tokuicode)::TEXT = ('QVC0000001'::CHARACTER VARYING)::TEXT)
                            THEN '321'::CHARACTER VARYING
                        WHEN (
                                ((h.tokuicode)::TEXT = ('CC00100001'::CHARACTER VARYING)::TEXT)
                                OR ((h.tokuicode)::TEXT = ('CC00100000'::CHARACTER VARYING)::TEXT)
                                )
                            THEN '111'::CHARACTER VARYING
                        ELSE CASE 
                                WHEN (
                                        (h.fskbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                                        OR (h.fskbn = ((0)::NUMERIC)::NUMERIC(18, 0))
                                        )
                                    THEN '511'::CHARACTER VARYING
                                ELSE CASE 
                                        WHEN ((h.shokuikibunrui)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                                            THEN '312'::CHARACTER VARYING
                                        WHEN ((h.shokuikibunrui)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                                            THEN '313'::CHARACTER VARYING
                                        WHEN ((h.shokuikibunrui)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                                            THEN '314'::CHARACTER VARYING
                                        ELSE CASE 
                                                WHEN (
                                                        "substring" (
                                                            (h.tokuicode)::TEXT,
                                                            2,
                                                            9
                                                            ) <= ('000499999'::CHARACTER VARYING)::TEXT
                                                        )
                                                    THEN '311'::CHARACTER VARYING
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
                                                    THEN '412'::CHARACTER VARYING
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
                                                    THEN '312'::CHARACTER VARYING
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
                                                    THEN '311'::CHARACTER VARYING
                                                WHEN (
                                                        "substring" (
                                                            (h.tokuicode)::TEXT,
                                                            2,
                                                            9
                                                            ) >= ('000900000'::CHARACTER VARYING)::TEXT
                                                        )
                                                    THEN CASE 
                                                            WHEN ((h.tokuicode)::TEXT LIKE ('000099%'::CHARACTER VARYING)::TEXT)
                                                                THEN '411'::CHARACTER VARYING
                                                            WHEN ((h.tokuicode)::TEXT LIKE ('VIP%'::CHARACTER VARYING)::TEXT)
                                                                THEN '113'::CHARACTER VARYING
                                                            WHEN ((h.tokuicode)::TEXT LIKE ('CINEXT%'::CHARACTER VARYING)::TEXT)
                                                                THEN '111'::CHARACTER VARYING
                                                            ELSE '412'::CHARACTER VARYING
                                                            END
                                                ELSE ''::CHARACTER VARYING
                                                END
                                        END
                                END
                        END
            ELSE CASE 
                    WHEN ((h.torikeikbn)::TEXT = ('03'::CHARACTER VARYING)::TEXT)
                        THEN '211'::CHARACTER VARYING
                    WHEN ((h.torikeikbn)::TEXT = ('04'::CHARACTER VARYING)::TEXT)
                        THEN '212'::CHARACTER VARYING
                    WHEN ((h.torikeikbn)::TEXT = ('05'::CHARACTER VARYING)::TEXT)
                        THEN '213'::CHARACTER VARYING
                    WHEN ((h.torikeikbn)::TEXT = ('06'::CHARACTER VARYING)::TEXT)
                        THEN '214'::CHARACTER VARYING
                    ELSE NULL::CHARACTER VARYING
                    END
            END AS channel,
        ''::CHARACTER VARYING AS konyuchubuncode,
        h.torikeikbn,
        h.tokuicode,
        m.itemcode,
        "k".kosecode,
        h.juchkbn,
        sum((m.suryo * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))) AS suryo,
        sum(CASE 
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
                END) AS hensu,
        0 AS anbunmaegaku_tyouseimae,
        0 AS meisainukikingaku_tyouseimae,
        sum(m.meisainukikingaku) AS anbunmaegaku_tyouseigo,
        sum((m.meisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))) AS meisainukikingaku_tyouseigo,
        sum((m.anbunmeisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))) AS anbunmeisainukikingaku,
        h.henreasoncode,
        h.henreasonname,
        (t.sokoname)::CHARACTER VARYING AS sokoname,
        h.bmn_hyouji_cd,
        h.bmn_nms
    FROM (
        (
            (
                cit85osalh h JOIN cit86osalm m ON (((h.ourino)::TEXT = (m.ourino)::TEXT))
                ) LEFT JOIN tm14shkos "k" ON (((m.itemcode)::TEXT = ("k".itemcode)::TEXT))
            ) LEFT JOIN (
            SELECT tm.c_dstempocode AS sokocode,
                (((((tm.c_dstempocode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (tm.c_dstemponame)::TEXT) || (' '::CHARACTER VARYING)::TEXT) || (tm.c_dstemponame)::TEXT) AS sokoname
            FROM c_tbecclient tm
            ) t ON (((h.tenpocode)::TEXT = (t.sokocode)::TEXT))
        )
    WHERE (h.kakokbn = ((0)::NUMERIC)::NUMERIC(18, 0))
    GROUP BY trunc(((h.shukadate)::DOUBLE PRECISION / (100)::DOUBLE PRECISION)),
        CASE 
            WHEN ((h.torikeikbn)::TEXT = ('02'::CHARACTER VARYING)::TEXT)
                THEN CASE 
                        WHEN ((h.tokuicode)::TEXT = ('QVC0000001'::CHARACTER VARYING)::TEXT)
                            THEN '321'::CHARACTER VARYING
                        WHEN (
                                ((h.tokuicode)::TEXT = ('CC00100001'::CHARACTER VARYING)::TEXT)
                                OR ((h.tokuicode)::TEXT = ('CC00100000'::CHARACTER VARYING)::TEXT)
                                )
                            THEN '111'::CHARACTER VARYING
                        ELSE CASE 
                                WHEN (
                                        (h.fskbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                                        OR (h.fskbn = ((0)::NUMERIC)::NUMERIC(18, 0))
                                        )
                                    THEN '511'::CHARACTER VARYING
                                ELSE CASE 
                                        WHEN ((h.shokuikibunrui)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                                            THEN '312'::CHARACTER VARYING
                                        WHEN ((h.shokuikibunrui)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                                            THEN '313'::CHARACTER VARYING
                                        WHEN ((h.shokuikibunrui)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                                            THEN '314'::CHARACTER VARYING
                                        ELSE CASE 
                                                WHEN (
                                                        "substring" (
                                                            (h.tokuicode)::TEXT,
                                                            2,
                                                            9
                                                            ) <= ('000499999'::CHARACTER VARYING)::TEXT
                                                        )
                                                    THEN '311'::CHARACTER VARYING
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
                                                    THEN '412'::CHARACTER VARYING
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
                                                    THEN '312'::CHARACTER VARYING
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
                                                    THEN '311'::CHARACTER VARYING
                                                WHEN (
                                                        "substring" (
                                                            (h.tokuicode)::TEXT,
                                                            2,
                                                            9
                                                            ) >= ('000900000'::CHARACTER VARYING)::TEXT
                                                        )
                                                    THEN CASE 
                                                            WHEN ((h.tokuicode)::TEXT LIKE ('000099%'::CHARACTER VARYING)::TEXT)
                                                                THEN '411'::CHARACTER VARYING
                                                            WHEN ((h.tokuicode)::TEXT LIKE ('VIP%'::CHARACTER VARYING)::TEXT)
                                                                THEN '113'::CHARACTER VARYING
                                                            WHEN ((h.tokuicode)::TEXT LIKE ('CINEXT%'::CHARACTER VARYING)::TEXT)
                                                                THEN '111'::CHARACTER VARYING
                                                            ELSE '412'::CHARACTER VARYING
                                                            END
                                                ELSE ''::CHARACTER VARYING
                                                END
                                        END
                                END
                        END
            ELSE CASE 
                    WHEN ((h.torikeikbn)::TEXT = ('03'::CHARACTER VARYING)::TEXT)
                        THEN '211'::CHARACTER VARYING
                    WHEN ((h.torikeikbn)::TEXT = ('04'::CHARACTER VARYING)::TEXT)
                        THEN '212'::CHARACTER VARYING
                    WHEN ((h.torikeikbn)::TEXT = ('05'::CHARACTER VARYING)::TEXT)
                        THEN '213'::CHARACTER VARYING
                    WHEN ((h.torikeikbn)::TEXT = ('06'::CHARACTER VARYING)::TEXT)
                        THEN '214'::CHARACTER VARYING
                    ELSE NULL::CHARACTER VARYING
                    END
            END,
        h.torikeikbn,
        h.tokuicode,
        m.itemcode,
        "k".kosecode,
        h.juchkbn,
        h.henreasoncode,
        h.henreasonname,
        t.sokoname,
        h.bmn_hyouji_cd,
        h.bmn_nms
    ),
t5
AS (
    SELECT trunc(((h.shukadate)::DOUBLE PRECISION / (100)::DOUBLE PRECISION)) AS shukanengetu,
        CASE 
            WHEN ((h.tokuicode)::TEXT LIKE ('000099%'::CHARACTER VARYING)::TEXT)
                THEN '411'::CHARACTER VARYING
            WHEN ((h.tokuicode)::TEXT LIKE ('000091%'::CHARACTER VARYING)::TEXT)
                THEN '413'::CHARACTER VARYING
            ELSE '412'::CHARACTER VARYING
            END AS channel,
        ''::CHARACTER VARYING AS konyuchubuncode,
        h.torikeikbn,
        h.tokuicode,
        m.itemcode,
        "k".kosecode,
        h.juchkbn,
        sum((m.suryo * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))) AS suryo,
        sum(CASE 
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
                END) AS hensu,
        0 AS anbunmaegaku_tyouseimae,
        0 AS meisainukikingaku_tyouseimae,
        sum(m.meisainukikingaku) AS anbunmaegaku_tyouseigo,
        sum((m.meisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))) AS meisainukikingaku_tyouseigo,
        sum((m.anbunmeisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))) AS anbunmeisainukikingaku,
        h.henreasoncode,
        h.henreasonname,
        ''::CHARACTER VARYING AS sokoname,
        h.bmn_hyouji_cd,
        h.bmn_nms
    FROM (
        (
            cit85osalh_kaigai h JOIN cit86osalm_kaigai m ON (((h.ourino)::TEXT = (m.ourino)::TEXT))
            ) LEFT JOIN tm14shkos "k" ON (((m.itemcode)::TEXT = ("k".itemcode)::TEXT))
        )
    WHERE (h.kakokbn = ((0)::NUMERIC)::NUMERIC(18, 0))
    GROUP BY trunc(((h.shukadate)::DOUBLE PRECISION / (100)::DOUBLE PRECISION)),
        CASE 
            WHEN ((h.tokuicode)::TEXT LIKE ('000099%'::CHARACTER VARYING)::TEXT)
                THEN '411'::CHARACTER VARYING
            WHEN ((h.tokuicode)::TEXT LIKE ('000091%'::CHARACTER VARYING)::TEXT)
                THEN '413'::CHARACTER VARYING
            ELSE '412'::CHARACTER VARYING
            END,
        h.torikeikbn,
        h.tokuicode,
        m.itemcode,
        "k".kosecode,
        h.juchkbn,
        h.henreasoncode,
        h.henreasonname,
        h.bmn_hyouji_cd,
        h.bmn_nms
    ),
t
AS (
    SELECT *
    FROM T1
    
    UNION ALL
    
    SELECT *
    FROM T2
    
    UNION ALL
    
    SELECT *
    FROM T3
    
    UNION ALL
    
    SELECT *
    FROM T4
    
    UNION ALL
    
    SELECT *
    FROM T5
    ),
FINAL
AS (
    SELECT trim(t.shukanengetu) AS shukanengetu,
        trim(t.channel) AS channel,
        trim(t.konyuchubuncode) AS konyuchubuncode,
        trim(t.torikeikbn) AS torikeikbn,
        trim(((trim(t.tokuicode)::TEXT || (nvl2(trim(cim02.tokuiname), ' : '::CHARACTER VARYING, ''::CHARACTER VARYING))::TEXT) || COALESCE(((trim(cim02.tokuiname)::TEXT || (' '::CHARACTER VARYING)::TEXT) || trim(cim02.tokuiname_ryaku)::TEXT), ((''::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT))) AS tokuicode,
        trim(((COALESCE(trim(t.itemcode), ''::CHARACTER VARYING))::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (COALESCE(COALESCE(trim(ia.itemname), ''::CHARACTER VARYING), (COALESCE(trim(ia2.itemname), ''::CHARACTER VARYING))::CHARACTER VARYING(65535)))::TEXT) AS hanbaiitem,
        trim(((COALESCE(TRIM((ib.bar_cd2)::TEXT), (trim(ib.itemcode)::CHARACTER VARYING(65535))::TEXT) || (' : '::CHARACTER VARYING)::TEXT) || (COALESCE(trim(ib.itemname), trim(ib2.itemname)::CHARACTER VARYING(65535)))::TEXT)) AS kouseiitem,
        trim(t.kosecode) AS kosecode,
        trim(t.juchkbn) AS juchkbn,
        trim(tm67.cname) AS juchkbncname,
        trim(t.suryo) AS suryo,
        trim(t.hensu) hensu,
        trim((t.suryo + t.hensu)) AS urisuryo,
        trim(t.anbunmaegaku_tyouseimae) AS anbunmaegaku_tyouseimae,
        trim(t.meisainukikingaku_tyouseimae) AS meisainukikingaku_tyouseimae,
        trim(t.anbunmaegaku_tyouseigo) AS anbunmaegaku_tyouseigo,
        trim(t.meisainukikingaku_tyouseigo) AS meisainukikingaku_tyouseigo,
        trim(t.anbunmeisainukikingaku) AS anbunmeisainukikingaku,
        CASE 
            WHEN (
                    (
                        (
                            (
                                (trim(t.channel)::TEXT = ('111'::CHARACTER VARYING)::TEXT)
                                OR (trim(t.channel)::TEXT = ('112'::CHARACTER VARYING)::TEXT)
                                )
                            OR (trim(t.channel)::TEXT = ('113'::CHARACTER VARYING)::TEXT)
                            )
                        OR (trim(t.channel)::TEXT = ('114'::CHARACTER VARYING)::TEXT)
                        )
                    OR (trim(t.channel)::TEXT = ('121'::CHARACTER VARYING)::TEXT)
                    )
                THEN '通信販売'::CHARACTER VARYING
            WHEN (
                    (
                        (
                            (trim(t.channel)::TEXT = ('211'::CHARACTER VARYING)::TEXT)
                            OR (trim(t.channel)::TEXT = ('212'::CHARACTER VARYING)::TEXT)
                            )
                        OR (trim(t.channel)::TEXT = ('213'::CHARACTER VARYING)::TEXT)
                        )
                    OR (trim(t.channel)::TEXT = ('214'::CHARACTER VARYING)::TEXT)
                    )
                THEN '対面販売'::CHARACTER VARYING
            WHEN (
                    (
                        (
                            (
                                (trim(t.channel)::TEXT = ('311'::CHARACTER VARYING)::TEXT)
                                OR (trim(t.channel)::TEXT = ('312'::CHARACTER VARYING)::TEXT)
                                )
                            OR (trim(t.channel)::TEXT = ('313'::CHARACTER VARYING)::TEXT)
                            )
                        OR (trim(t.channel)::TEXT = ('314'::CHARACTER VARYING)::TEXT)
                        )
                    OR (trim(t.channel)::TEXT = ('321'::CHARACTER VARYING)::TEXT)
                    )
                THEN '卸売'::CHARACTER VARYING
            WHEN (
                    (
                        (trim(t.channel)::TEXT = ('411'::CHARACTER VARYING)::TEXT)
                        OR (trim(t.channel)::TEXT = ('412'::CHARACTER VARYING)::TEXT)
                        )
                    OR (trim(t.channel)::TEXT = ('413'::CHARACTER VARYING)::TEXT)
                    )
                THEN '海外'::CHARACTER VARYING
            WHEN (
                    (trim(t.channel)::TEXT = ('511'::CHARACTER VARYING)::TEXT)
                    OR (trim(t.channel)::TEXT = ('512'::CHARACTER VARYING)::TEXT)
                    )
                THEN 'その他'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS channel1,
        CASE 
            WHEN (
                    (
                        (
                            (trim(t.channel)::TEXT = ('111'::CHARACTER VARYING)::TEXT)
                            OR (trim(t.channel)::TEXT = ('112'::CHARACTER VARYING)::TEXT)
                            )
                        OR (trim(t.channel)::TEXT = ('113'::CHARACTER VARYING)::TEXT)
                        )
                    OR (trim(t.channel)::TEXT = ('114'::CHARACTER VARYING)::TEXT)
                    )
                THEN '通販'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('121'::CHARACTER VARYING)::TEXT)
                THEN 'WEB'::CHARACTER VARYING
            WHEN (
                    (
                        (
                            (trim(t.channel)::TEXT = ('211'::CHARACTER VARYING)::TEXT)
                            OR (trim(t.channel)::TEXT = ('212'::CHARACTER VARYING)::TEXT)
                            )
                        OR (trim(t.channel)::TEXT = ('213'::CHARACTER VARYING)::TEXT)
                        )
                    OR (trim(t.channel)::TEXT = ('214'::CHARACTER VARYING)::TEXT)
                    )
                THEN '店舗'::CHARACTER VARYING
            WHEN (
                    (
                        (
                            (trim(t.channel)::TEXT = ('311'::CHARACTER VARYING)::TEXT)
                            OR (trim(t.channel)::TEXT = ('312'::CHARACTER VARYING)::TEXT)
                            )
                        OR (trim(t.channel)::TEXT = ('313'::CHARACTER VARYING)::TEXT)
                        )
                    OR (trim(t.channel)::TEXT = ('314'::CHARACTER VARYING)::TEXT)
                    )
                THEN '卸売'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('321'::CHARACTER VARYING)::TEXT)
                THEN 'QVC'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('411'::CHARACTER VARYING)::TEXT)
                THEN 'JJ'::CHARACTER VARYING
            WHEN (
                    (trim(t.channel)::TEXT = ('412'::CHARACTER VARYING)::TEXT)
                    OR (trim(t.channel)::TEXT = ('413'::CHARACTER VARYING)::TEXT)
                    )
                THEN '海外'::CHARACTER VARYING
            WHEN (
                    (trim(t.channel)::TEXT = ('511'::CHARACTER VARYING)::TEXT)
                    OR (trim(t.channel)::TEXT = ('512'::CHARACTER VARYING)::TEXT)
                    )
                THEN 'その他'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS channel2,
        CASE 
            WHEN (trim(t.channel)::TEXT = ('111'::CHARACTER VARYING)::TEXT)
                THEN '通販'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('112'::CHARACTER VARYING)::TEXT)
                THEN '社販'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('113'::CHARACTER VARYING)::TEXT)
                THEN 'VIP'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('114'::CHARACTER VARYING)::TEXT)
                THEN '通販'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('121'::CHARACTER VARYING)::TEXT)
                THEN 'WEB'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('211'::CHARACTER VARYING)::TEXT)
                THEN '買取'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('212'::CHARACTER VARYING)::TEXT)
                THEN '直営'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('213'::CHARACTER VARYING)::TEXT)
                THEN '消化'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('214'::CHARACTER VARYING)::TEXT)
                THEN 'アウトレット'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('311'::CHARACTER VARYING)::TEXT)
                THEN '代理店'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('312'::CHARACTER VARYING)::TEXT)
                THEN '職域（特販）'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('313'::CHARACTER VARYING)::TEXT)
                THEN '職域（代理店）'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('314'::CHARACTER VARYING)::TEXT)
                THEN '職域（販売会）'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('321'::CHARACTER VARYING)::TEXT)
                THEN 'QVC'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('411'::CHARACTER VARYING)::TEXT)
                THEN 'JJ'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('412'::CHARACTER VARYING)::TEXT)
                THEN '国内免税'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('413'::CHARACTER VARYING)::TEXT)
                THEN '海外免税'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('511'::CHARACTER VARYING)::TEXT)
                THEN 'FS'::CHARACTER VARYING
            WHEN (trim(t.channel)::TEXT = ('512'::CHARACTER VARYING)::TEXT)
                THEN 'その他'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS channel3,
        trim(t.bmn_hyouji_cd) AS bmn_hyouji_cd,
        trim(t.bmn_nms) AS bmn_nms,
        trim(t.henreasoncode) AS henreasoncode,
        ((trim(t.henreasoncode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || trim(t.henreasonname)::TEXT) AS henreasonname,
        trim(t.sokoname) AS sokoname,
        CASE 
            WHEN (trim(z.bumon6_kubun2) IS NULL)
                THEN misettei1."区分名称その他"
            ELSE trim(z.bumon6_kubun2)
            END AS kubun2,
        CASE 
            WHEN (trim(z.bumon6_syohingun) IS NULL)
                THEN misettei2."区分名称その他"
            ELSE trim(z.bumon6_syohingun)
            END AS syohingun,
        CASE 
            WHEN (trim(z.bumon6_jyuutenitem) IS NULL)
                THEN misettei3."区分名称その他"
            ELSE trim(z.bumon6_jyuutenitem)
            END AS jyuutenitem,
        trim(ib.itemkbnname) AS item_bunr_val1,
        trim(cim24_daidai.itbunname) AS item_bunr_val2,
        trim(ib.bunruicode3_nm) AS item_bunr_val3,
        trim(z.bumon7_add_attr1) AS bumon7_add_attr1,
        trim(z.bumon7_add_attr2) AS bumon7_add_attr2,
        trim(z.bumon7_add_attr3) AS bumon7_add_attr3,
        trim(z.bumon7_add_attr4) AS bumon7_add_attr4,
        trim(z.bumon7_add_attr5) AS bumon7_add_attr5,
        trim(z.bumon7_add_attr6) AS bumon7_add_attr6,
        trim(z.bumon7_add_attr7) AS bumon7_add_attr7,
        trim(z.bumon7_add_attr8) AS bumon7_add_attr8,
        trim(z.bumon7_add_attr9) AS bumon7_add_attr9,
        trim(z.bumon7_add_attr10) AS bumon7_add_attr10,
        trim(z.bumon6_20kisyohingun) AS bumon6_20kisyohingun,
        trim(z.bumon6_20kinaieki1) AS bumon6_20kinaieki1,
        trim(z.bumon6_20kinaieki2) AS bumon6_20kinaieki2,
        trim(z.bumon6_20kinaieki3) AS bumon6_20kinaieki3,
        trim(z.bumon6_zyutensyohinyobi1) AS bumon6_zyutensyohinyobi1,
        trim(z.bumon6_zyutensyohinyobi2) AS bumon6_zyutensyohinyobi2,
        trim(z.bumon6_zyutensyohinyobi3) AS bumon6_zyutensyohinyobi3,
        trim(z.bumon6_zyutensyohinyobi4) AS bumon6_zyutensyohinyobi4,
        trim(z.bumon6_zyutensyohinyobi5) AS bumon6_zyutensyohinyobi5,
        trim(z.bumon6_okikaename) AS bumon6_okikaename,
        trim(z.bumon6_zukyuyosoku1) AS bumon6_zukyuyosoku1,
        trim(z.bumon6_zukyuyosoku2) AS bumon6_zukyuyosoku2,
        trim(z.bumon6_zukyuyosoku3) AS bumon6_zukyuyosoku3
    FROM t
    LEFT JOIN cim03item_hanbai ia ON ((rtrim(t.itemcode)::TEXT = rtrim(ia.itemcode)::TEXT))
    LEFT JOIN cim03item_zaiko ib ON ((trim(t.kosecode)::TEXT = rtrim(ib.itemcode)::TEXT))
    LEFT JOIN cim03item_zaiko ia2 ON ((rtrim(t.itemcode)::TEXT = rtrim(ia2.itemcode)::TEXT))
    LEFT JOIN cim03item_hanbai ib2 ON ((rtrim(t.kosecode)::TEXT = rtrim(ib2.itemcode)::TEXT))
    LEFT JOIN zaiko_shohin_attr z ON ((rtrim(t.kosecode)::TEXT = rtrim(z.shohin_code)::TEXT))
    LEFT JOIN tm67juch_nm tm67 ON ((rtrim(t.juchkbn)::TEXT = rtrim(tm67.code)::TEXT))
    LEFT JOIN cim02tokui cim02 ON ((rtrim(t.tokuicode)::TEXT = rtrim(cim02.tokuicode)::TEXT))
    LEFT JOIN cim24itbun cim24_daidai ON (
            (
                (
                    (rtrim(ib.bunruicode5)::TEXT = rtrim(cim24_daidai.itbuncode)::TEXT)
                    AND (rtrim(cim24_daidai.itbunshcode)::TEXT = ((5)::CHARACTER VARYING)::TEXT)
                    )
                AND (rtrim(ib.syutoku_kbn)::TEXT = ('PORT'::CHARACTER VARYING)::TEXT)
                )
            )
    JOIN misettei1 ON ((1 = 1))
    JOIN misettei2 ON ((1 = 1))
    JOIN misettei3 ON ((1 = 1))
    JOIN misettei4 ON ((1 = 1))
    JOIN misettei5 ON ((1 = 1))
    JOIN misettei6 ON ((1 = 1))
    JOIN misettei7 ON ((1 = 1))
    JOIN misettei8 ON ((1 = 1))
    )
select * from final
