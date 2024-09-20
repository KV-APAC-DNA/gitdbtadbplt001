WITH cit80saleh
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cit80saleh') }}
    ),
CIM02TOKUI AS
(
    SELECT *
    FROM {{ref('jpndcledw_integration__cim02tokui')}}
),
cit85osalh AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__cit85osalh') }}
),
cit85osalh_kaigai AS 
(
    SELECT * FROM {{ ref('jpndcledw_integration__cit85osalh_kaigai') }}
),
t1
AS (
    SELECT cit80saleh.saleno,
        cit80saleh.shukadate AS shukanengetu,
        CASE 
            WHEN (cit80saleh.kakokbn = ((0)::NUMERIC)::NUMERIC(18, 0))
                THEN CASE 
                        WHEN (cit80saleh.smkeiroid = ((5)::NUMERIC)::NUMERIC(18, 0))
                            THEN '121'::CHARACTER VARYING
                        ELSE CASE 
                                WHEN (cit80saleh.smkeiroid = ((6)::NUMERIC)::NUMERIC(18, 0))
                                    THEN '112'::CHARACTER VARYING
                                WHEN (cit80saleh.marker = ((4)::NUMERIC)::NUMERIC(18, 0))
                                    THEN '511'::CHARACTER VARYING
                                ELSE '111'::CHARACTER VARYING
                                END
                        END
            ELSE CASE 
                    WHEN ((cit80saleh.kaisha)::TEXT = ('000'::CHARACTER VARYING)::TEXT)
                        THEN CASE 
                                WHEN ((cit80saleh.daihanrobunname)::TEXT = ('Web'::CHARACTER VARYING)::TEXT)
                                    THEN '121'::CHARACTER VARYING
                                ELSE '111'::CHARACTER VARYING
                                END
                    WHEN ((cit80saleh.kaisha)::TEXT = ('001'::CHARACTER VARYING)::TEXT)
                        THEN '112'::CHARACTER VARYING
                    ELSE '114'::CHARACTER VARYING
                    END
            END AS channel,
        'ダミーコード'::CHARACTER VARYING AS tokuicode,
        sum(cit80saleh.pointkominukikingaku) AS pointkomikingaku,
        sum(CASE 
                WHEN (cit80saleh.kakokbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                    THEN cit80saleh.sogokei
                ELSE (cit80saleh.nukikingaku + cit80saleh.tax)
                END) AS sogokei,
        sum(CASE 
                WHEN (cit80saleh.riyopoint = ((0)::NUMERIC)::NUMERIC(18, 0))
                    THEN ((0)::NUMERIC)::NUMERIC(18, 0)
                ELSE CASE 
                        WHEN (cit80saleh.kakokbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                            THEN CASE 
                                    WHEN (cit80saleh.shukadate >= ((20140401)::NUMERIC)::NUMERIC(18, 0))
                                        THEN (cit80saleh.riyopoint * 0.92)
                                    ELSE (cit80saleh.riyopoint * 0.95)
                                    END
                        ELSE (cit80saleh.riyopoint * 0.92)
                        END
                END) AS notaxpoint,
        sum(CASE 
                WHEN (cit80saleh.kakokbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                    THEN cit80saleh.riyopoint
                ELSE cit80saleh.riyopoint
                END) AS riyopoint,
        cit80saleh.bmn_hyouji_cd,
        cit80saleh.bmn_nms
    FROM cit80saleh
    WHERE (
            (cit80saleh.cancelflg = ((0)::NUMERIC)::NUMERIC(18, 0))
            AND ((cit80saleh.torikeikbn)::TEXT = ('01'::CHARACTER VARYING)::TEXT)
            )
    GROUP BY cit80saleh.saleno,
        cit80saleh.shukadate,
        CASE 
            WHEN (cit80saleh.kakokbn = ((0)::NUMERIC)::NUMERIC(18, 0))
                THEN CASE 
                        WHEN (cit80saleh.smkeiroid = ((5)::NUMERIC)::NUMERIC(18, 0))
                            THEN '121'::CHARACTER VARYING
                        ELSE CASE 
                                WHEN (cit80saleh.smkeiroid = ((6)::NUMERIC)::NUMERIC(18, 0))
                                    THEN '112'::CHARACTER VARYING
                                WHEN (cit80saleh.marker = ((4)::NUMERIC)::NUMERIC(18, 0))
                                    THEN '511'::CHARACTER VARYING
                                ELSE '111'::CHARACTER VARYING
                                END
                        END
            ELSE CASE 
                    WHEN ((cit80saleh.kaisha)::TEXT = ('000'::CHARACTER VARYING)::TEXT)
                        THEN CASE 
                                WHEN ((cit80saleh.daihanrobunname)::TEXT = ('Web'::CHARACTER VARYING)::TEXT)
                                    THEN '121'::CHARACTER VARYING
                                ELSE '111'::CHARACTER VARYING
                                END
                    WHEN ((cit80saleh.kaisha)::TEXT = ('001'::CHARACTER VARYING)::TEXT)
                        THEN '112'::CHARACTER VARYING
                    ELSE '114'::CHARACTER VARYING
                    END
            END,
        cit80saleh.bmn_hyouji_cd,
        cit80saleh.bmn_nms
    ),
t2
AS (
    SELECT cit85osalh.ourino,
        cit85osalh.shukadate AS shukanengetu,
        CASE 
            WHEN ((cit85osalh.torikeikbn)::TEXT = ('02'::CHARACTER VARYING)::TEXT)
                THEN CASE 
                        WHEN ((cit85osalh.tokuicode)::TEXT = ('QVC0000001'::CHARACTER VARYING)::TEXT)
                            THEN '321'::CHARACTER VARYING
                        WHEN (
                                ((cit85osalh.tokuicode)::TEXT = ('CC00100001'::CHARACTER VARYING)::TEXT)
                                OR ((cit85osalh.tokuicode)::TEXT = ('CC00100000'::CHARACTER VARYING)::TEXT)
                                )
                            THEN '111'::CHARACTER VARYING
                        ELSE CASE 
                                WHEN (
                                        (cit85osalh.fskbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                                        OR (cit85osalh.fskbn = ((0)::NUMERIC)::NUMERIC(18, 0))
                                        )
                                    THEN '511'::CHARACTER VARYING
                                ELSE CASE 
                                        WHEN ((cit85osalh.shokuikibunrui)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                                            THEN '312'::CHARACTER VARYING
                                        WHEN ((cit85osalh.shokuikibunrui)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                                            THEN '313'::CHARACTER VARYING
                                        WHEN ((cit85osalh.shokuikibunrui)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                                            THEN '314'::CHARACTER VARYING
                                        ELSE CASE 
                                                WHEN (
                                                        "substring" (
                                                            (cit85osalh.tokuicode)::TEXT,
                                                            2,
                                                            9
                                                            ) <= ('000499999'::CHARACTER VARYING)::TEXT
                                                        )
                                                    THEN '311'::CHARACTER VARYING
                                                WHEN (
                                                        (
                                                            "substring" (
                                                                (cit85osalh.tokuicode)::TEXT,
                                                                2,
                                                                9
                                                                ) >= ('000600000'::CHARACTER VARYING)::TEXT
                                                            )
                                                        AND (
                                                            "substring" (
                                                                (cit85osalh.tokuicode)::TEXT,
                                                                2,
                                                                9
                                                                ) <= ('000799999'::CHARACTER VARYING)::TEXT
                                                            )
                                                        )
                                                    THEN '412'::CHARACTER VARYING
                                                WHEN (
                                                        (
                                                            "substring" (
                                                                (cit85osalh.tokuicode)::TEXT,
                                                                2,
                                                                9
                                                                ) >= ('000500001'::CHARACTER VARYING)::TEXT
                                                            )
                                                        AND (
                                                            "substring" (
                                                                (cit85osalh.tokuicode)::TEXT,
                                                                2,
                                                                9
                                                                ) <= ('000599999'::CHARACTER VARYING)::TEXT
                                                            )
                                                        )
                                                    THEN '312'::CHARACTER VARYING
                                                WHEN (
                                                        (
                                                            "substring" (
                                                                (cit85osalh.tokuicode)::TEXT,
                                                                2,
                                                                9
                                                                ) >= ('000800001'::CHARACTER VARYING)::TEXT
                                                            )
                                                        AND (
                                                            "substring" (
                                                                (cit85osalh.tokuicode)::TEXT,
                                                                2,
                                                                9
                                                                ) <= ('000899999'::CHARACTER VARYING)::TEXT
                                                            )
                                                        )
                                                    THEN '311'::CHARACTER VARYING
                                                WHEN (
                                                        "substring" (
                                                            (cit85osalh.tokuicode)::TEXT,
                                                            2,
                                                            9
                                                            ) >= ('000900000'::CHARACTER VARYING)::TEXT
                                                        )
                                                    THEN CASE 
                                                            WHEN ((cit85osalh.tokuicode)::TEXT LIKE ('000099%'::CHARACTER VARYING)::TEXT)
                                                                THEN '411'::CHARACTER VARYING
                                                            WHEN ((cit85osalh.tokuicode)::TEXT LIKE ('VIP%'::CHARACTER VARYING)::TEXT)
                                                                THEN '113'::CHARACTER VARYING
                                                            WHEN ((cit85osalh.tokuicode)::TEXT LIKE ('CINEXT%'::CHARACTER VARYING)::TEXT)
                                                                THEN '111'::CHARACTER VARYING
                                                            ELSE '412'::CHARACTER VARYING
                                                            END
                                                ELSE ''::CHARACTER VARYING
                                                END
                                        END
                                END
                        END
            ELSE CASE 
                    WHEN ((cit85osalh.torikeikbn)::TEXT = ('03'::CHARACTER VARYING)::TEXT)
                        THEN '211'::CHARACTER VARYING
                    WHEN ((cit85osalh.torikeikbn)::TEXT = ('04'::CHARACTER VARYING)::TEXT)
                        THEN '212'::CHARACTER VARYING
                    WHEN ((cit85osalh.torikeikbn)::TEXT = ('05'::CHARACTER VARYING)::TEXT)
                        THEN '213'::CHARACTER VARYING
                    WHEN ((cit85osalh.torikeikbn)::TEXT = ('06'::CHARACTER VARYING)::TEXT)
                        THEN '214'::CHARACTER VARYING
                    ELSE NULL::CHARACTER VARYING
                    END
            END AS channel,
        cit85osalh.tokuicode,
        CASE 
            WHEN (cit85osalh.kakokbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                THEN sum((cit85osalh.sogokei - cit85osalh.tax))
            ELSE sum(cit85osalh.nukikingaku)
            END AS pointkomikingaku,
        CASE 
            WHEN (cit85osalh.kakokbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                THEN sum(cit85osalh.sogokei)
            ELSE sum((cit85osalh.nukikingaku + cit85osalh.tax))
            END AS sogokei,
        0 AS notaxpoint,
        0 AS riyopoint,
        cit85osalh.bmn_hyouji_cd,
        cit85osalh.bmn_nms
    FROM cit85osalh
    GROUP BY cit85osalh.ourino,
        cit85osalh.shukadate,
        CASE 
            WHEN ((cit85osalh.torikeikbn)::TEXT = ('02'::CHARACTER VARYING)::TEXT)
                THEN CASE 
                        WHEN ((cit85osalh.tokuicode)::TEXT = ('QVC0000001'::CHARACTER VARYING)::TEXT)
                            THEN '321'::CHARACTER VARYING
                        WHEN (
                                ((cit85osalh.tokuicode)::TEXT = ('CC00100001'::CHARACTER VARYING)::TEXT)
                                OR ((cit85osalh.tokuicode)::TEXT = ('CC00100000'::CHARACTER VARYING)::TEXT)
                                )
                            THEN '111'::CHARACTER VARYING
                        ELSE CASE 
                                WHEN (
                                        (cit85osalh.fskbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                                        OR (cit85osalh.fskbn = ((0)::NUMERIC)::NUMERIC(18, 0))
                                        )
                                    THEN '511'::CHARACTER VARYING
                                ELSE CASE 
                                        WHEN ((cit85osalh.shokuikibunrui)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                                            THEN '312'::CHARACTER VARYING
                                        WHEN ((cit85osalh.shokuikibunrui)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                                            THEN '313'::CHARACTER VARYING
                                        WHEN ((cit85osalh.shokuikibunrui)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                                            THEN '314'::CHARACTER VARYING
                                        ELSE CASE 
                                                WHEN (
                                                        "substring" (
                                                            (cit85osalh.tokuicode)::TEXT,
                                                            2,
                                                            9
                                                            ) <= ('000499999'::CHARACTER VARYING)::TEXT
                                                        )
                                                    THEN '311'::CHARACTER VARYING
                                                WHEN (
                                                        (
                                                            "substring" (
                                                                (cit85osalh.tokuicode)::TEXT,
                                                                2,
                                                                9
                                                                ) >= ('000600000'::CHARACTER VARYING)::TEXT
                                                            )
                                                        AND (
                                                            "substring" (
                                                                (cit85osalh.tokuicode)::TEXT,
                                                                2,
                                                                9
                                                                ) <= ('000799999'::CHARACTER VARYING)::TEXT
                                                            )
                                                        )
                                                    THEN '412'::CHARACTER VARYING
                                                WHEN (
                                                        (
                                                            "substring" (
                                                                (cit85osalh.tokuicode)::TEXT,
                                                                2,
                                                                9
                                                                ) >= ('000500001'::CHARACTER VARYING)::TEXT
                                                            )
                                                        AND (
                                                            "substring" (
                                                                (cit85osalh.tokuicode)::TEXT,
                                                                2,
                                                                9
                                                                ) <= ('000599999'::CHARACTER VARYING)::TEXT
                                                            )
                                                        )
                                                    THEN '312'::CHARACTER VARYING
                                                WHEN (
                                                        (
                                                            "substring" (
                                                                (cit85osalh.tokuicode)::TEXT,
                                                                2,
                                                                9
                                                                ) >= ('000800001'::CHARACTER VARYING)::TEXT
                                                            )
                                                        AND (
                                                            "substring" (
                                                                (cit85osalh.tokuicode)::TEXT,
                                                                2,
                                                                9
                                                                ) <= ('000899999'::CHARACTER VARYING)::TEXT
                                                            )
                                                        )
                                                    THEN '311'::CHARACTER VARYING
                                                WHEN (
                                                        "substring" (
                                                            (cit85osalh.tokuicode)::TEXT,
                                                            2,
                                                            9
                                                            ) >= ('000900000'::CHARACTER VARYING)::TEXT
                                                        )
                                                    THEN CASE 
                                                            WHEN ((cit85osalh.tokuicode)::TEXT LIKE ('000099%'::CHARACTER VARYING)::TEXT)
                                                                THEN '411'::CHARACTER VARYING
                                                            WHEN ((cit85osalh.tokuicode)::TEXT LIKE ('VIP%'::CHARACTER VARYING)::TEXT)
                                                                THEN '113'::CHARACTER VARYING
                                                            WHEN ((cit85osalh.tokuicode)::TEXT LIKE ('CINEXT%'::CHARACTER VARYING)::TEXT)
                                                                THEN '111'::CHARACTER VARYING
                                                            ELSE '412'::CHARACTER VARYING
                                                            END
                                                ELSE ''::CHARACTER VARYING
                                                END
                                        END
                                END
                        END
            ELSE CASE 
                    WHEN ((cit85osalh.torikeikbn)::TEXT = ('03'::CHARACTER VARYING)::TEXT)
                        THEN '211'::CHARACTER VARYING
                    WHEN ((cit85osalh.torikeikbn)::TEXT = ('04'::CHARACTER VARYING)::TEXT)
                        THEN '212'::CHARACTER VARYING
                    WHEN ((cit85osalh.torikeikbn)::TEXT = ('05'::CHARACTER VARYING)::TEXT)
                        THEN '213'::CHARACTER VARYING
                    WHEN ((cit85osalh.torikeikbn)::TEXT = ('06'::CHARACTER VARYING)::TEXT)
                        THEN '214'::CHARACTER VARYING
                    ELSE NULL::CHARACTER VARYING
                    END
            END,
        cit85osalh.tokuicode,
        cit85osalh.kakokbn,
        cit85osalh.bmn_hyouji_cd,
        cit85osalh.bmn_nms
    ),
t3
AS (
    SELECT cit85osalh_kaigai.ourino,
        cit85osalh_kaigai.shukadate AS shukanengetu,
        CASE 
            WHEN ((cit85osalh_kaigai.tokuicode)::TEXT LIKE ('000099%'::CHARACTER VARYING)::TEXT)
                THEN '411'::CHARACTER VARYING
            WHEN ((cit85osalh_kaigai.tokuicode)::TEXT LIKE ('000091%'::CHARACTER VARYING)::TEXT)
                THEN '413'::CHARACTER VARYING
            ELSE '412'::CHARACTER VARYING
            END AS channel,
        cit85osalh_kaigai.tokuicode,
        sum(cit85osalh_kaigai.nukikingaku) AS pointkomikingaku,
        sum((cit85osalh_kaigai.nukikingaku + cit85osalh_kaigai.tax)) AS sogokei,
        0 AS notaxpoint,
        0 AS riyopoint,
        cit85osalh_kaigai.bmn_hyouji_cd,
        cit85osalh_kaigai.bmn_nms
    FROM cit85osalh_kaigai
    WHERE (cit85osalh_kaigai.kakokbn = ((0)::NUMERIC)::NUMERIC(18, 0))
    GROUP BY cit85osalh_kaigai.ourino,
        cit85osalh_kaigai.shukadate,
        CASE 
            WHEN ((cit85osalh_kaigai.tokuicode)::TEXT LIKE ('000099%'::CHARACTER VARYING)::TEXT)
                THEN '411'::CHARACTER VARYING
            WHEN ((cit85osalh_kaigai.tokuicode)::TEXT LIKE ('000091%'::CHARACTER VARYING)::TEXT)
                THEN '413'::CHARACTER VARYING
            ELSE '412'::CHARACTER VARYING
            END,
        cit85osalh_kaigai.tokuicode,
        cit85osalh_kaigai.bmn_hyouji_cd,
        cit85osalh_kaigai.bmn_nms
    ),
t
AS (
    SELECT *
    FROM t1
    
    UNION ALL
    
    SELECT *
    FROM t2
    
    UNION ALL
    
    SELECT *
    FROM t3
    ),
FINAL
AS (
    SELECT t.shukanengetu,
        CASE 
            WHEN (
                    (
                        (
                            (
                                ((t.channel)::TEXT = ('111'::CHARACTER VARYING)::TEXT)
                                OR ((t.channel)::TEXT = ('112'::CHARACTER VARYING)::TEXT)
                                )
                            OR ((t.channel)::TEXT = ('113'::CHARACTER VARYING)::TEXT)
                            )
                        OR ((t.channel)::TEXT = ('114'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('121'::CHARACTER VARYING)::TEXT)
                    )
                THEN '通信販売'::CHARACTER VARYING
            WHEN (
                    (
                        (
                            ((t.channel)::TEXT = ('211'::CHARACTER VARYING)::TEXT)
                            OR ((t.channel)::TEXT = ('212'::CHARACTER VARYING)::TEXT)
                            )
                        OR ((t.channel)::TEXT = ('213'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('214'::CHARACTER VARYING)::TEXT)
                    )
                THEN '対面販売'::CHARACTER VARYING
            WHEN (
                    (
                        (
                            (
                                ((t.channel)::TEXT = ('311'::CHARACTER VARYING)::TEXT)
                                OR ((t.channel)::TEXT = ('312'::CHARACTER VARYING)::TEXT)
                                )
                            OR ((t.channel)::TEXT = ('313'::CHARACTER VARYING)::TEXT)
                            )
                        OR ((t.channel)::TEXT = ('314'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('321'::CHARACTER VARYING)::TEXT)
                    )
                THEN '卸売'::CHARACTER VARYING
            WHEN (
                    (
                        ((t.channel)::TEXT = ('411'::CHARACTER VARYING)::TEXT)
                        OR ((t.channel)::TEXT = ('412'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('413'::CHARACTER VARYING)::TEXT)
                    )
                THEN '海外'::CHARACTER VARYING
            WHEN (
                    ((t.channel)::TEXT = ('511'::CHARACTER VARYING)::TEXT)
                    OR ((t.channel)::TEXT = ('512'::CHARACTER VARYING)::TEXT)
                    )
                THEN 'その他'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS channel1,
        CASE 
            WHEN (
                    (
                        (
                            ((t.channel)::TEXT = ('111'::CHARACTER VARYING)::TEXT)
                            OR ((t.channel)::TEXT = ('112'::CHARACTER VARYING)::TEXT)
                            )
                        OR ((t.channel)::TEXT = ('113'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('114'::CHARACTER VARYING)::TEXT)
                    )
                THEN '通販'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('121'::CHARACTER VARYING)::TEXT)
                THEN 'WEB'::CHARACTER VARYING
            WHEN (
                    (
                        (
                            ((t.channel)::TEXT = ('211'::CHARACTER VARYING)::TEXT)
                            OR ((t.channel)::TEXT = ('212'::CHARACTER VARYING)::TEXT)
                            )
                        OR ((t.channel)::TEXT = ('213'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('214'::CHARACTER VARYING)::TEXT)
                    )
                THEN '店舗'::CHARACTER VARYING
            WHEN (
                    (
                        (
                            ((t.channel)::TEXT = ('311'::CHARACTER VARYING)::TEXT)
                            OR ((t.channel)::TEXT = ('312'::CHARACTER VARYING)::TEXT)
                            )
                        OR ((t.channel)::TEXT = ('313'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('314'::CHARACTER VARYING)::TEXT)
                    )
                THEN '卸売'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('321'::CHARACTER VARYING)::TEXT)
                THEN 'QVC'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('411'::CHARACTER VARYING)::TEXT)
                THEN 'JJ'::CHARACTER VARYING
            WHEN (
                    ((t.channel)::TEXT = ('412'::CHARACTER VARYING)::TEXT)
                    OR ((t.channel)::TEXT = ('413'::CHARACTER VARYING)::TEXT)
                    )
                THEN '海外'::CHARACTER VARYING
            WHEN (
                    ((t.channel)::TEXT = ('511'::CHARACTER VARYING)::TEXT)
                    OR ((t.channel)::TEXT = ('512'::CHARACTER VARYING)::TEXT)
                    )
                THEN 'その他'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS channel2,
        CASE 
            WHEN ((t.channel)::TEXT = ('111'::CHARACTER VARYING)::TEXT)
                THEN '通販'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('112'::CHARACTER VARYING)::TEXT)
                THEN '社販'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('113'::CHARACTER VARYING)::TEXT)
                THEN 'VIP'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('114'::CHARACTER VARYING)::TEXT)
                THEN '通販'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('121'::CHARACTER VARYING)::TEXT)
                THEN 'WEB'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('211'::CHARACTER VARYING)::TEXT)
                THEN '買取'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('212'::CHARACTER VARYING)::TEXT)
                THEN '直営'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('213'::CHARACTER VARYING)::TEXT)
                THEN '消化'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('214'::CHARACTER VARYING)::TEXT)
                THEN 'アウトレット'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('311'::CHARACTER VARYING)::TEXT)
                THEN '代理店'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('312'::CHARACTER VARYING)::TEXT)
                THEN '職域（特販）'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('313'::CHARACTER VARYING)::TEXT)
                THEN '職域（代理店）'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('314'::CHARACTER VARYING)::TEXT)
                THEN '職域（販売会）'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('321'::CHARACTER VARYING)::TEXT)
                THEN 'QVC'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('411'::CHARACTER VARYING)::TEXT)
                THEN 'JJ'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('412'::CHARACTER VARYING)::TEXT)
                THEN '国内免税'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('413'::CHARACTER VARYING)::TEXT)
                THEN '海外免税'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('511'::CHARACTER VARYING)::TEXT)
                THEN 'FS'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('512'::CHARACTER VARYING)::TEXT)
                THEN 'その他'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS channel3,
        t.tokuicode,
        (((t.tokuicode)::TEXT || (nvl2(cim02.tokuiname, ' : '::CHARACTER VARYING, ''::CHARACTER VARYING))::TEXT) || COALESCE((((cim02.tokuiname)::TEXT || (' '::CHARACTER VARYING)::TEXT) || (cim02.tokuiname_ryaku)::TEXT), ((' '::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT)) AS tokuiname,
        sum(t.pointkomikingaku) AS pointkomikingaku,
        sum(t.sogokei) AS zeikomikingaku,
        sum(t.notaxpoint) AS zeinukipoint,
        sum(t.riyopoint) AS riyopoint,
        t.bmn_hyouji_cd,
        t.bmn_nms
    FROM t
    LEFT JOIN cim02tokui cim02 ON (((t.tokuicode)::TEXT = (cim02.tokuicode)::TEXT))
    WHERE (1 = 1)
    GROUP BY t.shukanengetu,
        CASE 
            WHEN (
                    (
                        (
                            (
                                ((t.channel)::TEXT = ('111'::CHARACTER VARYING)::TEXT)
                                OR ((t.channel)::TEXT = ('112'::CHARACTER VARYING)::TEXT)
                                )
                            OR ((t.channel)::TEXT = ('113'::CHARACTER VARYING)::TEXT)
                            )
                        OR ((t.channel)::TEXT = ('114'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('121'::CHARACTER VARYING)::TEXT)
                    )
                THEN '通信販売'::CHARACTER VARYING
            WHEN (
                    (
                        (
                            ((t.channel)::TEXT = ('211'::CHARACTER VARYING)::TEXT)
                            OR ((t.channel)::TEXT = ('212'::CHARACTER VARYING)::TEXT)
                            )
                        OR ((t.channel)::TEXT = ('213'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('214'::CHARACTER VARYING)::TEXT)
                    )
                THEN '対面販売'::CHARACTER VARYING
            WHEN (
                    (
                        (
                            (
                                ((t.channel)::TEXT = ('311'::CHARACTER VARYING)::TEXT)
                                OR ((t.channel)::TEXT = ('312'::CHARACTER VARYING)::TEXT)
                                )
                            OR ((t.channel)::TEXT = ('313'::CHARACTER VARYING)::TEXT)
                            )
                        OR ((t.channel)::TEXT = ('314'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('321'::CHARACTER VARYING)::TEXT)
                    )
                THEN '卸売'::CHARACTER VARYING
            WHEN (
                    (
                        ((t.channel)::TEXT = ('411'::CHARACTER VARYING)::TEXT)
                        OR ((t.channel)::TEXT = ('412'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('413'::CHARACTER VARYING)::TEXT)
                    )
                THEN '海外'::CHARACTER VARYING
            WHEN (
                    ((t.channel)::TEXT = ('511'::CHARACTER VARYING)::TEXT)
                    OR ((t.channel)::TEXT = ('512'::CHARACTER VARYING)::TEXT)
                    )
                THEN 'その他'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END,
        CASE 
            WHEN (
                    (
                        (
                            ((t.channel)::TEXT = ('111'::CHARACTER VARYING)::TEXT)
                            OR ((t.channel)::TEXT = ('112'::CHARACTER VARYING)::TEXT)
                            )
                        OR ((t.channel)::TEXT = ('113'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('114'::CHARACTER VARYING)::TEXT)
                    )
                THEN '通販'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('121'::CHARACTER VARYING)::TEXT)
                THEN 'WEB'::CHARACTER VARYING
            WHEN (
                    (
                        (
                            ((t.channel)::TEXT = ('211'::CHARACTER VARYING)::TEXT)
                            OR ((t.channel)::TEXT = ('212'::CHARACTER VARYING)::TEXT)
                            )
                        OR ((t.channel)::TEXT = ('213'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('214'::CHARACTER VARYING)::TEXT)
                    )
                THEN '店舗'::CHARACTER VARYING
            WHEN (
                    (
                        (
                            ((t.channel)::TEXT = ('311'::CHARACTER VARYING)::TEXT)
                            OR ((t.channel)::TEXT = ('312'::CHARACTER VARYING)::TEXT)
                            )
                        OR ((t.channel)::TEXT = ('313'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((t.channel)::TEXT = ('314'::CHARACTER VARYING)::TEXT)
                    )
                THEN '卸売'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('321'::CHARACTER VARYING)::TEXT)
                THEN 'QVC'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('411'::CHARACTER VARYING)::TEXT)
                THEN 'JJ'::CHARACTER VARYING
            WHEN (
                    ((t.channel)::TEXT = ('412'::CHARACTER VARYING)::TEXT)
                    OR ((t.channel)::TEXT = ('413'::CHARACTER VARYING)::TEXT)
                    )
                THEN '海外'::CHARACTER VARYING
            WHEN (
                    ((t.channel)::TEXT = ('511'::CHARACTER VARYING)::TEXT)
                    OR ((t.channel)::TEXT = ('512'::CHARACTER VARYING)::TEXT)
                    )
                THEN 'その他'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END,
        CASE 
            WHEN ((t.channel)::TEXT = ('111'::CHARACTER VARYING)::TEXT)
                THEN '通販'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('112'::CHARACTER VARYING)::TEXT)
                THEN '社販'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('113'::CHARACTER VARYING)::TEXT)
                THEN 'VIP'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('114'::CHARACTER VARYING)::TEXT)
                THEN '通販'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('121'::CHARACTER VARYING)::TEXT)
                THEN 'WEB'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('211'::CHARACTER VARYING)::TEXT)
                THEN '買取'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('212'::CHARACTER VARYING)::TEXT)
                THEN '直営'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('213'::CHARACTER VARYING)::TEXT)
                THEN '消化'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('214'::CHARACTER VARYING)::TEXT)
                THEN 'アウトレット'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('311'::CHARACTER VARYING)::TEXT)
                THEN '代理店'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('312'::CHARACTER VARYING)::TEXT)
                THEN '職域（特販）'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('313'::CHARACTER VARYING)::TEXT)
                THEN '職域（代理店）'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('314'::CHARACTER VARYING)::TEXT)
                THEN '職域（販売会）'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('321'::CHARACTER VARYING)::TEXT)
                THEN 'QVC'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('411'::CHARACTER VARYING)::TEXT)
                THEN 'JJ'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('412'::CHARACTER VARYING)::TEXT)
                THEN '国内免税'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('413'::CHARACTER VARYING)::TEXT)
                THEN '海外免税'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('511'::CHARACTER VARYING)::TEXT)
                THEN 'FS'::CHARACTER VARYING
            WHEN ((t.channel)::TEXT = ('512'::CHARACTER VARYING)::TEXT)
                THEN 'その他'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END,
        t.tokuicode,
        (((t.tokuicode)::TEXT || (nvl2(cim02.tokuiname, ' : '::CHARACTER VARYING, ''::CHARACTER VARYING))::TEXT) || COALESCE((((cim02.tokuiname)::TEXT || (' '::CHARACTER VARYING)::TEXT) || (cim02.tokuiname_ryaku)::TEXT), ((' '::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT)),
        t.bmn_hyouji_cd,
        t.bmn_nms
    )
SELECT *
FROM

FINAL
