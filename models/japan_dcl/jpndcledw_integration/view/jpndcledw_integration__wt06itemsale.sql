WITH tm40shihanki
AS (
    SELECT *
    FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM40SHIHANKI
    ),
zaiko_shohin_attr
AS (
    SELECT *
    FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.ZAIKO_SHOHIN_ATTR
    ),
tm67juch_nm
AS (
    SELECT *
    FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM67JUCH_NM
    ),
tm66torikei_nm
AS (
    SELECT *
    FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM66TORIKEI_NM
    ),
tm44kaisya_nm
AS (
    SELECT *
    FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM44KAISYA_NM
    ),
cit86osalm
AS (
    SELECT *
    FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIT86OSALM
    ),
cit85osalh
AS (
    SELECT *
    FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIT85OSALH
    ),
cit81salem
AS (
    SELECT *
    FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIT81SALEM
    ),
cit80saleh
AS (
    SELECT *
    FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIT80SALEH
    ),
cim24itbun
AS (
    SELECT *
    FROM DEV_DNA_CORE.JPDCLEDW_INTEGRATION.CIM24ITBUN
    ),
cim03item_zaiko
AS (
    SELECT *
    FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM03ITEM_ZAIKO
    ),
cim02tokui
AS (
    SELECT *
    FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM02TOKUI_IKOU
    ),
item
AS (
    SELECT cim03.itemcode,
        COALESCE(cim03.itemname, 'その他'::CHARACTER VARYING) AS itemname,
        COALESCE(cim03.tanka, ((0)::NUMERIC)::NUMERIC(18, 0)) AS tanka,
        CASE 
            WHEN (
                    ((cim03.itemname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                    OR (
                        (cim03.itemname IS NULL)
                        AND (NULL IS NULL)
                        )
                    )
                THEN ('その他'::CHARACTER VARYING)::TEXT
            ELSE (((cim03.itemcode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim03.itemname)::TEXT)
            END AS itemcname,
        cim03.bunruicode3 AS chubuncode,
        COALESCE(cim24_chu.itbunname, 'その他'::CHARACTER VARYING) AS chubunname,
        CASE 
            WHEN (
                    ((cim24_chu.itbunname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                    OR (
                        (cim24_chu.itbunname IS NULL)
                        AND (NULL IS NULL)
                        )
                    )
                THEN ('その他'::CHARACTER VARYING)::TEXT
            ELSE (((cim03.bunruicode3)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim24_chu.itbunname)::TEXT)
            END AS chubuncname,
        cim03.bunruicode2 AS daibuncode,
        COALESCE(cim24_dai.itbunname, 'その他'::CHARACTER VARYING) AS daibunname,
        CASE 
            WHEN (
                    ((cim24_dai.itbunname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                    OR (
                        (cim24_dai.itbunname IS NULL)
                        AND (NULL IS NULL)
                        )
                    )
                THEN ('その他'::CHARACTER VARYING)::TEXT
            ELSE (((cim03.bunruicode2)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim24_dai.itbunname)::TEXT)
            END AS daibuncname,
        cim03.bunruicode5 AS daidaibuncode,
        COALESCE(cim24_daidai.itbunname, 'その他'::CHARACTER VARYING) AS daidaibunname,
        CASE 
            WHEN (
                    ((cim24_daidai.itbunname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                    OR (
                        (cim24_daidai.itbunname IS NULL)
                        AND (NULL IS NULL)
                        )
                    )
                THEN ('その他'::CHARACTER VARYING)::TEXT
            ELSE (((cim03.bunruicode5)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim24_daidai.itbunname)::TEXT)
            END AS daidaibuncname
    FROM (
        (
            (
                (
                    cim03item_zaiko cim03 LEFT JOIN cim24itbun cim24_syo ON (
                            (
                                ((cim03.bunruicode4)::TEXT = (cim24_syo.itbuncode)::TEXT)
                                AND ((cim24_syo.itbunshcode)::TEXT = ('4'::CHARACTER VARYING)::TEXT)
                                )
                            )
                    ) LEFT JOIN cim24itbun cim24_chu ON (
                        (
                            ((cim03.bunruicode3)::TEXT = (cim24_chu.itbuncode)::TEXT)
                            AND ((cim24_chu.itbunshcode)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                            )
                        )
                ) LEFT JOIN cim24itbun cim24_dai ON (
                    (
                        ((cim03.bunruicode2)::TEXT = (cim24_dai.itbuncode)::TEXT)
                        AND ((cim24_dai.itbunshcode)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                        )
                    )
            ) LEFT JOIN cim24itbun cim24_daidai ON (
                (
                    ((cim03.bunruicode5)::TEXT = (cim24_daidai.itbuncode)::TEXT)
                    AND ((cim24_daidai.itbunshcode)::TEXT = ('5'::CHARACTER VARYING)::TEXT)
                    )
                )
        )
    )
SELECT cit80.saleno,
    tm40_juch.fy AS juchfy,
    tm40_juch.nendo AS juchnendo,
    tm40_juch.fh AS juchfh,
    tm40_juch.hanki AS juchhanki,
    tm40_juch.qt AS juchqt,
    tm40_juch.shihanki AS juchshihanki,
    CASE 
        WHEN (length(((cit80.juchdate)::CHARACTER VARYING)::TEXT) = 8)
            THEN (
                    "substring" (
                        ((cit80.juchdate)::CHARACTER VARYING)::TEXT,
                        1,
                        4
                        )
                    )::CHARACTER VARYING
        ELSE NULL::CHARACTER VARYING
        END AS juchyear,
    CASE 
        WHEN (length(((cit80.juchdate)::CHARACTER VARYING)::TEXT) = 8)
            THEN (
                    "substring" (
                        ((cit80.juchdate)::CHARACTER VARYING)::TEXT,
                        5,
                        2
                        )
                    )::CHARACTER VARYING
        ELSE NULL::CHARACTER VARYING
        END AS juchmonth,
    CASE 
        WHEN (length(((cit80.juchdate)::CHARACTER VARYING)::TEXT) = 8)
            THEN cit80.juchdate
        ELSE ((99991231)::NUMERIC)::NUMERIC(18, 0)
        END AS juchdate,
    CASE 
        WHEN (length(((cit80.juchdate)::CHARACTER VARYING)::TEXT) = 8)
            THEN (to_char((to_date(((cit80.juchdate)::CHARACTER VARYING)::TEXT, ('YYYYMMDD'::CHARACTER VARYING)::TEXT))::TIMESTAMP without TIME zone, ('D'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING
        ELSE NULL::CHARACTER VARYING
        END AS juchyoubicode,
    tm40_shuka.fy AS shukafy,
    tm40_shuka.nendo AS shukanendo,
    tm40_shuka.fh AS shukafh,
    tm40_shuka.hanki AS shukahanki,
    tm40_shuka.qt AS shukaqt,
    tm40_shuka.shihanki AS shukashihanki,
    CASE 
        WHEN (length(((cit80.shukadate)::CHARACTER VARYING)::TEXT) = 8)
            THEN (
                    "substring" (
                        ((cit80.shukadate)::CHARACTER VARYING)::TEXT,
                        1,
                        4
                        )
                    )::CHARACTER VARYING
        ELSE NULL::CHARACTER VARYING
        END AS shukayear,
    CASE 
        WHEN (length(((cit80.shukadate)::CHARACTER VARYING)::TEXT) = 8)
            THEN (
                    "substring" (
                        ((cit80.shukadate)::CHARACTER VARYING)::TEXT,
                        5,
                        2
                        )
                    )::CHARACTER VARYING
        ELSE NULL::CHARACTER VARYING
        END AS shukamonth,
    CASE 
        WHEN (length(((cit80.shukadate)::CHARACTER VARYING)::TEXT) = 8)
            THEN cit80.shukadate
        ELSE ((99991231)::NUMERIC)::NUMERIC(18, 0)
        END AS shukadate,
    CASE 
        WHEN (length(((cit80.shukadate)::CHARACTER VARYING)::TEXT) = 8)
            THEN (to_char((to_date(((cit80.shukadate)::CHARACTER VARYING)::TEXT, ('YYYYMMDD'::CHARACTER VARYING)::TEXT))::TIMESTAMP without TIME zone, ('D'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING
        ELSE NULL::CHARACTER VARYING
        END AS shukayoubicode,
    cit80.juchkbn,
    COALESCE(tm67.name, 'その他'::CHARACTER VARYING) AS juchkbnname,
    (COALESCE(tm67.cname, ('その他'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS juchkbncname,
    cit80.torikeikbn,
    COALESCE(tm66.name, 'その他'::CHARACTER VARYING) AS torikeikbnname,
    (COALESCE(tm66.cname, ('その他'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS torikeikbncname,
    cit80.kaisha,
    COALESCE(tm44.name, 'その他'::CHARACTER VARYING) AS kaishaname,
    (COALESCE(tm44.cname, ('その他'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS kaishacname,
    '----------' AS tokuicode,
    'その他' AS tokuiname,
    'その他' AS tokuicname,
    CASE 
        WHEN (
                (cit80.kakokbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                OR (
                    (cit80.kakokbn IS NULL)
                    AND ('1' IS NULL)
                    )
                )
            THEN CASE 
                    WHEN ((cit80.kaisha)::TEXT = ('000'::CHARACTER VARYING)::TEXT)
                        THEN '01'::CHARACTER VARYING
                    WHEN ((cit80.kaisha)::TEXT = ('001'::CHARACTER VARYING)::TEXT)
                        THEN '02'::CHARACTER VARYING
                    ELSE '03'::CHARACTER VARYING
                    END
        ELSE CASE 
                WHEN (cit80.smkeiroid = ((6)::NUMERIC)::NUMERIC(18, 0))
                    THEN '02'::CHARACTER VARYING
                ELSE '01'::CHARACTER VARYING
                END
        END AS channel,
    CASE 
        WHEN (
                (cit80.kakokbn = ((1)::NUMERIC)::NUMERIC(18, 0))
                OR (
                    (cit80.kakokbn IS NULL)
                    AND ('1' IS NULL)
                    )
                )
            THEN CASE 
                    WHEN ((cit80.kaisha)::TEXT = ('000'::CHARACTER VARYING)::TEXT)
                        THEN '01 : 通販'::CHARACTER VARYING
                    WHEN ((cit80.kaisha)::TEXT = ('001'::CHARACTER VARYING)::TEXT)
                        THEN '02 : 社内販売'::CHARACTER VARYING
                    ELSE '03 : 職域販売'::CHARACTER VARYING
                    END
        ELSE CASE 
                WHEN (cit80.smkeiroid = ((6)::NUMERIC)::NUMERIC(18, 0))
                    THEN '02 : 社内販売'::CHARACTER VARYING
                ELSE '01 : 通販'::CHARACTER VARYING
                END
        END AS channelcname,
    cit80.kaisha AS channeldcode,
    CASE 
        WHEN ((cit80.kaisha)::TEXT = ('000'::CHARACTER VARYING)::TEXT)
            THEN '通販'::CHARACTER VARYING
        WHEN ((cit80.kaisha)::TEXT = ('001'::CHARACTER VARYING)::TEXT)
            THEN 'ドクターシーラボ'::CHARACTER VARYING
        ELSE tm44.name
        END AS channeldname,
    cit81.itemcode,
    item.itemname,
    (item.itemcname)::CHARACTER VARYING AS itemcname,
    item.chubuncode AS itemchucode,
    item.chubunname AS itemchuname,
    (item.chubuncname)::CHARACTER VARYING AS itemchucname,
    item.daibuncode AS itemdaicode,
    item.daibunname AS itemdainame,
    (item.daibuncname)::CHARACTER VARYING AS itemdaicname,
    item.daidaibuncode AS itemdaidaicode,
    item.daidaibunname AS itemdaidainame,
    (item.daidaibuncname)::CHARACTER VARYING AS itemdaidaicname,
    item.tanka,
    cit81.suryo,
    CASE 
        WHEN (
                (
                    "substring" (
                        (cit80.juchkbn)::TEXT,
                        1,
                        1
                        ) = ('9'::CHARACTER VARYING)::TEXT
                    )
                OR (
                    (
                        "substring" (
                            (cit80.juchkbn)::TEXT,
                            1,
                            1
                            ) IS NULL
                        )
                    AND ('9' IS NULL)
                    )
                )
            THEN ((0)::NUMERIC)::NUMERIC(18, 0)
        ELSE cit81.meisainukikingaku
        END AS meisainukikingaku,
    CASE 
        WHEN (
                (
                    "substring" (
                        (cit80.juchkbn)::TEXT,
                        1,
                        1
                        ) = ('9'::CHARACTER VARYING)::TEXT
                    )
                OR (
                    (
                        "substring" (
                            (cit80.juchkbn)::TEXT,
                            1,
                            1
                            ) IS NULL
                        )
                    AND ('9' IS NULL)
                    )
                )
            THEN cit81.hensu
        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
        END AS hensu,
    CASE 
        WHEN (
                (
                    "substring" (
                        (cit80.juchkbn)::TEXT,
                        1,
                        1
                        ) = ('9'::CHARACTER VARYING)::TEXT
                    )
                OR (
                    (
                        "substring" (
                            (cit80.juchkbn)::TEXT,
                            1,
                            1
                            ) IS NULL
                        )
                    AND ('9' IS NULL)
                    )
                )
            THEN cit81.meisainukikingaku
        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
        END AS henkingaku,
    (
        cit81.suryo + CASE 
            WHEN (
                    (
                        "substring" (
                            (cit80.juchkbn)::TEXT,
                            1,
                            1
                            ) = ('9'::CHARACTER VARYING)::TEXT
                        )
                    OR (
                        (
                            "substring" (
                                (cit80.juchkbn)::TEXT,
                                1,
                                1
                                ) IS NULL
                            )
                        AND ('9' IS NULL)
                        )
                    )
                THEN cit81.hensu
            ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
            END
        ) AS urisuryo,
    cit81.meisainukikingaku AS urikingaku,
    CASE 
        WHEN (length(((cit80.updatedate)::CHARACTER VARYING)::TEXT) = 8)
            THEN cit80.updatedate
        ELSE ((99991231)::NUMERIC)::NUMERIC(18, 0)
        END AS updatedate,
    cit80.henreasoncode,
    cit80.henreasonname,
    ((((cit80.henreasoncode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cit80.henreasonname)::TEXT))::CHARACTER VARYING AS henreasoncname,
    zattr.bumon1_add_attr1,
    zattr.bumon1_add_attr2,
    zattr.bumon1_add_attr3,
    zattr.bumon1_add_attr4,
    zattr.bumon1_add_attr5,
    zattr.bumon1_add_attr6,
    zattr.bumon1_add_attr7,
    zattr.bumon1_add_attr8,
    zattr.bumon1_add_attr9,
    zattr.bumon1_add_attr10,
    zattr.bumon2_add_attr1,
    zattr.bumon2_add_attr2,
    zattr.bumon2_add_attr3,
    zattr.bumon2_add_attr4,
    zattr.bumon2_add_attr5,
    zattr.bumon2_add_attr6,
    zattr.bumon2_add_attr7,
    zattr.bumon2_add_attr8,
    zattr.bumon2_add_attr9,
    zattr.bumon2_add_attr10,
    zattr.bumon3_add_attr1,
    zattr.bumon3_add_attr2,
    zattr.bumon3_add_attr3,
    zattr.bumon3_add_attr4,
    zattr.bumon3_add_attr5,
    zattr.bumon3_add_attr6,
    zattr.bumon3_add_attr7,
    zattr.bumon3_add_attr8,
    zattr.bumon3_add_attr9,
    zattr.bumon3_add_attr10,
    zattr.bumon4_add_attr1,
    zattr.bumon4_add_attr2,
    zattr.bumon4_add_attr3,
    zattr.bumon4_add_attr4,
    zattr.bumon4_add_attr5,
    zattr.bumon4_add_attr6,
    zattr.bumon4_add_attr7,
    zattr.bumon4_add_attr8,
    zattr.bumon4_add_attr9,
    zattr.bumon4_add_attr10,
    zattr.bumon5_add_attr1,
    zattr.bumon5_add_attr2,
    zattr.bumon5_add_attr3,
    zattr.bumon5_add_attr4,
    zattr.bumon5_add_attr5,
    zattr.bumon5_add_attr6,
    zattr.bumon5_add_attr7,
    zattr.bumon5_add_attr8,
    zattr.bumon5_add_attr9,
    zattr.bumon5_add_attr10,
    zattr.bumon6_add_attr1,
    zattr.bumon6_add_attr2,
    zattr.bumon6_add_attr3,
    zattr.bumon6_add_attr4,
    zattr.bumon6_add_attr5,
    zattr.bumon6_add_attr6,
    zattr.bumon6_add_attr7,
    zattr.bumon6_add_attr8,
    zattr.bumon6_add_attr9,
    zattr.bumon6_add_attr10,
    zattr.bumon6_add_attr11,
    zattr.bumon6_add_attr12,
    zattr.bumon6_add_attr13,
    zattr.bumon6_add_attr14,
    zattr.bumon6_add_attr15,
    zattr.bumon6_add_attr16,
    zattr.bumon6_add_attr17,
    zattr.bumon6_add_attr18,
    zattr.bumon6_add_attr19,
    zattr.bumon6_add_attr20
FROM cit80saleh cit80
JOIN cit81salem cit81 ON (((cit80.saleno)::TEXT = (cit81.saleno)::TEXT))
LEFT JOIN item ON (((cit81.itemcode)::TEXT = (item.itemcode)::TEXT))
LEFT JOIN tm40shihanki tm40_juch ON (
        (
            (cit80.juchdate >= tm40_juch.fromdate)
            AND (cit80.juchdate <= tm40_juch.todate)
            )
        )
LEFT JOIN tm40shihanki tm40_shuka ON (
        (
            (cit80.shukadate >= tm40_shuka.fromdate)
            AND (cit80.shukadate <= tm40_shuka.todate)
            )
        )
LEFT JOIN tm67juch_nm tm67 ON (((cit80.juchkbn)::TEXT = (tm67.code)::TEXT))
LEFT JOIN tm66torikei_nm tm66 ON (((cit80.torikeikbn)::TEXT = (tm66.code)::TEXT))
LEFT JOIN tm44kaisya_nm tm44 ON (((cit80.kaisha)::TEXT = (tm44.code)::TEXT))
LEFT JOIN zaiko_shohin_attr zattr ON (((cit81.itemcode)::TEXT = (zattr.shohin_code)::TEXT))
WHERE (
        (
            (
                ((cit80.torikeikbn)::TEXT = ('01'::CHARACTER VARYING)::TEXT)
                AND (cit80.cancelflg = ((0)::NUMERIC)::NUMERIC(18, 0))
                )
            AND ((cit81.itemcode)::TEXT <> ('9990000100'::CHARACTER VARYING)::TEXT)
            )
        AND ((cit81.itemcode)::TEXT <> ('9990000200'::CHARACTER VARYING)::TEXT)
        )

UNION ALL

SELECT cit85.ourino AS saleno,
    tm40_juch.fy AS juchfy,
    tm40_juch.nendo AS juchnendo,
    tm40_juch.fh AS juchfh,
    tm40_juch.hanki AS juchhanki,
    tm40_juch.qt AS juchqt,
    tm40_juch.shihanki AS juchshihanki,
    CASE 
        WHEN (length(((cit85.juchdate)::CHARACTER VARYING)::TEXT) = 8)
            THEN (
                    "substring" (
                        ((cit85.juchdate)::CHARACTER VARYING)::TEXT,
                        1,
                        4
                        )
                    )::CHARACTER VARYING
        ELSE NULL::CHARACTER VARYING
        END AS juchyear,
    CASE 
        WHEN (length(((cit85.juchdate)::CHARACTER VARYING)::TEXT) = 8)
            THEN (
                    "substring" (
                        ((cit85.juchdate)::CHARACTER VARYING)::TEXT,
                        5,
                        2
                        )
                    )::CHARACTER VARYING
        ELSE NULL::CHARACTER VARYING
        END AS juchmonth,
    CASE 
        WHEN (length(((cit85.juchdate)::CHARACTER VARYING)::TEXT) = 8)
            THEN cit85.juchdate
        ELSE ((99991231)::NUMERIC)::NUMERIC(18, 0)
        END AS juchdate,
    CASE 
        WHEN (length(((cit85.juchdate)::CHARACTER VARYING)::TEXT) = 8)
            THEN (to_char((to_date(((cit85.juchdate)::CHARACTER VARYING)::TEXT, ('YYYYMMDD'::CHARACTER VARYING)::TEXT))::TIMESTAMP without TIME zone, ('D'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING
        ELSE NULL::CHARACTER VARYING
        END AS juchyoubicode,
    tm40_shuka.fy AS shukafy,
    tm40_shuka.nendo AS shukanendo,
    tm40_shuka.fh AS shukafh,
    tm40_shuka.hanki AS shukahanki,
    tm40_shuka.qt AS shukaqt,
    tm40_shuka.shihanki AS shukashihanki,
    CASE 
        WHEN (length(((cit85.shukadate)::CHARACTER VARYING)::TEXT) = 8)
            THEN (
                    "substring" (
                        ((cit85.shukadate)::CHARACTER VARYING)::TEXT,
                        1,
                        4
                        )
                    )::CHARACTER VARYING
        ELSE NULL::CHARACTER VARYING
        END AS shukayear,
    CASE 
        WHEN (length(((cit85.shukadate)::CHARACTER VARYING)::TEXT) = 8)
            THEN (
                    "substring" (
                        ((cit85.shukadate)::CHARACTER VARYING)::TEXT,
                        5,
                        2
                        )
                    )::CHARACTER VARYING
        ELSE NULL::CHARACTER VARYING
        END AS shukamonth,
    CASE 
        WHEN (length(((cit85.shukadate)::CHARACTER VARYING)::TEXT) = 8)
            THEN cit85.shukadate
        ELSE ((99991231)::NUMERIC)::NUMERIC(18, 0)
        END AS shukadate,
    CASE 
        WHEN (length(((cit85.shukadate)::CHARACTER VARYING)::TEXT) = 8)
            THEN (to_char((to_date(((cit85.shukadate)::CHARACTER VARYING)::TEXT, ('YYYYMMDD'::CHARACTER VARYING)::TEXT))::TIMESTAMP without TIME zone, ('D'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING
        ELSE NULL::CHARACTER VARYING
        END AS shukayoubicode,
    cit85.juchkbn,
    COALESCE(tm67.name, 'その他'::CHARACTER VARYING) AS juchkbnname,
    (COALESCE(tm67.cname, ('その他'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS juchkbncname,
    cit85.torikeikbn,
    COALESCE(tm66.name, 'その他'::CHARACTER VARYING) AS torikeikbnname,
    (COALESCE(tm66.cname, ('その他'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS torikeikbncname,
    '---' AS kaisha,
    'その他' AS kaishaname,
    'その他' AS kaishacname,
    cit85.tokuicode,
    COALESCE(cim02.tokuiname, 'その他'::CHARACTER VARYING) AS tokuiname,
    (
        CASE 
            WHEN (
                    ((cim02.tokuiname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                    OR (
                        (cim02.tokuiname IS NULL)
                        AND (NULL IS NULL)
                        )
                    )
                THEN ('その他'::CHARACTER VARYING)::TEXT
            ELSE (((cim02.tokuicode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim02.tokuiname)::TEXT)
            END
        )::CHARACTER VARYING AS tokuicname,
    CASE 
        WHEN ((cit85.torikeikbn)::TEXT <> ('02'::CHARACTER VARYING)::TEXT)
            THEN '10'::CHARACTER VARYING
        ELSE CASE 
                WHEN ((cit85.shokuikibunrui)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                    THEN '05'::CHARACTER VARYING
                WHEN ((cit85.shokuikibunrui)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                    THEN '06'::CHARACTER VARYING
                WHEN ((cit85.shokuikibunrui)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                    THEN '07'::CHARACTER VARYING
                ELSE CASE 
                        WHEN (
                                "substring" (
                                    (cit85.tokuicode)::TEXT,
                                    2,
                                    9
                                    ) <= ('000499999'::CHARACTER VARYING)::TEXT
                                )
                            THEN '04'::CHARACTER VARYING
                        WHEN (
                                (
                                    "substring" (
                                        (cit85.tokuicode)::TEXT,
                                        2,
                                        9
                                        ) >= ('000500001'::CHARACTER VARYING)::TEXT
                                    )
                                AND (
                                    "substring" (
                                        (cit85.tokuicode)::TEXT,
                                        2,
                                        9
                                        ) <= ('000599999'::CHARACTER VARYING)::TEXT
                                    )
                                )
                            THEN '05'::CHARACTER VARYING
                        WHEN (
                                (
                                    "substring" (
                                        (cit85.tokuicode)::TEXT,
                                        2,
                                        9
                                        ) >= ('000600000'::CHARACTER VARYING)::TEXT
                                    )
                                AND (
                                    "substring" (
                                        (cit85.tokuicode)::TEXT,
                                        2,
                                        9
                                        ) <= ('000799999'::CHARACTER VARYING)::TEXT
                                    )
                                )
                            THEN '09'::CHARACTER VARYING
                        WHEN (
                                (
                                    "substring" (
                                        (cit85.tokuicode)::TEXT,
                                        2,
                                        9
                                        ) >= ('000800001'::CHARACTER VARYING)::TEXT
                                    )
                                AND (
                                    "substring" (
                                        (cit85.tokuicode)::TEXT,
                                        2,
                                        9
                                        ) <= ('000899999'::CHARACTER VARYING)::TEXT
                                    )
                                )
                            THEN '04'::CHARACTER VARYING
                        WHEN (
                                "substring" (
                                    (cit85.tokuicode)::TEXT,
                                    2,
                                    9
                                    ) >= ('000900000'::CHARACTER VARYING)::TEXT
                                )
                            THEN '09'::CHARACTER VARYING
                        ELSE ''::CHARACTER VARYING
                        END
                END
        END AS channel,
    CASE 
        WHEN ((cit85.torikeikbn)::TEXT <> ('02'::CHARACTER VARYING)::TEXT)
            THEN '10 : 百貨店'::CHARACTER VARYING
        ELSE CASE 
                WHEN ((cit85.shokuikibunrui)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                    THEN '05 : 職域（特販）'::CHARACTER VARYING
                WHEN ((cit85.shokuikibunrui)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                    THEN '06 : 職域（代理店）'::CHARACTER VARYING
                WHEN ((cit85.shokuikibunrui)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                    THEN '07 : 職域（販売会）'::CHARACTER VARYING
                ELSE CASE 
                        WHEN (
                                "substring" (
                                    (cit85.tokuicode)::TEXT,
                                    2,
                                    9
                                    ) <= ('000499999'::CHARACTER VARYING)::TEXT
                                )
                            THEN '04 : 代理店'::CHARACTER VARYING
                        WHEN (
                                (
                                    "substring" (
                                        (cit85.tokuicode)::TEXT,
                                        2,
                                        9
                                        ) >= ('000500001'::CHARACTER VARYING)::TEXT
                                    )
                                AND (
                                    "substring" (
                                        (cit85.tokuicode)::TEXT,
                                        2,
                                        9
                                        ) <= ('000599999'::CHARACTER VARYING)::TEXT
                                    )
                                )
                            THEN '05 : 職域（特販）'::CHARACTER VARYING
                        WHEN (
                                (
                                    "substring" (
                                        (cit85.tokuicode)::TEXT,
                                        2,
                                        9
                                        ) >= ('000600000'::CHARACTER VARYING)::TEXT
                                    )
                                AND (
                                    "substring" (
                                        (cit85.tokuicode)::TEXT,
                                        2,
                                        9
                                        ) <= ('000799999'::CHARACTER VARYING)::TEXT
                                    )
                                )
                            THEN '09 : QVC 他'::CHARACTER VARYING
                        WHEN (
                                (
                                    "substring" (
                                        (cit85.tokuicode)::TEXT,
                                        2,
                                        9
                                        ) >= ('000800001'::CHARACTER VARYING)::TEXT
                                    )
                                AND (
                                    "substring" (
                                        (cit85.tokuicode)::TEXT,
                                        2,
                                        9
                                        ) <= ('000899999'::CHARACTER VARYING)::TEXT
                                    )
                                )
                            THEN '04 : 代理店'::CHARACTER VARYING
                        WHEN (
                                "substring" (
                                    (cit85.tokuicode)::TEXT,
                                    2,
                                    9
                                    ) >= ('000900000'::CHARACTER VARYING)::TEXT
                                )
                            THEN '09 : QVC 他'::CHARACTER VARYING
                        ELSE ''::CHARACTER VARYING
                        END
                END
        END AS channelcname,
    cit85.tokuicode AS channeldcode,
    COALESCE(cim02.tokuiname, 'その他'::CHARACTER VARYING) AS channeldname,
    cit86.itemcode,
    item.itemname,
    (item.itemcname)::CHARACTER VARYING AS itemcname,
    item.chubuncode AS itemchucode,
    item.chubunname AS itemchuname,
    (item.chubuncname)::CHARACTER VARYING AS itemchucname,
    item.daibuncode AS itemdaicode,
    item.daibunname AS itemdainame,
    (item.daibuncname)::CHARACTER VARYING AS itemdaicname,
    item.daidaibuncode AS itemdaidaicode,
    item.daidaibunname AS itemdaidainame,
    (item.daidaibuncname)::CHARACTER VARYING AS itemdaidaicname,
    item.tanka,
    cit86.suryo,
    CASE 
        WHEN (
                (
                    "substring" (
                        (cit85.juchkbn)::TEXT,
                        1,
                        1
                        ) = ('9'::CHARACTER VARYING)::TEXT
                    )
                OR (
                    (
                        "substring" (
                            (cit85.juchkbn)::TEXT,
                            1,
                            1
                            ) IS NULL
                        )
                    AND ('9' IS NULL)
                    )
                )
            THEN ((0)::NUMERIC)::NUMERIC(18, 0)
        ELSE cit86.meisainukikingaku
        END AS meisainukikingaku,
    CASE 
        WHEN (
                (
                    "substring" (
                        (cit85.juchkbn)::TEXT,
                        1,
                        1
                        ) = ('9'::CHARACTER VARYING)::TEXT
                    )
                OR (
                    (
                        "substring" (
                            (cit85.juchkbn)::TEXT,
                            1,
                            1
                            ) IS NULL
                        )
                    AND ('9' IS NULL)
                    )
                )
            THEN cit86.henusu
        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
        END AS hensu,
    CASE 
        WHEN (
                (
                    "substring" (
                        (cit85.juchkbn)::TEXT,
                        1,
                        1
                        ) = ('9'::CHARACTER VARYING)::TEXT
                    )
                OR (
                    (
                        "substring" (
                            (cit85.juchkbn)::TEXT,
                            1,
                            1
                            ) IS NULL
                        )
                    AND ('9' IS NULL)
                    )
                )
            THEN cit86.meisainukikingaku
        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
        END AS henkingaku,
    (
        cit86.suryo + CASE 
            WHEN (
                    (
                        "substring" (
                            (cit85.juchkbn)::TEXT,
                            1,
                            1
                            ) = ('9'::CHARACTER VARYING)::TEXT
                        )
                    OR (
                        (
                            "substring" (
                                (cit85.juchkbn)::TEXT,
                                1,
                                1
                                ) IS NULL
                            )
                        AND ('9' IS NULL)
                        )
                    )
                THEN cit86.henusu
            ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
            END
        ) AS urisuryo,
    cit86.meisainukikingaku AS urikingaku,
    CASE 
        WHEN (length(((cit85.updatedate)::CHARACTER VARYING)::TEXT) = 8)
            THEN cit85.updatedate
        ELSE ((99991231)::NUMERIC)::NUMERIC(18, 0)
        END AS updatedate,
    cit85.henreasoncode,
    cit85.henreasonname,
    ((((cit85.henreasoncode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cit85.henreasonname)::TEXT))::CHARACTER VARYING AS henreasoncname,
    zattr.bumon1_add_attr1,
    zattr.bumon1_add_attr2,
    zattr.bumon1_add_attr3,
    zattr.bumon1_add_attr4,
    zattr.bumon1_add_attr5,
    zattr.bumon1_add_attr6,
    zattr.bumon1_add_attr7,
    zattr.bumon1_add_attr8,
    zattr.bumon1_add_attr9,
    zattr.bumon1_add_attr10,
    zattr.bumon2_add_attr1,
    zattr.bumon2_add_attr2,
    zattr.bumon2_add_attr3,
    zattr.bumon2_add_attr4,
    zattr.bumon2_add_attr5,
    zattr.bumon2_add_attr6,
    zattr.bumon2_add_attr7,
    zattr.bumon2_add_attr8,
    zattr.bumon2_add_attr9,
    zattr.bumon2_add_attr10,
    zattr.bumon3_add_attr1,
    zattr.bumon3_add_attr2,
    zattr.bumon3_add_attr3,
    zattr.bumon3_add_attr4,
    zattr.bumon3_add_attr5,
    zattr.bumon3_add_attr6,
    zattr.bumon3_add_attr7,
    zattr.bumon3_add_attr8,
    zattr.bumon3_add_attr9,
    zattr.bumon3_add_attr10,
    zattr.bumon4_add_attr1,
    zattr.bumon4_add_attr2,
    zattr.bumon4_add_attr3,
    zattr.bumon4_add_attr4,
    zattr.bumon4_add_attr5,
    zattr.bumon4_add_attr6,
    zattr.bumon4_add_attr7,
    zattr.bumon4_add_attr8,
    zattr.bumon4_add_attr9,
    zattr.bumon4_add_attr10,
    zattr.bumon5_add_attr1,
    zattr.bumon5_add_attr2,
    zattr.bumon5_add_attr3,
    zattr.bumon5_add_attr4,
    zattr.bumon5_add_attr5,
    zattr.bumon5_add_attr6,
    zattr.bumon5_add_attr7,
    zattr.bumon5_add_attr8,
    zattr.bumon5_add_attr9,
    zattr.bumon5_add_attr10,
    zattr.bumon6_add_attr1,
    zattr.bumon6_add_attr2,
    zattr.bumon6_add_attr3,
    zattr.bumon6_add_attr4,
    zattr.bumon6_add_attr5,
    zattr.bumon6_add_attr6,
    zattr.bumon6_add_attr7,
    zattr.bumon6_add_attr8,
    zattr.bumon6_add_attr9,
    zattr.bumon6_add_attr10,
    zattr.bumon6_add_attr11,
    zattr.bumon6_add_attr12,
    zattr.bumon6_add_attr13,
    zattr.bumon6_add_attr14,
    zattr.bumon6_add_attr15,
    zattr.bumon6_add_attr16,
    zattr.bumon6_add_attr17,
    zattr.bumon6_add_attr18,
    zattr.bumon6_add_attr19,
    zattr.bumon6_add_attr20
FROM (
    (
        (
            (
                (
                    (
                        (
                            (
                                cit85osalh cit85 JOIN cit86osalm cit86 ON (((cit85.ourino)::TEXT = (cit86.ourino)::TEXT))
                                ) LEFT JOIN (
                                SELECT cim03.itemcode,
                                    COALESCE(cim03.itemname, 'その他'::CHARACTER VARYING) AS itemname,
                                    COALESCE(cim03.tanka, ((0)::NUMERIC)::NUMERIC(18, 0)) AS tanka,
                                    CASE 
                                        WHEN (
                                                ((cim03.itemname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (cim03.itemname IS NULL)
                                                    AND (NULL IS NULL)
                                                    )
                                                )
                                            THEN ('その他'::CHARACTER VARYING)::TEXT
                                        ELSE (((cim03.itemcode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim03.itemname)::TEXT)
                                        END AS itemcname,
                                    cim03.bunruicode3 AS chubuncode,
                                    COALESCE(cim24_chu.itbunname, 'その他'::CHARACTER VARYING) AS chubunname,
                                    CASE 
                                        WHEN (
                                                ((cim24_chu.itbunname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (cim24_chu.itbunname IS NULL)
                                                    AND (NULL IS NULL)
                                                    )
                                                )
                                            THEN ('その他'::CHARACTER VARYING)::TEXT
                                        ELSE (((cim03.bunruicode3)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim24_chu.itbunname)::TEXT)
                                        END AS chubuncname,
                                    cim03.bunruicode2 AS daibuncode,
                                    COALESCE(cim24_dai.itbunname, 'その他'::CHARACTER VARYING) AS daibunname,
                                    CASE 
                                        WHEN (
                                                ((cim24_dai.itbunname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (cim24_dai.itbunname IS NULL)
                                                    AND (NULL IS NULL)
                                                    )
                                                )
                                            THEN ('その他'::CHARACTER VARYING)::TEXT
                                        ELSE (((cim03.bunruicode2)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim24_dai.itbunname)::TEXT)
                                        END AS daibuncname,
                                    cim03.bunruicode5 AS daidaibuncode,
                                    COALESCE(cim24_daidai.itbunname, 'その他'::CHARACTER VARYING) AS daidaibunname,
                                    CASE 
                                        WHEN (
                                                ((cim24_daidai.itbunname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (cim24_daidai.itbunname IS NULL)
                                                    AND (NULL IS NULL)
                                                    )
                                                )
                                            THEN ('その他'::CHARACTER VARYING)::TEXT
                                        ELSE (((cim03.bunruicode5)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim24_daidai.itbunname)::TEXT)
                                        END AS daidaibuncname
                                FROM (
                                    (
                                        (
                                            (
                                                cim03item_zaiko cim03 LEFT JOIN cim24itbun cim24_syo ON (
                                                        (
                                                            ((cim03.bunruicode4)::TEXT = (cim24_syo.itbuncode)::TEXT)
                                                            AND ((cim24_syo.itbunshcode)::TEXT = ('4'::CHARACTER VARYING)::TEXT)
                                                            )
                                                        )
                                                ) LEFT JOIN cim24itbun cim24_chu ON (
                                                    (
                                                        ((cim03.bunruicode3)::TEXT = (cim24_chu.itbuncode)::TEXT)
                                                        AND ((cim24_chu.itbunshcode)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    )
                                            ) LEFT JOIN cim24itbun cim24_dai ON (
                                                (
                                                    ((cim03.bunruicode2)::TEXT = (cim24_dai.itbuncode)::TEXT)
                                                    AND ((cim24_dai.itbunshcode)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                                                    )
                                                )
                                        ) LEFT JOIN cim24itbun cim24_daidai ON (
                                            (
                                                ((cim03.bunruicode5)::TEXT = (cim24_daidai.itbuncode)::TEXT)
                                                AND ((cim24_daidai.itbunshcode)::TEXT = ('5'::CHARACTER VARYING)::TEXT)
                                                )
                                            )
                                    )
                                ) item ON (((cit86.itemcode)::TEXT = (item.itemcode)::TEXT))
                            ) LEFT JOIN tm40shihanki tm40_juch ON (
                                (
                                    (cit85.juchdate >= tm40_juch.fromdate)
                                    AND (cit85.juchdate <= tm40_juch.todate)
                                    )
                                )
                        ) LEFT JOIN tm40shihanki tm40_shuka ON (
                            (
                                (cit85.shukadate >= tm40_shuka.fromdate)
                                AND (cit85.shukadate <= tm40_shuka.todate)
                                )
                            )
                    ) LEFT JOIN tm67juch_nm tm67 ON (((cit85.juchkbn)::TEXT = (tm67.code)::TEXT))
                ) LEFT JOIN tm66torikei_nm tm66 ON (((cit85.torikeikbn)::TEXT = (tm66.code)::TEXT))
            ) LEFT JOIN cim02tokui cim02 ON (((cit85.tokuicode)::TEXT = (cim02.tokuicode)::TEXT))
        ) LEFT JOIN zaiko_shohin_attr zattr ON (((cit86.itemcode)::TEXT = (zattr.shohin_code)::TEXT))
    )
