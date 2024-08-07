WITH hanyo_attr AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.hanyo_attr
),

zaiko_shohin_attr AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.zaiko_shohin_attr
),

tm67juch_nm AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.tm67juch_nm
),

cit86osalm_kaigai AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.cit86osalm_kaigai
),

cit85osalh_kaigai AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.cit85osalh_kaigai
),

cim03item_zaiko AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.cim03item_zaiko
),

cim02tokui AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.cim02tokui
),

ssmthsalhephedda AS
(
    SELECT *
    FROM dev_dna_core.snapjpdclitg_integration.ssmthsalhephedda
),

final AS
( 
    SELECT cit85osalh.ourino,
        cit85osalh.tokuicode,
        cit85osalh.tokuiname_aspac AS tokuiname,
        cim02.tokuiname_ryaku,
        cim02.tokuiname_kana,
        cit85osalh.juchkbn,
        cim03.itemkbn,
        cim03.itemkbnname,
        cit85osalh.sendenno AS senyodenno,
        cit86osalm.itemcode,
        cim03.jancode,
        CAST(NULL AS VARCHAR) AS tokuiitemcode,
        cim03.itemname,
        COALESCE(CASE 
                WHEN cit85osalh.juchkbn = '0' THEN cit86osalm.suryo
                WHEN cit85osalh.juchkbn = '90' THEN cit86osalm.hensu
                ELSE NULL
                END, 0) AS suryo,
        cit86osalm.tanka,
        cit86osalm.meisainukikingaku AS kingaku,
        cim03.tanka AS kouritanka,
        LEFT(cit86osalm.shimebi, 6) AS shimebi,
        COALESCE(hanyo.attr3, 'その他') AS sum_todofuken,
        COALESCE(hanyo.attr2, 'その他') AS tokuiname2,
        cim02.todofukennm,
        COALESCE(zaiko.bumon6_add_attr4, 'その他') AS sensyukai,
        COALESCE(zaiko.bumon6_add_attr5, 'その他') AS unity_itemname,
        COALESCE(hanyo.attr3, 'その他') AS sum_name,
        COALESCE(zaiko.bumon6_add_attr2, '0') AS hansoku_tanka,
        COALESCE(zaiko.bumon6_add_attr3, '0') AS hansoku_ext,
        COALESCE(hanyo.attr4, 'その他') AS sum_tokuiname,
        cim03.haiban_hin_cd AS haibanhinmokucode,
        CASE 
            WHEN cit85osalh.kakokbn = 1 THEN LEFT(cit86osalm.shimebi, 8)
            ELSE CASE 
                    WHEN tm67.code = '90' THEN ssmt.nyk_yti_dt
                    ELSE LEFT(cit86osalm.shimebi, 8)
                    END
            END AS nohindate,
        LEFT(cit85osalh.shukadate, 8) AS shukadate,
        cit85osalh.processtype_cd,
        CASE 
            WHEN cit85osalh.juchkbn = '0' THEN cit85osalh.juchno
            WHEN cit85osalh.juchkbn = '90' THEN cit85osalh.henpinno
            ELSE NULL
            END AS juchno,
        cit85osalh.torikeikbn,
        hanyo.attr5 AS tokuizokuseino,
        cit85osalh.skysk_name,
        cit85osalh.skysk_cd,
        cit85osalh.juchkbn AS urikbn,
        cit85osalh.shokei,
        cit85osalh.tax,
        cit85osalh.sogokei,
        cit85osalh.juch_bko,
        cit85osalh.daihyou_shukask_cd,
        cit85osalh.daihyou_shukask_nmr,
        cit86osalm.shohzei_ritsu
    FROM (
        (
            (
                (
                    (
                        (
                            (
                                cit85osalh_kaigai cit85osalh
                                JOIN cit86osalm_kaigai cit86osalm ON cit85osalh.ourino = cit86osalm.ourino
                            )
                            JOIN cim02tokui cim02 ON cim02.tokuicode = cit85osalh.tokuicode
                        )
                        JOIN cim03item_zaiko cim03 ON cim03.itemcode = cit86osalm.itemcode
                    )
                    LEFT JOIN hanyo_attr hanyo ON hanyo.kbnmei = 'TOKUI' AND hanyo.attr1 = cit85osalh.tokuicode
                )
                LEFT JOIN zaiko_shohin_attr zaiko ON zaiko.shohin_code = cit86osalm.itemcode
            )
            LEFT JOIN tm67juch_nm tm67 ON cit85osalh.juchkbn = tm67.code
        )
        LEFT JOIN ssmthsalhephedda ssmt ON cit85osalh.henpinno = ssmt.sal_hep_no AND ssmt.kaisha_cd = 'DCL'
    )
    WHERE cit85osalh.kakokbn = 0
      AND cit86osalm.kakokbn = 0
      AND cit85osalh.torikeikbn = '02'
)

SELECT *
FROM final