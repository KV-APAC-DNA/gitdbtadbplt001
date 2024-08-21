{{
    config
    (
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
    )
}}
WITH CIT85OSALH_KKNG
AS (
    SELECT *
    FROM {{ source('jpdcledw_integration','cit85osalh_kkng') }}
    ),
ZCMMNTORISK
AS (
    SELECT *
    FROM {{ source('jpdclitg_integration','zcmmntorisk') }}
    ),
HANYO_ATTR
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__hanyo_attr')}}
    ),
SSMTHSALHEDDA
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__ssmthsalhedda')}}
    ),
SSMTHSALHEPHEDDA
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__ssmthsalhephedda')}}
    ),
SSCMNRIYUUKANRI
AS (
    SELECT *
    FROM {{ source('jpdclitg_integration','sscmnriyuukanri') }}
    ),
ZCMMNDCLTOKUISK
AS (
    SELECT *
    FROM {{ source('jpdclitg_integration','zcmmndcltokuisk') }}
    ),
ZCMMNSOSHITAIK
AS (
    SELECT *
    FROM {{ source('jpdclitg_integration','zcmmnsoshitaik') }}
    ),
t1
AS (
    SELECT CIT85OSALH_KKNG.OURINO AS OURINO,
        CIT85OSALH_KKNG.JUCHKBN AS JUCHKBN,
        CIT85OSALH_KKNG.TORIKEIKBN AS TORIKEIKBN,
        CIT85OSALH_KKNG.JUCHDATE AS JUCHDATE,
        CIT85OSALH_KKNG.SHUKADATE AS SHUKADATE,
        CIT85OSALH_KKNG.TOKUICODE AS TOKUICODE,
        CIT85OSALH_KKNG.SHOKEI AS SHOKEI,
        CIT85OSALH_KKNG.TAX AS TAX,
        CIT85OSALH_KKNG.SOGOKEI AS SOGOKEI,
        CIT85OSALH_KKNG.SENDENNO AS SENDENNO,
        CIT85OSALH_KKNG.UPDATEDATE AS UPDATEDATE,
        CIT85OSALH_KKNG.KKNG_KBN AS KKNG_KBN,
        CIT85OSALH_KKNG.TUKA_CD AS TUKA_CD,
        CIT85OSALH_KKNG.SHOKEI_TUKA AS SHOKEI_TUKA,
        CIT85OSALH_KKNG.TAX_TUKA AS TAX_TUKA,
        CIT85OSALH_KKNG.SOGOKEI_TUKA AS SOGOKEI_TUKA,
        1 AS KAKOKBN,
        '00000' AS TENPOCODE,
        NVL(TORI.bunr_val_cd2, '0') AS TENPOBUNRUI,
        NVL(TORI.bunr_val_cd4, '0') AS SHOKUIKIBUNRUI,
        0 AS CANCELFLG,
        '00000000' AS KOKYANO,
        '000' AS HENREASONCODE,
        ' ' AS HENREASONNAME,
        CIT85OSALH_KKNG.OURINO AS DISPOURINO,
        ' ' AS TOKUINAME_ASPAC,
        ' ' AS SKYSK_NAME,
        ' ' AS SKYSK_CD,
        ' ' AS JUCH_BKO,
        CIT85OSALH_KKNG.TOKUINM AS TOKUINAME_KAIGAI,
        '0000000000' AS TOUROKU_USER,
        1 AS MARKER,
        9 AS FSKBN,
        0 AS NUKIKINGAKU,
        '0' AS PROCESSTYPE_CD,
        '' AS HENPINNO,
        '0000000000' AS KOKYANO2,
        '' AS BMN_HYOUJI_CD,
        '' AS BMN_NMS,
        NULL AS ROWID1,
        NULL AS ROWID2,
        NULL AS ROWID3,
        NULL AS ROWID4,
        NULL AS ROWID5,
        NULL AS ROWID6,
        NULL AS ROWID7,
        NULL AS ROWID8,
        NULL AS ROWID9,
        NULL AS ROWID10
    FROM CIT85OSALH_KKNG,
        ZCMMNTORISK TORI,
        HANYO_ATTR KAISYA
    WHERE CIT85OSALH_KKNG.JUCHKBN IN ('90', '91', '92')
        AND KAISYA.KBNMEI = 'KAISYA'
        AND (
            TORI.kaisha_cd = KAISYA.ATTR1
            OR TORI.kaisha_cd IS NULL
            )
        AND CIT85OSALH_KKNG.TOKUICODE = TORI.torisk_cd(+)
    ),
t2
AS (
    SELECT H.sal_no AS OURINO,
        '90' AS JUCHKBN,
        DECODE(TORI.bunr_val_cd2, '1', '04', '2', '05', '3', '03', '4', '06', '5', '03', '02') AS TORIKEIKBN,
        0 AS JUCHDATE,
        NVL(H.sal_keij_dt, 0) AS SHUKADATE,
        NVL(H.tokuisk_cd, '0000000000') AS TOKUICODE,
        NVL(H.shoumi_sal_sum_kin, 0) AS SHOKEI,
        NVL(H.den_naf_sal_sz_kin, 0) AS TAX,
        NVL(H.den_naf_sal_kin, 0) AS SOGOKEI,
        NVL(HH.ringisho_no, '000000') AS SENDENNO,
        NVL(CAST(SUBSTRING(HH.koushin_date, 1, 8) AS NUMERIC), 0) AS UPDATEDATE,
        NVL(DECODE(TORI.BUNR_VAL_CD3, '40', '2', '1'), '1') AS KKNG_KBN,
        NVL(H.ksai_tuka_cd, '000') AS TUKA_CD,
        NVL(H.shoumi_sal_sum_kin_ksai, 0) AS SHOKEI_TUKA,
        NVL(H.den_naf_sal_sz_kin_ksai, 0) AS TAX_TUKA,
        NVL(H.den_naf_sal_kin_ksai, 0) AS SOGOKEI_TUKA,
        0 AS KAKOKBN,
        NVL(DTOKUI.DCLTENPO_SYSTEM_TENPO_CD, '00000') AS TENPOCODE,
        NVL(TORI.bunr_val_cd2, '0') AS TENPOBUNRUI,
        NVL(TORI.bunr_val_cd4, '0') AS SHOKUIKIBUNRUI,
        NVL(H.can_flg, 0) AS CANCELFLG,
        NVL(H.tokuisk_cd, '00000000') AS KOKYANO,
        NVL(HH.sal_hep_riyuu_cd, '000') AS HENREASONCODE,
        NVL(RIYU.riyuu_naiy, ' ') AS HENREASONNAME,
        MOTOH.sal_no AS DISPOURINO,
        NVL(TORI.torisk_nms1, ' ') AS TOKUINAME_ASPAC,
        NVL(TORI.torisk_nmr, ' ') AS SKYSK_NAME,
        NVL(H.skysk_cd, ' ') AS SKYSK_CD,
        ' ' AS JUCH_BKO,
        ' ' AS TOKUINAME_KAIGAI,
        NVL(H.touroku_user, '0000000000') AS TOUROKU_USER,
        2 AS MARKER,
        CASE 
            WHEN H.tokuisk_cd = 'FS00000001'
                AND FS.ATTR3 = '卸FS'
                THEN 0
            WHEN H.tokuisk_cd = 'FS00000001'
                AND FS.ATTR3 = '店舗FS'
                THEN 1
            ELSE 9
            END AS FSKBN,
        NVL(H.den_nf_shm_sal_kin, 0) AS NUKIKINGAKU,
        H.PROCESSTYPE_CD AS PROCESSTYPE_CD,
        HH.SAL_HEP_NO AS HENPINNO,
        '0000000000' AS KOKYANO2,
        NVL(ST.BMN_HYOUJI_CD, '') AS BMN_HYOUJI_CD,
        NVL(ST.BMN_NMS, '') AS BMN_NMS,
        NULL AS ROWID1,
        NULL AS ROWID2,
        NULL AS ROWID3,
        NULL AS ROWID4,
        NULL AS ROWID5,
        NULL AS ROWID6,
        NULL AS ROWID7,
        NULL AS ROWID8,
        NULL AS ROWID9,
        NULL AS ROWID10
    FROM SSMTHSALHEDDA H,
        SSMTHSALHEPHEDDA HH,
        SSMTHSALHEDDA MOTOH,
        ZCMMNTORISK TORI,
        SSCMNRIYUUKANRI RIYU,
        ZCMMNDCLTOKUISK DTOKUI,
        HANYO_ATTR DAILY,
        HANYO_ATTR KAISYA,
        (
            SELECT *
            FROM HANYO_ATTR
            WHERE KBNMEI = 'FAMILYSALE'
            ) FS,
        ZCMMNSOSHITAIK ST
    WHERE SUBSTRING(H.touroku_date, 1, 8) >= DAILY.ATTR3
        AND DAILY.KBNMEI = 'DAILYFROM'
        AND H.kaisha_cd = KAISYA.ATTR1
        AND KAISYA.KBNMEI = 'KAISYA'
        AND ST.BMN_HYOUJI_CD = FS.ATTR1(+)
        AND H.rend_mt_den_shubt = '109'
        AND H.tokuisk_cd <> 'CINEXT0001'
        AND NVL(H.CAN_FLG, '0') = '0'
        AND (
            HH.tokuisk_cd <> 'BI00000001'
            AND HH.tokuisk_cd <> 'BI00000002'
            AND HH.tokuisk_cd <> 'BI00000003'
            )
        AND H.kaisha_cd = HH.kaisha_cd
        AND H.rend_mt_den_no = HH.sal_hep_no
        AND HH.kaisha_cd = MOTOH.kaisha_cd(+)
        AND HH.rend_mt_den_no = MOTOH.sal_no(+)
        AND NVL(HH.CAN_FLG, '0') = '0'
        AND HH.kaisha_cd = TORI.kaisha_cd(+)
        AND HH.tokuisk_cd = TORI.torisk_cd(+)
        AND H.kaisha_cd = DTOKUI.kaisha_cd(+)
        AND H.tokuisk_cd = DTOKUI.tokuisk_cd(+)
        AND HH.kaisha_cd = RIYU.kaisha_cd(+)
        AND HH.sal_hep_riyuu_cd = RIYU.riyuu_cd(+)
        AND RIYU.riyuu_shur_kbn(+) = '40'
        AND HH.KAISHA_CD = ST.KAISHA_CD(+)
        AND HH.SAL_HEP_BMN_NAIBU_NO = ST.BMN_NAIBUKANRI_NO(+)
        AND NOT (
            HH.tokuisk_cd = 'FS00000001'
            AND ST.BMN_HYOUJI_CD NOT IN (
                SELECT ATTR1
                FROM HANYO_ATTR
                WHERE KBNMEI = 'FAMILYSALE'
                    AND (
                        ATTR3 = '卸FS'
                        OR ATTR3 = '店舗FS'
                        )
                )
            )
    ),
union_of
AS (
    SELECT *
    FROM t1
    
    UNION ALL
    
    SELECT *
    FROM t2
    ),
final 
AS
(
    SELECT OURINO::VARCHAR(18)  AS OURINO,
        JUCHKBN::VARCHAR(3) AS JUCHKBN,
        TORIKEIKBN::VARCHAR(3) AS TORIKEIKBN,
        JUCHDATE::NUMBER(18,0) AS JUCHDATE,
        SHUKADATE::NUMBER(18,0) AS SHUKADATE,
        TOKUICODE::VARCHAR(22) AS TOKUICODE,
        SHOKEI::NUMBER(18,0) AS SHOKEI,
        TAX::NUMBER(18,0) AS TAX,
        SOGOKEI::NUMBER(18,0) AS SOGOKEI,
        SENDENNO::VARCHAR(120) AS SENDENNO,
        UPDATEDATE::NUMBER(18,0) AS UPDATEDATE,
        KKNG_KBN::VARCHAR(2) AS KKNG_KBN,
        TUKA_CD::VARCHAR(15) AS TUKA_CD,
        SHOKEI_TUKA::NUMBER(18,0) AS SHOKEI_TUKA,
        TAX_TUKA::NUMBER(18,0) AS TAX_TUKA,
        SOGOKEI_TUKA::NUMBER(18,0) AS SOGOKEI_TUKA,
        KAKOKBN::NUMBER(18,0) AS KAKOKBN,
        TENPOCODE::VARCHAR(7) AS TENPOCODE,
        TENPOBUNRUI::VARCHAR(6) AS TENPOBUNRUI,
        SHOKUIKIBUNRUI::VARCHAR(6) AS SHOKUIKIBUNRUI,
        CANCELFLG::NUMBER(18,0) AS CANCELFLG,
        KOKYANO::VARCHAR(22) AS KOKYANO,
        HENREASONCODE::VARCHAR(6) AS HENREASONCODE,
        HENREASONNAME::VARCHAR(120) AS HENREASONNAME,
        DISPOURINO::VARCHAR(18) AS DISPOURINO,
        TOKUINAME_ASPAC::VARCHAR(90) AS TOKUINAME_ASPAC,
        SKYSK_NAME::VARCHAR(120) AS SKYSK_NAME,
        SKYSK_CD::VARCHAR(22) AS SKYSK_CD,
        JUCH_BKO::VARCHAR(1) AS JUCH_BKO,
        TOKUINAME_KAIGAI::VARCHAR(90) AS TOKUINAME_KAIGAI,
        TOUROKU_USER::VARCHAR(22) AS TOUROKU_USER,
        MARKER::NUMBER(18,0) AS MARKER,
        FSKBN::NUMBER(18,0) AS FSKBN,
        NUKIKINGAKU::NUMBER(18,0) AS NUKIKINGAKU,
        PROCESSTYPE_CD::VARCHAR(15) AS PROCESSTYPE_CD,
        HENPINNO::VARCHAR(18) AS HENPINNO,
        KOKYANO2::VARCHAR(10) AS KOKYANO2,
        BMN_HYOUJI_CD::VARCHAR(15) AS BMN_HYOUJI_CD,
        BMN_NMS::VARCHAR(240) AS BMN_NMS,
        ROWID1::VARCHAR(1) AS ROWID1,
        ROWID2::VARCHAR(1) AS ROWID2,
        ROWID3::VARCHAR(1) AS ROWID3,
        ROWID4::VARCHAR(1) AS ROWID4,
        ROWID5::VARCHAR(1) AS ROWID5,
        ROWID6::VARCHAR(1) AS ROWID6,
        ROWID7::VARCHAR(1) AS ROWID7,
        ROWID8::VARCHAR(1) AS ROWID8,
        ROWID9::VARCHAR(1) AS ROWID9,
        ROWID10::VARCHAR(1) AS ROWID10,
        CURRENT_TIMESTAMP()::TIMESTAMP_TZ(9)  AS INSERTED_DATE,
        'ETL_Batch'::VARCHAR(100)  AS INSERTED_BY,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9)  AS UPDATED_DATE,
        NULL::VARCHAR(100) AS UPDATED_BY
    FROM UNION_OF)
SELECT *
FROM

final
