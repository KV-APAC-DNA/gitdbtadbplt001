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
SSMTHJUCHHEDDA
AS (
    SELECT *
    FROM {{source('jpdclitg_integration', 'ssmthjuchhedda')}}
    ),
SSMTHDCLTHANJUCHHEDDA
AS (
    SELECT *
    FROM {{ source('jpdclitg_integration','ssmthdclthanjuchhedda') }}
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
TBECORDER
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__tbecorder')}}
    ),
C_TBECKESAI
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__c_tbeckesai')}}
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
        NVL(ZCMMNTORISK.bunr_val_cd2, '0') AS TENPOBUNRUI,
        NVL(ZCMMNTORISK.bunr_val_cd4, '0') AS SHOKUIKIBUNRUI,
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
        '' AS JUCHNO,
        '' AS DAIHYOU_SHUKASK_CD,
        '' AS DAIHYOU_SHUKASK_NMR,
        '0000000000' AS KOKYANO2,
        '' AS BMN_HYOUJI_CD,
        '' AS BMN_NMS,
        NULL AS ROWID1,
        NULL AS ROWID2,
        NULL AS ROWID3,
        NULL AS ROWID4,
        NULL AS ROWID5,
        NULL AS ROWID6,
        NULL AS ROWID7
    FROM CIT85OSALH_KKNG,
        ZCMMNTORISK,
        HANYO_ATTR KAISYA
    WHERE CIT85OSALH_KKNG.JUCHKBN NOT IN ('90', '91', '92')
        AND KAISYA.KBNMEI = 'KAISYA'
        AND (
            ZCMMNTORISK.kaisha_cd = KAISYA.ATTR1
            OR ZCMMNTORISK.kaisha_cd IS NULL
            )
        AND CIT85OSALH_KKNG.TOKUICODE = ZCMMNTORISK.torisk_cd(+)
    ),
t2
AS (
    SELECT SSMTHSALHEDDA.sal_no AS OURINO,
    '0' AS JUCHKBN,
    DECODE(ZCMMNTORISK.bunr_val_cd2, '1', '04', '2', '05', '3', '03', '4', '06', '5', '03', '02') AS TORIKEIKBN,
    NVL(SSMTHJUCHHEDDA.juch_dt, '0') AS JUCHDATE,
    NVL(SSMTHSALHEDDA.sal_keij_dt, '0') AS SHUKADATE,
    NVL(SSMTHSALHEDDA.tokuisk_cd, '0000000000') AS TOKUICODE,
    NVL(SSMTHSALHEDDA.shoumi_sal_sum_kin, '0') AS SHOKEI,
    NVL(SSMTHSALHEDDA.sal_shohzei_sum_kin, '0') AS TAX,
    NVL(SSMTHSALHEDDA.sal_sum_kin, '0') AS SOGOKEI,
    NVL(SSMTHSALHEDDA.snpo_chumon_no, '000000') AS SENDENNO,
    NVL(TO_NUMBER(SUBSTRING(SSMTHSALHEDDA.koushin_date, 1, 8), '99999999'), '0') AS UPDATEDATE,
    NVL(DECODE(ZCMMNTORISK.BUNR_VAL_CD3, '40', '2', '1'), '1') AS KKNG_KBN,
    NVL(SSMTHSALHEDDA.ksai_tuka_cd, '000') AS TUKA_CD,
    NVL(SSMTHSALHEDDA.shoumi_sal_sum_kin_ksai, '0') AS SHOKEI_TUKA,
    NVL(SSMTHSALHEDDA.sal_sz_sum_kin_ksai, '0') AS TAX_TUKA,
    NVL(SSMTHSALHEDDA.sal_sum_kin_ksai, '0') AS SOGOKEI_TUKA,
    0 AS KAKOKBN,
    NVL(ZCMMNDCLTOKUISK.DCLTENPO_SYSTEM_TENPO_CD, '00000') AS TENPOCODE,
    NVL(ZCMMNTORISK.bunr_val_cd2, '0') AS TENPOBUNRUI,
    NVL(ZCMMNTORISK.bunr_val_cd4, '0') AS SHOKUIKIBUNRUI,
    NVL(SSMTHSALHEDDA.can_flg, '0') AS CANCELFLG,
    NVL(SSMTHSALHEDDA.tokuisk_cd, '00000000') AS KOKYANO,
    '000' AS HENREASONCODE,
    ' ' AS HENREASONNAME,
    SSMTHSALHEDDA.sal_no AS DISPOURINO,
    NVL(ZCMMNTORISK.torisk_nms1, ' ') AS TOKUINAME_ASPAC,
    NVL(ZCMMNTORISK.torisk_nmr, ' ') AS SKYSK_NAME,
    NVL(SSMTHSALHEDDA.skysk_cd, ' ') AS SKYSK_CD,
    NVL(SSMTHJUCHHEDDA.juch_bko, ' ') AS JUCH_BKO,
    ' ' AS TOKUINAME_KAIGAI,
    NVL(SSMTHSALHEDDA.touroku_user, '0000000000') AS TOUROKU_USER,
    2 AS MARKER,
    9 AS FSKBN,
    NVL(SSMTHSALHEDDA.den_nf_shm_sal_kin, '0') AS NUKIKINGAKU,
    SSMTHSALHEDDA.PROCESSTYPE_CD AS PROCESSTYPE_CD,
    SSMTHSALHEDDA.hassei_mt_den_no AS JUCHNO,
    SSMTHJUCHHEDDA.DAIHYOU_SHUKASK_CD AS DAIHYOU_SHUKASK_CD,
    SSMTHJUCHHEDDA.DAIHYOU_SHUKASK_NMR AS DAIHYOU_SHUKASK_NMR,
    '0000000000' AS KOKYANO2,
    NVL(ZCMMNSOSHITAIK.BMN_HYOUJI_CD, '') AS BMN_HYOUJI_CD,
    NVL(ZCMMNSOSHITAIK.BMN_NMS, '') AS BMN_NMS,
    NULL AS ROWID1,
    NULL AS ROWID2,
    NULL AS ROWID3,
    NULL AS ROWID4,
    NULL AS ROWID5,
    NULL AS ROWID6,
    NULL AS ROWID7 FROM SSMTHSALHEDDA,
        SSMTHJUCHHEDDA,
        ZCMMNTORISK,
        SSMTHDCLTHANJUCHHEDDA,
        ZCMMNDCLTOKUISK,
        HANYO_ATTR DAILY,
        HANYO_ATTR KAISYA,
        ZCMMNSOSHITAIK
    WHERE SUBSTRING(SSMTHSALHEDDA.touroku_date, 1, 8) >= DAILY.ATTR3
        AND DAILY.KBNMEI = 'DAILYFROM'
        AND SSMTHSALHEDDA.kaisha_cd = KAISYA.ATTR1
        AND KAISYA.KBNMEI = 'KAISYA'
        AND SSMTHSALHEDDA.rend_mt_den_shubt = '105'
        AND SSMTHSALHEDDA.kaisha_cd = SSMTHJUCHHEDDA.kaisha_cd(+)
        AND SSMTHSALHEDDA.hassei_mt_den_no = SSMTHJUCHHEDDA.juch_no(+)
        AND NVL(SSMTHSALHEDDA.CAN_FLG, '0') = '0'
        AND NVL(SSMTHJUCHHEDDA.CAN_FLG, '0') = '0'
        AND SSMTHSALHEDDA.kaisha_cd = ZCMMNTORISK.kaisha_cd(+)
        AND SSMTHSALHEDDA.tokuisk_cd = ZCMMNTORISK.torisk_cd(+)
        AND SSMTHSALHEDDA.kaisha_cd = ZCMMNDCLTOKUISK.kaisha_cd(+)
        AND SSMTHSALHEDDA.tokuisk_cd = ZCMMNDCLTOKUISK.tokuisk_cd(+)
        AND SSMTHSALHEDDA.kaisha_cd = SSMTHDCLTHANJUCHHEDDA.kaisha_cd(+)
        AND SSMTHSALHEDDA.hassei_mt_den_no = SSMTHDCLTHANJUCHHEDDA.juch_no(+)
        AND SSMTHDCLTHANJUCHHEDDA.juch_no IS NULL
        AND (
            SSMTHSALHEDDA.tokuisk_cd <> 'BI00000001'
            AND SSMTHSALHEDDA.tokuisk_cd <> 'BI00000002'
            AND SSMTHSALHEDDA.tokuisk_cd <> 'BI00000003'
            )
        AND SSMTHSALHEDDA.KAISHA_CD = ZCMMNSOSHITAIK.KAISHA_CD(+)
        AND SSMTHSALHEDDA.SAL_BMN_NAIBU_NO = ZCMMNSOSHITAIK.BMN_NAIBUKANRI_NO(+)
    ),
t3
AS (
    SELECT SSMTHSALHEDDA.sal_no AS OURINO,
        NVL(tbecorder.c_dsorderkbn, '0') AS JUCHKBN,
        DECODE(ZCMMNTORISK.bunr_val_cd2, '1', '04', '2', '05', '3', '03', '4', '06', '5', '03', '02') AS TORIKEIKBN,
        CAST(TO_CHAR(tbEcOrder.dsOrderDt, 'YYYYMMDD') AS NUMERIC) AS JUCHDATE,
        NVL(SSMTHSALHEDDA.sal_keij_dt, '0') AS SHUKADATE,
        NVL(SSMTHSALHEDDA.tokuisk_cd, '0000000000') AS TOKUICODE,
        NVL(SSMTHSALHEDDA.shoumi_sal_sum_kin, '0') AS SHOKEI,
        NVL(SSMTHSALHEDDA.sal_shohzei_sum_kin, '0') AS TAX,
        NVL(SSMTHSALHEDDA.sal_sum_kin, '0') AS SOGOKEI,
        NVL(SSMTHSALHEDDA.snpo_chumon_no, '000000') AS SENDENNO,
        NVL(CAST(SUBSTRING(SSMTHSALHEDDA.koushin_date, 1, 8) AS NUMERIC), '0') AS UPDATEDATE,
        NVL(DECODE(ZCMMNTORISK.BUNR_VAL_CD3, '40', '2', '1'), '1') AS KKNG_KBN,
        NVL(SSMTHSALHEDDA.ksai_tuka_cd, '000') AS TUKA_CD,
        NVL(SSMTHSALHEDDA.shoumi_sal_sum_kin_ksai, '0') AS SHOKEI_TUKA,
        NVL(SSMTHSALHEDDA.sal_sz_sum_kin_ksai, '0') AS TAX_TUKA,
        NVL(SSMTHSALHEDDA.sal_sum_kin_ksai, '0') AS SOGOKEI_TUKA,
        0 AS KAKOKBN,
        NVL(ZCMMNDCLTOKUISK.DCLTENPO_SYSTEM_TENPO_CD, '00000') AS TENPOCODE,
        NVL(ZCMMNTORISK.bunr_val_cd2, '0') AS TENPOBUNRUI,
        NVL(ZCMMNTORISK.bunr_val_cd4, '0') AS SHOKUIKIBUNRUI,
        NVL(SSMTHSALHEDDA.can_flg, '0') AS CANCELFLG,
        NVL(SSMTHSALHEDDA.tokuisk_cd, '00000000') AS KOKYANO,
        '000' AS HENREASONCODE,
        ' ' AS HENREASONNAME,
        SSMTHSALHEDDA.sal_no AS DISPOURINO,
        NVL(ZCMMNTORISK.torisk_nms1, ' ') AS TOKUINAME_ASPAC,
        NVL(ZCMMNTORISK.torisk_nmr, ' ') AS SKYSK_NAME,
        NVL(SSMTHSALHEDDA.skysk_cd, ' ') AS SKYSK_CD,
        ' ' AS JUCH_BKO,
        ' ' AS TOKUINAME_KAIGAI,
        NVL(SSMTHSALHEDDA.touroku_user, '0000000000') AS TOUROKU_USER,
        3 AS MARKER,
        9 AS FSKBN,
        NVL(SSMTHSALHEDDA.den_nf_shm_sal_kin, '0') AS NUKIKINGAKU,
        SSMTHSALHEDDA.PROCESSTYPE_CD AS PROCESSTYPE_CD,
        CAST(TBECORDER.DIORDERID AS VARCHAR) AS JUCHNO,
        '' AS DAIHYOU_SHUKASK_CD,
        '' AS DAIHYOU_SHUKASK_NMR,
        LPAD(C_TBECKESAI.diEcUsrID, 10, '0') AS KOKYANO2,
        NVL(ZCMMNSOSHITAIK.BMN_HYOUJI_CD, '') AS BMN_HYOUJI_CD,
        NVL(ZCMMNSOSHITAIK.BMN_NMS, '') AS BMN_NMS,
        NULL AS ROWID1,
        NULL AS ROWID2,
        NULL AS ROWID3,
        NULL AS ROWID4,
        NULL AS ROWID5,
        NULL AS ROWID6,
        NULL AS ROWID7
    FROM SSMTHSALHEDDA,
        TBECORDER,
        ZCMMNTORISK,
        C_TBECKESAI,
        ZCMMNDCLTOKUISK,
        HANYO_ATTR DAILY,
        HANYO_ATTR KAISYA,
        ZCMMNSOSHITAIK
    WHERE SUBSTRING(SSMTHSALHEDDA.touroku_date, 1, 8) >= DAILY.ATTR3
        AND DAILY.KBNMEI = 'DAILYFROM'
        --会社コード絞込
        AND SSMTHSALHEDDA.kaisha_cd = KAISYA.ATTR1
        AND KAISYA.KBNMEI = 'KAISYA'
        AND SSMTHSALHEDDA.rend_mt_den_shubt IS NULL
        AND SSMTHSALHEDDA.CAN_FLG = '0'
        AND c_tbEcKesai.diorderid = tbEcOrder.diOrderID
        AND SSMTHSALHEDDA.kaisha_cd = ZCMMNTORISK.kaisha_cd(+)
        AND SSMTHSALHEDDA.tokuisk_cd = ZCMMNTORISK.torisk_cd(+)
        AND SSMTHSALHEDDA.kaisha_cd = ZCMMNDCLTOKUISK.kaisha_cd(+)
        AND SSMTHSALHEDDA.tokuisk_cd = ZCMMNDCLTOKUISK.tokuisk_cd(+)
        AND SSMTHSALHEDDA.SAL_JISK_IMP_SNSH_NO = CAST(C_TBECKESAI.C_DIKESAIID AS VARCHAR)
        AND (
            SSMTHSALHEDDA.tokuisk_cd <> 'BI00000001'
            AND SSMTHSALHEDDA.tokuisk_cd <> 'BI00000002'
            AND SSMTHSALHEDDA.tokuisk_cd <> 'BI00000003'
            )
        AND SSMTHSALHEDDA.KAISHA_CD = ZCMMNSOSHITAIK.KAISHA_CD(+)
        AND SSMTHSALHEDDA.SAL_BMN_NAIBU_NO = ZCMMNSOSHITAIK.BMN_NAIBUKANRI_NO(+)
    ),
t4
AS (
    SELECT SSMTHSALHEDDA.sal_no AS OURINO,
        NVL(tbecorder.c_dsorderkbn, '0') AS JUCHKBN,
        DECODE(ZCMMNTORISK.bunr_val_cd2, '1', '04', '2', '05', '3', '03', '4', '06', '5', '03', '02') AS TORIKEIKBN,
        CAST(TO_CHAR(tbEcOrder.dsOrderDt, 'YYYYMMDD') AS NUMERIC) AS JUCHDATE,
        NVL(SSMTHSALHEDDA.sal_keij_dt, '0') AS SHUKADATE,
        NVL(SSMTHSALHEDDA.tokuisk_cd, '0000000000') AS TOKUICODE,
        NVL(SSMTHSALHEDDA.shoumi_sal_sum_kin, '0') AS SHOKEI,
        NVL(SSMTHSALHEDDA.sal_shohzei_sum_kin, '0') AS TAX,
        NVL(SSMTHSALHEDDA.sal_sum_kin, '0') AS SOGOKEI,
        NVL(SSMTHSALHEDDA.snpo_chumon_no, '000000') AS SENDENNO,
        NVL(CAST(SUBSTRING(SSMTHSALHEDDA.koushin_date, 1, 8) AS NUMERIC), '0') AS UPDATEDATE,
        NVL(DECODE(ZCMMNTORISK.BUNR_VAL_CD3, '40', '2', '1'), '1') AS KKNG_KBN,
        NVL(SSMTHSALHEDDA.ksai_tuka_cd, '000') AS TUKA_CD,
        NVL(SSMTHSALHEDDA.shoumi_sal_sum_kin_ksai, '0') AS SHOKEI_TUKA,
        NVL(SSMTHSALHEDDA.sal_sz_sum_kin_ksai, '0') AS TAX_TUKA,
        NVL(SSMTHSALHEDDA.sal_sum_kin_ksai, '0') AS SOGOKEI_TUKA,
        0 AS KAKOKBN,
        NVL(ZCMMNDCLTOKUISK.DCLTENPO_SYSTEM_TENPO_CD, '00000') AS TENPOCODE,
        NVL(ZCMMNTORISK.bunr_val_cd2, '0') AS TENPOBUNRUI,
        NVL(ZCMMNTORISK.bunr_val_cd4, '0') AS SHOKUIKIBUNRUI,
        NVL(SSMTHSALHEDDA.can_flg, '0') AS CANCELFLG,
        NVL(SSMTHSALHEDDA.tokuisk_cd, '00000000') AS KOKYANO,
        '000' AS HENREASONCODE,
        ' ' AS HENREASONNAME,
        SSMTHSALHEDDA.sal_no AS DISPOURINO,
        NVL(ZCMMNTORISK.torisk_nms1, ' ') AS TOKUINAME_ASPAC,
        NVL(ZCMMNTORISK.torisk_nmr, ' ') AS SKYSK_NAME,
        NVL(SSMTHSALHEDDA.skysk_cd, ' ') AS SKYSK_CD,
        ' ' AS JUCH_BKO,
        ' ' AS TOKUINAME_KAIGAI,
        NVL(SSMTHSALHEDDA.touroku_user, '0000000000') AS TOUROKU_USER,
        4 AS MARKER,
        CASE 
            WHEN SSMTHSALHEDDA.tokuisk_cd = 'FS00000001'
                AND FS.ATTR3 = '卸FS'
                THEN 0
            WHEN SSMTHSALHEDDA.tokuisk_cd = 'FS00000001'
                AND FS.ATTR3 = '店舗FS'
                THEN 1
            ELSE 9
            END AS FSKBN,
        NVL(SSMTHSALHEDDA.den_nf_shm_sal_kin, '0') AS NUKIKINGAKU,
        SSMTHSALHEDDA.PROCESSTYPE_CD AS PROCESSTYPE_CD,
        CAST(TBECORDER.DIORDERID AS VARCHAR) AS JUCHNO,
        '' AS DAIHYOU_SHUKASK_CD,
        '' AS DAIHYOU_SHUKASK_NMR,
        LPAD(C_TBECKESAI.diEcUsrID, 10, '0') AS KOKYANO2,
        NVL(ZCMMNSOSHITAIK.BMN_HYOUJI_CD, '') AS BMN_HYOUJI_CD,
        NVL(ZCMMNSOSHITAIK.BMN_NMS, '') AS BMN_NMS,
        NULL AS ROWID1,
        NULL AS ROWID2,
        NULL AS ROWID3,
        NULL AS ROWID4,
        NULL AS ROWID5,
        NULL AS ROWID6,
        NULL AS ROWID7
    FROM SSMTHSALHEDDA,
        TBECORDER,
        ZCMMNTORISK,
        C_TBECKESAI,
        ZCMMNDCLTOKUISK,
        HANYO_ATTR DAILY,
        HANYO_ATTR KAISYA,
        (
            SELECT *
            FROM HANYO_ATTR
            WHERE KBNMEI = 'FAMILYSALE'
            ) FS,
        ZCMMNSOSHITAIK
    WHERE SUBSTRING(SSMTHSALHEDDA.touroku_date, 1, 8) >= DAILY.ATTR3
        AND DAILY.KBNMEI = 'DAILYFROM'
        AND SSMTHSALHEDDA.kaisha_cd = KAISYA.ATTR1
        AND KAISYA.KBNMEI = 'KAISYA'
        AND ZCMMNSOSHITAIK.BMN_HYOUJI_CD = FS.ATTR1(+)
        AND SSMTHSALHEDDA.SAL_SHUR_KBN = '2'
        AND SSMTHSALHEDDA.rend_mt_den_shubt IS NULL
        AND SSMTHSALHEDDA.CAN_FLG = '0'
        AND SSMTHSALHEDDA.kaisha_cd = ZCMMNTORISK.kaisha_cd(+)
        AND SSMTHSALHEDDA.tokuisk_cd = ZCMMNTORISK.torisk_cd(+)
        AND SSMTHSALHEDDA.kaisha_cd = ZCMMNDCLTOKUISK.kaisha_cd(+)
        AND SSMTHSALHEDDA.tokuisk_cd = ZCMMNDCLTOKUISK.tokuisk_cd(+)
        AND c_tbEcKesai.diorderid = tbEcOrder.diOrderID(+)
        AND tbEcOrder.diOrderID IS NULL
        AND SSMTHSALHEDDA.SAL_JISK_IMP_SNSH_NO = CAST(C_TBECKESAI.C_DIKESAIID(+) AS VARCHAR)
        AND CAST(C_TBECKESAI.C_DIKESAIID AS VARCHAR) IS NULL
        AND (
            SSMTHSALHEDDA.tokuisk_cd <> 'BI00000001'
            AND SSMTHSALHEDDA.tokuisk_cd <> 'BI00000002'
            AND SSMTHSALHEDDA.tokuisk_cd <> 'BI00000003'
            )
        AND SSMTHSALHEDDA.KAISHA_CD = ZCMMNSOSHITAIK.KAISHA_CD(+)
        AND SSMTHSALHEDDA.SAL_BMN_NAIBU_NO = ZCMMNSOSHITAIK.BMN_NAIBUKANRI_NO(+)
        AND NOT (
            SSMTHSALHEDDA.tokuisk_cd = 'FS00000001'
            AND ZCMMNSOSHITAIK.BMN_HYOUJI_CD NOT IN (
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
    
    UNION ALL
    
    SELECT *
    FROM t3
    
    UNION ALL
    
    SELECT *
    FROM t4
    ),
final AS
(
    SELECT OURINO::VARCHAR(18) AS OURINO,
        JUCHKBN::VARCHAR(3) AS JUCHKBN,
        TORIKEIKBN::VARCHAR(3) AS TORIKEIKBN,
        JUCHDATE::NUMBER(10,0) AS JUCHDATE,
        SHUKADATE::NUMBER(18,0) AS SHUKADATE,
        TOKUICODE::VARCHAR(22) AS TOKUICODE,
        SHOKEI::NUMBER(18,0) AS SHOKEI,
        TAX::NUMBER(18,0) AS TAX,
        SOGOKEI::NUMBER(18,0) AS SOGOKEI,
        SENDENNO::VARCHAR(80) AS SENDENNO,
        UPDATEDATE::NUMBER(18,0) AS UPDATEDATE,
        KKNG_KBN::VARCHAR(2) AS KKNG_KBN,
        TUKA_CD::VARCHAR(15) AS TUKA_CD,
        SHOKEI_TUKA::NUMBER(18,0) AS SHOKEI_TUKA,
        TAX_TUKA::NUMBER(18,0) AS TAX_TUKA,
        SOGOKEI_TUKA::NUMBER(18,0) AS SOGOKEI_TUKA,
        KAKOKBN::NUMBER(18,0) AS KAKOKBN,
        TENPOCODE::VARCHAR(8) AS TENPOCODE,
        TENPOBUNRUI::VARCHAR(6) AS TENPOBUNRUI,
        SHOKUIKIBUNRUI::VARCHAR(6) AS SHOKUIKIBUNRUI,
        CANCELFLG::NUMBER(18,0) AS CANCELFLG,
        KOKYANO::VARCHAR(22) AS KOKYANO,
        HENREASONCODE::VARCHAR(3) AS HENREASONCODE,
        HENREASONNAME::VARCHAR(1) AS HENREASONNAME,
        DISPOURINO::VARCHAR(18) AS DISPOURINO,
        TOKUINAME_ASPAC::VARCHAR(100) AS TOKUINAME_ASPAC,
        SKYSK_NAME::VARCHAR(240) AS SKYSK_NAME,
        SKYSK_CD::VARCHAR(22) AS SKYSK_CD,
        JUCH_BKO::VARCHAR(800) AS JUCH_BKO,
        TOKUINAME_KAIGAI::VARCHAR(90) AS TOKUINAME_KAIGAI,
        TOUROKU_USER::VARCHAR(22) AS TOUROKU_USER,
        MARKER::NUMBER(18,0) AS MARKER,
        FSKBN::NUMBER(18,0) AS FSKBN,
        NUKIKINGAKU::NUMBER(18,0) AS NUKIKINGAKU,
        PROCESSTYPE_CD::VARCHAR(15) AS PROCESSTYPE_CD,
        JUCHNO::VARCHAR(80) AS JUCHNO,
        DAIHYOU_SHUKASK_CD::VARCHAR(22) DAIHYOU_SHUKASK_CD,
        DAIHYOU_SHUKASK_NMR::VARCHAR(240) AS DAIHYOU_SHUKASK_NMR,
        KOKYANO2::VARCHAR(240) AS KOKYANO2,
        BMN_HYOUJI_CD::VARCHAR(15) AS BMN_HYOUJI_CD,
        BMN_NMS::VARCHAR(240) AS BMN_NMS,
        ROWID1::VARCHAR(1) AS ROWID1,
        ROWID2::VARCHAR(1) AS ROWID2,
        ROWID3::VARCHAR(1) AS ROWID3,
        ROWID4::VARCHAR(1) AS ROWID4,
        ROWID5::VARCHAR(1) AS ROWID5,
        ROWID6::VARCHAR(1) AS ROWID6,
        ROWID7::VARCHAR(1) AS ROWID7,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS INSERTED_DATE,
        'ETL_Batch'::VARCHAR(100) AS INSERTED_BY,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS UPDATED_DATE,
        NULL::VARCHAR(100) AS UPDATED_BY
    FROM UNION_OF
)
SELECT *
FROM

final
