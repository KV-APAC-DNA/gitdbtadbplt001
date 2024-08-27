{{
    config
    (
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
    )
}}

WITH kesai_m_data_mart_sub_old_chsi
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__kesai_m_data_mart_sub_old_chsi') }}
    ),
kesai_m_data_mart_sub_old
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__kesai_m_data_mart_sub_old') }}
    ),
kesai_m_data_mart_sub
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__kesai_m_data_mart_sub') }}
    ),
kesai_m_data_mart_sub_n_wari
AS (
    SELECT *
    FROM {{source('jpdcledw_integration', 'kesai_m_data_mart_sub_n_wari') }}
    ),
kesai_h_data_mart_sub
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__kesai_h_data_mart_sub') }}
    ),
final
AS (
    SELECT nvl(SALENO, '') AS SALENO_KEY,
        SALENO,
        GYONO,
        BUN_GYONO,
        MEISAIKBN,
        ITEMCODE,
        NULL AS SETITEMNM,
        BUN_ITEMCODE,
        'DUMMY' AS DIID,
        'DUMMY' AS DISETID,
        SURYO,
        TANKA,
        KINGAKU,
        MEISAINUKIKINGAKU,
        WARIRITU,
        HENSU,
        WARIMAEKOMITANKA,
        WARIMAEKOMIKINGAKU,
        WARIMAENUKIKINGAKU,
        MEISAITAX,
        DISPSALENO,
        KESAIID,
        SALENO_TRIM,
        0 AS DIORDERID,
        NULL AS HENPINSTS,
        NULL AS C_DSPOINTITEMFLG,
        NULL AS C_DIITEMTYPE,
        0 AS C_DIADJUSTPRC,
        0 AS DITOTALPRC,
        0 AS C_DIITEMTOTALPRC,
        0 AS C_DIDISCOUNTMEISAI,
        BUN_SURYO,
        BUN_TANKA,
        BUN_KINGAKU,
        BUN_MEISAINUKIKINGAKU,
        BUN_WARIRITU,
        BUN_HENSU,
        BUN_WARIMAEKOMITANKA,
        BUN_WARIMAEKOMIKINGAKU,
        BUN_WARIMAENUKIKINGAKU,
        BUN_MEISAITAX,
        CI_PORT_FLG,
        0 AS MAKER,
        SALENO AS SALENO_P,
        GYONO_P,
        BUN_GYONO_P,
        MEISAIKBN_P,
        BUN_ITEMCODE_P,
        BUN_ITEMCODE_P AS ITEMCODE_HANBAI_P,
        '1' AS KAKOKBN,
        BUN_SURYO_P AS SURYO_P,
        BUN_TANKA_P AS TANKA_P,
        BUN_HENSU_P AS HENSU_P,
        BUN_HENUSU AS HENUSU_P,
        JYU_SURYO AS JYU_SURYO,
        NULL AS OYAFLG_P,
        0 AS JUCHGYONO_P,
        NULL AS DISPSALENO_P,
        CAST(JUCH_SHUR AS VARCHAR) AS JUCH_SHUR_P,
        TYOSEIKIKINGAKU AS TYOSEIKIKINGAKU_P,
        0 AS DEN_NEBIKI_ABN_KIN_P,
        0 AS DEN_NB_AB_SZ_KIN_P,
        NULL AS DCLSM_HIN_HIN_NIBU_ID_P,
        BUN_KINGAKU AS KINGAKU_P,
        BUN_MEISAINUKIKINGAKU AS MEISAINUKIKINGAKU_P,
        BUN_MEISAITAX AS MEISAITAX_P,
        BUN_MEISAINUKIKINGAKU_P AS ANBUNMEISAINUKIKINGAKU_P,
        BUN_KINGAKU AS KINGAKU_TUKA_P,
        BUN_MEISAINUKIKINGAKU AS MEISAINUKIKINGAKU_TUKA_P,
        BUN_MEISAITAX AS MEISAITAX_TUKA_P,
        ANBN_BN_KINGAKU + HASUU_KINGAKU AS HASUU_KINGAKU,
        ANBN_BN_MEISAINUKIKINGAKU + HASUU_MEISAINUKIKINGAKU AS HASUU_MEISAINUKIKINGAKU,
        ANBN_BN_MEISAITAX + HASUU_MEISAITAX AS HASUU_MEISAITAX,
        ANBN_BN_ANBUNMEISAINUKIKINGAKU + HASUU_ANBUNMEISAINUKIKINGAKU AS HASUU_ANBUNMEISAINUKIKINGAKU,
        ANBN_BN_KINGAKU_TUKA + HASUU_KINGAKU_TUKA AS HASUU_KINGAKU_TUKA,
        ANBN_BN_MEISAINUKIKINGAKU_TUKA + HASUU_MEISAINUKIKINGAKU_TUKA AS HASUU_MEISAINUKIKINGAKU_TUKA,
        ANBN_BN_MEISAITAX_TUKA + HASUU_MEISAITAX_TUKA AS HASUU_MEISAITAX_TUKA,
        HAS_BN_KINGAKU AS HAS_BN_KINGAKU,
        HAS_BN_MEISAINUKIKINGAKU AS HAS_BN_MEISAINUKIKINGAKU,
        HAS_BN_MEISAITAX AS HAS_BN_MEISAITAX,
        HAS_BN_ANBUNMEISAINUKIKINGAKU AS HAS_BN_ANBUNMEISAINUKIKINGAKU,
        HAS_BN_KINGAKU_TUKA AS HAS_BN_KINGAKU_TUKA,
        HAS_BN_MEISAINUKIKINGAKU_TUKA AS HAS_BN_MEISAINUKIKINGAKU_TUKA,
        HAS_BN_MEISAITAX_TUKA AS HAS_BN_MEISAITAX_TUKA,
        0 AS MARKER_P,
        NULL AS URI_HEN_KBN_P,
        NULL AS SAL_JISK_IMP_SNSH_NO_P,
        NULL AS DCLJUCH_ID_P
    FROM KESAI_M_DATA_MART_SUB_OLD SUB_OLD
    
    UNION ALL
    
    SELECT cast((nvl(SUB_NEW.SALENO, '') || nvl(SUB_NEW.SALENO_P, '')) AS VARCHAR) AS SALENO_KEY,
        SUB_NEW.SALENO,
        SUB_NEW.GYONO,
        SUB_NEW.GYONO AS BUN_GYONO,
        SUB_NEW.MEISAIKBN,
        SUB_NEW.SETITEMCD AS ITEMCODE,
        SUB_NEW.SETITEMNM,
        SUB_NEW.ITEMCODE AS BUN_ITEMCODE,
        SUB_NEW.DIID,
        SUB_NEW.DISETID,
        CASE 
            WHEN SUB_NEW.DISETID = SUB_NEW.DIID
                THEN CASE 
                        WHEN (
                                NEWH.JUCHKBN <> '90'
                                AND NEWH.JUCHKBN <> '91'
                                AND NEWH.JUCHKBN <> '92'
                                )
                            THEN SUB_NEW.SURYO
                        ELSE 0
                        END
            ELSE 0
            END SURYO,
        CASE 
            WHEN SUB_NEW.DISETID = SUB_NEW.DIID
                THEN SUB_NEW.TANKA
            ELSE 0
            END TANKA,
        CASE 
            WHEN SUB_NEW.DISETID = SUB_NEW.DIID
                THEN SUB_NEW.KINGAKU
            ELSE 0
            END KINGAKU,
        CASE 
            WHEN SUB_NEW.DISETID = SUB_NEW.DIID
                THEN SUB_NEW.MEISAINUKIKINGAKU
            ELSE 0
            END MEISAINUKIKINGAKU,
        CASE 
            WHEN SUB_NEW.DISETID = SUB_NEW.DIID
                THEN SUB_NEW.WARIRITU
            ELSE 0
            END WARIRITU,
        CASE 
            WHEN SUB_NEW.DISETID = SUB_NEW.DIID
                THEN CASE 
                        WHEN (
                                NEWH.JUCHKBN <> '0'
                                AND NEWH.JUCHKBN <> '1'
                                AND NEWH.JUCHKBN <> '2'
                                )
                            THEN - 1 * SUB_NEW.SURYO
                        ELSE 0
                        END
            ELSE 0
            END HENSU,
        CASE 
            WHEN SUB_NEW.DISETID = SUB_NEW.DIID
                THEN SUB_NEW.WARIMAEKOMITANKA
            ELSE 0
            END WARIMAEKOMITANKA,
        CASE 
            WHEN SUB_NEW.DISETID = SUB_NEW.DIID
                THEN SUB_NEW.WARIMAEKOMIKINGAKU
            ELSE 0
            END WARIMAEKOMIKINGAKU,
        CASE 
            WHEN SUB_NEW.DISETID = SUB_NEW.DIID
                THEN SUB_NEW.WARIMAENUKIKINGAKU
            ELSE 0
            END WARIMAENUKIKINGAKU,
        CASE 
            WHEN SUB_NEW.DISETID = SUB_NEW.DIID
                THEN SUB_NEW.DIITEMTAX
            ELSE 0
            END MEISAITAX,
        SUB_NEW.DISPSALENO,
        CAST(SUB_NEW.KESAIID AS VARCHAR) AS KESAIID,
        TRIM(SUB_NEW.SALENO) AS SALENO_TRIM,
        SUB_NEW.DIORDERID,
        SUB_NEW.HENPINSTS,
        SUB_NEW.C_DSPOINTITEMFLG,
        SUB_NEW.C_DIITEMTYPE,
        SUB_NEW.C_DIADJUSTPRC,
        SUB_NEW.DITOTALPRC,
        SUB_NEW.C_DIITEMTOTALPRC,
        SUB_NEW.C_DIDISCOUNTMEISAI,
        CASE 
            WHEN (
                    SUB_NEW.DISETID = SUB_NEW.DIID
                    AND SUB_NEW.C_DSSETITEMKBN <> '1'
                    )
                OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
                THEN CASE 
                        WHEN (
                                NEWH.JUCHKBN <> '90'
                                AND NEWH.JUCHKBN <> '91'
                                AND NEWH.JUCHKBN <> '92'
                                )
                            THEN SUB_NEW.SURYO
                        ELSE 0
                        END
            ELSE 0
            END BUN_SURYO,
        CASE 
            WHEN (
                    SUB_NEW.DISETID = SUB_NEW.DIID
                    AND SUB_NEW.C_DSSETITEMKBN <> '1'
                    )
                OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
                THEN SUB_NEW.BUN_TANKA
            ELSE 0
            END BUN_TANKA,
        CASE 
            WHEN (
                    SUB_NEW.DISETID = SUB_NEW.DIID
                    AND SUB_NEW.C_DSSETITEMKBN <> '1'
                    )
                OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
                THEN SUB_NEW.BUN_KINGAKU
            ELSE 0
            END BUN_KINGAKU,
        CASE 
            WHEN (
                    SUB_NEW.DISETID = SUB_NEW.DIID
                    AND SUB_NEW.C_DSSETITEMKBN <> '1'
                    )
                OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
                THEN SUB_NEW.BUN_MEISAINUKIKINGAKU
            ELSE 0
            END BUN_MEISAINUKIKINGAKU,
        CASE 
            WHEN (
                    SUB_NEW.DISETID = SUB_NEW.DIID
                    AND SUB_NEW.C_DSSETITEMKBN <> '1'
                    )
                OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
                THEN SUB_NEW.BUN_WARIRITU
            ELSE 0
            END BUN_WARIRITU,
        CASE 
            WHEN (
                    SUB_NEW.DISETID = SUB_NEW.DIID
                    AND SUB_NEW.C_DSSETITEMKBN <> '1'
                    )
                OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
                THEN CASE 
                        WHEN (
                                NEWH.JUCHKBN <> '0'
                                AND NEWH.JUCHKBN <> '1'
                                AND NEWH.JUCHKBN <> '2'
                                )
                            THEN - 1 * SUB_NEW.SURYO
                        ELSE 0
                        END
            ELSE 0
            END BUN_HENSU,
        CASE 
            WHEN (
                    SUB_NEW.DISETID = SUB_NEW.DIID
                    AND SUB_NEW.C_DSSETITEMKBN <> '1'
                    )
                OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
                THEN SUB_NEW.BUN_WARIMAEKOMITANKA
            ELSE 0
            END BUN_WARIMAEKOMITANKA,
        CASE 
            WHEN (
                    SUB_NEW.DISETID = SUB_NEW.DIID
                    AND SUB_NEW.C_DSSETITEMKBN <> '1'
                    )
                OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
                THEN SUB_NEW.BUN_WARIMAEKOMIKINGAKU
            ELSE 0
            END BUN_WARIMAEKOMIKINGAKU,
        CASE 
            WHEN (
                    SUB_NEW.DISETID = SUB_NEW.DIID
                    AND SUB_NEW.C_DSSETITEMKBN <> '1'
                    )
                OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
                THEN SUB_NEW.BUN_WARIMAENUKIKINGAKU
            ELSE 0
            END BUN_WARIMAENUKIKINGAKU,
        CASE 
            WHEN (
                    SUB_NEW.DISETID = SUB_NEW.DIID
                    AND SUB_NEW.C_DSSETITEMKBN <> '1'
                    )
                OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
                THEN SUB_NEW.DIITEMTAX
            ELSE 0
            END BUN_MEISAITAX,
        CASE SUB_NEW.MEISAIKBN
            WHEN '商品'
                THEN '1'
            WHEN '特典'
                THEN '1'
            WHEN '送料'
                THEN '1'
            ELSE '0'
            END AS CI_PORT_FLG,
        SUB_NEW.MAKER,
        SUB_NEW.SALENO_P,
        SUB_NEW.GYONO_P,
        SUB_NEW.GYONO_P AS BUN_GYONO_P,
        NULL AS MEISAIKBN_P,
        SUB_NEW.ITEMCODE_P,
        SUB_NEW.ITEMCODE_HANBAI_P,
        '0' AS KAKOKBN,
        SUB_NEW.SURYO_P AS SURYO_P,
        SUB_NEW.TANKA_P AS TANKA_P,
        SUB_NEW.HENSU_P AS HENSU_P,
        SUB_NEW.HENSU_P AS HENUSU_P,
        SUB_NEW.JYU_SURYO_P,
        SUB_NEW.OYAFLG_P,
        SUB_NEW.JUCHGYONO_P,
        SUB_NEW.DISPSALENO_P,
        SUB_NEW.JUCH_SHUR_P,
        SUB_NEW.TYOSEIKIKINGAKU_P,
        SUB_NEW.DEN_NEBIKI_ABN_KIN_P,
        SUB_NEW.DEN_NB_AB_SZ_KIN_P,
        SUB_NEW.DCLSM_HIN_HIN_NIBU_ID_P,
        SUB_NEW.KINGAKU_P AS KINGAKU_P,
        SUB_NEW.MEISAINUKIKINGAKU_P AS MEISAINUKIKINGAKU_P,
        SUB_NEW.MEISAITAX_P AS MEISAITAX_P,
        SUB_NEW.ANBUNMEISAINUKIKINGAKU_P AS ANBUNMEISAINUKIKINGAKU_P,
        SUB_NEW.KINGAKU_TUKA_P AS KINGAKU_TUKA_P,
        SUB_NEW.MEISAINUKIKINGAKU_TUKA_P AS MEISAINUKIKINGAKU_TUKA_P,
        SUB_NEW.MEISAITAX_TUKA_P AS MEISAITAX_TUKA_P,
        0 AS HASUU_KINGAKU,
        0 AS HASUU_MEISAINUKIKINGAKU,
        0 AS HASUU_MEISAITAX,
        0 AS HASUU_ANBUNMEISAINUKIKINGAKU,
        0 AS HASUU_KINGAKU_TUKA,
        0 AS HASUU_MEISAINUKIKINGAKU_TUKA,
        0 AS HASUU_MEISAITAX_TUKA,
        SUB_NEW.KINGAKU_P AS HAS_BN_KINGAKU,
        SUB_NEW.MEISAINUKIKINGAKU_P AS HAS_BN_MEISAINUKIKINGAKU,
        SUB_NEW.MEISAITAX_P AS HAS_BN_MEISAITAX,
        SUB_NEW.ANBUNMEISAINUKIKINGAKU_P AS HAS_BN_ANBUNMEISAINUKIKINGAKU,
        CAST((SUB_NEW.KINGAKU_TUKA_P) AS NUMERIC) AS HAS_BN_KINGAKU_TUKA,
        CAST((SUB_NEW.MEISAINUKIKINGAKU_TUKA_P) AS NUMERIC) AS HAS_BN_MEISAINUKIKINGAKU_TUKA,
        Cast((SUB_NEW.MEISAITAX_TUKA_P) AS NUMERIC) AS HAS_BN_MEISAITAX_TUKA,
        Cast((SUB_NEW.MARKER_P) AS NUMERIC),
        SUB_NEW.URI_HEN_KBN_P,
        SUB_NEW.SAL_JISK_IMP_SNSH_NO_P,
        SUB_NEW.DCLJUCH_ID_P
    FROM KESAI_M_DATA_MART_SUB SUB_NEW
    INNER JOIN (
        SELECT SALENO,
            MIN(JUCHKBN) JUCHKBN
        FROM KESAI_H_DATA_MART_SUB
        GROUP BY SALENO
        ) NEWH ON SUB_NEW.SALENO = NEWH.SALENO
    LEFT JOIN KESAI_M_DATA_MART_SUB_N_WARI WARI ON SUB_NEW.SALENO = WARI.SALENO
        AND SUB_NEW.GYONO = WARI.GYONO
    
    UNION ALL
    
    SELECT nvl(OLD_CHSI.SALENO, '') || '調整行DUMMY' AS SALENO_KEY,
        OLD_CHSI.SALENO AS SALENO,
        0 AS GYONO,
        0 AS BUN_GYONO,
        'DUMMY' AS MEISAIKBN,
        'DUMMY' AS ITEMCODE,
        '調整行(ヘッダーと明細の差分)' AS SETITEMNM,
        'DUMMY' AS BUN_ITEMCODE,
        'DUMMY' AS DIID,
        'DUMMY' AS DISETID,
        1 AS SURYO,
        OLD_CHSI.KINGAKU AS TANKA,
        OLD_CHSI.KINGAKU AS KINGAKU,
        OLD_CHSI.MEISAINUKIKINGAKU AS MEISAINUKIKINGAKU,
        0 AS WARIRITU,
        0 AS HENSU,
        OLD_CHSI.KINGAKU AS WARIMAEKOMITANKA,
        OLD_CHSI.KINGAKU AS WARIMAEKOMIKINGAKU,
        OLD_CHSI.MEISAINUKIKINGAKU AS WARIMAENUKIKINGAKU,
        0 AS MEISAITAX,
        OLD_CHSI.SALENO AS DISPSALENO,
        OLD_CHSI.KESAIID AS KESAIID,
        TRIM(OLD_CHSI.SALENO) AS SALENO_TRIM,
        0 AS DIORDERID,
        'DUMMY' AS HENPINSTS,
        'DUMMY' AS C_DSPOINTITEMFLG,
        'DUMMY' AS C_DIITEMTYPE,
        0 AS C_DIADJUSTPRC,
        0 AS DITOTALPRC,
        0 AS C_DIITEMTOTALPRC,
        0 AS C_DIDISCOUNTMEISAI,
        1 AS BUN_SURYO,
        OLD_CHSI.KINGAKU AS BUN_TANKA,
        OLD_CHSI.KINGAKU AS BUN_KINGAKU,
        OLD_CHSI.MEISAINUKIKINGAKU AS BUN_MEISAINUKIKINGAKU,
        OLD_CHSI.KINGAKU AS BUN_WARIRITU,
        0 AS BUN_HENSU,
        OLD_CHSI.KINGAKU AS BUN_WARIMAEKOMITANKA,
        OLD_CHSI.KINGAKU AS BUN_WARIMAEKOMIKINGAKU,
        OLD_CHSI.MEISAINUKIKINGAKU AS BUN_WARIMAENUKIKINGAKU,
        0 AS BUN_MEISAITAX,
        '0' AS CI_PORT_FLG,
        0 AS MAKER,
        '調整行DUMMY' AS SALENO_P,
        CAST((NULL) AS NUMERIC) AS GYONO_P,
        CAST((NULL) AS NUMERIC) AS BUN_GYONO_P,
        NULL AS MEISAIKBN_P,
        NULL AS BUN_ITEMCODE_P,
        NULL AS ITEMCODE_HANBAI_P,
        '1' AS KAKOKBN,
        CAST((NULL) AS NUMERIC) AS SURYO_P,
        CAST((NULL) AS NUMERIC) AS TANKA_P,
        CAST((NULL) AS NUMERIC) AS HENSU_P,
        CAST((NULL) AS NUMERIC) AS HENUSU_P,
        CAST((NULL) AS NUMERIC) AS JYU_SURYO,
        NULL AS OYAFLG_P,
        CAST((NULL) AS NUMERIC) AS JUCHGYONO_P,
        NULL AS DISPSALENO_P,
        NULL AS JUCH_SHUR_P,
        CAST((NULL) AS NUMERIC) AS TYOSEIKIKINGAKU_P,
        CAST((NULL) AS NUMERIC) AS DEN_NEBIKI_ABN_KIN_P,
        CAST((NULL) AS NUMERIC) AS DEN_NB_AB_SZ_KIN_P,
        NULL AS DCLSM_HIN_HIN_NIBU_ID_P,
        CAST((NULL) AS NUMERIC) AS KINGAKU_P,
        CAST((NULL) AS NUMERIC) AS MEISAINUKIKINGAKU_P,
        CAST((NULL) AS NUMERIC) AS MEISAITAX_P,
        CAST((NULL) AS NUMERIC) AS ANBUNMEISAINUKIKINGAKU_P,
        CAST((NULL) AS NUMERIC) AS KINGAKU_TUKA_P,
        CAST((NULL) AS NUMERIC) AS MEISAINUKIKINGAKU_TUKA_P,
        CAST((NULL) AS NUMERIC) AS MEISAITAX_TUKA_P,
        0 AS HASUU_KINGAKU,
        0 AS HASUU_MEISAINUKIKINGAKU,
        0 AS HASUU_MEISAITAX,
        0 AS HASUU_ANBUNMEISAINUKIKINGAKU,
        0 AS HASUU_KINGAKU_TUKA,
        0 AS HASUU_MEISAINUKIKINGAKU_TUKA,
        0 AS HASUU_MEISAITAX_TUKA,
        CAST((NULL) AS NUMERIC) AS HAS_BN_KINGAKU,
        CAST((NULL) AS NUMERIC) AS HAS_BN_MEISAINUKIKINGAKU,
        CAST((NULL) AS NUMERIC) AS HAS_BN_MEISAITAX,
        CAST((NULL) AS NUMERIC) AS HAS_BN_ANBUNMEISAINUKIKINGAKU,
        CAST((NULL) AS NUMERIC) AS HAS_BN_KINGAKU_TUKA,
        Cast((NULL) AS NUMERIC) AS HAS_BN_MEISAINUKIKINGAKU_TUKA,
        Cast((NULL) AS NUMERIC) AS HAS_BN_MEISAITAX_TUKA,
        CAST((NULL) AS NUMERIC) AS MARKER_P,
        NULL AS URI_HEN_KBN_P,
        NULL AS SAL_JISK_IMP_SNSH_NO_P,
        NULL AS DCLJUCH_ID_P
    FROM KESAI_M_DATA_MART_SUB_OLD_CHSI OLD_CHSI
    )
SELECT *
FROM final
