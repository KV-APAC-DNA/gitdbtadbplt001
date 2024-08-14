WITH zcmmntorisk
AS (
    SELECT * 
    FROM  {{ source('jpdclitg_integration', 'zcmmntorisk') }}
    ),
zcmmnjuusho
AS (
    SELECT *
    FROM {{ source('jpdclitg_integration', 'zcmmnjuusho') }}
    ),
hanyo_attr
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__hanyo_attr')}}
    ),
cim02tokui_ikou
AS (
    SELECT *
    FROM {{ source('jpdcledw_integration', 'cim02tokui_ikou') }}
    ),
T1
AS (
    SELECT DISTINCT torisk.torisk_cd::VARCHAR(22) AS tokuicode,
        (CASE 
            WHEN (torisk.bunr_val_cd2 IS NOT NULL)
                THEN '02'::CHARACTER VARYING
            WHEN (torisk.bunr_val_cd4 IS NOT NULL)
                THEN '03'::CHARACTER VARYING
            ELSE '01'::CHARACTER VARYING
            END)::VARCHAR(3) AS tokubunkbn,
        torisk.torisk_nms1::VARCHAR(240) AS tokuiname,
        torisk.torisk_nmr::VARCHAR(80) AS tokuiname_ryaku,
        torisk.torisk_nmk::VARCHAR(160) AS tokuiname_kana,
        juusyo.todoufuken_nms::VARCHAR(15) AS todofukennm,
        ((((torisk.addr1)::TEXT || (torisk.addr2)::TEXT) || (torisk.addr3)::TEXT))::VARCHAR(450) AS address,
        torisk.bunr_val_cd2::VARCHAR(6) AS bunr_val_cd2,
        torisk.bunr_val_cd3::VARCHAR(6) AS bunr_val_cd3,
        torisk.bunr_val_cd4::VARCHAR(6) AS bunr_val_cd4
    FROM (
        zcmmntorisk torisk LEFT JOIN zcmmnjuusho juusyo ON (((torisk.yuubin_no)::TEXT = (juusyo.yuubin_no)::TEXT))
        )
    WHERE (
            EXISTS (
                SELECT hanyo.attr1
                FROM hanyo_attr hanyo
                WHERE (
                        ((hanyo.kbnmei)::TEXT = ('KAISYA'::CHARACTER VARYING)::TEXT)
                        AND ((hanyo.attr1)::TEXT = (torisk.kaisha_cd)::TEXT)
                        )
                )
            )
    ),
T2
AS (
    SELECT cim02.tokuicode,
        cim02.tokubunkbn,
        cim02.tokuiname,
        '-' AS tokuiname_ryaku,
        '-' AS tokuiname_kana,
        cim02.todofukencode AS todofukennm,
        ((((cim02.address1)::TEXT || (cim02.address2)::TEXT) || (cim02.address3)::TEXT))::CHARACTER VARYING AS address,
        NULL AS bunr_val_cd2,
        NULL AS bunr_val_cd3,
        NULL AS bunr_val_cd4
    FROM cim02tokui_ikou cim02
    WHERE (
            NOT (
                EXISTS (
                    SELECT 1
                    FROM zcmmntorisk torisk_2
                    WHERE ((torisk_2.torisk_cd)::TEXT = (cim02.tokuicode)::TEXT)
                    )
                )
            )
    ),
FINAL
AS (
    SELECT *
    FROM T1
    
    UNION
    
    SELECT *
    FROM T2
    )
SELECT *
FROM FINAL
