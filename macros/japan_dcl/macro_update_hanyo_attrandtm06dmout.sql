{% macro macro_update_hanyo_attrandtm06dmout() %}
    {% set query %}
    UPDATE {{this}}
    SET ATTR2 = DMP.ATTR2_SEL,
        ATTR4 = DMP.ATTR4_SEL,
        INSERTDATE = DMP.INSERTDATE_SEL,
        updated_date = GETDATE(),
        updated_by = 'ETL_Batch'
    FROM (
        SELECT DMP.DMPRNNO,
                SUBSTRING(DMP.COMMENT1, 1, 40) AS ATTR2_SEL,
                CASE 
                    WHEN DMP.COMMENT1 LIKE '%CLﾌﾟﾚﾐｱﾑ%'
                            THEN REGEXP_REPLACE(REGEXP_SUBSTR(DMP.COMMENT1, 'CLﾌﾟﾚﾐｱﾑ.*号'), 'CLﾌﾟﾚﾐｱﾑ', '')
                    WHEN DMP.COMMENT1 LIKE '%ｼｰﾗﾊﾞｰ%'
                            THEN REGEXP_REPLACE(REGEXP_SUBSTR(DMP.COMMENT1, 'ｼｰﾗﾊﾞｰ.*号'), 'ｼｰﾗﾊﾞｰ', '')
                    END AS ATTR4_SEL,
                DMP.INSERTDATE AS INSERTDATE_SEL,
                ATT.ATTR2,
                ATT.ATTR4,
                ATT.INSERTDATE
        FROM {{ ref('jpndcledw_integration__cir07dmprn') }} DMP
        INNER JOIN {{this}} ATT ON ATT.KBNMEI = 'DMCHUKUBUN'
                AND ATT.ATTR1 = DMP.DMPRNNO
        WHERE DMP.DMPRNNO IN (
                    SELECT DMPRNNO
                    FROM {{ ref('jpndcledw_integration__cir07dmprn') }} DMP
                    LEFT JOIN {{this}} ATTR ON ATTR.KBNMEI = 'DMCHUKUBUN'
                            AND ATTR.ATTR1 = DMP.DMPRNNO
                    WHERE DMP.INSERTDATE > ATTR.INSERTDATE
                    )
        ) DMP
    WHERE ATTR1 = DMP.DMPRNNO
        AND KBNMEI = 'DMCHUKUBUN';

    UPDATE {{this}}
    SET ATTR2 = DMP.ATTR2_SEL,
        ATTR4 = DMP.ATTR4_SEL,
        INSERTDATE = DMP.INSERTDATE_SEL,
        updated_date = GETDATE(),
        updated_by = 'ETL_Batch'
    FROM (
        SELECT DMP.DMPRNNO,
                SUBSTRING(DMP.COMMENT1, 1, 40) AS ATTR2_SEL,
                CASE 
                    WHEN DMP.COMMENT1 LIKE '%CLﾌﾟﾚﾐｱﾑ%'
                            THEN '0003'
                    WHEN DMP.COMMENT1 LIKE '%ｼｰﾗﾊﾞｰ%'
                            THEN '0001'
                    END AS ATTR3_SEL,
                CASE 
                    WHEN DMP.COMMENT1 LIKE '%CLﾌﾟﾚﾐｱﾑ%'
                            THEN REGEXP_SUBSTR(DMP.COMMENT1, 'CLﾌﾟﾚﾐｱﾑ.*号')
                    WHEN DMP.COMMENT1 LIKE '%ｼｰﾗﾊﾞｰ%'
                            THEN REGEXP_SUBSTR(DMP.COMMENT1, 'ｼｰﾗﾊﾞｰ.*号')
                    END AS ATTR4_SEL,
                DMP.INSERTDATE AS INSERTDATE_SEL,
                ATT.ATTR2,
                ATT.ATTR4,
                ATT.INSERTDATE
        FROM {{ ref('jpndcledw_integration__cir07dmprn') }} DMP
        INNER JOIN {{this}} ATT ON ATT.KBNMEI = 'DMSYOKUBUN'
                AND ATT.ATTR1 = DMP.DMPRNNO
        WHERE DMP.DMPRNNO IN (
                    SELECT DMPRNNO
                    FROM {{ ref('jpndcledw_integration__cir07dmprn') }} DMP
                    LEFT JOIN {{this}} ATTR ON ATTR.KBNMEI = 'DMSYOKUBUN'
                            AND ATTR.ATTR1 = DMP.DMPRNNO
                    WHERE DMP.INSERTDATE > ATTR.INSERTDATE
                    )
        ) DMP
    WHERE ATTR1 = DMP.DMPRNNO
        AND KBNMEI = 'DMSYOKUBUN';

    UPDATE {{ ref('jpndcledw_integration__tm06dmout') }}
    SET DMDAIBUCODE = HANYO.DMDAIBUNCODE,
        DMDAIBUNNAME = HANYO.DMDAIBUNNAME,
        DMCHUBUNCODE = HANYO.DMCHUBUNCODE,
        DMCHUBUNNAME = HANYO.DMCHUBUNNAME,
        DMSYOBUNCODE = HANYO.DMSYOBUNCODE,
        DMSYOBUNNAME = HANYO.DMSYOBUNNAME,
        updated_date = GETDATE(),
        updated_by = 'ETL_Batch'
    FROM (
        SELECT HANYO_DAI.ATTR1 AS DMPRNNO,
                HANYO_DAI.ATTR3 AS DMDAIBUNCODE,
                HANYO_DAI.ATTR4 AS DMDAIBUNNAME,
                HANYO_CHU.ATTR3 AS DMCHUBUNCODE,
                HANYO_CHU.ATTR4 AS DMCHUBUNNAME,
                HANYO_SYO.ATTR3 AS DMSYOBUNCODE,
                HANYO_SYO.ATTR4 AS DMSYOBUNNAME
        FROM (
                SELECT *
                FROM {{this}} HANYO_ATTR
                WHERE KBNMEI = 'DMDAIKUBUN'
                ) HANYO_DAI
        INNER JOIN (
                SELECT *
                FROM {{this}} HANYO_ATTR
                WHERE KBNMEI = 'DMCHUKUBUN'
                ) HANYO_CHU ON HANYO_DAI.ATTR1 = HANYO_CHU.ATTR1
        INNER JOIN (
                SELECT *
                FROM {{this}} HANYO_ATTR
                WHERE KBNMEI = 'DMSYOKUBUN'
                ) HANYO_SYO ON HANYO_CHU.ATTR1 = HANYO_SYO.ATTR1
        ) HANYO
    WHERE {{ ref('jpndcledw_integration__tm06dmout') }}.DMPRNNO = HANYO.DMPRNNO;
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}