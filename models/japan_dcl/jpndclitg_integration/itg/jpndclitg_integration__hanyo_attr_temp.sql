with cir07dmprn as (
    select * from  {{ ref('jpndcledw_integration__cir07dmprn') }}
),
tm06dmout as (
    select * from  {{ ref('jpndcledw_integration__tm06dmout') }}
),

ct1 AS
(
    SELECT 'DMDAIKUBUN' AS KBNMEI,
        DM.DMPRNNO AS ATTR1,
        SUBSTRING(DM.COMMENT1, 1, 40) AS ATTR2,
        '01' AS ATTR3,
        '会報誌' AS ATTR4,
        NULL AS ATTR5
    FROM CIR07DMPRN DM
    WHERE DM.COMMENT1 LIKE '%CLﾌﾟﾚﾐｱﾑ%'
      OR DM.COMMENT1 LIKE '%ｼｰﾗﾊﾞｰ%'
),


ct2 as
(
    SELECT 'DMCHUKUBUN' AS KBNMEI,
        DM.DMPRNNO AS ATTR1,
        SUBSTRING(DM.COMMENT1, 1, 40) AS ATTR2,
        TM06_NEXT.DMCHUBUNCODE AS ATTR3,
        CASE 
                WHEN DM.COMMENT1 LIKE '%CLﾌﾟﾚﾐｱﾑ%'
                    THEN REPLACE(REGEXP_SUBSTR(DM.COMMENT1, 'CLﾌﾟﾚﾐｱﾑ.*号'), 'CLﾌﾟﾚﾐｱﾑ', '')
                WHEN DM.COMMENT1 LIKE '%ｼｰﾗﾊﾞｰ%'
                    THEN REPLACE(REGEXP_SUBSTR(DM.COMMENT1, 'ｼｰﾗﾊﾞｰ.*号'), 'ｼｰﾗﾊﾞｰ', '')
                END AS ATTR4,
        NULL AS ATTR5
    FROM CIR07DMPRN DM
    INNER JOIN (
        SELECT CAST(MAX(LPAD(DMCHUBUNCODE, 3, '0')) + 1 AS VARCHAR) AS DMCHUBUNCODE
        FROM TM06DMOUT TM06DMOUT
        WHERE DMDAIBUNNAME = '会報誌'
        ) TM06_NEXT ON 1 = 1
    WHERE DM.COMMENT1 LIKE '%CLﾌﾟﾚﾐｱﾑ%'
        OR DM.COMMENT1 LIKE '%ｼｰﾗﾊﾞｰ%'
),

ct3 AS
(
    SELECT DISTINCT 'DMSYOKUBUN' AS KBNMEI,
        DM.DMPRNNO AS ATTR1,
        SUBSTRING(DM.COMMENT1, 1, 40) AS ATTR2,
        CASE 
                WHEN DM.COMMENT1 LIKE '%CLﾌﾟﾚﾐｱﾑ%'
                    THEN '0003'
                WHEN DM.COMMENT1 LIKE '%ｼｰﾗﾊﾞｰ%'
                    THEN '0001'
                END AS ATTR3,
        CASE 
                WHEN DM.COMMENT1 LIKE '%CLﾌﾟﾚﾐｱﾑ%'
                    THEN REGEXP_SUBSTR(DM.COMMENT1, 'CLﾌﾟﾚﾐｱﾑ.*号')
                WHEN DM.COMMENT1 LIKE '%ｼｰﾗﾊﾞｰ%'
                    THEN REGEXP_SUBSTR(DM.COMMENT1, 'ｼｰﾗﾊﾞｰ.*号')
                END AS ATTR4,
        NULL AS ATTR5
    FROM CIR07DMPRN DM
    WHERE DM.COMMENT1 LIKE '%CLﾌﾟﾚﾐｱﾑ%'
        OR DM.COMMENT1 LIKE '%ｼｰﾗﾊﾞｰ%'
),

final as
(
    select * from ct1
    union all
    select * from ct2
    union all
    select * from ct3
)

select * from final