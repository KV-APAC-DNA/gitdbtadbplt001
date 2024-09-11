WITH c_tbeckesai
AS (
    SELECT *
    FROM {{ ref ('jpndclitg_integration__c_tbeckesai') }}
    ),
cil02nayol
AS (
    SELECT *
    FROM {{ ref ('jpndcledw_integration__cil02nayol') }}
    ),
hanyo_attr
AS (
    SELECT *
    FROM {{ ref ('jpndcledw_integration__hanyo_attr') }}
    ),
han
AS (
    SELECT hanyo_attr.attr1,
        hanyo_attr.attr2
    FROM hanyo_attr
    WHERE ((hanyo_attr.kbnmei)::TEXT = ('DAILYFROM'::CHARACTER VARYING)::TEXT)
    ),
han2
AS (
    SELECT TO_NUMBER(TO_CHAR(DATEADD('day', han.attr1::INTEGER, CONVERT_TIMEZONE('Asia/Tokyo', CURRENT_TIMESTAMP)), 'YYYYMMDD'), '99999999') AS attr1,
        TO_NUMBER(TO_CHAR(DATEADD('day', han.attr2::INTEGER, CONVERT_TIMEZONE('Asia/Tokyo', CURRENT_TIMESTAMP)), 'YYYYMMDD'), '99999999') AS attr2
    FROM han
    ),
un1
AS (
    SELECT DISTINCT (cil02.nayosesakino)::CHARACTER VARYING AS kokyano
    FROM cil02nayol cil02,
        han2
    WHERE (
            (cil02.lastdate >= han2.attr1)
            AND (cil02.lastdate <= han2.attr2)
            )
    ),
un2
AS (
    SELECT DISTINCT LPAD(CAST(kesai.diecusrid AS TEXT), 10, '0') AS kokyano
    FROM c_tbeckesai kesai,
        han
    WHERE TO_CHAR(kesai.dsren, 'YYYYMMDD') >= TO_CHAR(DATEADD('day', han.attr1::INTEGER, CONVERT_TIMEZONE('Asia/Tokyo', CURRENT_TIMESTAMP)), 'YYYYMMDD')
        AND TO_CHAR(kesai.dsren, 'YYYYMMDD') <= TO_CHAR(DATEADD('day', han.attr2::INTEGER, CONVERT_TIMEZONE('Asia/Tokyo', CURRENT_TIMESTAMP)), 'YYYYMMDD')
    ),
final
AS (
    SELECT *
    FROM un1
    
    UNION
    
    SELECT *
    FROM un2
    )
SELECT *
FROM final
