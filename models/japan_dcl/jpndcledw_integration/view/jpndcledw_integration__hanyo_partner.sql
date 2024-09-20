WITH edw_mds_jp_dcl_partner_master
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__edw_mds_jp_dcl_partner_master') }}
    ),
final
AS (
    SELECT 'TOKUI' AS kbnmei,
        tokuicode AS attr1,
        column33 AS attr2,
        column34 AS attr3,
        column35 AS attr4,
        column36 AS attr5,
        NULL AS insertdate,
        NULL AS updatedate
    FROM edw_mds_jp_dcl_partner_master
    WHERE (column33 IS NOT NULL)
    )
SELECT *
FROM

final
