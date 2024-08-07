WITH tm62item_nm
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__tm62item_nm')}}
    ),
tm62item_nm_add_qv
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__tm62item_nm_add_qv')}}
    ),
final
AS (
    SELECT tm62item_nm.code,
        tm62item_nm.name
    FROM tm62item_nm
    
    UNION ALL
    
    SELECT tm62item_nm_add_qv.code,
        tm62item_nm_add_qv.name
    FROM tm62item_nm_add_qv
    )
SELECT *
FROM final
