WITH tbecitem
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__tbecitem')}}
    ),
final
AS (
    SELECT item.dsitemid,
        item.diid
    FROM tbecitem item
    WHERE ((item.dsoption001)::TEXT = ('販売商品'::CHARACTER VARYING)::TEXT)
    )
SELECT *
FROM final
