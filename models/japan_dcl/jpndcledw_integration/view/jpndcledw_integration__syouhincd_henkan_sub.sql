with tbecitem as 
(
   select * from {{ ref('jpndclitg_integration__tbecitem') }}
),
tbecsetitem as(
    select * from  {{ ref('jpndclitg_integration__tbecsetitem') }}
),
final as(
SELECT item_hanbai.dsitemid AS itemcode,
    COALESCE(item_zaiko.dsitemid, (item_hanbai.dsitemid)::CHARACTER VARYING(65535)) AS koseiocode
FROM (
    (
        TBECITEM item_hanbai LEFT JOIN TBECSETITEM seti ON (
                (
                    (
                        (item_hanbai.diid = seti.diid)
                        AND ((item_hanbai.dsoption001)::TEXT = ('販売商品'::CHARACTER VARYING)::TEXT)
                        )
                    AND ((item_hanbai.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
                    )
                )
        ) LEFT JOIN TBECITEM item_zaiko ON (
            (
                (seti.disetitemid = item_zaiko.diid)
                AND ((item_zaiko.dsoption001)::TEXT = ('在庫商品'::CHARACTER VARYING)::TEXT)
                )
            )
    )
WHERE (
        (
            ((item_hanbai.dsoption001)::TEXT = ('販売商品'::CHARACTER VARYING)::TEXT)
            AND ((item_zaiko.dsoption001)::TEXT = ('在庫商品'::CHARACTER VARYING)::TEXT)
            )
        AND ((item_hanbai.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
        )
)
select * from final
