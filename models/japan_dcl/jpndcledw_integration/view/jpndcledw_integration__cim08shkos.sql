with TBECSETITEM as 
(
    select * from SNAPJPDCLITG_INTEGRATION.TBECSETITEM
),
TBECITEM
as(
    select * from SNAPJPDCLITG_INTEGRATION.TBECITEM
),final as
(

SELECT item.dsitemid AS itemcode,
    item_kosei.dsitemid AS koseiocode,
    COALESCE(((seti.diitemnum)::NUMERIC)::NUMERIC(18, 0), (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(38, 18)) AS suryo
FROM (
    (
        TBECSETITEM seti JOIN TBECITEM item ON (
                (
                    (
                        (seti.diid = item.diid)
                        AND ((item.dsoption001)::TEXT = ('販売商品'::CHARACTER VARYING)::TEXT)
                        )
                    AND ((item.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
                    )
                )
        ) JOIN TBECITEM item_kosei ON (
            (
                (
                    (seti.disetitemid = item_kosei.diid)
                    AND ((item_kosei.dsoption001)::TEXT = ('販売商品'::CHARACTER VARYING)::TEXT)
                    )
                AND ((item.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
                )
            )
    )
)
select * from final 