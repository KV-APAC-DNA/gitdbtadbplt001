with cim08shkos_bunkai_qv as (
    select * from {{ ref('jpndcledw_integration__cim08shkos_bunkai_qv') }}
),

final as (
select itemcode as "itemcode",
    itemname as "itemname",
    itemcname as "itemcname",
    tanka as "tanka",
    chubuncode as "chubuncode",
    chubunname as "chubunname",
    chubuncname as "chubuncname",
    daibuncode as "daibuncode",
    daibunname as "daibunname",
    daibuncname as "daibuncname",
    daidaibuncode as "daidaibuncode",
    daidaibunname as "daidaibunname",
    daidaibuncname as "daidaibuncname",
    bunkai_itemcode as "bunkai_itemcode",
    bunkai_itemname as "bunkai_itemname",
    bunkai_itemcname as "bunkai_itemcname",
    bunkai_tanka as "bunkai_tanka",
    bunkai_kossu as "bunkai_kossu",
    bunkai_kosritu as "bunkai_kosritu",
    insertdate as "insertdate"
from cim08shkos_bunkai_qv
)

select * from final