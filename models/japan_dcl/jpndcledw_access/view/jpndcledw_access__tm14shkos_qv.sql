with tm14shkos_qv as (
    select * from {{ ref('jpndcledw_integration__tm14shkos_qv') }}
),

final as(
select itemcode as "itemcode",
    kosecode as "kosecode",
    suryo as "suryo",
    kosetanka as "kosetanka",
    koseritsu as "koseritsu",
    koseanbuntanka as "koseanbuntanka",
    motoinsertdate as "motoinsertdate",
    motoupdatedate as "motoupdatedate",
    insertdate as "insertdate",
    inserttime as "inserttime",
    insertid as "insertid",
    updatedate as "updatedate",
    updatetime as "updatetime",
    updateid as "updateid"
from tm14shkos_qv
)

select * from final