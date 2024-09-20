with kr_new_stage_point as (
    select * from {{ ref('jpndcledw_integration__kr_new_stage_point') }}
),

final as (
select yyyymm as "yyyymm",
    kokyano as "kokyano",
    usrid as "usrid",
    thistotalprc as "thistotalprc",
    stage as "stage",
    thispoint as "thispoint",
    prevpoint as "prevpoint",
    point as "point",
    insertdate as "insertdate"
from kr_new_stage_point
)

select * from final