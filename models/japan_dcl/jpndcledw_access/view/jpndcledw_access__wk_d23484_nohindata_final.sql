with wk_d23484_nohindata_final as (
    select * from {{ ref('jpndcledw_integration__wk_d23484_nohindata_final') }}
),

final as (
select  
usrid as "usrid",
point as "point",
rankdt as "rankdt",
sum as "sum"
from wk_d23484_nohindata_final
)

select * from final