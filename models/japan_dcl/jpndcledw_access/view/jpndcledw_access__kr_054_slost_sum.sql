with kr_054_slost_sum as (
    select * from {{ ref('jpndcledw_integration__kr_054_slost_sum') }}
),

final as (
select  
lost_label as "lost_label",
lost_ym as "lost_ym",
lost_yy as "lost_yy",
lost_mm as "lost_mm",
lpoint as "lpoint"
from kr_054_slost_sum
)

select * from final