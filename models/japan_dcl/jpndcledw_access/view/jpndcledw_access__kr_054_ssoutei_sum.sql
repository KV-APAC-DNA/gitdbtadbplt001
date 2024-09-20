with kr_054_ssoutei_sum as (
    select * from {{ ref('jpndcledw_integration__kr_054_ssoutei_sum') }}
),

final as (
select  
fuyo_label as "fuyo_label",
point_ym as "point_ym",
point_yy as "point_yy",
point_mm as "point_mm",
point as "point"
from kr_054_ssoutei_sum
)

select * from final