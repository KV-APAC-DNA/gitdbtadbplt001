with kr_054_plyotei_sum as (
    select * from {{ ref('jpndcledw_integration__kr_054_plyotei_sum') }}
),

final as (
select  
yotei_label as "yotei_label",
yotei_ym as "yotei_ym",
yotei_yy as "yotei_yy",
yotei_mm as "yotei_mm",
point_ym as "point_ym",
point_yy as "point_yy",
point_mm as "point_mm",
ypoint as "ypoint"
from kr_054_plyotei_sum
)

select * from final