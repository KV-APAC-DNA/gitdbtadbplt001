with kr_054_cal_v as (
    select * from {{ ref('jpndcledw_integration__kr_054_cal_v') }}
),

final as(
select  
yymm as "yymm",
db_refresh_date as "db_refresh_date"
from kr_054_cal_v
)

select * from final