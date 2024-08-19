with dm_user_status as (
    select * from {{ ref('jpndcledw_integration__dm_user_status') }}
),

final as (
select base as "base",
    kokyano as "kokyano",
    dt as "dt",
    status as "status"
from dm_user_status
)

select * from final