with dm_user_attr_spending_trend_v as (
    select * from {{ ref('jpndcledw_integration__dm_user_attr_spending_trend_v') }}
),

final as (
select yyyy as "yyyy",
    kokyano as "kokyano",
    main_channel as "main_channel",
    order_amt_term_start as "order_amt_term_start",
    order_amt_term_end as "order_amt_term_end",
    order_amt_total as "order_amt_total"
from dm_user_attr_spending_trend_v
)

select * from final