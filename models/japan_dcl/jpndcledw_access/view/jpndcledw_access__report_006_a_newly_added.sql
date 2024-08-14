with report_006_a_newly_added as (
    select * from {{ ref('jpndcledw_integration__report__006_a_newly_added') }}
),

final as (
select  
channel_name as "channel_name",
channel_id as "channel_id",
yymm as "yymm",
total as "total",
day_of_week as "day_of_week",
report_exec_date as "report_exec_date",
year_445 as "year_445",
month_445 as "month_445",
month_445_nm as "month_445_nm"
from report_006_a_newly_added
)

select * from final