with source as 
(
    select * from {{ ref('inditg_integration__itg_mds_in_businessplan_channel_final') }}
)
select
    region as "region",
    gsm as "gsm",
    channel_name as "channel_name",
    plan as "plan",
    year as "year",
    month as "month",
    month1 as "month1"
from source
