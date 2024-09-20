with source as 
(
    select * from {{ ref('inditg_integration__itg_mds_in_businessplan_zonewise_final') }}
)
select
    zone_name as "zone_name",
    region_name as "region_name",
    gsm as "gsm",
    inv_business_plan as "inv_business_plan",
    ret_business_plan as "ret_business_plan",
    plan_name as "plan_name",
    year as "year",
    month as "month",
    month1 as "month1"
from source
