with source as 
(
    select * from {{ ref('inditg_integration__itg_mds_in_businessplan_brand_final') }}
)
select
    year as "year",
    month as "month",
    gsm as "gsm",
    region as "region",
    variant_tableau as "variant_tableau",
    variant_name as "variant_name",
    franchise_dsr as "franchise_dsr",
    brand_dsr as "brand_dsr",
    business_plan as "business_plan",
    date as "date"
from source
