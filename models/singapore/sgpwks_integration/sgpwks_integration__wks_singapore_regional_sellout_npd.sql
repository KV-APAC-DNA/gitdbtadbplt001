
with wks_singapore_regional_sellout as (
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout') }}
),
final as (select
  *,
  min(cal_date::date) over (partition by sap_parent_customer_key) as customer_min_date,
  min(cal_date::date) over (partition by country_name) as market_min_date,
  rank() over (partition by sap_parent_customer_key, pka_product_key order by cal_date) as rn_cus,
  rank() over (partition by country_name, pka_product_key order by cal_date) as rn_mkt,
  min(cal_date::date) over (partition by sap_parent_customer_key, pka_product_key) as customer_product_min_date,
  min(cal_date::date) over (partition by country_name, pka_product_key) as market_product_min_date
from wks_singapore_regional_sellout
)

select * from final