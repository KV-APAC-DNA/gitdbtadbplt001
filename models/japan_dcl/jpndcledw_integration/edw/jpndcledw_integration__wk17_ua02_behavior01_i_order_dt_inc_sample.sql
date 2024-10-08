
with dm_kesai_mart_dly_general as (
select * from {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}
),
final as (
select 
  distinct kokyano::varchar(68) as Customer_No, 
  min(order_dt)::date as I_Order_dt_inc_sample 
from 
  dm_kesai_mart_dly_general 
where 
  --channel in ('通販', 'Web', '直営・百貨店') and 
  juchkbn in (0, 1, 2) 
group by 
  1)
select * from final



