with dm_kesai_mart_dly_general as (
select * from {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}
),
KESAI_H_DATA_MART_MV as (
select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_mv_kizuna') }}
),
WEB as (
  select 
    distinct g.kokyano, 
    max(g.order_dt) as L_order_dt_web 
  from 
    dm_kesai_mart_dly_general g 
    left join kesai_h_data_mart_mv h On g.saleno = h.saleno 
  where 
    g.channel = 'Web' 
    and g.juchkbn in (0, 1, 2) 
    and h.smkeiroid not in ('6') 
    and g.meisainukikingaku <> 0 
  group by 
    1
), 
CAL as (
  select 
    distinct kokyano, 
    max(order_dt) as L_order_dt_call 
  from 
    dm_kesai_mart_dly_general 
  where 
    channel = '通販' 
    and juchkbn in (0, 1, 2) 
    and meisainukikingaku <> 0 
  group by 
    1
), 
STORE as (
  select 
    distinct kokyano, 
    max(order_dt) as L_order_dt_store 
  from 
    dm_kesai_mart_dly_general 
  where 
    channel = '直営・百貨店' 
    and juchkbn in (0, 1, 2) 
    and meisainukikingaku <> 0 
  group by 
    1
), 
final as(
select 
  distinct dkmd.kokyano::varchar(68) as Customer_No, 
  WEB.L_order_dt_web::date as L_order_dt_web, 
  CAL.L_order_dt_call::date as L_order_dt_call, 
  STORE.L_order_dt_store::date as L_order_dt_store 
from 
  dm_kesai_mart_dly_general dkmd 
  left join WEB on dkmd.kokyano = WEB.kokyano 
  left join CAL on dkmd.kokyano = CAL.kokyano 
  left join STORE on dkmd.kokyano = STORE.kokyano 
where 
  dkmd.channel in (
    '通販', 'Web', '直営・百貨店'
  ) 
  and dkmd.juchkbn in (0, 1, 2) 
  and dkmd.meisainukikingaku <> 0
  )
  select * from final
