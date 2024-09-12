with dm_kesai_mart_dly_general as (
select * from {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}
),
sub_cnt_call AS (
  SELECT 
    kokyano, 
    count(distinct saleno) AS order_cnt_call_exc_sub_1y 
  FROM 
    dm_kesai_mart_dly_general 
  WHERE 
    order_dt >= dateadd(year,-1,date_trunc('day', current_timestamp()))  
    AND channel IN ('通販') 
    AND (
      juchkbn :: text = 0 :: CHARACTER VARYING :: text 
      OR (
        juchkbn :: text = 1 :: CHARACTER VARYING :: text 
        AND sub_cnt = 1
      ) 
      OR juchkbn :: text = 2 :: CHARACTER VARYING :: text
    ) 
    AND meisainukikingaku > 0 :: numeric :: numeric(18, 0) 
  GROUP BY 
    1
), 
sub_cnt_web AS (
  SELECT 
    kokyano, 
    count(distinct saleno) AS order_cnt_web_exc_sub_1y 
  FROM 
    dm_kesai_mart_dly_general 
  WHERE 
    order_dt >= dateadd(year,-1,date_trunc('day', current_timestamp()))
    AND channel IN ('Web') 
    AND (
      juchkbn :: text = 0 :: CHARACTER VARYING :: text 
      OR (
        juchkbn :: text = 1 :: CHARACTER VARYING :: text 
        AND sub_cnt = 1
      ) 
      OR juchkbn :: text = 2 :: CHARACTER VARYING :: text
    ) 
    AND meisainukikingaku > 0 :: numeric :: numeric(18, 0) 
  GROUP BY 
    1
), 
sub_cnt_store AS (
  SELECT 
    kokyano, 
    count(distinct saleno) AS order_cnt_store_exc_sub_1y 
  FROM 
    dm_kesai_mart_dly_general 
  WHERE 
    order_dt >= dateadd(year,-1,date_trunc('day', current_timestamp())) 
    AND channel IN ('直営・百貨店') 
    AND (
      juchkbn :: text = 0 :: CHARACTER VARYING :: text 
      OR (
        juchkbn :: text = 1 :: CHARACTER VARYING :: text 
        AND sub_cnt = 1
      ) 
      OR juchkbn :: text = 2 :: CHARACTER VARYING :: text
    ) 
    AND meisainukikingaku > 0 :: numeric :: numeric(18, 0) 
  GROUP BY 
    1
) ,
transformed as (
SELECT 
  a.kokyano AS Customer_No, 
  sub_cnt_call.order_cnt_call_exc_sub_1y AS order_cnt_call_exc_sub_1y, 
  sub_cnt_web.order_cnt_web_exc_sub_1y AS order_cnt_web_exc_sub_1y, 
  sub_cnt_store.order_cnt_store_exc_sub_1y AS order_cnt_store_exc_sub_1y 
FROM 
  dm_kesai_mart_dly_general a 
  LEFT JOIN sub_cnt_call ON a.kokyano = sub_cnt_call.kokyano 
  LEFT JOIN sub_cnt_web ON a.kokyano = sub_cnt_web.kokyano 
  LEFT JOIN sub_cnt_store ON a.kokyano = sub_cnt_store.kokyano 
WHERE 
  a.order_dt >= '2019-01-01' 
  AND channel IN (
    '通販', 'Web', '直営・百貨店'
  ) 
GROUP BY 
  1, 
  2, 
  3, 
  4
),
final as (
select
customer_no::varchar(60) as  customer_no,
order_cnt_call_exc_sub_1y::number(18,0) as  order_cnt_call_exc_sub_1y,
order_cnt_web_exc_sub_1y::number(18,0) as  order_cnt_web_exc_sub_1y,
order_cnt_store_exc_sub_1y::number(18,0) as  order_cnt_store_exc_sub_1y
from transformed
)
select * from final