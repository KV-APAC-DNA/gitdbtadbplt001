with dm_kesai_mart_dly_general as (
select * from {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}
),
this_yr_call AS (
  SELECT 
    kokyano, 
    count(distinct saleno) as Order_Cnt_call_this_year 
  FROM 
    dm_kesai_mart_dly_general 
  WHERE 
    order_dt BETWEEN DATE_TRUNC('YEAR', SYSDATE()) 
    AND SYSDATE() 
    AND channel IN ('通販') 
    AND (
      juchkbn :: text = 0 :: CHARACTER VARYING :: text 
      OR juchkbn :: text = 1 :: CHARACTER VARYING :: text 
      OR juchkbn :: text = 2 :: CHARACTER VARYING :: text
    ) 
    AND meisainukikingaku <> 0 :: numeric :: numeric(18, 0) 
  GROUP BY 
    1
), 
this_yr_web AS (
  SELECT 
    kokyano, 
    count(distinct saleno) as Order_Cnt_web_this_year 
  FROM 
    dm_kesai_mart_dly_general 
  WHERE 
    order_dt BETWEEN DATE_TRUNC('YEAR', SYSDATE()) 
    AND SYSDATE() 
    AND channel IN ('Web') 
    AND (
      juchkbn :: text = 0 :: CHARACTER VARYING :: text 
      OR juchkbn :: text = 1 :: CHARACTER VARYING :: text 
      OR juchkbn :: text = 2 :: CHARACTER VARYING :: text
    ) 
    AND meisainukikingaku <> 0 :: numeric :: numeric(18, 0) 
  GROUP BY 
    1
), 
this_yr_store AS (
  SELECT 
    kokyano, 
    count(distinct saleno) as Order_Cnt_store_this_year 
  FROM 
    dm_kesai_mart_dly_general 
  WHERE 
    order_dt BETWEEN DATE_TRUNC('YEAR', SYSDATE()) 
    AND SYSDATE() 
    AND channel IN ('直営・百貨店') 
    AND (
      juchkbn :: text = 0 :: CHARACTER VARYING :: text 
      OR juchkbn :: text = 1 :: CHARACTER VARYING :: text 
      OR juchkbn :: text = 2 :: CHARACTER VARYING :: text
    ) 
    AND meisainukikingaku <> 0 :: numeric :: numeric(18, 0) 
  GROUP BY 
    1
),
transformed as (
SELECT 
  a.kokyano AS Customer_No, 
  this_yr_call.Order_Cnt_call_this_year AS Order_Cnt_call_this_year, 
  this_yr_web.Order_Cnt_web_this_year AS Order_Cnt_web_this_year, 
  this_yr_store.Order_Cnt_store_this_year AS Order_Cnt_store_this_year 
FROM 
  dm_kesai_mart_dly_general a 
  LEFT JOIN this_yr_call ON a.kokyano = this_yr_call.kokyano 
  LEFT JOIN this_yr_web ON a.kokyano = this_yr_web.kokyano 
  LEFT JOIN this_yr_store ON a.kokyano = this_yr_store.kokyano 
WHERE 
  a.order_dt >= '2019-01-01' 
  AND channel IN (
    '通販', 'Web', '直営・百貨店'
  ) 
  AND (
    juchkbn :: text = 0 :: CHARACTER VARYING :: text 
    OR juchkbn :: text = 1 :: CHARACTER VARYING :: text 
    OR juchkbn :: text = 2 :: CHARACTER VARYING :: text
  ) 
  AND meisainukikingaku <> 0 :: numeric :: numeric(18, 0) 
GROUP BY 
  1, 
  2, 
  3, 
  4
  ),
final as (
select
customer_no::varchar(60) as customer_no,
order_cnt_call_this_year::number(18,0) as order_cnt_call_this_year,
order_cnt_web_this_year::number(18,0) as order_cnt_web_this_year,
order_cnt_store_this_year::number(18,0) as order_cnt_store_this_year 
from transformed
)
select * from final
