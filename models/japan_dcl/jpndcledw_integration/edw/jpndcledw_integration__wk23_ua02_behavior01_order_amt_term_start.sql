
with dm_kesai_mart_dly_general as (
select * from {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}
),
term_start_call AS (
SELECT
  kokyano 
 ,SUM(gts) AS order_amt_call_1y_term_start
 FROM
  dm_kesai_mart_dly_general
WHERE
             -- (first day of last year, will change once in a year) and order dt <=yyyy1231 (last day of last year)
    order_dt BETWEEN DATE_TRUNC('YEAR',sysdate()) - INTERVAL '1 YEAR' AND DATE_TRUNC('YEAR',sysdate()) - INTERVAL '1 DAY'
AND
  channel IN ('通販')
  AND (juchkbn::text = 0::CHARACTER VARYING::text OR juchkbn::text = 1::CHARACTER VARYING::text OR juchkbn::text = 2::CHARACTER VARYING::text) AND meisainukikingaku <> 0::numeric::numeric(18,0)
GROUP BY 1
 ),
term_start_web AS (
SELECT
  kokyano
 ,SUM(gts) AS order_amt_web_1y_term_start
 FROM
  dm_kesai_mart_dly_general
WHERE
             -- (first day of last year, will change once in a year) and order dt <=yyyy1231 (last day of last year)
    order_dt BETWEEN DATE_TRUNC('YEAR',sysdate()) - INTERVAL '1 YEAR' AND DATE_TRUNC('YEAR',sysdate()) - INTERVAL '1 DAY'
AND
  channel IN ('Web')
  AND (juchkbn::text = 0::CHARACTER VARYING::text OR juchkbn::text = 1::CHARACTER VARYING::text OR juchkbn::text = 2::CHARACTER VARYING::text) AND meisainukikingaku <> 0::numeric::numeric(18,0)
GROUP BY 1
 ),
term_start_store AS (
SELECT
  kokyano 
 ,SUM(gts) AS order_amt_store_1y_term_start
 FROM
  dm_kesai_mart_dly_general
WHERE
             -- (first day of last year, will change once in a year) and order dt <=yyyy1231 (last day of last year)
    order_dt BETWEEN DATE_TRUNC('YEAR',sysdate()) - INTERVAL '1 YEAR' AND DATE_TRUNC('YEAR',sysdate()) - INTERVAL '1 DAY'
AND
  channel IN ('直営・百貨店')
  AND (juchkbn::text = 0::CHARACTER VARYING::text OR juchkbn::text = 1::CHARACTER VARYING::text OR juchkbn::text = 2::CHARACTER VARYING::text) AND meisainukikingaku <> 0::numeric::numeric(18,0)
GROUP BY 1
 ),
transformed as (
SELECT 
  a.kokyano AS Customer_No,
  term_start_call.order_amt_call_1y_term_start AS order_amt_call_1y_term_start,
  term_start_web.order_amt_web_1y_term_start AS order_amt_web_1y_term_start,
  term_start_store.order_amt_store_1y_term_start AS order_amt_store_1y_term_start
  
 FROM dm_kesai_mart_dly_general a
 LEFT JOIN term_start_call ON a.kokyano=term_start_call.kokyano
 LEFT JOIN term_start_web ON a.kokyano=term_start_web.kokyano
 LEFT JOIN term_start_store ON a.kokyano=term_start_store.kokyano
 WHERE
    a.order_dt>= '2019-01-01'
 AND
  channel IN ('通販', 'Web', '直営・百貨店')
 AND (juchkbn::text = 0::CHARACTER VARYING::text OR juchkbn::text = 1::CHARACTER VARYING::text OR juchkbn::text = 2::CHARACTER VARYING::text) AND meisainukikingaku <> 0::numeric::numeric(18,0)
 GROUP BY 1,2,3,4
),
final as (
select
customer_no::varchar(60) as customer_no,
order_amt_call_1y_term_start::float as order_amt_call_1y_term_start,
order_amt_web_1y_term_start::float as order_amt_web_1y_term_start,
order_amt_store_1y_term_start::float as order_amt_store_1y_term_start 
from transformed
)
select * from final