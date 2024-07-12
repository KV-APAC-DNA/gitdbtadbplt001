with dm_kesai_mart_dly_general as (
select * from {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}
) ,
item_zaiko_v as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.ITEM_ZAIKO_V
),
one_year AS (
  SELECT 
    kesai.kokyano, 
    COUNT(
      DISTINCT zaiko.bumon7_add_attr10
    ) AS order_sku_cnt_1y 
  FROM 
    dm_kesai_mart_dly_general kesai 
    LEFT JOIN item_zaiko_v zaiko ON kesai.z_item_code = zaiko.z_itemcode 
  WHERE 
    kesai.order_dt >= add_months(date_trunc(day, sysdate()),-12)
    AND zaiko.bumon7_add_attr6 = '10_商品' --this will filter out promotion product 
    AND channel IN (
      '通販', 'Web', '直営・百貨店'
    ) 
  GROUP BY 
    kesai.kokyano
), 
term_start AS(
  SELECT 
    kesai.kokyano, 
    COUNT(
      DISTINCT zaiko.bumon7_add_attr10
    ) AS order_sku_cnt_1y_term_start 
  FROM 
    dm_kesai_mart_dly_general kesai 
    LEFT JOIN item_zaiko_v zaiko ON kesai.z_item_code = zaiko.z_itemcode 
  WHERE 
    -- (first day of last year, will change once in a year) and order dt <=yyyy1231 (last day of last year)
    kesai.order_dt BETWEEN  add_months(date_trunc(year, sysdate()),-12)
    AND dateadd(day,-1,DATE_TRUNC('YEAR', sysdate())) 
    AND zaiko.bumon7_add_attr6 = '10_商品' --this will filter out promotion product 
    AND channel IN (
      '通販', 'Web', '直営・百貨店'
    ) 
  GROUP BY 
    kesai.kokyano
) ,
transformed as (
SELECT 
  a.kokyano AS Customer_No, 
  one_year.order_sku_cnt_1y AS order_sku_cnt_1y, 
  term_start.order_sku_cnt_1y_term_start 
FROM 
  dm_kesai_mart_dly_general a 
  LEFT JOIN one_year ON a.kokyano = one_year.kokyano 
  LEFT JOIN term_start ON a.kokyano = term_start.kokyano 
WHERE 
  a.order_dt >= '2019-01-01' 
  AND channel IN (
    '通販', 'Web', '直営・百貨店'
  ) 
GROUP BY 
  1, 
  2, 
  3
),
final as (
select
customer_no::varchar(60) as customer_no,
order_sku_cnt_1y::number(18,0) as order_sku_cnt_1y,
order_sku_cnt_1y_term_start::number(18,0) as order_sku_cnt_1y_term_start
from transformed
)
select * from final
