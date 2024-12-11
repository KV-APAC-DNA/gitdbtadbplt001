with d2c_data AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}  -- Reference to the d2c order transaction data
),
customer as (
    SELECT * 
    FROM {{ ref('jpndcledw_integration__cim01kokya') }}
),
customer_status AS (
    SELECT * 
    FROM {{ ref('jpndcledw_integration__dm_user_status') }} --is this purchase funnel only for new customer or include existing one?
),
calendar_445 AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cld_m') }}  -- Reference to 445 period
), 
f_orders_data AS (
SELECT left(Y.ymonth_445,4) || substr(Y.ymonth_445,6,2) as month_id, k.kokyano,
sum(case when k.f_order = 1 then k.nts end) as f1,
sum(case when k.f_order = 2 then k.nts end) as f2, 
sum(case when k.f_order = 3 then k.nts end) as f3, 
sum(case when k.f_order = 4 then k.nts end) as f4, 
sum(case when k.f_order = 5 then k.nts end) as f5, 
sum(case when k.f_order = 6 then k.nts end) as "f6+"
FROM d2c_data K LEFT JOIN calendar_445 Y 
ON K.order_dt =  Y.ymd_dt
LEFT JOIN (select dt, kokyano, status from customer_status where base = 'order') U
ON K.kokyano::varchar = U.kokyano::varchar AND  K.order_dt = to_date(U.dt) 
WHERE K.order_dt >= to_char(extract(year from dateadd(year, -3, current_date)))  || '-01-01' 
and K.gts > 0
and u.status in ('New', 'Lapsed')
GROUP BY k.kokyano,month_id
),
final as  (
SELECT k.month_id, birthday_yearmonth, count(distinct k.kokyano) total_customer,
sum(f1) f1, sum(f2) f2, sum(f3) f3, sum(f4) f4, sum(f5) f5, sum("f6+") "f6+"    
FROM ( 
    SELECT k.*, substr(c.birthday::varchar, 1,4) || substr(c.birthday::varchar, 5,2) birthday_yearmonth 
    FROM f_orders_data K LEFT JOIN customer C 
    ON K.kokyano = C.kokyano
    ) K
GROUP BY k.month_id, birthday_yearmonth 
ORDER BY k.month_id desc, birthday_yearmonth
) 

SELECT *,
current_timestamp()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by ,
current_timestamp()::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
from final