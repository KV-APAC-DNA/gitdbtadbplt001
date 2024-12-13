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
SELECT K.kokyano, K.month_id, 
count(case when k.f_order = 1 then 1 end) as f1, 
count(case when k.f_order = 2 then 1 end) as f2, 
count(case when k.f_order = 3 then 1 end) as f3, 
count(case when k.f_order = 4 then 1 end) as f4, 
count(case when k.f_order = 5 then 1 end) as f5, 
count(case when k.f_order > 6 then 1 end) as "f6+"
FROM (
SELECT K.f_order, 
k.kokyano, 
left(k.order_dt, 4) || substr(K.order_dt, 6,2) month_id,
row_number () over (partition by k.kokyano, k.saleno order by k.kokyano) row_no
FROM  d2c_data K LEFT JOIN (SELECT dt, kokyano, status FROM customer_status where base = 'order') U
ON K.kokyano = U.kokyano AND  K.order_dt = to_date(U.dt) 
WHERE K.order_dt >= to_char(extract(year FROM dateadd(year, -3, current_date)))  || '-01-01' 
and K.gts > 0 
and K.meisaikbn = '商品'
and K.juchkbn in (0,1,2)
and u.status in ('New', 'Lapsed')
and K.f_order is not null  
) K where row_no = 1 
group by k.kokyano, month_id
),

final AS (
SELECT K.month_id,
substr(C.birthday::varchar, 1,4) || substr(c.birthday::varchar, 5,2) birthday_yearmonth,
sum(f1) f1, sum(f2) f2, sum(f3) f3, sum(f4) f4, sum(f5) f5, sum("f6+") "f6+"
FROM f_orders_data K
LEFT JOIN 
    (SELECT C.kokyano, C.birthday FROM customer C
    where length(c.birthday::varchar) = 8) C   
ON K.kokyano = C.kokyano
group by K.month_id, birthday_yearmonth 
) 

SELECT *,
current_timestamp()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by ,
current_timestamp()::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
FROM final