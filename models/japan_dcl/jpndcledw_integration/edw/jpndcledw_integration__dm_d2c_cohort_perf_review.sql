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
    FROM {{ ref('jpndcledw_integration__dm_user_status') }}
),
calendar_445 AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cld_m') }}  -- Reference to 445 period
),
base_cohort AS (
SELECT left(Y.ymonth_445,4) || substr(Y.ymonth_445,6,2) as month_id, 
k.kokyano, 
count(distinct k.saleno) as total_order, 
sum(nts) total_nts, 
sum(gts) total_gts, 
u.status
FROM d2c_data K LEFT JOIN calendar_445 Y 
ON K.order_dt =  Y.ymd_dt
LEFT JOIN (select dt, kokyano, status from customer_status where base = 'order') U
ON K.kokyano::varchar = U.kokyano::varchar AND  K.order_dt = to_date(U.dt) 
where K.order_dt >= to_char(extract(year from dateadd(year, -3, current_date)))  || '-01-01'
and K.channel in ('Web','通販', '直営・百貨店' )
and K.gts > 0
group by month_id, k.kokyano, u.status 
),
final AS ( 
select k.month_id,  substr(c.birthday::varchar, 1,4) || substr(c.birthday::varchar, 5,2) birthday_yearmonth,
count(distinct k.kokyano) total_costomer, sum(total_order) total_order, 
sum(total_nts) total_nts, sum(total_gts) total_gts, status
from BASE_COHORT K LEFT JOIN customer C 
ON K.kokyano::varchar = C.kokyano::varchar
group by month_id, status, birthday_yearmonth 
)
SELECT *, 
current_timestamp()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by ,
current_timestamp()::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
FROM final