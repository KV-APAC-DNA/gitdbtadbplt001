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
TITEM AS (
    SELECT *  
    FROM {{ ref('jpndcledw_integration__tm13item_qv') }}
),
MDS_PROD AS (
    SELECT * 
    FROM {{ ref('jpndcledw_integration__edw_mds_jp_dcl_mt_h_product') }} -- JPDCLEDW_ACCESS.EDW_MDS_JP_DCL_MT_H_PRODUCT 
),
ftab as (
  select 
    dkmdg.kokyano,
    dkmdg.order_dt as firstkonyudate,
    dkmdg.channel as first_channel,
    dkmdg.h_o_item_code as first_item,
    dkmdg.h_o_item_name as first_item_name,
    dkmdg.meisainukikingaku as fisrt_item_price,
    dus.status,
    cm.year_445 as first_year_445,
    cm.month_445 as first_month_445,
    cm.ymonth_445 as first_ymonth_445,
    cm.mweek_445 as first_mweek_445

  from
    d2c_data dkmdg 
    inner join 
    	customer_status dus 
    	on dkmdg.order_dt = dus.dt and dkmdg.kokyano =dus.kokyano
    left outer join 
        calendar_445 cm 
        on dkmdg.order_dt = cm.ymd_dt
  where 
    dus.status in ('New', 'Lapsed')
    and dus.base = 'order'
    and dkmdg.meisaikbn = '商品'
    and dkmdg.juchkbn in (0,1,2)
    and dkmdg.order_dt >= to_char(extract(year from dateadd(year, -3, current_date)))  || '-01-01'
    and dkmdg.channel in ('Web','通販')
    and dkmdg.f_order = 1
    and dkmdg.gts > 0
  group by 1,2,3,4,5,6,7,8,9,10,11
),
ltv as (
SELECT
    h.kokyano,
    DATEDIFF(YEAR,(case when LENGTH(to_char(c.birthday )) <= 6 then null else to_date(to_char(c.birthday), 'yyyymmdd') end ) , current_timestamp())+
(CASE WHEN 
(case when  LENGTH(to_char(c.birthday )) <= 6 then null else to_char(to_date(to_char(c.birthday), 'yyyymmdd'), 'MMDD')end) > to_char(current_timestamp(), 'MMDD') THEN - 1 ELSE 0 END) as current_age,
    DATEDIFF(YEAR,(case when LENGTH(to_char(c.birthday )) <= 6 then null else to_date(to_char(c.birthday), 'yyyymmdd') end ), 
    to_date(f.firstkonyudate))+
   (case when  (case when LENGTH(to_char(c.birthday )) <= 6 then null else to_char(to_date(to_char(c.birthday), 'yyyymmdd'), 'MMDD')end)  > to_char(current_timestamp(), 'MMDD') THEN - 1 ELSE 0 END) as firstpurchase_age,
    
    f.firstkonyudate,
    f.first_channel,
    f.first_item,
    f.first_item_name,
    f.fisrt_item_price,
    f.first_year_445,
    f.first_month_445,
    f.first_ymonth_445,
    f.first_mweek_445,
    f.status as first_status,
    ti.settanpinsetkbn,
    ti.teikikeiyaku,
    mthp."happy bag flag",
    mthp."outlet flag",
    mthp."family sale flag",
    mthp.flag01 as product_sale_kbn,
    mthp.flag02 as acquisition_kbn,
    mthp.flag03 as product_item_kbn,
    h.order_dt,
    h.saleno,
    h.diorderid,
    datediff('day', 
            f.firstkonyudate,
            h.order_dt
             ) days_diff,
    h.channel,
    sum(nts) as sales
FROM
        d2c_data h
        INNER JOIN
            customer c on h.kokyano = c.kokyano
        INNER JOIN
            ftab f on h.kokyano = f.kokyano
        LEFT OUTER JOIN 
            TITEM ti ON f.first_item = ti.itemcode
        LEFT OUTER JOIN 
            MDS_PROD mthp ON f.first_item = mthp."ci-code"
WHERE
    days_diff >=0 and days_diff <= 365
    and h.juchkbn in (0,1,2)
    and h.gts > 0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
),
ltv_raw_data as (
select
  *
  ,dense_rank() over(partition by kokyano,firstkonyudate order by order_dt,diorderid) as now_rowno
from
  ltv
-- order by kokyano, order_dt, saleno
), 
monthly_ltv  as (-- select * from ltv_raw_data where kokyano = '4089112534'
select kokyano, month_id, sum(sales) sales from (
select kokyano, substr(firstkonyudate, 1,4) || substr(firstkonyudate, 6, 2)  as month_id,
row_number() over (partition by kokyano, saleno order by order_dt ) rowno,
current_age, sales
from ltv_raw_data k ) where rowno  = 1 group by kokyano, month_id 
),
final as (
    select 
    k.*, 
    c.birthday,
    current_timestamp()::timestamp_ntz(9) as inserted_date,
    null::varchar(100) as inserted_by ,
    current_timestamp()::timestamp_ntz(9) as updated_date,
    null::varchar(100) as updated_by
    from monthly_ltv k INNER JOIN
    customer c on k.kokyano = c.kokyano
)
select * from final

-- 4089112534
-- --limit 200-- where firstkonyudate >= '2023-01-01' and firstkonyudate <= '2023-12-31'