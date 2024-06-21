with source as
(
    select * from {{ ref('inditg_integration__itg_xdm_businesscalendar') }}
),
final as 
(
    SELECT a.salinvdate AS caldate,
       CAST(SUBSTRING(a.salinvdate,1,4) ||SUBSTRING (a.salinvdate,6,2) ||SUBSTRING (a.salinvdate,9,2) AS INTEGER) AS day,
       CAST(SUBSTRING(a.week,1,1) AS INTEGER) AS week,
       CAST((a.year||lpad (monthkey,2,0)) AS INTEGER) AS mth_mm,
       monthkey AS mth_yyyymm,
       CASE
         WHEN MONTH IN ('January','February','March') THEN 1
         WHEN MONTH IN ('April','May','June') THEN 2
         WHEN MONTH IN ('July','August','September') THEN 3
         WHEN MONTH IN ('October','November','December') THEN 4
         ELSE 0
       END qtr,
       CAST(a.year|| (CASE WHEN MONTH IN ('January','February','March') THEN 1 WHEN MONTH IN ('April','May','June') THEN 2 WHEN MONTH IN ('July','August','September') THEN 3 WHEN MONTH IN ('October','November','December') THEN 4 ELSE 0 END) AS INTEGER) AS yyyyqtr,
       CAST(SUBSTRING(a.salinvdate,1,4) AS INTEGER) cal_yr,
       a.year AS fisc_yr,
       getdate() AS crt_dttm,
       getdate() AS updt_dttm,
       MONTH AS month_nm,
       SUBSTRING(MONTH,1,3) AS month_nm_shrt
        FROM source a 
        WHERE a.year >= 2015
)
select 
    caldate::timestamp_ntz(9) as caldate,
    day::number(18,0) as day,
    week::number(18,0) as week,
    mth_mm::number(18,0) as mth_mm,
    mth_yyyymm::number(18,0) as mth_yyyymm,
    qtr::number(18,0) as qtr,
    yyyyqtr::number(18,0) as yyyyqtr,
    cal_yr::number(18,0) as cal_yr,
    fisc_yr::number(18,0) as fisc_yr,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    month_nm::varchar(20) as month_nm,
    month_nm_shrt::varchar(3) as month_nm_shrt
from final