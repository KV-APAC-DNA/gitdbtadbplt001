
with wks_au_pharm_monthly as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_monthly') }}
),


---------------------------------------------------------------------------------
--	date range for each delivery dates. we are considering last 24 months.
--	for each delivery date of a month identify rolling months(dates) - last month, last three month, last six month and last twelve month dates

wks_au_pharma_dstrbtn_date_range as (

--identify the list of rolling month for each dates

(select distinct delvry_dt as delvry_dt,
       dateadd(month,-1,delvry_dt) lst_mnth_delvry_dt,
       dateadd(month,-3,delvry_dt) lst3_mnth_delvry_dt,
       dateadd(month,-6,delvry_dt) lst6_mnth_delvry_dt,
       dateadd(month,-12,delvry_dt) lst12_mnth_delvry_dt
from wks_au_pharm_monthly)
)
select * from wks_au_pharma_dstrbtn_date_range