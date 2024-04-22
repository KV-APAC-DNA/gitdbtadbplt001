
with edw_pacific_perenso_ims_analysis as (
select * from {{ ref('pcfedw_integration__edw_pacific_perenso_ims_analysis') }}
),
edw_time_dim as (
select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
edw_perenso_prod_probeid_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_prod_probeid_dim') }}
),
edw_perenso_account_probeid_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_account_probeid_dim') }}
),
edw_perenso_prod_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_prod_dim') }}
),
edw_perenso_account_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_account_dim') }}
),
edw_ps_msl_items as (
select * from {{ ref('pcfedw_integration__edw_ps_msl_items') }}
),
wks_au_pharm_monthly 
as (
select etd.max_cal_date as delvry_dt,
       etd.jj_mnth_id,
       eppia.prod_key,
       eppia.prod_probe_id,
       eppia.prod_desc,
       eppia.acct_key,
       eppia.acct_probe_id,
       eppia.acct_display_name,
       sum(unit_qty) as unit_qty,
       sum(nis) as nis,
       avg(list_price) list_price
from edw_pacific_perenso_ims_analysis eppia,
     (select etd.cal_date,
             etdm.max_cal_date,
             etd.jj_mnth_id
      from (select cal_date, jj_mnth_id from edw_time_dim) etd,
           (select max(cal_date) as max_cal_date,
                   jj_mnth_id
            from edw_time_dim
            group by jj_mnth_id) etdm
      where etd.jj_mnth_id = etdm.jj_mnth_id) etd
where eppia.order_type = 'Shipped Weekly'
and   eppia.delvry_dt = etd.cal_date
group by etd.max_cal_date,
         etd.jj_mnth_id,
         eppia.prod_key,
         eppia.prod_probe_id,
         eppia.prod_desc,
         eppia.acct_key,
         eppia.acct_probe_id,
         eppia.acct_display_name
)
select * from wks_au_pharm_monthly