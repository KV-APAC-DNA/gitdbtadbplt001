with edw_pacific_perenso_ims_analysis as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PACIFIC_PERENSO_IMS_ANALYSIS 
),
edw_time_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_TIME_DIM--matching
),
EDW_PERENSO_PROD_DIM as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_PROD_DIM--2119
),
EDW_PERENSO_ACCOUNT_METCASH_DIM as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_ACCOUNT_METCASH_DIM--3359
),
EDW_PERENSO_ACCOUNT_DIM as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_ACCOUNT_DIM--14622
),
edw_ps_msl_items as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PS_MSL_ITEMS
),
wks_au_metcash_monthly as (
--	aggregate the weekly pharmacy data into month.
--	sum: nis, qty
--	avg: list price
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
       case when sum(unit_qty) = '0'
            then 0
            else cast(cast(sum(nis) as numeric(10,2))/sum(unit_qty) as numeric(10,2)) 
       end list_price
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
where eppia.order_type = 'Metcash Weekly'
and   eppia.delvry_dt = etd.cal_date
group by etd.max_cal_date,
         etd.jj_mnth_id,
         eppia.prod_key,
         eppia.prod_probe_id,
         eppia.prod_desc,
         eppia.acct_key,
         eppia.acct_probe_id,
         eppia.acct_display_name)
select * from wks_au_metcash_monthly
