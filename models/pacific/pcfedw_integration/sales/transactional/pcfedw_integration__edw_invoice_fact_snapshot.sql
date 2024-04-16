



----------------------------------------------------------------------------------
-- CREATE WORK TABLE USING EDW AS SOURCE AND SELECTING ONLY REQUIRED MONTHS - CURRENCT & PREVIOUS
with edw_time_dim  as (
       select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_TIME_DIM
),
edw_invoice_fact as (
       select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_INVOICE_FACT
),
dly_sls_cust_attrb_lkp as (
       select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.DLY_SLS_CUST_ATTRB_LKP
),
wks_invoice_fact_snapshot as
(
select snapshot_date,

       jj_mnth_id,

       co_cd,

       cust_num,

       matl_num,

       sls_doc,

       curr_key,

       gros_trd_sls

from {{this}} eifs,

     (select cast(to_char(add_months (to_date(t1.jj_mnth_id,'YYYYMM'),- 1),'YYYYMM') as integer) as jj_period

      from edw_time_dim t1

      where trunc(t1.cal_date) = trunc(convert_timezone('aedt',sysdate))

      union

      select t1.jj_mnth_id as jj_period

      from edw_time_dim t1

      where trunc(t1.cal_date) = trunc(convert_timezone('aedt',sysdate))) etd

where eifs.jj_mnth_id = etd.jj_period
)
transformed as (
select snapshot_date,

       jj_mnth_id,

       co_cd,

       cust_num,

       matl_num,

       sls_doc,

       curr_key,

       gros_trd_sls

from wks_invoice_fact_snapshot



-- INSERT CURRENCT DAY SNAPSHOT DATA INTO EDW INVOICE FACT TABLE

union all
select trunc(convert_timezone ('aedt',sysdate)) as snapshot_date,

       etd.jj_mnth_id,

       orders.co_cd,

       orders.cust_num,

       orders.matl_num,

       orders.sls_doc,

       orders.curr_key,

       orders.gros_trd_sls

from (select eif.co_cd,

             eif.cust_num,

             eif.matl_num,

             eif.sls_doc,

             cast(eif.fisc_yr_src as numeric) as fisc_yr_src,

             eif.curr_key,

             sum(eif.gros_trd_sls) as gros_trd_sls

      from (select a.co_cd,

                   ltrim(a.cust_num,'0') as cust_num,

                   ltrim(a.matl_num,'0') as matl_num,

                   a.gros_trd_sls,

                   ltrim(a.sls_doc,'0') as sls_doc,

                   substring(a.fisc_yr_src,1,4) ||substring(a.fisc_yr_src,6,2) as fisc_yr_src,

                   a.curr_key,

                   a.nts_bill,

                   a.fut_sls_qty

            from edw_invoice_fact a

            where a.nts_bill <> 0

            and   a.fut_sls_qty <> 0) eif,

           (select distinct dly_sls_cust_attrb_lkp.cmp_id

            from dly_sls_cust_attrb_lkp) lkp

      where eif.co_cd = lkp.cmp_id

      group by eif.co_cd,

               eif.cust_num,

               eif.matl_num,

               eif.sls_doc,

               cast(eif.fisc_yr_src as numeric),

               eif.curr_key

      having sum(eif.nts_bill) <> 0

         and sum(eif.fut_sls_qty) <> 0) orders,

     (select distinct jj_mnth_id

      from edw_time_dim t1

      where trunc(t1.cal_date) = trunc(convert_timezone ('aedt',sysdate))) etd

where etd.jj_mnth_id = orders.fisc_yr_src
)
select * from final


