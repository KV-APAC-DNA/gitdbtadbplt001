{{
    config(
        pre_hook="{{build_pcfedw_integration__edw_invoice_fact_snapshot()}}"
    )
}}

with edw_time_dim as (
select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
edw_invoice_fact as (
select * from {{ ref('aspedw_integration__edw_invoice_fact') }}
),
dly_sls_cust_attrb_lkp as (
select * from {{ ref('pcfedw_integration__dly_sls_cust_attrb_lkp') }}
),
wks_invoice_fact_snapshot as (
select snapshot_date,

       jj_mnth_id,

       co_cd,

       cust_num,

       matl_num,

       sls_doc,

       curr_key,

       gros_trd_sls

from {{this}} eifs,

     (select cast(to_char(add_months (to_date(t1.jj_mnth_id::varchar,'YYYYMM'),- 1),'YYYYMM') As integer) as jj_period

      from edw_time_dim t1

      where t1.cal_date::date = convert_timezone('Australia/Sydney',current_timestamp())::date

      union

      select t1.jj_mnth_id as jj_period

      from edw_time_dim t1

      where t1.cal_date::date = convert_timezone('Australia/Sydney',current_timestamp())::date) etd

where eifs.jj_mnth_id = etd.jj_period ),


final_1 as (

select snapshot_date,

       jj_mnth_id,

       co_cd,

       cust_num,

       matl_num,

       sls_doc,

       curr_key,

       gros_trd_sls

from wks_invoice_fact_snapshot
),


final_2 as (

select convert_timezone ('Australia/Sydney',current_timestamp())::date as snapshot_date,

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
                
            and   a.fut_sls_qty <> 0
            
            {% if is_incremental() %}
                -- this filter will only be applied on an incremental run
                and convert_timezone ('Australia/Sydney',current_timestamp())::date > (select max(snapshot_date)::date from {{ this }}) 
            {% endif %}
            ) eif,

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

      where t1.cal_date::date = convert_timezone ('Australia/Sydney',current_timestamp())::date) etd

where etd.jj_mnth_id = orders.fisc_yr_src
),
transformed as (
select * from final_1
union all
select * from final_2
),
final as (
select
snapshot_date::date as snapshot_date,
jj_mnth_id::number(18,0) as jj_mnth_id,
co_cd::varchar(4) as co_cd,
cust_num::varchar(10) as cust_num,
matl_num::varchar(40) as matl_num,
sls_doc::varchar(50) as sls_doc,
curr_key::varchar(10) as curr_key,
gros_trd_sls::number(38,7) as gros_trd_sls,
from transformed
)
select * from final