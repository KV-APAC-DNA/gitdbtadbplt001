with wks_au_pharm_monthly as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_monthly') }}
),
wks_au_pharm_prod_acct_date_comb as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_prod_acct_date_comb') }}
),
wks_au_pharma_dstrbtn_date_range as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_dstrbtn_date_range') }}
),
wks_joined as (
select to_date(a.delvry_dt) as delvry_dt,
             a.acct_key,
             a.prod_key,
             to_date(b.lst6_mnth_delvry_dt) as lst6_mnth_delvry_dt
from wks_au_pharm_prod_acct_date_comb a,
    wks_au_pharma_dstrbtn_date_range b
      where a.delvry_dt = b.delvry_dt
),
c1 as (
select to_date(c.delvry_dt) as delvry_dt,c.acct_key,count(c.unit_qty) as unit_qty
              from wks_au_pharm_monthly c
              where c.unit_qty <> 0
           group by   c.delvry_dt,c.acct_key 
),
c2 as (
select to_date(c.delvry_dt) as delvry_dt,c.prod_key,count(c.unit_qty) as unit_qty
              from wks_au_pharm_monthly c
              where c.unit_qty <> 0
           group by   c.delvry_dt,c.prod_key 
),

wks_au_pharma_lst6_mnth_dstrbtn_check_prod as (
select delvry_dt,
       acct_key,
       prod_key,
       sum(prod_lst6_mnth_qty) as prod_lst6_mnth_qty
from (select a.delvry_dt,
             a.acct_key,
             a.prod_key,
             c2.unit_qty as prod_lst6_mnth_qty
      from wks_joined a
      left join c2
      on c2.delvry_dt > a.lst6_mnth_delvry_dt
              and   c2.delvry_dt <= a.delvry_dt
              and   c2.prod_key = a.prod_key 
      )
      group by delvry_dt,
       acct_key,
       prod_key
),

wks_au_pharma_lst6_mnth_dstrbtn_check_acct as (
select delvry_dt,
       acct_key,
       prod_key,
       sum(acct_lst6_mnth_qty)  as acct_lst6_mnth_qty
from (select a.delvry_dt,
             a.acct_key,
             a.prod_key,
             c1.unit_qty as acct_lst6_mnth_qty
      from wks_joined a
        left join c1
      on c1.delvry_dt > a.lst6_mnth_delvry_dt
              and   c1.delvry_dt <= a.delvry_dt
              and   c1.acct_key = a.acct_key
      )
      group by delvry_dt,
       acct_key,
       prod_key
),

final as (
select p.delvry_dt::timestamp_ntz(9) as delvry_dt,
       p.acct_key,
       p.prod_key,
       case when p.prod_lst6_mnth_qty is null 
        then 0 else p.prod_lst6_mnth_qty end as prod_lst6_mnth_qty,
       case when a.acct_lst6_mnth_qty is null 
        then 0 else a.acct_lst6_mnth_qty end as acct_lst6_mnth_qty
from wks_au_pharma_lst6_mnth_dstrbtn_check_prod p,
    wks_au_pharma_lst6_mnth_dstrbtn_check_acct a
    where p.delvry_dt = a.delvry_dt
      and p.acct_key = a.acct_key
      and  p.prod_key = a.prod_key
      )

select * from final