with wks_au_pharm_monthly as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_monthly') }}
),
wks_au_pharm_prod_acct_date_comb as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_prod_acct_date_comb') }}
),
wks_au_pharma_dstrbtn_date_range as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_dstrbtn_date_range') }}
),

wks_au_pharma_lst12_mnth_dstrbtn_check 
as (
select delvry_dt,
       acct_key,
       prod_key,
       prod_lst12_mnth_qty,
       acct_lst12_mnth_qty
from (select a.delvry_dt,
             a.acct_key,
             a.prod_key,
              count(c1.unit_qty) as prod_lst12_mnth_qty,
              count(c2.unit_qty) as acct_lst12_mnth_qty,
      from wks_au_pharm_prod_acct_date_comb a join
           wks_au_pharma_dstrbtn_date_range b
           on  a.delvry_dt = b.delvry_dt
           left join wks_au_pharm_monthly c1 on 
           c1.delvry_dt > b.lst12_mnth_delvry_dt
              and   c1.delvry_dt <= b.delvry_dt
              and   c1.prod_key = a.prod_key
              and c1.unit_qty <>0

            left join wks_au_pharm_monthly  c2 on
            c2.delvry_dt > b.lst12_mnth_delvry_dt
              and   c2.delvry_dt <= b.delvry_dt
              and   c2.acct_key = a.acct_key
              and c2.unit_qty <> 0 
              
              group by a.delvry_dt,
             a.acct_key,
             a.prod_key
              )

where delvry_dt >(select distinct dateadd(month,-24,wapm.delvry_dt)
                  from wks_au_pharm_monthly wapm,
                       (select max(delvry_dt) delvry_dt
                        from wks_au_pharm_monthly) maxd
                  where wapm.delvry_dt = maxd.delvry_dt)
)
select * from wks_au_pharma_lst12_mnth_dstrbtn_check