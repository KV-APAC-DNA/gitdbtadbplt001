with wks_au_pharm_monthly as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_monthly') }}
),
wks_au_pharm_prod_acct_date_comb as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_prod_acct_date_comb') }}
),
wks_au_pharma_dstrbtn_date_range as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_dstrbtn_date_range') }}
),
wks_au_pharma_lst3_mnth_dstrbtn_check 
as (
select delvry_dt,
       acct_key,
       prod_key,
       prod_lst3_mnth_qty,
       acct_lst3_mnth_qty
from (select a.delvry_dt,
             a.acct_key,
             a.prod_key,
             (select count(c.unit_qty)
              from wks_au_pharm_monthly c
              where c.delvry_dt > b.lst3_mnth_delvry_dt
              and   c.delvry_dt <= b.delvry_dt
              and   c.prod_key = a.prod_key
              and   c.unit_qty <> 0) as prod_lst3_mnth_qty,
             (select count(c.unit_qty)
              from wks_au_pharm_monthly c
              where c.delvry_dt > b.lst3_mnth_delvry_dt
              and   c.delvry_dt <= b.delvry_dt
              and   c.acct_key = a.acct_key
              and   c.unit_qty <> 0) as acct_lst3_mnth_qty
      from wks_au_pharm_prod_acct_date_comb a,
           wks_au_pharma_dstrbtn_date_range b
      where a.delvry_dt = b.delvry_dt)
where delvry_dt >(select distinct dateadd(month,-24,wapm.delvry_dt)
                  from wks_au_pharm_monthly wapm,
                       (select max(delvry_dt) delvry_dt
                        from wks_au_pharm_monthly) maxd
                  where wapm.delvry_dt = maxd.delvry_dt)
)
select * from wks_au_pharma_lst3_mnth_dstrbtn_check