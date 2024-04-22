with wks_au_pharm_monthly as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_monthly') }}
),
wks_au_pharm_prod_acct_date_comb as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_prod_acct_date_comb') }}
),
wks_au_pharma_dstrbtn_date_range as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_dstrbtn_date_range') }}
),
wks_au_pharma_curnt_mnth 
as (
select delvry_dt,
       acct_key,
       prod_key,
       curnt_mnth_nis,
       curnt_mnth_qty,
	     curnt_mnth_avg_lp
from (select a.delvry_dt,
             a.acct_key,
             a.prod_key,
             (select sum(c.nis)
              from wks_au_pharm_monthly c
              where c.delvry_dt = b.delvry_dt
              and   c.acct_key = a.acct_key
              and   c.prod_key = a.prod_key) as curnt_mnth_nis,
             (select sum(c.unit_qty)
              from wks_au_pharm_monthly c
              where c.delvry_dt = b.delvry_dt
              and   c.acct_key = a.acct_key
              and   c.prod_key = a.prod_key) as curnt_mnth_qty,
			  (select avg(c.list_price)
              from wks_au_pharm_monthly c
              where c.delvry_dt = b.delvry_dt
              and   c.acct_key = a.acct_key
              and   c.prod_key = a.prod_key) as curnt_mnth_avg_lp
      from wks_au_pharm_prod_acct_date_comb a,
           wks_au_pharma_dstrbtn_date_range b
      where a.delvry_dt = b.delvry_dt)
where delvry_dt >(select distinct dateadd(month,-24,wapm.delvry_dt)
                  from wks_au_pharm_monthly wapm,
                       (select max(delvry_dt) delvry_dt
                        from wks_au_pharm_monthly) maxd
                  where wapm.delvry_dt = maxd.delvry_dt)
)
select * from wks_au_pharma_curnt_mnth