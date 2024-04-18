
with
wks_au_pharm_monthly as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_monthly') }} 
),
wks_au_pharm_prod_acct_date_comb_filter as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_prod_acct_date_comb') }}
    where delvry_dt >(select distinct dateadd(month,-24,wapm.delvry_dt)
                  from wks_au_pharm_monthly wapm,
                       (select max(delvry_dt) delvry_dt
                        from wks_au_pharm_monthly) maxd
                  where wapm.delvry_dt = maxd.delvry_dt)
)
select * from wks_au_pharm_prod_acct_date_comb_filter
