with 
wks_au_pharm_monthly as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_monthly') }}
),
final as (
    select a.delvry_dt,
             a.acct_key,
             a.prod_key,
             (select count(c.unit_qty)
              from wks_au_pharm_monthly c
              where c.delvry_dt = a.delvry_dt
              and   c.prod_key = a.prod_key
              and   c.unit_qty <> 0) as prod_curnt_mnth_qty,
             (select count(c.unit_qty)
              from wks_au_pharm_monthly c
              where c.delvry_dt = a.delvry_dt
              and   c.acct_key = a.acct_key
              and   c.unit_qty <> 0) as acct_curnt_mnth_qty
      from {{ ref('temp3') }} as a
)
select * from final