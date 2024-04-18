
with
wks_au_pharm_monthly as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_monthly') }} 
),
wks_au_pharma_dstrbtn_date_range_filter as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_dstrbtn_date_range') }}
    where delvry_dt >(select distinct dateadd(month,-24,wapm.delvry_dt)
                  from wks_au_pharm_monthly wapm,
                       (select max(delvry_dt) delvry_dt
                        from wks_au_pharm_monthly) maxd
                  where wapm.delvry_dt = maxd.delvry_dt)
)
select * from wks_au_pharma_dstrbtn_date_range_filter
