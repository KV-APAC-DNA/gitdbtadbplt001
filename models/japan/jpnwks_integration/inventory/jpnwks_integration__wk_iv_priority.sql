
with wk_iv_edi as(
    select * from dev_dna_core.snapjpnwks_integration.wk_iv_edi
),
mt_iv_priority as(
    select * from {{ source('jpnedw_integration', 'mt_iv_priority') }}
),
wk as(
    select distinct to_char(invt_dt, 'YYYYMM') invt_yyyymm,
            last_day(invt_dt) invt_last_day,
            to_date(to_char(invt_dt, 'YYYYMM') || '20', 'YYYYMMDD') invt_day_20
        from wk_iv_edi
        
        union
        
        select distinct to_char(add_months(invt_dt, - 1), 'YYYYMM') invt_yyyymm,
            last_day(add_months(invt_dt, - 1)) invt_last_day,
            to_date(to_char(add_months(invt_dt, - 1), 'YYYYMM') || '20', 'YYYYMMDD') invt_day_20
        from wk_iv_edi
),
final as(
    select wk.invt_yyyymm as invt_yyyymm,
        jip.add_date as add_date,
        dateadd(day, jip.add_date::integer, wk.invt_last_day) as invt_last_day,
        dateadd(day, jip.add_date::integer, wk.invt_day_20) as invt_day_20,
        jip.priority as priority
    from  wk
    cross join mt_iv_priority jip
    where jip.delete_flag = '0'
)
select * from final