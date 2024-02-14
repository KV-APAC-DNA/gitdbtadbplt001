--Import CTE
with source as (
    select * from {{ source('bwa_access', 'bwa_account_attr') }}
),

--Logical CTE

--Final CTE
final as (
    select 
        chrt_accts,
        account,
        'A' as objvers,
        '' as changed,
        bal_flag,
        cstel_flag,
        glacc_flag,
        logsys,
        sem_posit,
        bic_zbravol1 as zbravol1,
        bic_zbravol2 as zbravol2,
        bic_zbravol3 as zbravol3,
        bic_zbravol4 as zbravol4,
        bic_zbravol5 as zbravol5,
        bic_zbravol6 as zbravol6,
        bic_zciwhl1 as zciwhl1,
        bic_zciwhl2 as zciwhl2,
        bic_zciwhl3 as zciwhl3,
        bic_zciwhl4 as zciwhl4,
        bic_zciwhl5 as zciwhl5,
        bic_zciwhl6 as zciwhl6,
        '' as zpb_gl_ty,
        null as filename,
        null as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)

--Final select
select * from final
