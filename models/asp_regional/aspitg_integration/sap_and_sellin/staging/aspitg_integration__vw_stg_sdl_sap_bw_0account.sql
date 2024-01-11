with 

source as (

    select * from {{ source('bwa_access', 'bwa_account_attr') }}

),

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
        bic_zbravol6 as glacctext,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source

)

select * from final
