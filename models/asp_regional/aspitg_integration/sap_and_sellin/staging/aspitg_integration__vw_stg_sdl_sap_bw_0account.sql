with 

source as (

    select * from {{ source('bwa_access', 'bwa_account_attr') }}

),

final as (

    select
        coalesce(chrt_accts,'') as chrt_accts,
        coalesce(account,'') as account,
        'A' as objvers,
        '' as changed,
        coalesce(bal_flag,'') as bal_flag,
        coalesce(cstel_flag,'') as cstel_flag,
        coalesce(glacc_flag,'') as glacc_flag,
        coalesce(logsys,'') as logsys,
        coalesce(sem_posit,'') as sem_posit,
        coalesce(bic_zbravol1,'') as zbravol1,
        coalesce(bic_zbravol2,'') as zbravol2,
        coalesce(bic_zbravol3,'') as zbravol3,
        coalesce(bic_zbravol4,'') as zbravol4,
        coalesce(bic_zbravol5,'') as zbravol5,
        coalesce(bic_zbravol6,'') as glacctext,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where _deleted_='F'

)

select * from final
