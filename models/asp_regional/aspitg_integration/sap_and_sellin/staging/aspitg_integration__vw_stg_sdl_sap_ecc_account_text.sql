with 

source as (

    select * from {{ source('bwa_access', 'bwa_account_text') }}

),

final as (

    select
        nvl(chrt_accts,'') as chrt_accts,
        nvl(account,'') as account,
        nvl(langu,'') as langu,
        replace(trim(txtsh),'–','') as txtsh,
        replace(trim(txtmd),'–','') as txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source 
    where _deleted_='F'
)

select * from final

