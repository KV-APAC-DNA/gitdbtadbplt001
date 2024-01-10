with 

source as (

    select * from {{ source('bwa_access', 'bwa_account_text') }}

),

final as (

    select
        chrt_accts,
        account,
        langu,
        txtsh,
        txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm

    from source

)

select * from final
