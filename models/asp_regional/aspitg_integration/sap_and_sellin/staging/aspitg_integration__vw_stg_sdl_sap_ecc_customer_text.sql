
with 

source as (

    select * from {{ source('bwa_access', 'bwa_tcustomer') }}

),

final as (

    select
        '888' as MANDT,
        customer as kunnr,
        txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where _deleted_='F'

)

select * from final
