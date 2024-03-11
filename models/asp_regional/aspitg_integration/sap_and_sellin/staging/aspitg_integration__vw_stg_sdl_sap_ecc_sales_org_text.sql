
with 

source as (

    select * from {{ source('bwa_access', 'bwa_tsalesorg') }}

),

final as (

    select 
        '888' as mandt,
        nvl(langu,'') as spras,
        nvl(salesorg,'') as vkorg,
        txtlg as vtext,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
        where langu = 'E' and _deleted_='F'

)

select * from final
