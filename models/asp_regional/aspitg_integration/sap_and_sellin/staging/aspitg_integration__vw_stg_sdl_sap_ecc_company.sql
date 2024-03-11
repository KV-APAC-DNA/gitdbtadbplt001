

with 
source as (

    select * from {{ source('bwa_access', 'bwa_mcomp_code') }}

),

final as (

    select
        '888' as mandt,
        nvl(comp_code,'') as bukrs,
        country as land1,
        currency as waers,
        chrt_accts as ktopl,
        c_ctr_area as kkber,
        fiscvarnt as periv,
        company as rcomp,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where _deleted_='F'
)

select * from final
