with 

source as (

    select * from {{ source('bwa_access', 'bwa_tcomp_code') }}

),

final as (

    select
        '888' as mandt,
        comp_code as bukrs,
        txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source

)

select * from final
