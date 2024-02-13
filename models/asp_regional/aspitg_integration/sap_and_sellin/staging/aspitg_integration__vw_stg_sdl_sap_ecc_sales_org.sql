
with 

source as (

    select * from {{ source('bwa_access', 'bwa_msalesorg') }}

),

final as (

    select '888' as mandt,
            salesorg as vkorg,
            stat_curr as waers,
            comp_code as bukrs,
            '' as kunnr,
            country as land1,
            currency as waers1,
            fiscvarnt as periv,
            current_timestamp()::timestamp_ntz(9) as crt_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
        from source

)

select * from final
