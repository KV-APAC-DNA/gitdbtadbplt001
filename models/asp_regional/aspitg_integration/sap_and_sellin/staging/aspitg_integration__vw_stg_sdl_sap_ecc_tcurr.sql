
with 

source as (

    select * from {{ source('bwa_access', 'bwa_tcurr') }}

),

final as (

    select
        mandt::number as mandt,
        kurst,
        fcurr,
        tcurr,
        gdatu,
        case when right(ukurs, 1) = '-' then concat('-', replace(ukurs, '-', '')) else ukurs end::number(20,5) as ukurs,
        ffact::number(20,5) as ffact,
        tfact::number(20,5) as tfact,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source

)

select * from final
