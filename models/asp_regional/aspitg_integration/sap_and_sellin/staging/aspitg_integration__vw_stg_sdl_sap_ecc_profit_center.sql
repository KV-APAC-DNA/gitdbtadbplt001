

with 

source as (

    select * from {{ source('bwa_access', 'bwa_mprofit_ctr') }}

),

final as (

    select  
        nvl(co_area,'') as kokrs,
        nvl(profit_ctr,'') as prctr,
        TRY_TO_DATE(dateto,'YYYYMMDD') as dateto,
        TRY_TO_DATE(datefrom,'YYYYMMDD') as datefrom,
        resp_pers as verak,
        obj_curr as waers,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where resp_pers <> ' '
        and dateto = '99991231'
        or (
            profit_ctr in ('0074700707', '0000000799')
            and resp_pers <> ' '
        )
    and  _deleted_='F'

)

select * from final
