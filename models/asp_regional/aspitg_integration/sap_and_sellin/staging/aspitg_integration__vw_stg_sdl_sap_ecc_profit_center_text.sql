

with 

source as (

    select * from {{ source('bwa_access', 'bwa_tprofit_ctr') }}

),

final as (

    select 
            nvl(LANGU,'') as LANGU,
            nvl(CO_AREA,'') as KOKRS,
            nvl(PROFIT_CTR,'') as PRCTR,
            to_date(dateto,'YYYYmmdd') as dateto,
            to_date(datefrom,'YYYYmmdd') as datefrom,
            replace(txtsh,'–','') as txtsh,
            replace(txtmd,'–','') as txtmd,
            current_timestamp()::timestamp_ntz(9) as crt_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
        from source
        where txtsh <> ' '
            and dateto = '99991231'
            or (
                profit_ctr in (
                    '0074700707',
                    '0000000799',
                    '0000005556',
                    '0000500169'
                )
                and txtsh <> ' '
            ) and _deleted_='F'

)

select * from final
