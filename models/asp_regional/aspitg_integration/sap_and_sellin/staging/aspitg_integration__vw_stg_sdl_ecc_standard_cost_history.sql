
with 

source as (

    select * from {{ source('apc_access', 'apc_mbewh') }}

),

final as (
    select 
        nvl(trim(MANDT),'') as MANDT,
        nvl(trim(MATNR),'') as MATNR,
        nvl(trim(BWKEY),'') as BWKEY,
        nvl(trim(BWTAR),'') as BWTAR,
        LFGJA,
        LFMON,
        replace(LBKUM,'*','') as LBKUM,
        replace(SALK3,'*','') as SALK3,
        VPRSV,
        replace(VERPR,'*','') as VERPR,
        replace(STPRS,'*','') as STPRS,
        PEINH,
        BKLAS,
        replace(SALKV,'*','') as SALKV,
        VKSAL,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where _deleted_='F' and mandt='888'

)

select * from final
