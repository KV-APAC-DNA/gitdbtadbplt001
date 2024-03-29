
--Import CTE
with source as (
    select * from {{ source('apc_access', 'apc_tvm2t') }}
),

--Logical CTE

--Final CTE
final as (
    select 
        mandt,
        nvl(spras,'') as spras,
        nvl(mvgr2,'') as mvgr2,
        bezei,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
        where spras = 'E' and _deleted_='F' and mandt='888'
)

--Final select
select * from final