

--import CTE
with source as(
    select * from {{ source('bwa_access', 'bwa_tsales_off') }}
),
--logical CTE
final as(
    select 
        '888' as mandt,
        nvl(langu,'')	as	spras,
        nvl(sales_off,'') as   vkbur,		
        txtsh  as bezei,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        null as file_name
    from source
    where langu='E' and _deleted_='F'
)
--final select
select * from final
