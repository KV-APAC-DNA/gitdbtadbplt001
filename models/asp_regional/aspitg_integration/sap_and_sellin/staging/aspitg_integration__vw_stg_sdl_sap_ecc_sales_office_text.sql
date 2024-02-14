

--import CTE
with source as(
    select * from {{ source('bwa_access', 'bwa_tsales_off') }}
),
--logical CTE
final as(
    select 
        '888' as mandt,
        langu	as	spras,
        sales_off as   vkbur,		
        txtsh  as bezei	
    from source
    where langu='E' and _deleted_='F'
)
--final select
select * from final
