--Import CTE
with source as (
    select * from {{ source('bwa_access', 'bwa_product_hierarchy_text') }}
),

--Logical CTE

--Final CTE
final as (
    select 
        coalesce(prod_hier,'') as prod_hier,
        coalesce(langu,'') as langu,
        replace(replace(replace(txtmd,'#',''),'*',''),',','') as txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where _deleted_='F'
)

--Final select
select * from final