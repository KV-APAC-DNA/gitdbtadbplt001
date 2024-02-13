--Import CTE
with source as (
    select * from {{ source('bwa_access', 'bwa_product_hierarchy_text') }}
),

--Logical CTE

--Final CTE
final as (
    select 
        prod_hier,
        langu,
        replace(replace(replace(txtmd,'#',''),'*',''),',','') as txtmd
    from source
)

--Final select
select * from final