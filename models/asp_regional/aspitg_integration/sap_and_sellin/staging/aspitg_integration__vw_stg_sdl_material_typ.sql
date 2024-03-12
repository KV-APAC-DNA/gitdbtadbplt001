--Import CTE
with source as (
    select * from {{ source('bwa_access', 'bwa_material_type_text') }}
),

--Final CTE
final as (
    select 
        nvl(matl_type,'') as matl_type,
        nvl(langu,'') as langu,
        txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where _deleted_='F'
)

--Final select
select * from final