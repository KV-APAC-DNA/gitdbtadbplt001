--Import CTE
with source as (
    select * from {{ source('bwa_access', 'bwa_material_type_text') }}
),

--Final CTE
final as (
    select 
        matl_type,
        langu,
        txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

--Final select
select * from final