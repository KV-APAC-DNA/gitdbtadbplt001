with dim_profile as (
    select * from {{ ref('hcposeedw_integration__dim_profile') }}
),


final as (
    select *
    from dim_profile
    where lower(created_by_id) 
    not like '%_rg'
)

select * from final