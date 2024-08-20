with dim_profile as (
    select * from DEV_DNA_CORE.HCPOSEEDW_INTEGRATION.DIM_PROFILE
),


final as (
    select *
    from dim_profile
    where lower(created_by_id) 
    not like '%_rg'
)

select * from final