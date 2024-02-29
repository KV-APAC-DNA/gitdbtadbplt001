with source as (
    select * from {{ ref('mysitg_integration__sdl_my_monthly_sellout_sales_fact_mds_sync') }}
),
final as (
    select * from source
)
select * from final