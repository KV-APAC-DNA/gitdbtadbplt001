--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_rg_profit_center_franchise_mapping') }}
),

--Logical CTE

--Final CTE
final as (
    select
        prod_minor::varchar(10) as prod_minor,
        profit_center::varchar(10) as profit_center,
        prod_minor_desc::varchar(100) as prod_minor_desc,
        prod_need_state::varchar(100) as prod_need_state,
        need_state::varchar(100) as need_state,
        franchise_l1::varchar(100) as franchise_l1,
        franchise_l2::varchar(100) as franchise_l2,
        franchise_l3::varchar(100) as franchise_l3,
        franchise_l4::varchar(100) as franchise_l4
    from source
)

--Final select
select * from final
