with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_product_group_lvl') }}
),
final as(
    select
        prod_grp_lev_key::number(10,0) as prod_grp_lev_key,
        prod_lev_desc::varchar(100) as prod_lev_desc,
        prod_lev_index::number(10,0) as prod_lev_index,
        field_key::number(10,0) as field_key,
        run_id::number(14,0) as run_id,
        current_timestamp()::timestamp_ntz(9) as create_dt,
        current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final
