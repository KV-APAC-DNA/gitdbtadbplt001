with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_product_group_reln') }}
),
final as(
    select
        prod_key::number(10,0) as prod_key,
        prod_grp_key::number(10,0) as prod_grp_key,
        run_id::number(14,0) as run_id,
        current_timestamp()::timestamp_ntz(9) as create_dt,
        current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final
