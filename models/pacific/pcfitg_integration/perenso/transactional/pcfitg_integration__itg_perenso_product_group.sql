with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_product_group') }}
),
final as(
    select
        prod_grp_key::number(10,0) as prod_grp_key,
        prod_grp_lev_key::number(10,0) as prod_grp_lev_key,
        prod_grp_desc::varchar(100) as prod_grp_desc,
        dsp_order::number(10,0) as dsp_order,
        parent_key::number(10,0) as parent_key,
        run_id::number(14,0) as run_id,
        current_timestamp()::timestamp_ntz(9) as create_dt,
        current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final
