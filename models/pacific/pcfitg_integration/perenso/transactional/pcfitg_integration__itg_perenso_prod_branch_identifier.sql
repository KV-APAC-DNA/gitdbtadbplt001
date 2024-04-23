with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_prod_branch_identifier') }}
),
final as(
    select
        prod_key::number(10,0) as prod_key,
        branch_key::number(10,0) as branch_key,
        identifier::varchar(250) as identifier,
        run_id::number(14,0) as run_id,
        current_timestamp()::timestamp_ntz(9) as create_dt,
        current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final

