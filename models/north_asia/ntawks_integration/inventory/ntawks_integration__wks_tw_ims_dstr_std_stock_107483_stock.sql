with source as(
    select * from {{ source('ntasdl_raw', 'sdl_tw_ims_dstr_std_stock_107483_stock')}}  where filename not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_tw_ims_dstr_std_stock_107483_stock__test_date_format_odd_eve_leap') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_tw_ims_dstr_std_stock_107483_stock__null_test') }}
    )
),
final as(
    select 
        distributor_code,
        ean,
        distributor_product_code,
        quantity,
        total_cost,
        inventory_date,
        distributors_product_name,
        uom,
        storage_name,
        crt_dttm,
        updt_dttm,
        filename
    from source
)
select * from final