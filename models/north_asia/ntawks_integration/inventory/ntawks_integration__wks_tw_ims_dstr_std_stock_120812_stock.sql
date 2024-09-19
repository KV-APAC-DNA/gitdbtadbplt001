with source as(
    select * from {{ source('ntasdl_raw', 'sdl_tw_ims_dstr_std_stock_120812_stock')}}
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