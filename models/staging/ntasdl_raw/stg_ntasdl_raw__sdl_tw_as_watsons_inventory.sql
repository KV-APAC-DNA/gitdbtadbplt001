with 

source as (

    select * from {{ source('ntasdl_raw', 'sdl_tw_as_watsons_inventory') }}

),

renamed as (

    select
        year,
        week_no,
        supplier,
        item_cd,
        buy_code,
        home_cdesc,
        prdt_grp,
        grp_desc,
        prdt_cat,
        category_desc,
        item_desc,
        type,
        avg_sls_cost_value,
        total_stock_qty,
        total_stock_value,
        weeks_holding_sales,
        weeks_holding,
        first_recv_date,
        turn_type_sales,
        turn_type,
        uda73,
        discontinue_date,
        stock_class,
        pog,
        ean_num,
        filename,
        crtd_dttm

    from source

)

select * from renamed
