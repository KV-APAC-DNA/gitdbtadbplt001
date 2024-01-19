{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['trx_date']
    )
}}

--import CTE
with source as (
    select * from {{ source('sgpsdl_raw','sdl_sg_scan_data_redmart') }}
),

edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),

--logical CTE
final as (
    select
        trx_date::date as trx_date,
        left(trx_date, 4)::varchar(20) as year,
        cal.cal_mo_1::varchar(23) as mnth_id,
        cal.cal_wk::varchar(20) as week,
        item_code::varchar(100) as item_code,
        product_code::varchar(300) as product_code,
        item_desc::varchar(500) as item_desc,
        packsize::varchar(100) as pack_size,
        brand::varchar(300) as brand,
        supplier_id::varchar(100) as supplier_id,
        supplier_name::varchar(100) as supplier_name,
        manufacturer::varchar(200) as manufacturer,
        'NA'::varchar(255) as barcode,
        category_1::varchar(150) as category_1,
        category_2::varchar(150) as category_2,
        category_3::varchar(150) as category_3,
        category_4::varchar(150) as category_4,
        gmv::number(10, 4) as net_sales,
        units_sold::number(10, 0) as sales_qty,
        store::varchar(50) as store,
        store_name::varchar(50) as store_name,
        cdl_dttm::varchar(255) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        file_name::varchar(255) as file_name,
        run_id::varchar(255) as run_id
    from source as sg_rdm
    left join edw_calendar_dim as cal
        on sg_rdm.trx_date = cal.cal_day
)

select * from final
