{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['trx_date']
    )
}}

--import CTE
with source as (
    select * from {{ source('sgpsdl_raw','sdl_sg_scan_data_scommerce') }}
),

edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),

--logical CTE
final as (
    select
        date_id::date as trx_date,
        left(trx_date, 4)::varchar(20) as year,
        cal.cal_mo_1::varchar(23) as mnth_id,
        cal.cal_wk::varchar(20) as week,
        ordersn::varchar(50) as order_sn,
        itemid::varchar(50) as item_code,
        modelid::varchar(50) as model_id,
        sku_id::varchar(100) as sku_id,
        item_name::varchar(500) as item_desc,
        'NA'::varchar(255) as barcode,
        sales_qty::number(10, 0) as sales_qty,
        net_sales::number(10, 6) as net_sales,
        store::varchar(50) as store,
        store_name::varchar(50) as store_name,
        null::varchar(300) as brand,
        cdl_dttm::varchar(255) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        file_name::varchar(255) as file_name,
        run_id::number(14, 0) as run_id
    from source as sg_scm
    left join edw_calendar_dim as cal
        on sg_scm.date_id = cal.cal_day

)

select * from final
