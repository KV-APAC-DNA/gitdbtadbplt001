{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['dstrbtr_id', 'material_code', 'wh_code', 'date', 'bat_number', 'expiry_date'],
        pre_hook= "delete from {{this}} where (dstrbtr_id, material_code, wh_code, date, bat_number, expiry_date) in ( select dstrbtr_id, material_code, wh_code, to_date(date, 'MM/DD/YYYY HH12:MI:SS AM') as date, bat_number, to_date(expiry_date, 'MM/DD/YYYY HH12:MI:SS AM') as expiry_date from {{ source('vnmsdl_raw', 'sdl_vn_dms_sales_stock_fact') }} );"
    )
}}

with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_sales_stock_fact') }}
),
final as(
    select
        dstrbtr_id::varchar(30) as dstrbtr_id,
        trim(cntry_code)::varchar(2) as cntry_code,
        wh_code::varchar(20) as wh_code,
        to_date(date, 'MM/DD/YYYY HH12:MI:SS AM') as date,
        material_code::varchar(50) as material_code,
        bat_number::varchar(20) as bat_number,
        to_date(expiry_date, 'MM/DD/YYYY HH12:MI:SS AM') as expiry_date,
        cast(quantity as int) as quantity,
        trim(uom)::varchar(2) as uom,
        cast(amount as decimal(15, 4)) as amount,
        curr_date::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        run_id::number(14,0) as run_id
    from source
)
select * from final