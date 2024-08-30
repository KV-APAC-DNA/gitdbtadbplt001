{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['dstrbtr_id', 'material_code', 'wh_code', 'date', 'bat_number', 'expiry_date'],
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where (dstrbtr_id, material_code, wh_code, date, bat_number, expiry_date) in ( select dstrbtr_id, material_code, wh_code, to_date(date, 'MM/DD/YYYY HH12:MI:SS AM') as date, bat_number, to_date(expiry_date, 'MM/DD/YYYY HH12:MI:SS AM') as expiry_date from {{ source('vnmsdl_raw', 'sdl_vn_dms_sales_stock_fact') }}
        where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_stock_fact__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_stock_fact__duplicate_test')}}
    ));{%endif%}"
    )
}}

with source as(
    select *, dense_rank() over (partition by dstrbtr_id, material_code, wh_code, to_date(date, 'MM/DD/YYYY HH12:MI:SS AM'), bat_number, to_date(expiry_date, 'MM/DD/YYYY HH12:MI:SS AM') order by file_name desc) rnk
    from {{ source('vnmsdl_raw', 'sdl_vn_dms_sales_stock_fact') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_stock_fact__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_stock_fact__duplicate_test')}}
    ) qualify rnk = 1
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
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name
    from source
)
select * from final