{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where coalesce(inv_date, '9999-12-31') in (select distinct coalesce(transaction_date, '9999-12-31') from {{ source('ntasdl_raw', 'sdl_kr_ecom_dstr_sellout_stock') }} where upper(data_src) = 'INVENTORY');
        {% endif %}"
    )
}}

with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_ecom_dstr_sellout_stock') }}
    where upper(data_src) = 'INVENTORY'
),
final as (
    select 
        dstr_cd::varchar(10) as dstr_cd,
        sap::varchar(30) as matl_num,
        ean_code::varchar(30) as ean,
        brand::varchar(100) as brand_name,
        sku_name::varchar(500) as sku_name,
        transaction_date::date as inv_date,
        coalesce(quantity, 0)::number(38,5) as inventory_qty,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        file_name::varchar(100) as file_name,
        run_id::varchar(50) as run_id
    from source
)
select * from final