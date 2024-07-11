{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where upper(trim(date)) || upper(trim(outlet_no)) || upper(trim(product_barcode)) || upper(trim(answer)) || upper(trim(yearmo)) in (select distinct upper(trim(date)) || upper(trim(outlet_no)) || upper(trim(product_barcode)) || upper(trim(answer)) || upper(trim(yearmo)) from {{ source('myssdl_raw','sdl_my_perfectstore_osa') }});
        {% endif %}"
    )}}

with source as (
     select * from {{ source('myssdl_raw','sdl_my_perfectstore_osa') }}
),
final as (
    select
        date::date as date,
        channel::varchar(255) as channel,
        chain::varchar(255) as chain,
        region::varchar(255) as region,
        outlet::varchar(255) as outlet,
        outlet_no::varchar(255) as outlet_no,
        category::varchar(255) as category,
        brand::varchar(255) as brand,
        sub_category::varchar(255) as sub_category,
        sub_brand::varchar(255) as sub_brand,
        packsize::varchar(255) as packsize,
        product_barcode::varchar(255) as product_barcode,
        sku_description::varchar(255) as sku_description,
        answer::varchar(255) as answer,
        run_id::numeric(14) as run_id,
        file_name::varchar(255) as file_name,
        yearmo::varchar(255) as yearmo,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final