{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where UPPER(TRIM(DATE)) || UPPER(TRIM(outlet_no)) || UPPER(TRIM(product_barcode)) || UPPER(TRIM(answer)) || UPPER(TRIM(yearmo)) in (select distinct UPPER(TRIM(DATE)) || UPPER(TRIM(outlet_no)) || UPPER(TRIM(product_barcode)) || UPPER(TRIM(answer)) || UPPER(TRIM(yearmo)) from dev_dna_load.snaposesdl_raw.sdl_my_perfectstore_oos 
        );
        {% endif %}"
    )
}}

with source as
(
    -- select * from {{ source('myssdl_raw','sdl_my_perfectstore_oos') }}
    select * from dev_dna_load.snaposesdl_raw.sdl_my_perfectstore_oos
),
final as
(
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
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name,
        yearmo::varchar(255) as yearmo,
        convert_timezone('Asia/Singapore',current_timestamp()::timestamp_ntz(9)) AS crtd_dttm
    from source
)
select * from final