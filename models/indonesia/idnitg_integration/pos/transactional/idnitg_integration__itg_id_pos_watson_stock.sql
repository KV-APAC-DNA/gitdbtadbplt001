{{
    config(
        materialized='incremental',
        incremental_strategy= "append",
        unique_key= ["filename"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where (filename) in (
        select filename from {{ source('idnsdl_raw', 'sdl_id_pos_watson_stock') }});
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_watson_stock') }}
),
final as
(
    select
    grp::varchar(10) as grp,
    group_name::varchar(100) as group_name,
    dept::varchar(10) as dept,
    dept_name::varchar(100) as dept_name,
    item::varchar(50) as item,
    item_name::varchar(200) as item_name,
    product_type::varchar(20) as product_type,
    item_brand::varchar(50) as item_brand,
    supplier::varchar(50) as supplier,
    supplier_name::varchar(100) as supplier_name,
    sup_code2::varchar(10) as sup_code2,
    product_plnmod::varchar(50) as product_plnmod,
    pog::varchar(10) as pog,
    top500::varchar(10) as top500,
    soh_qty::number(18,2) as soh_qty,
    avg_sales::number(18,2) as avg_sales,
    week_cover::number(18,2) as week_cover,
    store::varchar(10) as store,
    "values"::number(18,2) as "values",
    pos_cust::varchar(50) as pos_cust,
    yearmonth::varchar(10) as yearmonth,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz as crtd_dttm,
    filename::varchar(100) as filename
    from source
)
select * from final