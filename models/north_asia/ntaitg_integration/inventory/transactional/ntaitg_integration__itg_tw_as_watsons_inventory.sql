
{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where nvl(year,'#')||nvl(week_no,'#')||nvl(item_cd,'#') in (select distinct nvl(year,'#')||nvl(week_no,'#')||nvl(item_cd,'#') from  {{ source('ntasdl_raw', 'sdl_tw_as_watsons_inventory') }});
        {% endif %}"
    )
}}
with sdl_tw_as_watsons_inventory as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_as_watsons_inventory') }}
),
final as (
    select
        year::varchar(30) as year,
        week_no::varchar(30) as week_no,
        supplier::varchar(30) as supplier,
        item_cd::varchar(30) as item_cd,
        buy_code::varchar(30) as buy_code,
        home_cdesc::varchar(255) as home_cdesc,
        prdt_grp::varchar(30) as prdt_grp,
        grp_desc::varchar(255) as grp_desc,
        prdt_cat::varchar(30) as prdt_cat,
        category_desc::varchar(255) as category_desc,
        item_desc::varchar(255) as item_desc,
        type::varchar(50) as type,
        avg_sls_cost_value::number(20,4) as avg_sls_cost_value,
        total_stock_qty::number(20,4) as total_stock_qty,
        total_stock_value::number(20,4) as total_stock_value,
        weeks_holding_sales::number(20,4) as weeks_holding_sales,
        weeks_holding::number(20,4) as weeks_holding,
        first_recv_date::date as first_recv_date,
        turn_type_sales::varchar(100) as turn_type_sales,
        turn_type::varchar(100) as turn_type,
        uda73::varchar(50) as uda73,
        discontinue_date::date as discontinue_date,
        stock_class::varchar(30) as stock_class,
        pog::varchar(30) as pog,
        ean_num::varchar(30) as ean_num,
        filename::varchar(255) as filename,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm
    from sdl_tw_as_watsons_inventory
)
select * from sdl_tw_as_watsons_inventory