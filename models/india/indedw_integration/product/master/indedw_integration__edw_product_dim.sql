{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["product_code","chng_flg"],
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} a using {{ ref('indwks_integration__wks_product_dim') }} b
        where a.product_code = b.product_code
        and b.chng_flg = 'U';
        {% endif %}"
    )
}}

with source as 
(
    select * from {{ ref('indwks_integration__wks_product_dim') }}
),
final as 
(
    select product_code::varchar(50) as product_code,
        product_name::varchar(50) as product_name,
        product_desc::varchar(100) as product_desc,
        franchise_name::varchar(50) as franchise_name,
        brand_name::varchar(50) as brand_name,
        product_category_name::varchar(150) as product_category_name,
        variant_name::varchar(150) as variant_name,
        mothersku_name::varchar(150) as mothersku_name,
        uom::number(18,0) as uom,
        std_nr::number(18,2) as std_nr,
        case_lot::number(18,2) as case_lot,
        sale_uom::number(18,0) as sale_uom,
        sale_conversion_factor::number(18,0) as sale_conversion_factor,
        base_uom::number(18,0) as base_uom,
        int_uom::number(18,0) as int_uom,
        gross_wt::number(13,3) as gross_wt,
        net_wt::number(13,3) as net_wt,
        active_flag::varchar(1) as active_flag,
        delete_flag::varchar(1) as delete_flag,
        shelf_life::number(18,0) as shelf_life,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        franchise_code::varchar(50) as franchise_code,
        brand_code::varchar(50) as brand_code,
        product_category_code::varchar(50) as product_category_code,
        variant_code::varchar(50) as variant_code,
        mothersku_code::varchar(50) as mothersku_code
        from source
)
select * from final