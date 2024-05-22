with source as(
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_cost_of_goods') }}
),
final as (
    select
        sap_code::varchar(200) as sap_code,
        sku_name::varchar(510) as sku_name,
        free_good_value::number(31,3) as free_good_value,
        pre_apsc_cogs::number(31,3) as pre_apsc_cogs,
        package_cost::number(31,3) as package_cost,
        labour_cost::number(31,3) as labour_cost,
        attribute_value::number(31,3) as attribute_value,
        valid_from::date as valid_from,
        valid_to::date as valid_to,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final