with source as(
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_cost_of_goods') }}
),
final as (
    select
        sap_code::varchar(200) as sap_code,
        sku_name::varchar(510) as sku_name,
        trunc(free_good_value,3)::number(31,3) as free_good_value,
        trunc(pre_apsc_cogs,3)::number(31,3) as pre_apsc_cogs,
        trunc(package_cost,3)::number(31,3) as package_cost,
        trunc(labour_cost,3)::number(31,3) as labour_cost,
        trunc(attribute_value,3)::number(31,3) as attribute_value,
        valid_from::date as valid_from,
        valid_to::date as valid_to,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final