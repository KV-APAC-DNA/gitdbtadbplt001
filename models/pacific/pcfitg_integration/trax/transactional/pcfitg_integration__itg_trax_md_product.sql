with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_trax_md_product') }}
),
final as
(    
    select 
        datasource::varchar(124) as datasource,
        businessunitid::varchar(8) as businessunitid,
        metarequeststring::varchar(2048) as metarequeststring,
        fetcheddatetime::timestamp_ntz(9) as fetcheddatetime,
        fetchedsequence::number(38,0) as fetchedsequence,
        product_name::varchar(256) as product_name,
        product_type::varchar(256) as product_type,
        product_uuid::varchar(256) as product_uuid,
        product_local_name::varchar(256) as product_local_name,
        product_short_name::varchar(256) as product_short_name,
        brand_name::varchar(256) as brand_name,
        brand_local_name::varchar(256) as brand_local_name,
        manufacturer_name::varchar(256) as manufacturer_name,
        manufacturer_local_name::varchar(256) as manufacturer_local_name,
        is_deleted::boolean as is_deleted,
        container_type::varchar(256) as container_type,
        product_client_code::varchar(2048) as product_client_code,
        size::float as size,
        unit_measurement::varchar(32) as unit_measurement,
        product_additional_attributes::varchar(256) as product_additional_attributes,
        is_active::boolean as is_active,
        category_name::varchar(256) as category_name,
        category_local_name::varchar(256) as category_local_name,
        subcategory_local_name::varchar(256) as subcategory_local_name,
        discovered_by_brand_watch::varchar(256) as discovered_by_brand_watch,
        cdl_datetime::varchar(48) as cdl_datetime,
        cdl_source_file::varchar(512) as cdl_source_file,
        load_key::varchar(512) as load_key,
        current_timestamp::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final

