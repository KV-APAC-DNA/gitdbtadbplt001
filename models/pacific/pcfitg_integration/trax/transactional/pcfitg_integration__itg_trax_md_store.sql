with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_trax_md_store') }}
),
final as
(    
    Select 
        datasource::varchar(124) as datasource,
        businessunitid::varchar(8) as businessunitid,
        metarequeststring::varchar(2048) as metarequeststring,
        fetcheddatetime::timestamp_ntz(9) as fetcheddatetime,
        fetchedsequence::number(38,0) as fetchedsequence,
        store_number::varchar(64) as store_number,
        store_name::varchar(128) as store_name,
        store_display_name::varchar(128) as store_display_name,
        store_type_name::varchar(64) as store_type_name,
        street::varchar(128) as street,
        city::varchar(128) as city,
        postal_code::varchar(32) as postal_code,
        latitude::float as latitude,
        longitude::float as longitude,
        is_active::boolean as is_active,
        manager_name::varchar(128) as manager_name,
        is_deleted::boolean as is_deleted,
        manager_phone::varchar(32) as manager_phone,
        manager_email::varchar(128) as manager_email,
        region_name::varchar(128) as region_name,
        district_name::varchar(128) as district_name,
        branch_name::varchar(128) as branch_name,
        retailer_name::varchar(128) as retailer_name,
        state_code::varchar(128) as state_code,
        attribute_1::varchar(128) as attribute_1,
        attribute_2::varchar(128) as attribute_2,
        attribute_3::varchar(128) as attribute_3,
        attribute_4::varchar(128) as attribute_4,
        attribute_5::varchar(128) as attribute_5,
        attribute_6::varchar(128) as attribute_6,
        attribute_7::varchar(128) as attribute_7,
        attribute_8::varchar(128) as attribute_8,
        attribute_9::varchar(128) as attribute_9,
        attribute_10::varchar(128) as attribute_10,
        attribute_11::varchar(128) as attribute_11,
        attribute_12::varchar(128) as attribute_12,
        attribute_13::varchar(128) as attribute_13,
        attribute_14::varchar(128) as attribute_14,
        attribute_15::varchar(128) as attribute_15,
        cdl_datetime::varchar(48) as cdl_datetime,
        cdl_source_file::varchar(512) as cdl_source_file,
        load_key::varchar(512) as load_key,
        current_timestamp::timestamp_ntz(9) as crt_dttm
    from source
)
select * from source

