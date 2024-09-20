with sdl_mds_cn_ecom_sapcustomer_map as
(
    select * from {{source('chnsdl_raw', 'sdl_mds_cn_ecom_sapcustomer_map')}}
),
final as
(
    select
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as load_date,
    map.code::varchar(500) as code,
    map.sap_cust_name::VARCHAR(50) as SAP_CUST_NAME,
    map.local_retailer_name::varchar(255) as local_retailer_name,
    map.retailer_name_english::varchar(255) as retailer_name_english,
    map.sap_cust_code::varchar(255) as sap_cust_code
    from sdl_mds_cn_ecom_sapcustomer_map map 
)
select * from final