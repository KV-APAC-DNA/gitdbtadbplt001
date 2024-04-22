with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_product_mapping_ww') }}
),
final as(
    Select 
        code as article_code,
        jnj_sap_code as sap_code,
        prod_desc_ww as article_name,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source where jnj_sap_code is not null or jnj_sap_code<>''
)
select * from final