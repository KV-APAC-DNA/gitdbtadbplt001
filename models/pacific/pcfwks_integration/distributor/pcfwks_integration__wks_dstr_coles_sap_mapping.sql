with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_product_mapping_coles') }}
),
final as(
    Select 
        jnj_sap_code as sap_code,
        code as item_idnt,
        prod_desc_coles as item_desc,
        current_timestamp()::timestamp_ntz(9) as crtd_dtmm
    from source where code is not null or code<>''
)
select * from final