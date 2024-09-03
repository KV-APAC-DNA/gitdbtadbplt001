with source as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_711_product_dim') }}
),
final as
(    
    select 
        yearmo::varchar(100) as yearmonth,
        'PSC'::varchar(100) as cust_cd,
        cust_item_cd::varchar(100) as item_cd,
        cust_item_desc::varchar(255) as item_nm,
        jnj_item_cd::varchar(100) as sap_item_cd,
        jnj_item_desc::varchar(255) as sap_item_nm,
        conv_factor::number(20,4) as cust_conv_factor,
        last_prd::varchar(100) as lst_period,
        early_bk_prd::varchar(100) as early_bk_period,
        current_timestamp::timestamp_ntz(9) as crtd_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm,
        file_name::varchar(255) as file_name
    from source
)
select * from final