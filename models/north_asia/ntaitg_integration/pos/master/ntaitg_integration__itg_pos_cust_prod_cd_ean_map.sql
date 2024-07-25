{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["cust_prod_cd","barcd", "cust_nm"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with source as(
    select * from {{ ref('ntawks_integration__wks_itg_pos_cust_prod_cd_ean_map') }}
),
final as(
    select 
        trim(customer)::varchar(100) as cust_nm,
        trim(customer_hierarchy_code)::varchar(100) as cust_hier_cd,
        trim(cust_prod_cd)::varchar(100) as cust_prod_cd,
        rtrim(REGEXP_REPLACE(barcode,'[\xC2\xA0]', ''))::varchar(100) as barcd,
        trim(sap_product_code)::varchar(100) as sap_prod_cd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm 
    FROM source
)
select * from final