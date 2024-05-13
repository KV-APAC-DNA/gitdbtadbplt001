{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["cust_prod_cd","barcd", "cust_nm"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with source as(
    select * from DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_CUST_PROD_CD_EAN_MAP
),
final as(
    select 
        customer::varchar(100) as cust_nm,
        customer_hierarchy_code::varchar(100) as cust_hier_cd,
        cust_prod_cd::varchar(100) as cust_prod_cd,
        barcode::varchar(100) as barcd,
        sap_product_code::varchar(100) as sap_prod_cd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm 
    FROM source
)
select * from final