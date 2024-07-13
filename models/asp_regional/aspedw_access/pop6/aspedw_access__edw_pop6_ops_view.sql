with source as (
    select * from {{ ref('aspedw_integration__edw_pop6_ops_view') }}
),
final as (
    select
        cntry_cd as "cntry_cd",
        src_file_date as "src_file_date",
        product_group as "product_group",
        product_list as "product_list",
        productdb_id as "productdb_id",
        sku as "sku",
        msl_ranking as "msl_ranking",
        popdb_id as "popdb_id",
        pop_code as "pop_code",
        pop_name as "pop_name",
        pop_attribute as "pop_attribute",
        pop_attribute_value as "pop_attribute_value",
        prod_grp_date as "prod_grp_date"
    from source
)
select * from final