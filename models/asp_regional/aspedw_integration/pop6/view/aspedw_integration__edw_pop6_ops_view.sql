with itg_pop6_product_lists_products as (
    select * from {{ ref('aspitg_integration__itg_pop6_product_lists_products') }}
),
itg_pop6_product_lists_pops as (
    select * from {{ ref('aspitg_integration__itg_pop6_product_lists_pops') }}
),
itg_pop6_product_lists_allocation as (
    select * from {{ ref('aspitg_integration__itg_pop6_product_lists_allocation') }}
),
final as (
    SELECT prod_list_prods.cntry_cd,
        prod_list_prods.src_file_date,
        prod_list_alloc.product_group,
        prod_list_prods.product_list,
        prod_list_prods.productdb_id,
        prod_list_prods.sku,
        prod_list_prods.msl_ranking,
        prod_list_pops.popdb_id,
        prod_list_pops.pop_code,
        prod_list_pops.pop_name,
        prod_list_alloc.pop_attribute,
        prod_list_alloc.pop_attribute_value,
        prod_list_prods.prod_grp_date
    FROM itg_pop6_product_lists_products prod_list_prods,
        itg_pop6_product_lists_pops prod_list_pops,
        itg_pop6_product_lists_allocation prod_list_alloc
    WHERE (prod_list_prods.cntry_cd)::text = (prod_list_pops.cntry_cd)::text
        AND prod_list_prods.prod_grp_date = prod_list_pops.prod_grp_date
        AND (prod_list_prods.product_list)::text = (prod_list_pops.product_list)::text
        AND (prod_list_prods.cntry_cd)::text = (prod_list_alloc.cntry_cd)::text
        AND prod_list_prods.prod_grp_date = prod_list_alloc.prod_grp_date
        AND (prod_list_prods.product_list)::text = (prod_list_alloc.product_list)::text
)
select * from final