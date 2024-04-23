with itg_trax_fct_psd_kpis as
(
select * from {{ ref('pcfitg_integration__itg_trax_fct_psd_kpis') }}
),
itg_trax_md_store as(
select * from {{ ref('pcfitg_integration__itg_trax_md_store') }}
),
final as
(
    SELECT trax_fact.datasource
        ,trax_fact.source
        ,trax_fact.prv_product_level
        ,trax_fact.prv_country
        ,trax_fact.country
        ,trax_fact.visit_date
        ,trax_fact.category
        ,trax_fact.subcategory
        ,trax_fact.manufacturer
        ,trax_fact.brand
        ,trax_fact.subbrand
        ,trax_fact.productid
        ,trax_fact.product
        ,trax_fact.storeid
        ,trax_fact.store
        ,trax_fact.re
        ,trax_fact.retailer
        ,trax_fact.retailer_group
        ,trax_fact.orig_task_name
        ,trax_fact.jj_shelf
        ,trax_fact.all_shelf
        ,trax_fact.jj_oos
        ,trax_fact.all_oos
        ,trax_fact.jj_he
        ,trax_fact.all_he
        ,trax_fact.jj_promo
        ,trax_fact.all_promo
    FROM (
        itg_trax_fct_psd_kpis trax_fact JOIN itg_trax_md_store trax_store ON (((trax_fact.storeid)::TEXT = (trax_store.store_number)::TEXT))
        )
)
select * from final