with itg_trax_fct_psd_kpis as
(
select * from {{ source('aspacshare_integration','v_rpt_psd_kpis_redshift_snap') }}
where datasource = 'Trax' and prv_country = 'PC'
),
itg_trax_md_store as(
select * from {{ source('aspacshare_integration','trax_md_store') }}
where businessunitid = 'PC'
),
final as
(
    SELECT trax_fact.datasource as datasource
        ,trax_fact.source as source
        ,trax_fact.prv_product_level as prv_product_level
        ,trax_fact.prv_country as prv_country
        ,trax_fact.country as country
        ,trax_fact.visit_date as visit_date
        ,trax_fact.category as category
        ,trax_fact.subcategory as subcategory
        ,trax_fact.manufacturer as manufacturer
        ,trax_fact.brand as brand
        ,trax_fact.subbrand as subbrand
        ,trax_fact.productid as productid
        ,trax_fact.product as product
        ,trax_fact.storeid as storeid
        ,trax_fact.store as store
        ,trax_fact.re as re
        ,trax_fact.retailer as retailer
        ,trax_fact.retailer_group as retailer_group
        ,trax_fact.orig_task_name as orig_task_name
        ,trax_fact.jj_shelf as jj_shelf
        ,trax_fact.all_shelf as all_shelf
        ,trax_fact.jj_oos as jj_oos
        ,trax_fact.all_oos as all_oos
        ,trax_fact.jj_he as jj_he
        ,trax_fact.all_he as all_he
        ,trax_fact.jj_promo as jj_promo
        ,trax_fact.all_promo as all_promo
    FROM (
        itg_trax_fct_psd_kpis trax_fact JOIN itg_trax_md_store trax_store ON (((trax_fact.storeid)::TEXT = (trax_store.store_number)::TEXT))
        )
    WHERE (to_char((trax_fact.visit_date):: timestamp without time zone,'YYYY' :: text) >= ((select year(current_timestamp())-2 AS "date_part")):: text)
)
select * from final