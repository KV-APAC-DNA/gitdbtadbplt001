with source as 
(
    select * from {{ ref('indedw_integration__v_rpt_pos_offtake') }}
)
select
    store_cd as "store_cd",
    article_cd as "article_cd",
    period as "period",
    fisc_year as "fisc_year",
    fis_month as "fis_month",
    "level" as "level",
    sls_qty as "sls_qty",
    sls_val_lcy as "sls_val_lcy",
    account_name as "account_name",
    store_name as "store_name",
    "region" as "region",
    zone as "zone",
    re as "re",
    promotor as "promotor",
    mother_sku_name as "mother_sku_name",
    sap_cd as "sap_cd",
    brand_name as "brand_name",
    franchise_name as "franchise_name",
    internal_category as "internal_category",
    internal_subcategory as "internal_subcategory",
    external_category as "external_category",
    external_subcategory as "external_subcategory",
    promos as "promos",
    file_upload_date as "file_upload_date",
    article_name as "article_name",
    product_name as "product_name",
    product_category_name as "product_category_name",
    variant_name as "variant_name",
    quarter as "quarter",
    nr as "nr",
    achivement_nr as "achivement_nr"
from source
