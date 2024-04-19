with
edw_perenso_prod_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_PROD_DIM
),
itg_perenso_product_reln_id as (
select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_PRODUCT_RELN_ID 
),
itg_perenso_product_fields as (
select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_PRODUCT_FIELDS
),
transformed as (
select edw.prod_key,

       edw.prod_desc,

       edw.prod_id,

       edw.prod_ean,

       edw.prod_jj_franchise,

       edw.prod_jj_category,

       edw.prod_jj_brand,

       edw.prod_sap_franchise,

       edw.prod_sap_profit_centre,

       edw.prod_sap_product_major,

       edw.prod_grocery_franchise,

       edw.prod_grocery_category,

       edw.prod_grocery_brand,

       edw.prod_active_nz_pharma,

       edw.prod_active_au_grocery,

       edw.prod_active_metcash,

       edw.prod_active_nz_grocery,

       edw.prod_active_au_pharma,

       edw.prod_pbs,

       edw.prod_ims_brand,

       edw.prod_nz_code,

       edw.prod_metcash_code,

       reln.id as product_probe_id,

       edw.prod_old_id,

       edw.prod_old_ean ,

       edw.prod_tax,

       edw.prod_bwp_aud,

       edw.prod_bwp_nzd

from edw_perenso_prod_dim edw,

     itg_perenso_product_reln_id reln,

     itg_perenso_product_fields fields

where edw.prod_key = reln.prod_key

and   reln.field_key = fields.field_key

and   upper(fields.field_desc) = 'IMS PROBE'
),
final as (
select
prod_key::number(10,0) as prod_key,
prod_desc::varchar(100) as prod_desc,
prod_id::varchar(50) as prod_id,
prod_ean::varchar(50) as prod_ean,
prod_jj_franchise::varchar(100) as prod_jj_franchise,
prod_jj_category::varchar(100) as prod_jj_category,
prod_jj_brand::varchar(100) as prod_jj_brand,
prod_sap_franchise::varchar(100) as prod_sap_franchise,
prod_sap_profit_centre::varchar(100) as prod_sap_profit_centre,
prod_sap_product_major::varchar(100) as prod_sap_product_major,
prod_grocery_franchise::varchar(100) as prod_grocery_franchise,
prod_grocery_category::varchar(100) as prod_grocery_category,
prod_grocery_brand::varchar(100) as prod_grocery_brand,
prod_active_nz_pharma::varchar(100) as prod_active_nz_pharma,
prod_active_au_grocery::varchar(100) as prod_active_au_grocery,
prod_active_metcash::varchar(100) as prod_active_metcash,
prod_active_nz_grocery::varchar(100) as prod_active_nz_grocery,
prod_active_au_pharma::varchar(100) as prod_active_au_pharma,
prod_pbs::varchar(100) as prod_pbs,
prod_ims_brand::varchar(100) as prod_ims_brand,
prod_nz_code::varchar(100) as prod_nz_code,
prod_metcash_code::varchar(100) as prod_metcash_code,
product_probe_id::varchar(100) as product_probe_id,
prod_old_id::varchar(50) as prod_old_id,
prod_old_ean::varchar(50) as prod_old_ean,
prod_tax::varchar(50) as prod_tax,
prod_bwp_aud::varchar(50) as prod_bwp_aud,
prod_bwp_nzd::varchar(50) as prod_bwp_nzd
from transformed
)
select * from final