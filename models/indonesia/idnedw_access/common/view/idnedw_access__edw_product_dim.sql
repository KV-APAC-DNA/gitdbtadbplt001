with source as
(
    select * from {{ref('idnedw_integration__edw_product_dim')}}
)

select 
    jj_sap_prod_id as "jj_sap_prod_id",
    jj_sap_prod_desc as "jj_sap_prod_desc",
    franchise as "franchise",
    brand as "brand",
    variant1 as "variant1",
    variant2 as "variant2",
    variant3 as "variant3",
    status as "status",
    put_up as "put_up",
    uom as "uom",
    jj_sap_upgrd_prod_id as "jj_sap_upgrd_prod_id",
    jj_sap_upgrd_prod_desc as "jj_sap_upgrd_prod_desc",
    price as "price",
    prod_class as "prod_class",
    jj_sap_cd_mp_prod_id as "jj_sap_cd_mp_prod_id",
    jj_sap_cd_mp_prod_desc as "jj_sap_cd_mp_prod_desc",
    price_vmr as "price_vmr",
    pft_sm as "pft_sm",
    pft_mm as "pft_mm",
    pft_ws as "pft_ws",
    pft_prov as "pft_prov",
    pft_ds as "pft_ds",
    pft_mws as "pft_mws",
    pft_apt as "pft_apt",
    pft_bs as "pft_bs",
    pft_cs as "pft_cs",
    crtd_dttm as "crtd_dttm",
    uptd_dttm as "uptd_dttm",
    effective_from as "effective_from",
    effective_to as "effective_to"
from source