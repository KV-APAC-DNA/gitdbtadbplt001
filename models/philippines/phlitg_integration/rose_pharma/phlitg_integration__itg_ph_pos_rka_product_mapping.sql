{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=  ['mnth_id','item_cd','sap_item_cd']
      
    )
}}
with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_rosepharmacy_product_mapping') }}
),
transformed as 
(
    select 
effectivesalescycle||'-'||acctcode||'-'||rka_skucode,
salescycle as mnth_id,
rka_skucode  as item_cd,
null as bar_cd,
rka_skudesc as item_nm,
jj_skucode as sap_item_cd,
jj_skudesc as sap_item_desc,
'RKA007-ROSE PHARMACY' as parent_cust_cd,
null as parent_cust_nm,
null as jnj_item_desc,
null as jnj_matl_cse_barcode,
null as jnj_matl_pc_barcode,
null as early_bk_period,
null as cust_conv_factor,
null as cust_item_prc,
null as jnj_matl_shipper_barcode,
null as jnj_matl_consumer_barcode,
jj_conversion as jnj_pc_per_cust_unit,
null as computed_price_per_unit,
null as jj_price_per_unit,
null as cust_sku_grp,
null  as uom,
null as jnj_pc_per_cse,
null as lst_period,
'ROSE PHARMACY' as cust_cd,
null as cust_cd2,
current_timestamp as last_chg_datetime,
'1990-01-01 00:00:00' as effective_from,
'9999-12-31 00:00:00.000' as effective_to,
'Y' as active,
current_timestamp as crtd_dttm,
current_timestamp as updt_dttm
from source 
)
select * from transformed