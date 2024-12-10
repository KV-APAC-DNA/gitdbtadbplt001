
with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_pos_product_adftemp') }}
),
transformed as 
(
    select 
code as code,
effective_sales_cycle as mnth_id,
item_cd  as item_cd,
null as bar_cd,
item_nm as item_nm,
sap_item_cd_code as sap_item_cd,
sap_item_cd_name as sap_item_desc,
'RKA007-ROSE PHARMACY' as parent_cust_cd,
null as parent_cust_nm,
JJ_ITEM_DESCRIPTION as jnj_item_desc,
null as jnj_matl_cse_barcode,
null as jnj_matl_pc_barcode,
null as early_bk_period,
null as cust_conv_factor,
null as cust_item_prc,
null as jnj_matl_shipper_barcode,
null as jnj_matl_consumer_barcode,
JNJ_PC_PER_CUST_UNIT as jnj_pc_per_cust_unit,
null as computed_price_per_unit,
null as jj_price_per_unit,
null as cust_sku_grp,
null  as uom,
jnj_pc_per_cse as jnj_pc_per_cse,
lst_period as lst_period,
cust_cd as cust_cd,
null as cust_cd2,
current_timestamp as last_chg_datetime,
'1990-01-01 00:00:00' as effective_from,
'9999-12-31 00:00:00.000' as effective_to,
'Y' as active,
current_timestamp as crtd_dttm,
current_timestamp as updt_dttm
from source 
where PREFIX='RPI'
)
select * from transformed