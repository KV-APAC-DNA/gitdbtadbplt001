with source as 
(
    select * from {{ ref('phledw_integration__edw_ph_hcp_selllout_final') }}
),
final  as 
(
    select 
DATA_SRC  AS  "data_src",
JJ_MNTH_ID   AS  "jj_mnth_id",
JJ_YEAR   AS  "jj_year",
SKU   AS  "sku",
ITEM_CD   AS  "item_cd",
POS_GTS   AS  "pos_gts",
POS_NTS   AS  "pos_nts",
POS_QTY   AS  "pos_qty",
STORE_CODE   AS  "store_code",
GROUP_VARIANT_CODE   AS  "group_variant_code",
TERRITORY_CODE_CODE   AS  "territory_code_code",
TEAM_CODE   AS  "team_code"

    
    from source
)
select * from final 
