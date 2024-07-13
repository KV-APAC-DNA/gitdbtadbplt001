with wks_pilot_sku_recom_tbl1 as
(
    select * from {{ ref('indwks_integration__wks_pilot_sku_recom_tbl1') }}
),
itg_pilot_nup_target as
(
    select * from {{ source('inditg_integration', 'itg_pilot_nup_target') }}
),
wks_pilot_edw_prod_dim as
(
    select * from {{ ref('indwks_integration__wks_pilot_edw_prod_dim') }}
),
wks_pilot_sm_master as
(
    select * from {{ ref('indwks_integration__wks_pilot_sm_master') }}
),
final as
(
    SELECT sku.mth_mm,
       sku.qtr,
       sku.fisc_yr,
       sku."month" as month,
       sku.cust_cd,
       sku.customer_name,
       sku.rtruniquecode,
       sku.retailer_cd,
       sku.mother_sku_cd,
       sku.oos_flag,
       sku.ms_flag,
       sku.cs_flag,
       sku.soq,
       sku.region_name,
       sku.zone_name,
       sku.territory_name,
       sku.class_desc,
       CASE WHEN sku.class_desc IN ('A','SA') THEN 1 ELSE 0 END AS class_desc_a_sa_flag,
       sku.channel_name,
       sku.business_channel,
       sku.status_desc,
       sku.actv_flg,
       trim(sku.retailer_name) as retailer_name,
       sku.retailer_category_name,
       sku.csrtrcode,
       nup_tgt.nup_target,
       pd.franchise_name,
       pd.brand_name,
       pd.product_category_name,
       pd.variant_name,
       pd.mothersku_name,
       sm.smcode AS salesman_code,
       sm.smname AS salesman_name,
       sm.uniquesalescode AS unique_sales_code,
       trim(sm.rmcode) AS route_code,
       trim(sm.rmname) AS route_name
    FROM  wks_pilot_sku_recom_tbl1 sku
    LEFT JOIN (SELECT mth_mm, rtruniquecode, MAX(nup_target) AS nup_target
            FROM itg_pilot_nup_target
            GROUP BY mth_mm, rtruniquecode) nup_tgt
        ON sku.rtruniquecode = nup_tgt.rtruniquecode
        AND sku.mth_mm = nup_tgt.mth_mm
    LEFT JOIN wks_pilot_edw_prod_dim pd
        ON sku.mother_sku_cd = pd.mothersku_code
    LEFT JOIN wks_pilot_sm_master sm
        ON sku.cust_cd = sm.distcode
        AND sku.retailer_cd = sm.rtrcode
)
select * from final