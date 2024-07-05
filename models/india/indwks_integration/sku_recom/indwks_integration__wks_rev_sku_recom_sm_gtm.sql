with wks_rev_sku_recom_tbl1 as
(
    select * from {{ ref('indwks_integration__wks_rev_sku_recom_tbl1') }}
),
wks_rev_udc_details as
(
    select * from {{ ref('indwks_integration__wks_rev_udc_details') }}
),
wks_rev_itg_in_rsalesman as
(
    select * from {{ ref('indwks_integration__wks_rev_itg_in_rsalesman') }}
),
itg_xdm_salesmanskulinemapping as
(
    select * from {{ ref('inditg_integration__itg_xdm_salesmanskulinemapping') }}
),
final as
(
    SELECT sku.mnth_id,
       sku.cust_cd,
       sku.retailer_cd,
       sku.mother_sku_cd,
       sku.oos_flag,
       sku.ms_flag,
       sku.cs_flag,
       sku.soq,
       sku.reco_date,
       sku.mth_mm,
       sku.qtr,
       sku.fisc_yr,
       sku.month,
       sku.customer_name,
       sku.region_name,
       sku.zone_name,
       sku.territory_name,
       sku.rtruniquecode,
       sku.class_desc,
       sku.channel_name,
       sku.business_channel,
       sku.status_desc,
       sku.actv_flg,
       sku.retailer_name,
       sku.retailer_category_name,
       sku.csrtrcode,
       sku.retailer_channel_level_1,
       sku.retailer_channel_level_2,
       sku.retailer_channel_level_3,
       sm.smcode AS salesman_code,
       sm.rmcode AS route_code,
       sm.smname AS salesman_name,
       sm.uniquesalescode AS unique_sales_code,
       sm.rmname AS route_name
FROM wks_rev_sku_recom_tbl1 sku
  INNER JOIN wks_rev_udc_details udc
          ON COALESCE (udc.distcode,'0'::CHARACTER VARYING)::TEXT = COALESCE (sku.cust_cd,'0'::CHARACTER VARYING)::TEXT
         AND COALESCE (udc.mastervaluecode,'0'::CHARACTER VARYING)::TEXT = COALESCE (sku.retailer_cd,'0'::CHARACTER VARYING)::TEXT
         AND UPPER (udc.columnvalue) = 'YES'::CHARACTER VARYING::TEXT
  LEFT JOIN (SELECT sm.*,
                    msku.motherskucode
             FROM wks_rev_itg_in_rsalesman sm
               INNER JOIN itg_xdm_salesmanskulinemapping msku
                       ON sm.distcode::TEXT = msku.distrcode::TEXT
                      AND sm.smcode::TEXT = msku.salesmancode::TEXT) sm
         ON sku.cust_cd::TEXT = sm.distcode::TEXT
        AND sku.retailer_cd::TEXT = sm.rtrcode::CHARACTER VARYING::TEXT
        AND sku.mother_sku_cd::TEXT = sm.motherskucode::TEXT
)
select * from final
