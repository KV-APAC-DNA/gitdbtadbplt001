with wks_issue_rev_sku_recom_tmp1 as(
    select * from DEV_DNA_CORE.snapindwks_integration.wks_issue_rev_sku_recom_tmp1
),
wks_issue_rev_udc_details as(
    select * from DEV_DNA_CORE.snapindwks_integration.wks_issue_rev_udc_details
),
itg_xdm_salesmanskulinemapping as(
    select * from DEV_DNA_CORE.snapinditg_integration.itg_xdm_salesmanskulinemapping
),
wks_issue_rev_itg_in_rsalesman as(
    select * from DEV_DNA_CORE.snapindwks_integration.wks_issue_rev_itg_in_rsalesman
),
transformed as(
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
        sku.retailer_name,
        sku.retailer_category_name,
        sku.csrtrcode,
        sku.retailer_channel_level_3,
        sm.smcode AS salesman_code,
        sm.rmcode AS route_code,
        sm.smname AS salesman_name,
        sm.uniquesalescode AS unique_sales_code,
        sm.rmname AS route_name
    FROM wks_issue_rev_sku_recom_tmp1 sku
    INNER JOIN wks_issue_rev_udc_details udc
            ON COALESCE (udc.distcode,'0'::CHARACTER VARYING)::TEXT = COALESCE (sku.cust_cd,'0'::CHARACTER VARYING)::TEXT
            AND COALESCE (udc.mastervaluecode,'0'::CHARACTER VARYING)::TEXT = COALESCE (sku.retailer_cd,'0'::CHARACTER VARYING)::TEXT
            AND UPPER (udc.columnvalue) = 'YES'::CHARACTER VARYING::TEXT
    LEFT JOIN (SELECT sm.*,
                        msku.motherskucode
                FROM wks_issue_rev_itg_in_rsalesman sm
                INNER JOIN itg_xdm_salesmanskulinemapping msku
                        ON sm.distcode::TEXT = msku.distrcode::TEXT
                        AND sm.smcode::TEXT = msku.salesmancode::TEXT) sm
            ON sku.cust_cd::TEXT = sm.distcode::TEXT
            AND sku.retailer_cd::TEXT = sm.rtrcode::CHARACTER VARYING::TEXT
            AND sku.mother_sku_cd::TEXT = sm.motherskucode::TEXT
)
select * from transformed