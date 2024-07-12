with v_bi_slscyc_slsc_vst as
(
    select * from {{ ref('aspedw_integration__v_bi_slscyc_slsc_vst') }}
),
v_bi_mrch_resp_prod as
(
    select * from {{ ref('aspedw_integration__v_bi_mrch_resp_prod') }}
),
edw_product_attr_dim as
(
    select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
c as 
(
    SELECT 
        v_bi_slscyc_slsc_vst.itemid AS customerid,
        v_bi_slscyc_slsc_vst.customername,
        v_bi_slscyc_slsc_vst.remotekey,
        CASE
            WHEN UPPER(v_bi_slscyc_slsc_vst.country) = 'HK' THEN 'HK'
            WHEN UPPER(v_bi_slscyc_slsc_vst.country) = 'TW' THEN 'TW'
            WHEN UPPER(v_bi_slscyc_slsc_vst.country) = 'KR' THEN 'KR'
            WHEN UPPER(v_bi_slscyc_slsc_vst.country) = 'KOREA' THEN 'KR'
            ELSE v_bi_slscyc_slsc_vst.country
        END AS country,
        v_bi_slscyc_slsc_vst.storetype,
        v_bi_slscyc_slsc_vst.channel,
        v_bi_slscyc_slsc_vst.salesgroup,
        v_bi_slscyc_slsc_vst.visitid,
        v_bi_slscyc_slsc_vst.scheduleddate,
        v_bi_slscyc_slsc_vst.scheduledtime,
        v_bi_slscyc_slsc_vst.status,
        v_bi_slscyc_slsc_vst.salespersonid
    FROM v_bi_slscyc_slsc_vst
),
p as 
(
    SELECT v_bi_mrch_resp_prod.visitid AS prodvisit,
        v_bi_mrch_resp_prod.merchandisingresponseid,
        v_bi_mrch_resp_prod.salespersonid,
        v_bi_mrch_resp_prod.salescampaignid,
        v_bi_mrch_resp_prod.merchandisingid,
        v_bi_mrch_resp_prod.presence,
        v_bi_mrch_resp_prod.pricepresence,
        v_bi_mrch_resp_prod.pricedetails,
        v_bi_mrch_resp_prod.stockcount,
        v_bi_mrch_resp_prod.outofstock_cause,
        v_bi_mrch_resp_prod.productid,
        v_bi_mrch_resp_prod.customerid,
        v_bi_mrch_resp_prod.mustcarryitem,
        v_bi_mrch_resp_prod.facings,
        v_bi_mrch_resp_prod.verticalposition,
        v_bi_mrch_resp_prod.storeposition,
        v_bi_mrch_resp_prod.promodetails,
        v_bi_mrch_resp_prod.prod_remotekey,
        v_bi_mrch_resp_prod.eannumber,
        v_bi_mrch_resp_prod.productname,
        v_bi_mrch_resp_prod.startdate
    FROM v_bi_mrch_resp_prod
),
h as 
(
    SELECT cntry,
        ean,
        CASE
            WHEN prod_hier_l1 IS NULL THEN CASE
                WHEN cntry = 'HK' THEN 'Hong Kong'
                WHEN cntry = 'TW' THEN 'Taiwan'
                WHEN cntry = 'KR' THEN 'Korea'
                ELSE cntry
            END
            ELSE 
                CASE
                    WHEN prod_hier_l1 = 'HK' THEN 'Hong Kong'
                    WHEN prod_hier_l1 = 'TW' THEN 'Taiwan'
                    WHEN prod_hier_l1 = 'KR' THEN 'Korea'
                    ELSE prod_hier_l1
                END
        END AS prod_hier_l1,
        prod_hier_l2,
        prod_hier_l3,
        prod_hier_l4,
        prod_hier_l5,
        prod_hier_l6,
        prod_hier_l7,
        prod_hier_l8,
        prod_hier_l9
    FROM edw_product_attr_dim
),
final as
(   
    SELECT 
        c.customerid,
        c.customername,
        c.remotekey,
        CASE
            WHEN UPPER(c.country) = 'HK' THEN 'Hong Kong'
            WHEN UPPER(c.country) = 'TW' THEN 'Taiwan'
            WHEN UPPER(c.country) = 'KR' THEN 'Korea'
            WHEN UPPER(c.country) = 'KOREA' THEN 'Korea'
            ELSE c.country
        END AS country,
        c.storetype,
        c.channel,
        c.salesgroup,
        c.visitid,
        p.startdate AS scheduleddate,
        c.scheduledtime,
        c.status,
        p.merchandisingresponseid,
        c.salespersonid,
        p.salescampaignid,
        p.presence,
        p.pricepresence,
        p.pricedetails,
        p.outofstock_cause,
        p.stockcount,
        p.mustcarryitem,
        p.facings,
        p.verticalposition,
        p.storeposition,
        p.promodetails,
        p.productid,
        p.prod_remotekey,
        p.eannumber,
        p.productname,
        h.prod_hier_l1,
        h.prod_hier_l2,
        h.prod_hier_l3,
        h.prod_hier_l4,
        h.prod_hier_l5,
        h.prod_hier_l6,
        h.prod_hier_l7,
        h.prod_hier_l8,
        h.prod_hier_l9
    FROM c
        LEFT JOIN p ON c.visitid = p.prodvisit
        LEFT JOIN h ON p.eannumber = h.ean
        AND UPPER (c.country) = UPPER (h.cntry)
)
select * from final