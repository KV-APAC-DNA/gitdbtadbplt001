with v_mrchr_merchandisingresponse as (
    select * from dev_dna_load.snapaspsdl_raw.v_mrchr_merchandisingresponse
),
v_mrchr_responses as (
    select * from dev_dna_load.snapaspsdl_raw.v_mrchr_responses
),
v_prod_product as (
    select * from dev_dna_load.snapaspsdl_raw.v_prod_product
),
mr as (
    SELECT DISTINCT mr.merchandisingresponseid,
        mr.customerid,
        mr.salespersonid,
        mr.salescampaignid,
        mr.visitid,
        mr.taskid,
        mr.mastertaskid,
        mr.merchandisingid,
        mr.startdate
    FROM v_mrchr_merchandisingresponse mr
    WHERE (mr.rank = 1)
),
r as (
    SELECT DISTINCT r.merchandisingresponseid,
        r.productid,
        r.primaryhierarchynodeid,
        r.mustcarryitem,
        r.presence,
        r.pricepresence,
        r.pricedetails,
        r.facings,
        r.stockcount,
        r.outofstock,
        r.verticalposition,
        r.storeposition,
        r.promodetails
    FROM v_mrchr_responses r
    WHERE (r.rank = 1)
),
mrs as (
    SELECT DISTINCT mr.merchandisingresponseid,
        mr.customerid,
        mr.salespersonid,
        mr.salescampaignid,
        mr.visitid,
        mr.taskid,
        mr.mastertaskid,
        mr.merchandisingid,
        mr.startdate,
        r.productid,
        r.primaryhierarchynodeid,
        r.mustcarryitem,
        r.presence,
        r.pricepresence,
        r.pricedetails,
        r.facings,
        r.stockcount,
        r.outofstock,
        r.verticalposition,
        r.storeposition,
        r.promodetails
    FROM mr
        FULL JOIN r ON (mr.merchandisingresponseid)::text = (r.merchandisingresponseid)::text
),
merch as (
    SELECT DISTINCT mrs.merchandisingresponseid,
        mrs.customerid,
        mrs.salespersonid,
        mrs.salescampaignid,
        mrs.visitid,
        mrs.taskid,
        mrs.mastertaskid,
        mrs.merchandisingid,
        mrs.productid,
        mrs.primaryhierarchynodeid,
        mrs.mustcarryitem,
        mrs.presence,
        mrs.pricepresence,
        mrs.pricedetails,
        mrs.facings,
        mrs.stockcount,
        mrs.outofstock AS outofstock_cause,
        mrs.verticalposition,
        mrs.storeposition,
        mrs.promodetails,
        mrs.startdate
    FROM mrs
),
p as (
    SELECT DISTINCT v_prod_product.productid,
        v_prod_product.remotekey,
        v_prod_product.eannumber,
        v_prod_product.productname
    FROM v_prod_product
    WHERE (v_prod_product.rank = 1)
),
final as (
    SELECT DISTINCT merch.visitid,
        merch.merchandisingresponseid,
        merch.salespersonid,
        merch.salescampaignid,
        merch.merchandisingid,
        merch.presence,
        merch.pricepresence,
        merch.pricedetails,
        merch.stockcount,
        merch.outofstock_cause,
        merch.productid,
        merch.customerid,
        merch.mustcarryitem,
        merch.facings,
        merch.verticalposition,
        merch.storeposition,
        merch.promodetails,
        merch.startdate,
        p.remotekey AS prod_remotekey,
        p.eannumber,
        p.productname
    FROM merch
        LEFT JOIN p ON (((merch.productid)::text = (p.productid)::text))
)
select * from final