with source as 
(
    select * from dev_dna_load.snapaspsdl_raw.cust_customer
),
final as
(   
    SELECT 
        rank() OVER(
            PARTITION BY t1.region,
            t1.customerid
            ORDER BY t1.azuredatetime DESC
        ) AS rank,
        t1.region,
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.customerid,
        t1.remotekey,
        t1.customername,
        t1.country,
        t1.county,
        t1.district,
        t1.city,
        t1.postcode,
        t1.streetname,
        t1.streetnumber,
        t1.storereference,
        t1.email,
        t1.phonenumber,
        t1.storetype,
        t1.website,
        t1.ecommerceflag,
        t1.marketingpermission,
        t1.channel,
        t1.salesgroup,
        t1.secondarytradecode,
        t1.secondarytradename,
        t1.soldtoparty,
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    FROM source t1
)
select * from final