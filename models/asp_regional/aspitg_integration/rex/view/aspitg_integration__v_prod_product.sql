with prod_product as 
(
    select * from {{ source('aspsdl_raw', 'prod_product') }}
),
final as
(   
    SELECT 
        rank() OVER( PARTITION BY t1.region, t1.productid ORDER BY t1.azuredatetime DESC ) AS rank,
        t1.region,
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.productid,
        t1.remotekey,
        t1.productname,
        t1.eannumber,
        t1.width,
        t1.materialnumber,
        t1.salesunitofmeasure,
        t1.unitofmeasure,
        t1.widthunitofmeasure,
        t1.deliveryunit,
        t1.islisted,
        t1.isorderable,
        t1.isreturnable,
        t1.maximumorderquantity,
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    FROM prod_product t1
)
select * from final