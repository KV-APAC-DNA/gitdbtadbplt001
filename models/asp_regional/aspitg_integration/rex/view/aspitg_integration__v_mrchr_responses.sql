with source as (
    select * from {{ source('aspsdl_raw', 'mrchr_responses') }}
),
final as (
    select rank() over(partition by t1.region, t1.merchandisingresponseid order by t1.azuredatetime desc) as rank,
        t1.region,
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.merchandisingresponseid,
        t1.productid,
        t1.primaryhierarchynodeid,
        t1.mustcarryitem,
        t1.presence,
        t1.pricepresence,
        t1.pricedetails,
        t1.promopresence,
        t1.promopackpresence,
        t1.facings,
        t1.stockcount,
        t1.outofstock,
        t1.horizontalposition,
        t1.verticalposition,
        t1.storeposition,
        t1.promodetails,
        t1.categorylength,
        t1.categoryfacings,
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    from source t1
)
select * from final