with source as (
    select * from {{ source('aspsdl_raw', 'prodtr_producttranslation') }}
),
final as (
    select rank() over(partition by t1.region, t1.producttranslationid, t1.language order by t1.azuredatetime desc) as rank,
        t1.region,
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.producttranslationid,
        t1.remotekey,
        t1.producttranslationname,
        t1.productid,
        t1.eannumber,
        t1.language,
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    from source t1
)
select * from final