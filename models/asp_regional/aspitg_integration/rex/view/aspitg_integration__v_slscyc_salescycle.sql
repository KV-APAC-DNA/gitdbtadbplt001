with source as 
(
    select * from {{ source('aspsdl_raw', 'slscyc_salescycle') }}
),
final as
(   
    SELECT 
        rank() OVER(PARTITION BY t1.region,t1.salescycleid ORDER BY t1.azuredatetime DESC) AS rank,
        t1.region as "region",
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.salescycleid,
        t1.salescyclename,
        t1.salescycledescription,
        t1.startdate,
        t1.enddate,
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    FROM source t1
)
select * from final