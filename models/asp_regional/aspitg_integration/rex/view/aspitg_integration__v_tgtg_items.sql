with source as 
(
    select * from {{ source('aspsdl_raw', 'tgtg_items') }}
),
final as
(   
    SELECT 
        rank() OVER( PARTITION BY t1.region, t1.targetgroupid ORDER BY t1.azuredatetime DESC ) AS rank,
        t1.region as "region",
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.targetgroupid,
        t1.itemid,
        t1.itemtext,
        t1.groupid,
        t1.grouptext,
        t1.groupkeyid,
        t1.groupkeytext,
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    FROM source t1
)
select * from final