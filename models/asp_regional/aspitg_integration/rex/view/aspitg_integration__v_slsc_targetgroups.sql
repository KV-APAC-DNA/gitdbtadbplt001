with source as 
(
    select * from {{ source('aspsdl_raw', 'slsc_targetgroups') }}
),
final as
(   
    SELECT 
        rank() OVER(PARTITION BY t1.region,t1.salescampaignid,t1.type ORDER BY t1.azuredatetime DESC) AS rank,
        t1.region as "region",
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.salescampaignid,
        t1.targetgroupid,
        t1.remotekey,
        t1.name,
        t1.type as "type",
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    FROM source t1
)
select * from final