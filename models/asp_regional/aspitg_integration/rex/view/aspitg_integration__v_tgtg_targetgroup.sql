with source as 
(
    select * from dev_dna_load.snapaspsdl_raw.tgtg_targetgroup
),
final as
(   
    SELECT 
        rank() OVER(PARTITION BY t1.region,t1.targetgroupid ORDER BY t1.azuredatetime DESC) AS rank,
        t1.region,
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.targetgroupid,
        t1.remotekey,
        t1.targetgroupname,
        t1.targetgroupdescription,
        t1.target,
        t1.groupkeyid,
        t1.groupkeytext,
        t1.groupcode,
        t1.parentgroupid,
        t1.parentgrouptext,
        t1.parentgroupcode,
        t1.count,
        t1.created,
        t1.createdby,
        t1.targetgroupformat,
        t1.level,
        t1.status,
        t1.topnodeid,
        t1.type,
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    FROM source t1
)
select * from final