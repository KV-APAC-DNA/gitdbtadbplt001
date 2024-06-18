with source as 
(
    select * from dev_dna_load.snapaspsdl_raw.sr_responses
),
final as
(   
    SELECT 
        rank() OVER(PARTITION BY t1.region,t1.surveyresponseid ORDER BY t1.azuredatetime DESC) AS rank,
        t1.region as "region",
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.surveyresponseid,
        t1.questionkey,
        t1.questiontext,
        t1.valuekey,
        t1.value,
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    FROM source t1
)
select * from final