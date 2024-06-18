with source as 
(
    select * from dev_dna_load.snapaspsdl_raw.sr_surveyresponse
),
final as
(   
    SELECT 
        rank() OVER( PARTITION BY t1.region, t1.surveyresponseid ORDER BY t1.azuredatetime DESC ) AS rank,
        t1.region as "region",
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.surveyresponseid,
        t1.businessunitid,
        t1.customerid,
        t1.salespersonid,
        t1.salescampaignid,
        t1.visitid,
        t1.taskid,
        t1.mastertaskid,
        t1.mastersurveyid,
        t1.status,
        t1.enddate,
        t1.endtime,
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    FROM source t1
)
select * from final