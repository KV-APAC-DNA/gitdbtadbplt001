with source as 
(
    select * from {{ source('aspsdl_raw', 'tsk_task') }}
),
final as
(   
    SELECT 
        rank() OVER(PARTITION BY t1.region,t1.taskid ORDER BY t1.azuredatetime DESC) AS rank,
        t1.region,
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.taskid,
        t1.businessunitid,
        t1.customerid,
        t1.salespersonid,
        t1.salescampaignid,
        t1.visitid,
        t1.mastertaskid,
        t1.surveyresponseid,
        t1.merchandisingresponseid,
        t1.startdate,
        t1.starttime,
        t1.enddate,
        t1.endtime,
        t1.status,
        t1.ismandatory,
        t1.ispriority,
        t1.isrecurring,
        t1.canaddcontacts,
        t1.canaddimages,
        t1.mustaddcontacts,
        t1.mustaddimages,
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    FROM source t1
)
select * from final