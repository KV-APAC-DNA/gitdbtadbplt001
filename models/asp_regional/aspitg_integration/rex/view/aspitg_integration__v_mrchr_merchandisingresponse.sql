with source as (
    select * from {{ source('aspsdl_raw', 'mrchr_merchandisingresponse') }}
),
final as (
    select rank() over(partition by t1.region, t1.merchandisingresponseid order by t1.azuredatetime desc) as rank,
        t1.region,
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.merchandisingresponseid,
        t1.customerid,
        t1.salespersonid,
        t1.salescampaignid,
        t1.visitid,
        t1.taskid,
        t1.mastertaskid,
        t1.merchandisingid,
        t1.businessunitid,
        t1.startdate,
        t1.starttime,
        t1.enddate,
        t1.endtime,
        t1.status,
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    from source t1
)
select * from final