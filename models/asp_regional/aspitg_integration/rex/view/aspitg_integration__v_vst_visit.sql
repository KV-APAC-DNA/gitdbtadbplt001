with vst_visit as 
(
    select * from {{ source('aspsdl_raw', 'vst_visit') }}
),
final as
(   
    SELECT 
        rank() OVER ( PARTITION BY t1.region, t1.visitid ORDER BY t1.azuredatetime DESC ) AS rank,
        t1.region,
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.visitid,
        t1.businessunitid,
        t1.customerid,
        t1.salespersonid,
        t1.salescycleid,
        t1.salesunitid,
        t1.targetgroupid,
        t1.scheduleddate,
        t1.scheduledtime,
        COALESCE(
            CASE
                WHEN (
                    (t1.duration)::TEXT = (''::CHARACTER VARYING)::TEXT
                ) THEN NULL::CHARACTER VARYING
                ELSE t1.duration
            END,
            '00:00'::CHARACTER VARYING
        ) AS duration,
        t1.STATUS,
        t1.defaultduration,
        t1.targetvisits,
        t1.visittargetid,
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    FROM vst_visit t1
)
select * from final