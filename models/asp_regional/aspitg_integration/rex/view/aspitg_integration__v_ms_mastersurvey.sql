with source as 
(
    select * from dev_dna_load.snapaspsdl_raw.ms_mastersurvey
),
final as
(   
    SELECT 
        rank() OVER(
            PARTITION BY t1.region,
            t1.mastersurveyid
            ORDER BY t1.azuredatetime DESC
        ) AS rank,
        t1.region as "region",
        t1.fetcheddatetime,
        t1.fetchedsequence,
        t1.azurefile,
        t1.azuredatetime,
        t1.mastersurveyid,
        t1.mastersurveyname,
        t1.mastersurveydescription,
        t1.cdl_datetime,
        t1.cdl_source_file,
        t1.load_key
    FROM source t1
)
select * from final