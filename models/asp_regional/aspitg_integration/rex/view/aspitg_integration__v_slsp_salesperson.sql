
with slsp_salesperson as (
    select * from DEV_DNA_LOAD.SNAPASPSDL_RAW.SLSP_SALESPERSON
),
final as (
SELECT rank() OVER (
        PARTITION BY t1.region,
        t1.salespersonid ORDER BY t1.azuredatetime DESC
        ) AS rank,
    t1.region,
    t1.fetcheddatetime,
    t1.fetchedsequence,
    t1.azurefile,
    t1.azuredatetime,
    t1.salespersonid,
    t1.remotekey,
    t1.title,
    t1.firstname,
    t1.lastname,
    t1.jobrole,
    t1.remoteenabled,
    t1.language,
    t1.cdl_datetime,
    t1.cdl_source_file,
    t1.load_key
FROM slsp_salesperson t1
)
select * from final