/*
This model is an exception for now as data from sap coming directly in wks table.
Once the cdl table is avaialable the definition of this model will change.
For now we are hard coding the database and schema name.
*/
--Import CTE
with source as (
    select * from aspwks_integration.wks_sap_mara_extract
),

--Logical CTE

--Final CTE
final as (
    select * from source
)

--Final select
select * from final