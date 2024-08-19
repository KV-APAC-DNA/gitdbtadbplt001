with c_tbdmsndhist as (
    select * from {{ ref('jpndclitg_integration__c_tbdmsndhist') }}
),

final as (
select c_disendid as "c_disendid",
    c_diusrid as "c_diusrid",
    c_dsdmnumber as "c_dsdmnumber",
    c_dsdmsendkubun as "c_dsdmsendkubun",
    c_dsdmsenddate as "c_dsdmsenddate",
    c_dsdmname as "c_dsdmname",
    c_dsextension1 as "c_dsextension1",
    c_dsextension2 as "c_dsextension2",
    c_dsextension3 as "c_dsextension3",
    c_dsextension4 as "c_dsextension4",
    c_dsextension5 as "c_dsextension5",
    c_dsdmimportid as "c_dsdmimportid",
    dsprep as "dsprep",
    dsren as "dsren",
    diprepusr as "diprepusr",
    direnusr as "direnusr",
    dielimflg as "dielimflg",
    dselim as "dselim",
    dielimusr as "dielimusr",
    c_diusrchanelid as "c_diusrchanelid",
    source_file_date as "source_file_date",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
from c_tbdmsndhist
)

select * from final