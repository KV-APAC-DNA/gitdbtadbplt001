with teikikeiyaku_data_mart_uni as (
    select * from {{ ref('jpndcledw_integration__teikikeiyaku_data_mart_uni') }}
),

final as (
select c_diregularcontractid as "c_diregularcontractid",
    c_diusrid as "c_diusrid",
    dirouteid as "dirouteid",
    keiyakubi as "keiyakubi",
    shokai_ym as "shokai_ym",
    kaiyakubi as "kaiyakubi",
    c_dsregularmeisaiid as "c_dsregularmeisaiid",
    header_flg as "header_flg",
    c_dsdeleveryym as "c_dsdeleveryym",
    dsitemid as "dsitemid",
    c_diregularcourseid as "c_diregularcourseid",
    diitemsalesprc as "diitemsalesprc",
    c_dsordercreatekbn as "c_dsordercreatekbn",
    c_dscontractchangekbn as "c_dscontractchangekbn",
    c_dicancelflg as "c_dicancelflg",
    kaiyaku_kbn as "kaiyaku_kbn",
    contract_kbn as "contract_kbn",
    diordercode as "diordercode",
    c_dikesaiid as "c_dikesaiid",
    dimeisaiid as "dimeisaiid",
    maker as "maker",
    kaiyakumoushidebi as "kaiyakumoushidebi"
from teikikeiyaku_data_mart_uni
)

select * from final