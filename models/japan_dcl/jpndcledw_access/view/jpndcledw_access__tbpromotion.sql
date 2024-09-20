with tbpromotion as (
    select * from {{ ref('jpndclitg_integration__tbpromotion') }}
),

final as (
select dipromid as "dipromid",
    dipromcateid as "dipromcateid",
    dspromname as "dspromname",
    dspromcode as "dspromcode",
    dipromregistflg as "dipromregistflg",
    dipromenqflg as "dipromenqflg",
    dipromorderflg as "dipromorderflg",
    dipromdivsts as "dipromdivsts",
    dsvalidfrom as "dsvalidfrom",
    dsvalidto as "dsvalidto",
    dspcurl as "dspcurl",
    dsmburl as "dsmburl",
    diinvalidsts as "diinvalidsts",
    dspcinvalidurl as "dspcinvalidurl",
    dsmbinvalidurl as "dsmbinvalidurl",
    c_diaffiliatekubun as "c_diaffiliatekubun",
    c_dsorderendtag as "c_dsorderendtag",
    c_dskoukokuhi as "c_dskoukokuhi",
    c_dsdistributenum as "c_dsdistributenum",
    c_dipublishkubun as "c_dipublishkubun",
    c_dipromcompanyid as "c_dipromcompanyid",
    c_didisppriority as "c_didisppriority",
    c_dicampaignid as "c_dicampaignid",
    c_diredirectflg as "c_diredirectflg",
    dsunredirectsts as "dsunredirectsts",
    dsunredirecturlpc as "dsunredirecturlpc",
    dsunredirecturlmobile as "dsunredirecturlmobile",
    dsprep as "dsprep",
    dsren as "dsren",
    diprepusr as "diprepusr",
    direnusr as "direnusr",
    dielimflg as "dielimflg",
    c_csid as "c_csid",
    c_dipcviewflg as "c_dipcviewflg",
    c_dideptid as "c_dideptid",
    source_file_date as "source_file_date",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
from tbpromotion
)

select * from final