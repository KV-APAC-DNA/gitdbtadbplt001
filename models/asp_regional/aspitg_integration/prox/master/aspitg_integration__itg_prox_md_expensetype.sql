with source as
(
    select * from {{ source('aspsdl_raw', 'sdl_prox_md_expensetype') }}
),
final as
(
    select id::varchar(50) as id,
    division::varchar(50) as division,
    segment::varchar(50) as segment,
    expensetypecode::varchar(50) as expensetypecode,
    expensetypename::varchar(1000) as expensetypename,
    expensedesc::varchar(1000) as expensedesc,
    expensecategory::varchar(1000) as expensecategory,
    expensecategoryname::varchar(1000) as expensecategoryname,
    expensemodel::varchar(50) as expensemodel,
    accountcode::varchar(50) as accountcode,
    expensesubject::varchar(200) as expensesubject,
    expensesubjectname::varchar(200) as expensesubjectname,
    accountname::varchar(1000) as accountname,
    status::varchar(50) as status,
    comment::varchar(1000) as comment,
    conditiontype::varchar(50) as conditiontype,
    parentid::varchar(50) as parentid,
    lever::number(38,0) as lever,
    orderreason::varchar(50) as orderreason,
    usagecode::varchar(50) as usagecode,
    financeyear::varchar(100) as financeyear,
    applicationid::varchar(50) as applicationid,
    version::varchar(50) as version,
    lastmodifyuserid::varchar(50) as lastmodifyuserid,
    lastmodifytime::timestamp_ntz(9) as lastmodifytime,
    copavaluefield::varchar(200) as copavaluefield,
    controlbytpm::varchar(50) as controlbytpm,
    oiaccountcode::varchar(50) as oiaccountcode,
    whtcode::varchar(50) as whtcode,
    accrualcode::varchar(50) as accrualcode,
    filename::varchar(100) as filename,
    run_id::varchar(50) as run_id,
    crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final