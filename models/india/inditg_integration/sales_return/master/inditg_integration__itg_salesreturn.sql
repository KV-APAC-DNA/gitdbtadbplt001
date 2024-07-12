{{
    config(
        materialized="incremental",
        incremental_strategy= "append",    
        pre_hook= ["delete from {{this}}
                    where(DistCode,srnrefno) in (select DistCode,srnrefno from {{ ref('inditg_integration__itg_salesreturn_del') }});",
                   "delete from {{this}} 
                    where modifieddate >= (Select min(s.modifieddate) from {{ source('indsdl_raw', 'sdl_csl_salesreturn') }} s 
                    where DATEDIFF(day, s.createddate, s.modifieddate) <= 6 );"
                   "delete from {{this}} where (distcode, srnrefno) in 
                   (select distcode, srnrefno from {{ source('indsdl_raw', 'sdl_csl_salesreturn') }} as s 
                   where datediff(day, s.createddate, s.modifieddate) > 6);"
                ]
    )
}}
with source as
(
    select * from {{ source('indsdl_raw', 'sdl_csl_salesreturn') }}
),
final as
(
    select
    distcode::varchar(50) as distcode,
    srnrefno::varchar(50) as srnrefno,
    srnreftype::varchar(100) as srnreftype,
    srndate::timestamp_ntz(9) as srndate,
    srnmode::varchar(100) as srnmode,
    srntype::varchar(100) as srntype,
    srngrossamt::number(38,6) as srngrossamt,
    srnspldiscamt::number(38,6) as srnspldiscamt,
    srnschdiscamt::number(38,6) as srnschdiscamt,
    srncashdiscamt::number(38,6) as srncashdiscamt,
    srndbdiscamt::number(38,6) as srndbdiscamt,
    srntaxamt::number(38,6) as srntaxamt,
    srnroundoffamt::number(38,6) as srnroundoffamt,
    srnnetamt::number(38,6) as srnnetamt,
    salesmanname::varchar(100) as salesmanname,
    salesroutename::varchar(100) as salesroutename,
    rtrid::number(18,0) as rtrid,
    rtrcode::varchar(100) as rtrcode,
    rtrname::varchar(100) as rtrname,
    prdsalinvno::varchar(50) as prdsalinvno,
    prdlcnid::number(18,0) as prdlcnid,
    prdlcncode::varchar(100) as prdlcncode,
    prdcode::varchar(50) as prdcode,
    prdbatcde::varchar(50) as prdbatcde,
    prdsalqty::number(18,0) as prdsalqty,
    prdunsalqty::number(18,0) as prdunsalqty,
    prdofferqty::number(18,0) as prdofferqty,
    prdselrate::number(38,6) as prdselrate,
    prdgrossamt::number(38,6) as prdgrossamt,
    prdspldiscamt::number(38,6) as prdspldiscamt,
    prdschdiscamt::number(38,6) as prdschdiscamt,
    prdcashdiscamt::number(38,6) as prdcashdiscamt,
    prddbdiscamt::number(38,6) as prddbdiscamt,
    prdtaxamt::number(38,6) as prdtaxamt,
    prdnetamt::number(38,6) as prdnetamt,
    uploadflag::varchar(10) as uploadflag,
    createduserid::number(18,0) as createduserid,
    createddate::timestamp_ntz(9) as createddate,
    migrationflag::varchar(1) as migrationflag,
    mrp::number(18,6) as mrp,
    syncid::number(38,0) as syncid,
    rtnfreeqtyvalue::number(38,6) as rtnfreeqtyvalue,
    rtnlinecount::number(18,0) as rtnlinecount,
    referencetype::varchar(100) as referencetype,
    salesmancode::varchar(200) as salesmancode,
    salesroutecode::varchar(200) as salesroutecode,
    nrvalue::number(18,6) as nrvalue,
    prdselrateaftertax::number(18,6) as prdselrateaftertax,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
    modifieddate::timestamp_ntz(9) as modifieddate
    from source
    {% if is_incremental() %}
    --this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)
select * from final
