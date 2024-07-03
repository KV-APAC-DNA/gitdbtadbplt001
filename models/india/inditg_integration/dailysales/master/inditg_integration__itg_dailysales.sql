{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where (distcode, salinvno) in (select distcode, salinvno from {{ source('indsdl_raw', 'sdl_csl_dailysales') }} as s where datediff(day, s.createddate, s.modifieddate) > 6);
        {% endif %}"
    )
}}
with source as
(
    select * from {{ source('indsdl_raw', 'sdl_csl_dailysales') }}
),
final as
(
    select
    distcode::varchar(50) as distcode,
    salinvno::varchar(50) as salinvno,
    salinvdate::timestamp_ntz(9) as salinvdate,
    saldlvdate::timestamp_ntz(9) as saldlvdate,
    salinvmode::varchar(100) as salinvmode,
    salinvtype::varchar(100) as salinvtype,
    salgrossamt::number(38,6) as salgrossamt,
    salspldiscamt::number(38,6) as salspldiscamt,
    salschdiscamt::number(38,6) as salschdiscamt,
    salcashdiscamt::number(38,6) as salcashdiscamt,
    saldbdiscamt::number(38,6) as saldbdiscamt,
    saltaxamt::number(38,6) as saltaxamt,
    salwdsamt::number(38,6) as salwdsamt,
    saldbadjamt::number(38,6) as saldbadjamt,
    salcradjamt::number(38,6) as salcradjamt,
    salonaccountamt::number(38,6) as salonaccountamt,
    salmktretamt::number(38,6) as salmktretamt,
    salreplaceamt::number(38,6) as salreplaceamt,
    salotherchargesamt::number(38,6) as salotherchargesamt,
    salinvleveldiscamt::number(38,6) as salinvleveldiscamt,
    saltotdedn::number(38,6) as saltotdedn,
    saltotaddn::number(38,6) as saltotaddn,
    salroundoffamt::number(38,6) as salroundoffamt,
    salnetamt::number(38,6) as salnetamt,
    lcnid::number(18,0) as lcnid,
    lcncode::varchar(100) as lcncode,
    salesmancode::varchar(100) as salesmancode,
    salesmanname::varchar(200) as salesmanname,
    salesroutecode::varchar(100) as salesroutecode,
    salesroutename::varchar(200) as salesroutename,
    rtrid::number(18,0) as rtrid,
    rtrcode::varchar(100) as rtrcode,
    rtrname::varchar(100) as rtrname,
    vechname::varchar(100) as vechname,
    dlvboyname::varchar(100) as dlvboyname,
    deliveryroutecode::varchar(100) as deliveryroutecode,
    deliveryroutename::varchar(200) as deliveryroutename,
    prdcode::varchar(50) as prdcode,
    prdbatcde::varchar(50) as prdbatcde,
    prdqty::number(18,0) as prdqty,
    prdselratebeforetax::number(38,6) as prdselratebeforetax,
    prdselrateaftertax::number(38,6) as prdselrateaftertax,
    prdfreeqty::number(18,0) as prdfreeqty,
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
    salinvlinecount::number(18,0) as salinvlinecount,
    mrp::number(18,6) as mrp,
    syncid::number(38,0) as syncid,
    creditnoteamt::number(38,6) as creditnoteamt,
    salfreeqtyvalue::number(38,6) as salfreeqtyvalue,
    nrvalue::number(18,6) as nrvalue,
    vcpschemeamount::number(18,6) as vcpschemeamount,
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