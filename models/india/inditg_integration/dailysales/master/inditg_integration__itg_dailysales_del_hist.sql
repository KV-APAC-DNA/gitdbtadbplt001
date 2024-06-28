{{
    config
    (
        materialized="incremental",
        incremental_strategy="append"
    )
}}
with source as 
(
    select * from {{ ref('inditg_integration__itg_dailysales_del') }}
),
final as 
(
    select
        distcode::varchar(50) as distcode,
        salinvno::varchar(50) as salinvno,
        salinvdate::timestamp_ntz(9) as salinvdate,
        saldlvdate::timestamp_ntz(9) as saldlvdate,
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
        lcncode::varchar(50) as lcncode,
        salesmancode::varchar(50) as salesmancode,
        salesmanname::varchar(400) as salesmanname,
        salesroutecode::varchar(50) as salesroutecode,
        salesroutename::varchar(400) as salesroutename,
        rtrcode::varchar(50) as rtrcode,
        rtrname::varchar(200) as rtrname,
        deliveryroutecode::varchar(50) as deliveryroutecode,
        deliveryroutename::varchar(400) as deliveryroutename,
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
        createddate::timestamp_ntz(9) as createddate,
        mrp::number(18,6) as mrp,
        salfreeqtyvalue::number(38,6) as salfreeqtyvalue,
        nrvalue::number(18,6) as nrvalue,
        vcpschemeamount::number(18,6) as vcpschemeamount,
        rtrurccode::varchar(200) as rtrurccode,
        syncid::number(38,6) as syncid,
        creditnoteamt::number(38,6) as creditnoteamt,
        modifieddate::timestamp_ntz(9) as modifieddate,
        salinvmode::varchar(30) as salinvmode,
        salinvtype::varchar(30) as salinvtype,
        vechname::varchar(100) as vechname,
        dlvboyname::varchar(100) as dlvboyname,
        createduserid::number(18,0) as createduserid,
        salinvlinecount::number(18,0) as salinvlinecount,
        mrpcs::number(18,6) as mrpcs,
        lpvalue::number(18,6) as lpvalue,
        createddt::timestamp_ntz(9) as createddt,
        run_id::number(14,0) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        file_name::varchar(50) as file_name,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as hist_updt_dttm
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %})
select * from final
