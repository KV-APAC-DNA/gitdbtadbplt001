{{
    config
    (
        materialized="incremental",
        incremental_strategy="append"
    )
}}
with source as 
(
    select * from {{ ref('inditg_integration__itg_salesreturn_del') }}
),
final as 
(
    select
        distcode::varchar(50) as distcode,
        srnrefno::varchar(50) as srnrefno,
        srnreftype::varchar(200) as srnreftype,
        srndate::timestamp_ntz(9) as srndate,
        srnmode::varchar(50) as srnmode,
        srntype::varchar(50) as srntype,
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
        rtrcode::varchar(50) as rtrcode,
        rtrname::varchar(100) as rtrname,
        prdsalinvno::varchar(50) as prdsalinvno,
        prdlcncode::varchar(50) as prdlcncode,
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
        mrp::number(18,6) as mrp,
        rtnfreeqtyvalue::number(38,6) as rtnfreeqtyvalue,
        referencetype::varchar(100) as referencetype,
        salesmancode::varchar(50) as salesmancode,
        salesroutecode::varchar(50) as salesroutecode,
        nrvalue::number(18,6) as nrvalue,
        prdselrateaftertax::number(18,6) as prdselrateaftertax,
        mrpcs::number(18,6) as mrpcs,
        lpvalue::number(18,6) as lpvalue,
        rtnwindowdisplayamt::number(38,6) as rtnwindowdisplayamt,
        cradjamt::number(38,6) as cradjamt,
        rtrurccode::varchar(50) as rtrurccode,
        createddate::timestamp_ntz(9) as createddate,
        modifieddate::timestamp_ntz(9) as modifieddate,
        syncid::number(38,6) as syncid,
        rtnlinecount::number(18,6) as rtnlinecount,
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
