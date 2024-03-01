{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid','arcode']
    )
}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_dms_customer_dim') }}
),
final as(
    select 
        distributorid::varchar(10) as distributorid,
        arcode::varchar(20) as arcode,
        arname::varchar(500) as arname,
        araddress::varchar(500) as araddress,
        telephone::varchar(150) as telephone,
        fax::varchar(150) as fax,
        city::varchar(500) as city,
        region::varchar(20) as region,
        saledistrict::varchar(200) as saledistrict,
        saleoffice::varchar(10) as saleoffice,
        salegroup::varchar(10) as salegroup,
        artypecode::varchar(10) as artypecode,
        saleemployee::varchar(150) as saleemployee,
        salename::varchar(250) as salename,
        billno::varchar(500) as billno,
        billmoo::varchar(250) as billmoo,
        billsoi::varchar(255) as billsoi,
        billroad::varchar(255) as billroad,
        billsubdist::varchar(30) as billsubdist,
        billdistrict::varchar(30) as billdistrict,
        billprovince::varchar(30) as billprovince,
        billzipcode::varchar(50) as billzipcode,
        activestatus::number(18,0) as activestatus,
        routestep1::varchar(10) as routestep1,
        routestep2::varchar(10) as routestep2,
        routestep3::varchar(10) as routestep3,
        routestep4::varchar(10) as routestep4,
        routestep5::varchar(10) as routestep5,
        routestep6::varchar(10) as routestep6,
        routestep7::varchar(10) as routestep7,
        routestep8::varchar(10) as routestep8,
        routestep9::varchar(10) as routestep9,
        routestep10::varchar(10) as routestep10,
        store::varchar(200) as store,
        sourcefile::varchar(255) as sourcefile,
        old_custid::varchar(25) as old_custid,
        modifydate::timestamp_ntz(9) as modifydate,
        current_timestamp()::timestamp_ntz(9) as curr_date,
        run_id::number(18,0) as run_id
    from source
)
select * from final