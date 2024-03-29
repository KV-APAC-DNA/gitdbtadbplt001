{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=["snapshot_date"],
        pre_hook= "delete from {{this}} where to_char(snapshot_date,'yyyymm') = to_char(DATEADD(day, -1, convert_timezone('Asia/Bangkok', getdate())),'yyyymm')
                  and case when (select count(*) from {{ ref('thaitg_integration__itg_la_gt_customer') }}) > 0 then 1 else 0 end = 1 "
    )
}}


with source as(
    select * from {{ ref('thaitg_integration__itg_la_gt_customer') }}
),
final as(
    select 
        DATEADD(day, -1, convert_timezone('Asia/Bangkok', getdate()))::timestamp_ntz(9) as snapshot_date,
        distributorid::varchar(10) as distributorid,
        arcode::varchar(20) as arcode,
        arname::varchar(500) as arname,
        araddress::varchar(500) as araddress,
        telephone::varchar(150) as telephone,
        fax::varchar(150) as fax,
        city::varchar(500) as city,
        region::varchar(20) as region,
        saledistrict::varchar(10) as saledistrict,
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
        latitude::varchar(10) as latitude,
        longitude::varchar(10) as longitude,
        routestep10::varchar(10) as routestep10,
        store::varchar(200) as store,
        pricelevel::varchar(50) as pricelevel,
        salesareaname::varchar(150) as salesareaname,
        branchcode::varchar(50) as branchcode,
        branchname::varchar(150) as branchname,
        frequencyofvisit::varchar(50) as frequencyofvisit,
        filename::varchar(50) as filename,
        run_id::varchar(14) as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where
    case
    when (select count(*) from source) > 0
        then 1
        else 0
    end = 1
)
select * from final