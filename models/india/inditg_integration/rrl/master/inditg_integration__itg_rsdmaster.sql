{{
    config
    (
        materializede="incremental",
        incremental_strategy="delete+insert",
        unique_key=["rsdcode","rsrcode","distributorcode"]
    )
}}
with source as
(
    select * from {{ source('indsdl_raw', 'sdl_rrl_rsdmaster') }}
),
 trans as
(
    select * from
    (
    select
    sdl_rsdm.rsdcode::varchar(50) as rsdcode,
    sdl_rsdm.rsdname::varchar(50) as rsdname,
    sdl_rsdm.rsdfirm::varchar(50) as rsdfirm,
    sdl_rsdm.rsrcode::varchar(50) as rsrcode,
    sdl_rsdm.villagecode::varchar(50) as villagecode,
    sdl_rsdm.distributorcode::varchar(50) as distributorcode,
    sdl_rsdm.montlyincome::number(18,2) as montlyincome,
    sdl_rsdm.manpower::number(18,0) as manpower,
    sdl_rsdm.godownspace::varchar(50) as godownspace,
    sdl_rsdm.address::varchar(200) as address,
    sdl_rsdm.contactno::varchar(50) as contactno,
    sdl_rsdm.druglicense::varchar(50) as druglicense,
    sdl_rsdm.foodlicense::varchar(50) as foodlicense,
    sdl_rsdm.isownhouse::varchar(1) as isownhouse,
    sdl_rsdm.isnative::varchar(1) as isnative,
    sdl_rsdm.drugvaliditydate::timestamp_ntz(9) as drugvaliditydate,
    sdl_rsdm.fssaivaliditydate::timestamp_ntz(9) as fssaivaliditydate,
    sdl_rsdm.isapproved::varchar(1) as isapproved,
    sdl_rsdm.flag::varchar(1) as flag,
    sdl_rsdm.isactive::boolean as sactive,
    sdl_rsdm.createdby::varchar(50) as createdby,
    sdl_rsdm.createddate::timestamp_ntz(9) as createddate,
    sdl_rsdm.modifiedby::varchar(50) as modifiedby,
    sdl_rsdm.modifieddate::timestamp_ntz(9) as modifieddate,
    sdl_rsdm.deleteddate::timestamp_ntz(9) as deleteddate,
    sdl_rsdm.channelname::varchar(100) as hannelname,
    sdl_rsdm.subchannelid::number(38,0) as subchannelid,
    sdl_rsdm.subchannelname::varchar(100) as subchannelname,
    sdl_rsdm.categoryid::number(38,0) as categoryid,
    sdl_rsdm.categoryname::varchar(100) as categoryname,
    sdl_rsdm.outlettype::varchar(30) as outlettype,
    sdl_rsdm.modaloutlet::varchar(30) as modaloutlet,
    sdl_rsdm.synctimestamp::timestamp_ntz(9) as ynctimestamp,
    sdl_rsdm.contactperson::varchar(100) as contactperson,
    sdl_rsdm.rsdemailid::varchar(50) as rsdemailid,
    sdl_rsdm.druglicenseno2::varchar(50) as druglicenseno2,
    sdl_rsdm.rsdemailid1::varchar(50) as rsdemailid1,
    sdl_rsdm.rsdemailid2::varchar(50)as rsdemailid2,
    sdl_rsdm.salesrepemailid::varchar(50) as salesrepemailid,
    sdl_rsdm.routecode::varchar(100) as routecode,
    sdl_rsdm.rtrclassid::number(18,0) as rtrclassid,
    sdl_rsdm.filename::varchar(100) as filename,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
    row_number() over (partition by upper(sdl_rsdm.rsdcode),upper(sdl_rsdm.rsrcode),upper(sdl_rsdm.distributorcode) order by sdl_rsdm.crt_dttm desc) rnum
      from source sdl_rsdm)
WHERE rnum = '1'
),
final as
(select 
    rsdcode::varchar(50) as rsdcode,
    rsdname::varchar(50) as rsdname,
    rsdfirm::varchar(50) as rsdfirm,
    rsrcode::varchar(50) as rsrcode,
    villagecode::varchar(50) as villagecode,
    distributorcode::varchar(50) as distributorcode,
    montlyincome::number(18,2) as montlyincome,
    manpower::number(18,0) as manpower,
    godownspace::varchar(50) as godownspace,
    address::varchar(200) as address,
    contactno::varchar(50) as contactno,
    druglicense::varchar(50) as druglicense,
    foodlicense::varchar(50) as foodlicense,
    isownhouse::varchar(1) as isownhouse,
    isnative::varchar(1) as isnative,
    drugvaliditydate::timestamp_ntz(9) as drugvaliditydate,
    fssaivaliditydate::timestamp_ntz(9) as fssaivaliditydate,
    isapproved::varchar(1) as isapproved,
    flag::varchar(1) as flag,
    isactive::boolean as sactive,
    createdby::varchar(50) as createdby,
    createddate::timestamp_ntz(9) as createddate,
    modifiedby::varchar(50) as modifiedby,
    modifieddate::timestamp_ntz(9) as modifieddate,
    deleteddate::timestamp_ntz(9) as deleteddate,
    channelname::varchar(100) as hannelname,
    subchannelid::number(38,0) as subchannelid,
    subchannelname::varchar(100) as subchannelname,
    categoryid::number(38,0) as categoryid,
    categoryname::varchar(100) as categoryname,
    outlettype::varchar(30) as outlettype,
    modaloutlet::varchar(30) as modaloutlet,
    synctimestamp::timestamp_ntz(9) as ynctimestamp,
    contactperson::varchar(100) as contactperson,
    rsdemailid::varchar(50) as rsdemailid,
    druglicenseno2::varchar(50) as druglicenseno2,
    rsdemailid1::varchar(50) as rsdemailid1,
    rsdemailid2::varchar(50)as rsdemailid2,
    salesrepemailid::varchar(50) as salesrepemailid,
    routecode::varchar(100) as routecode,
    rtrclassid::number(18,0) as rtrclassid,
    filename::varchar(100) as filename,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm

    from trans
    {% if is_incremental() %}
    --this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final