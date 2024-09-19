{{
    config(
        materialized="incremental",
        incremental_strategy="append"
        )
}}

with source as
(
    select * from {{ source('aspsdl_raw', 'sdl_prox_md_customer') }}
),
final as
(
    select id::varchar(50) as id,
    division::varchar(50) as division,
    segment::varchar(50) as segment,
    customercode::varchar(50) as customercode,
    customername::varchar(1000) as customername,
    customernamelocal::varchar(1000) as customernamelocal,
    customertype::varchar(50) as customertype,
    customertelephone::varchar(100) as customertelephone,
    customeraddress::varchar(1000) as customeraddress,
    citycode::varchar(50) as citycode,
    channelcode::varchar(50) as channelcode,
    subchannelcode::varchar(50) as subchannelcode,
    accountgroupcode::varchar(50) as accountgroupcode,
    deptcode::varchar(50) as deptcode,
    salesarea::varchar(100) as salesarea,
    vendorcode::varchar(50) as vendorcode,
    directsupplier::varchar(50) as directsupplier,
    acctatcust::varchar(50) as acctatcust,
    blockcompany::varchar(10) as blockcompany,
    blockorder::varchar(10) as blockorder,
    headquartersoldtocode::varchar(50) as headquartersoldtocode,
    distributorcode::varchar(256) as distributorcode,
    ishighrisk::varchar(10) as ishighrisk,
    status::varchar(50) as status,
    comment::varchar(4000) as comment,
    org::varchar(10) as org,
    customerhead::varchar(50) as customerhead,
    accountgroupheader::varchar(50) as accountgroupheader,
    taxnumber::varchar(50) as taxnumber,
    buslic::varchar(50) as buslic,
    mobilephone::varchar(50) as mobilephone,
    isfollow::varchar(1) as isfollow,
    followdate::timestamp_ntz(9) as followdate,
    spopenid::varchar(50) as spopenid,
    pnopenid::varchar(50) as pnopenid,
    contact::varchar(50) as contact,
    post::varchar(50) as post,
    isnew::varchar(1) as isnew,
    financeyear::varchar(100) as financeyear,
    salesorgcode::varchar(100) as salesorgcode,
    salesorgname::varchar(100) as salesorgname,
    applicationid::varchar(50) as applicationid,
    version::varchar(50) as version,
    lastmodifyuserid::varchar(50) as lastmodifyuserid,
    lastmodifytime::timestamp_ntz(9) as lastmodifytime,
    cityname::varchar(200) as cityname,
    citycluster::varchar(50) as citycluster,
    cityclustername::varchar(200) as cityclustername,
    provincecode::varchar(50) as provincecode,
    provincename::varchar(200) as provincename,
    newtime::timestamp_ntz(9) as newtime,
    datasource::number(38,0) as datasource,
    payercode::varchar(50) as payercode,
    payername::varchar(1000) as payername,
    hqcitycode::varchar(50) as hqcitycode,
    indirectstoretye::varchar(50) as indirectstoretye,
    accountgrouptype::varchar(50) as accountgrouptype,
    rtmchannelcode::varchar(50) as rtmchannelcode,
    vatcode::varchar(50) as vatcode,
    filename::varchar(100) as filename,
    run_id::varchar(50) as run_id,
    crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final