with source as
(
    select * from {{ source('aspsdl_raw', 'sdl_prox_md_accountgroup') }}
),
final as
(
    select id::varchar(50) as id,
    division::varchar(50) as division,
    segment::varchar(50) as segment,
    accountgroupcode::varchar(50) as accountgroupcode,
    accountgroupname::varchar(1000) as accountgroupname,
    channelcode::varchar(50) as channelcode,
    subchannelcode::varchar(50) as subchannelcode,
    accountcategory::varchar(50) as accountcategory,
    accountchaincode::varchar(50) as accountchaincode,
    accountchainname::varchar(1000) as accountchainname,
    directsupplier::varchar(50) as directsupplier,
    headquartersoldtocode::varchar(50) as headquartersoldtocode,
    distributorcode::varchar(50) as distributorcode,
    status::varchar(50) as status,
    comment::varchar(4000) as comment,
    applicationid::varchar(50) as applicationid,
    version::varchar(50) as version,
    lastmodifyuserid::varchar(50) as lastmodifyuserid,
    lastmodifytime::timestamp_ntz(9) as lastmodifytime,
    datasource::number(38,0) as datasource,
    planrequired::varchar(50) as planrequired,
    accountgrouptype::varchar(50) as accountgrouptype,
    rtmchannelcode::varchar(50) as rtmchannelcode,
    filename::varchar(100) as filename,
    run_id::varchar(50) as run_id,
    crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final