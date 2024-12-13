with sdl_mds_in_hcp_selfcare as
(
    select * from {{ source('hcpsdl_raw', 'sdl_mds_in_hcp_selfcare') }}
),
final as(
select 
    id::number(38,0) as id,
    muid::varchar(40) as muid,
    versionname::varchar(100) as versionname,
    versionnumber::number(38,0) as versionnumber,
    version_id::number(38,0) as version_id,
    versionflag::varchar(100) as versionflag,
    name::varchar(500) as name,
    code::varchar(500) as code,
    changetrackingmask::number(38,0) as changetrackingmask,
    distributor_type_code::varchar(500) as distributor_type_code,
    distributor_type_name::varchar(500) as distributor_type_name,
    distributor_type_id::number(38,0) as distributor_type_id,
    rbm_code::varchar(500) as rbm_code,
    rbm_name::varchar(500) as rbm_name,
    rbm_id::number(38,0) as rbm_id,
    zbm_code::varchar(500) as zbm_code,
    zbm_name::varchar(500) as zbm_name,
    zbm_id::number(38,0) as zbm_id,
    fbm_code::varchar(500) as fbm_code,
    fbm_name::varchar(500) as fbm_name,
    fbm_id::number(38,0) as fbm_id,
    hq_code::varchar(500) as hq_code,
    hq_name::varchar(500) as hq_name,
    hq_id::number(38,0) as hq_id,
    enterdatetime::timestamp_ntz(9) as enterdatetime,
    enterusername::varchar(200) as enterusername,
    enterversionnumber::number(38,0) as enterversionnumber,
    lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
    lastchgusername::varchar(200) as lastchgusername,
    lastchgversionnumber::number(38,0) as lastchgversionnumber,
    validationstatus::varchar(500) as validationstatus,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm

FROM
    sdl_mds_in_hcp_selfcare
)
select * from final

