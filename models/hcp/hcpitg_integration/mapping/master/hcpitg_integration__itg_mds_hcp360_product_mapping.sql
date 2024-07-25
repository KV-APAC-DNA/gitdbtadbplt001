with sdl_mds_in_hcp360_product_mapping as (
    select * from {{ source('hcpsdl_raw', 'sdl_mds_in_hcp360_product_mapping') }}
),
final as (
    select
        id::number(18,0) as id,
        muid::varchar(36) as muid,
        versionname::varchar(100) as versionname,
        versionnumber::number(18,0) as versionnumber,
        version_id::number(18,0) as version_id,
        versionflag::varchar(100) as versionflag,
        name::varchar(500) as name,
        code::varchar(500) as code,
        changetrackingmask::number(18,0) as changetrackingmask,
        brand::varchar(200) as brand,
        veeva::varchar(200) as veeva,
        ventasys::varchar(200) as ventasys,
        iqvia::varchar(200) as iqvia,
        product_key_description::varchar(200) as product_key_description,
        enterdatetime::timestamp_ntz(9) as enterdatetime,
        enterusername::varchar(200) as enterusername,
        enterversionnumber::number(18,0) as enterversionnumber,
        lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
        lastchgusername::varchar(200) as lastchgusername,
        lastchgversionnumber::number(18,0) as lastchgversionnumber,
        validationstatus::varchar(500) as validationstatus,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
from
       sdl_mds_in_hcp360_product_mapping
)
select * from final           

