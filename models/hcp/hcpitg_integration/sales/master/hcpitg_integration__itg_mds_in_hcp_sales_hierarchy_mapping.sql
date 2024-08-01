with sdl_mds_in_hcp_sales_hierarchy_mapping as
(
    select * from {{ source('hcpsdl_raw', 'sdl_mds_in_hcp_sales_hierarchy_mapping') }}
),
final as(
select 
    brand_name_code::varchar(500) as brand_name_code,
    brand_name_id::number(18,0) as brand_name_id,
    brand_name_name::varchar(500) as brand_name_name,
    changetrackingmask::number(18,0) as changetrackingmask,
    code::varchar(500) as code,
    compositecode::varchar(200) as compositecode,
    enterdatetime::timestamp_ntz(9) as enterdatetime,
    enterusername::varchar(200) as enterusername,
    enterversionnumber::number(18,0) as enterversionnumber,
    id::number(18,0) as id,
    lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
    lastchgusername::varchar(200) as lastchgusername,
    lastchgversionnumber::number(18,0) as lastchgversionnumber,
    muid::varchar(36) as muid,
    rds_name::varchar(500) as rds_name,
    rds_code::number(31,0) as rds_code,
    region_code::varchar(500) as region_code,
    region_id::number(18,0) as region_id,
    region_name::varchar(500) as region_name,
    sales_area_code::varchar(500) as sales_area_code,
    sales_area_id::number(18,0) as sales_area_id,
    sales_area_name::varchar(500) as sales_area_name,
    validationstatus::varchar(500) as validationstatus,
    version_id::number(18,0) as version_id,
    versionflag::varchar(100) as versionflag,
    versionname::varchar(100) as versionname,
    versionnumber::number(18,0) as versionnumber,
    zone_code::varchar(500) as zone_code,
    zone_id::number(18,0) as zone_id,
    zone_name::varchar(500) as zone_name,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
FROM
        sdl_mds_in_hcp_sales_hierarchy_mapping
)
select * from final

