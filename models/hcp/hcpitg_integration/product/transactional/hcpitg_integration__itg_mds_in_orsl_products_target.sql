with sdl_mds_in_orsl_products_target as
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_orsl_products_target') }}
),
final as(
select 
    changetrackingmask::number(18,0) as changetrackingmask,
    code::varchar(500) as code,
    enterdatetime::timestamp_ntz(9) as enterdatetime,
    enterusername::varchar(200) as enterusername,
    enterversionnumber::number(18,0) as enterversionnumber,
    hq_code::varchar(500) as hq_code,
    hq_id::number(18,0) as hq_id,
    hq_name::varchar(500) as hq_name,
    id::number(18,0) as id,
    lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
    lastchgusername::varchar(200) as lastchgusername,
    lastchgversionnumber::number(18,0) as lastchgversionnumber,
    month_code::varchar(500) as month_code,
    month_id::number(18,0) as month_id,
    month_name::varchar(500) as month_name,
    muid::varchar(36) as muid,
    name::varchar(500) as name,
    product_category_code::varchar(500) as product_category_code,
    product_category_id::number(18,0) as product_category_id,
    product_category_name::varchar(500) as product_category_name,
    product_code::number(31,0) as product_code,
    product_name::varchar(200) as product_name,
    region_code::varchar(500) as region_code,
    region_id::number(18,0) as region_id,
    region_name::varchar(500) as region_name,
    target::number(31,0) as target,
    validationstatus::varchar(500) as validationstatus,
    version_id::number(18,0) as version_id,
    versionflag::varchar(100) as versionflag,
    versionname::varchar(100) as versionname,
    versionnumber::number(18,0) as versionnumber,
    year_code::varchar(500) as year_code,
    year_id::number(18,0) as year_id,
    year_name::varchar(500) as year_name,
    zone_code::varchar(500) as zone_code,
    zone_id::number(18,0) as zone_id,
    zone_name::varchar(500) as zone_name
FROM
        sdl_mds_in_orsl_products_target
)
select * from final

