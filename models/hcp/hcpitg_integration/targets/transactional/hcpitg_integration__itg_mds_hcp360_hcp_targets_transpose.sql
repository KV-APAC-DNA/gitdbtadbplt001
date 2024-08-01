with sdl_mds_in_hcp_targets as
(
    select * from {{ source('hcpsdl_raw', 'sdl_mds_in_hcp_targets') }}
),
jan as
(
  select
    id,
    muid,
    versionname,
    versionnumber,
    version_id,
    versionflag,
    name,
    code,
    changetrackingmask,
    country,
    brand,
    channel_code,
    channel_name,
    channel_id,
    activity_type_code,
    activity_type_name,
    activity_type_id,
    kpi_code,
    kpi_name,
    kpi_id,
    year,
    to_date((cast(year as varchar) || '-' || '01'), 'yyyy-mm') as year_month,
    jan as value,
    enterdatetime,
    enterusername,
    enterversionnumber,
    lastchgdatetime,
    lastchgusername,
    lastchgversionnumber,
    validationstatus
  from
    sdl_mds_in_hcp_targets
  where
    jan is not null
),
feb as 
(
  select
    id,
    muid,
    versionname,
    versionnumber,
    version_id,
    versionflag,
    name,
    code,
    changetrackingmask,
    country,
    brand,
    channel_code,
    channel_name,
    channel_id,
    activity_type_code,
    activity_type_name,
    activity_type_id,
    kpi_code,
    kpi_name,
    kpi_id,
    year,
    to_date((cast(year as varchar) || '-' || '02'), 'yyyy-mm') as year_month,
    feb as value,
    enterdatetime,
    enterusername,
    enterversionnumber,
    lastchgdatetime,
    lastchgusername,
    lastchgversionnumber,
    validationstatus
  from
    sdl_mds_in_hcp_targets
  where
    feb is not null
),
mar as 
(
  select 
    id,
    muid,
    versionname,
    versionnumber,
    version_id,
    versionflag,
    name,
    code,
    changetrackingmask,
    country,
    brand,
    channel_code,
    channel_name,
    channel_id,
    activity_type_code,
    activity_type_name,
    activity_type_id,
    kpi_code,
    kpi_name,
    kpi_id,
    year,
    to_date((cast(year as varchar) || '-' || '03'), 'yyyy-mm') as year_month,
    mar as value,
    enterdatetime,
    enterusername,
    enterversionnumber,
    lastchgdatetime,
    lastchgusername,
    lastchgversionnumber,
    validationstatus
  from
    sdl_mds_in_hcp_targets
  where
    mar is not null
),
apr as 
(
  select
    id,
    muid,
    versionname,
    versionnumber,
    version_id,
    versionflag,
    name,
    code,
    changetrackingmask,
    country,
    brand,
    channel_code,
    channel_name,
    channel_id,
    activity_type_code,
    activity_type_name,
    activity_type_id,
    kpi_code,
    kpi_name,
    kpi_id,
    year,
    to_date((cast(year as varchar) || '-' || '04'), 'yyyy-mm') as year_month,
    apr as value,
    enterdatetime,
    enterusername,
    enterversionnumber,
    lastchgdatetime,
    lastchgusername,
    lastchgversionnumber,
    validationstatus
  from
    sdl_mds_in_hcp_targets
  where
    apr is not null
),
may as 
(
  select
    id,
    muid,
    versionname,
    versionnumber,
    version_id,
    versionflag,
    name,
    code,
    changetrackingmask,
    country,
    brand,
    channel_code,
    channel_name,
    channel_id,
    activity_type_code,
    activity_type_name,
    activity_type_id,
    kpi_code,
    kpi_name,
    kpi_id,
    year,
    to_date((cast(year as varchar) || '-' || '05'), 'yyyy-mm') as year_month,
    may as value,
    enterdatetime,
    enterusername,
    enterversionnumber,
    lastchgdatetime,
    lastchgusername,
    lastchgversionnumber,
    validationstatus
  from
    sdl_mds_in_hcp_targets
  where
    may is not null
),
jun as 
(
  select
    id,
    muid,
    versionname,
    versionnumber,
    version_id,
    versionflag,
    name,
    code,
    changetrackingmask,
    country,
    brand,
    channel_code,
    channel_name,
    channel_id,
    activity_type_code,
    activity_type_name,
    activity_type_id,
    kpi_code,
    kpi_name,
    kpi_id,
    year,
    to_date((cast(year as varchar) || '-' || '06'), 'yyyy-mm') as year_month,
    jun as value,
    enterdatetime,
    enterusername,
    enterversionnumber,
    lastchgdatetime,
    lastchgusername,
    lastchgversionnumber,
    validationstatus
  from
    sdl_mds_in_hcp_targets
  where
    jun is not null
),
jul as
(
  select
    id,
    muid,
    versionname,
    versionnumber,
    version_id,
    versionflag,
    name,
    code,
    changetrackingmask,
    country,
    brand,
    channel_code,
    channel_name,
    channel_id,
    activity_type_code,
    activity_type_name,
    activity_type_id,
    kpi_code,
    kpi_name,
    kpi_id,
    year,
    to_date((cast(year as varchar) || '-' || '07'), 'yyyy-mm') as year_month,
    jul as value,
    enterdatetime,
    enterusername,
    enterversionnumber,
    lastchgdatetime,
    lastchgusername,
    lastchgversionnumber,
    validationstatus
  from
    sdl_mds_in_hcp_targets
  where
    jul is not null
),
aug as
(
  select
    id,
    muid,
    versionname,
    versionnumber,
    version_id,
    versionflag,
    name,
    code,
    changetrackingmask,
    country,
    brand,
    channel_code,
    channel_name,
    channel_id,
    activity_type_code,
    activity_type_name,
    activity_type_id,
    kpi_code,
    kpi_name,
    kpi_id,
    year,
    to_date((cast(year as varchar) || '-' || '08'), 'yyyy-mm') as year_month,
    aug as value,
    enterdatetime,
    enterusername,
    enterversionnumber,
    lastchgdatetime,
    lastchgusername,
    lastchgversionnumber,
    validationstatus
  from
    sdl_mds_in_hcp_targets
  where
    aug is not null
),
sep as
(
  select
    id,
    muid,
    versionname,
    versionnumber,
    version_id,
    versionflag,
    name,
    code,
    changetrackingmask,
    country,
    brand,
    channel_code,
    channel_name,
    channel_id,
    activity_type_code,
    activity_type_name,
    activity_type_id,
    kpi_code,
    kpi_name,
    kpi_id,
    year,
    to_date((cast(year as varchar) || '-' || '09'), 'yyyy-mm') as year_month,
    sep as value,
    enterdatetime,
    enterusername,
    enterversionnumber,
    lastchgdatetime,
    lastchgusername,
    lastchgversionnumber,
    validationstatus
  from
    sdl_mds_in_hcp_targets
  where
    sep is not null
),
oct as
(
  select
    id,
    muid,
    versionname,
    versionnumber,
    version_id,
    versionflag,
    name,
    code,
    changetrackingmask,
    country,
    brand,
    channel_code,
    channel_name,
    channel_id,
    activity_type_code,
    activity_type_name,
    activity_type_id,
    kpi_code,
    kpi_name,
    kpi_id,
    year,
    to_date((cast(year as varchar) || '-' || '10'), 'yyyy-mm') as year_month,
    oct as value,
    enterdatetime,
    enterusername,
    enterversionnumber,
    lastchgdatetime,
    lastchgusername,
    lastchgversionnumber,
    validationstatus
  from
    sdl_mds_in_hcp_targets
  where
    oct is not null
),
nov as
(
   select
    id,
    muid,
    versionname,
    versionnumber,
    version_id,
    versionflag,
    name,
    code,
    changetrackingmask,
    country,
    brand,
    channel_code,
    channel_name,
    channel_id,
    activity_type_code,
    activity_type_name,
    activity_type_id,
    kpi_code,
    kpi_name,
    kpi_id,
    year,
    to_date((cast(year as varchar) || '-' || '11'), 'yyyy-mm') as year_month,
    nov as value,
    enterdatetime,
    enterusername,
    enterversionnumber,
    lastchgdatetime,
    lastchgusername,
    lastchgversionnumber,
    validationstatus
  from
    sdl_mds_in_hcp_targets
  where
    nov is not null
),
dec as
(
   select
    id,
    muid,
    versionname,
    versionnumber,
    version_id,
    versionflag,
    name,
    code,
    changetrackingmask,
    country,
    brand,
    channel_code,
    channel_name,
    channel_id,
    activity_type_code,
    activity_type_name,
    activity_type_id,
    kpi_code,
    kpi_name,
    kpi_id,
    year,
    to_date((cast(year as varchar) || '-' || '12'), 'yyyy-mm') as year_month,
    dec as value,
    enterdatetime,
    enterusername,
    enterversionnumber,
    lastchgdatetime,
    lastchgusername,
    lastchgversionnumber,
    validationstatus
  from
    sdl_mds_in_hcp_targets
  where
    dec is not null
),
final as 
(
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
        country::varchar(200) as country,
        brand::varchar(200) as brand,
        channel_code::varchar(500) as channel_code,
        channel_name::varchar(500) as channel_name,
        channel_id::number(18,0) as channel_id,
        activity_type_code::varchar(500) as activity_type_code,
        activity_type_name::varchar(500) as activity_type_name,
        activity_type_id::number(18,0) as activity_type_id,
        kpi_code::varchar(500) as kpi_code,
        kpi_name::varchar(500) as kpi_name,
        kpi_id::number(18,0) as kpi_id,
        year::number(31,0) as year,
        year_month::date as year_month,
        value::number(31,0) as value,
        enterdatetime::timestamp_ntz(9) as enterdatetime,
        enterusername::varchar(200) as enterusername,
        enterversionnumber::number(18,0) as enterversionnumber,
        lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
        lastchgusername::varchar(200) as lastchgusername,
        lastchgversionnumber::number(18,0) as lastchgversionnumber,
        validationstatus::varchar(500) as validationstatus,
        convert_timezone('utc', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('utc', current_timestamp())::timestamp_ntz(9) as updt_dttm 
    from 
    (
        select * from jan
        union
        select * from feb
        union
        select * from mar
        union
        select * from apr
        union
        select * from may
        union
        select * from jun
        union
        select * from jul
        union
        select * from aug
        union
        select * from sep
        union
        select * from oct
        union
        select * from nov
        union
        select * from dec
    )
    order by
    id
)
select * from final
