with edw_pop6_pm as (
    select * from {{ ref('ntaedw_integration__edw_pop6_pm') }}
),
itg_sfa_pm as (
    select * from {{ source('aspsdl_raw','itg_sfa_pm') }}
),
cte1 as (
    select distinct data_type,
        taskid,
        filename,
        path,
        brand,
        cast(mrchr_visitdate as varchar) mrchr_visitdate,
        customername,
        salesgroup,
        storetype,
        dist_chnl,
        country,
        salescyclename,
        salescampaignname,
        field_label,
        field_code,
        salesperson_firstname,
        salesperson_lastname,
        customerid,
        remotekey,
        secondarytradecode,
        secondarytradename
    from edw_pop6_pm
),
cte2 as (
    select distinct 'REX' as data_source,
        taskid,
        filename,
        path,
        case
            when brand = 'Cetaphil'
            or brand = 'Dove' then 'Others' -- competitor brands
            else brand
        end as brand,
        mrchr_visitdate,
        customername,
        salesgroup,
        storetype,
        dist_chnl,
        case
            when country = 'kr' then 'Korea'
            when country = 'tw' then 'Taiwan '
            when country = 'hk' then 'HongKong'
            else country
        end as country,
        salescyclename,
        salescampaignname,
        mastertaskname as field_code,
        mastertaskname as field_label,
        salesperson_firstname,
        salesperson_lastname,
        customerid,
        remotekey,
        secondarytradecode,
        secondarytradename
    from itg_sfa_pm
    where trim(taskid) not in (
            select distinct filename || 'MySales'
            from edw_pop6_pm
        )
),
transformed as (
    select
        data_type,
        taskid,
        filename,
        path,
        brand,
        mrchr_visitdate,
        customername,
        salesgroup,
        storetype,
        dist_chnl,
        country,
        salescyclename,
        salescampaignname,
        field_label,
        field_code,
        case when country = 'Korea' then '' else salesperson_firstname end as salesperson_firstname,
        case when country = 'Korea' then '' else salesperson_lastname end as salesperson_lastname,
        customerid,
        remotekey,
        secondarytradecode,
        secondarytradename
    from(
        select * from cte1
        union all
        select * from cte2
    )
),
final as (
    select
        null::number(38,0) as id, -- check the autoincrement
        data_type::varchar(20) as data_type,
        taskid::varchar(255) as taskid,
        filename::varchar(255) as filename,
        path::varchar(255) as path,
        brand::varchar(255) as brand,
        mrchr_visitdate::varchar(255) as mrchr_visitdate,
        customername::varchar(255) as customername,
        salesgroup::varchar(255) as salesgroup,
        storetype::varchar(255) as storetype,
        dist_chnl::varchar(255) as dist_chnl,
        country::varchar(255) as country,
        salescyclename::varchar(255) as salescyclename,
        salescampaignname::varchar(255) as salescampaignname,
        field_label::varchar(255) as field_label,
        field_code::varchar(255) as field_code,
        salesperson_firstname::varchar(128) as salesperson_firstname,
        salesperson_lastname::varchar(128) as salesperson_lastname,
        customerid::varchar(64) as customerid,
        remotekey::varchar(64) as remotekey,
        secondarytradecode::varchar(64) as secondarytradecode,
	    secondarytradename::varchar(256) as secondarytradename
    from transformed
)
select * from final