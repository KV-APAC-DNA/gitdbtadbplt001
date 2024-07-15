with source as(
    select * from {{ source('jpnsdl_raw', 'sdl_mds_log') }}
),
result as(
    select 
        replace(replace(split_part(query, ' ', 2), ']', ''), '[', '') as table_name,
        result as result,
        case 
            when result like 'records affected%'
           then 'success'
        else 'failure'
        end as status,
        case 
           when result like 'records affected%'
            then split_part(result, ' ', 3)
       else null end as rec_count,
        current_timestamp() as crtd_dt
    from source
    where result is not null or query is not null
    
)

select    
    table_name::varchar(255) as table_name,
    result::varchar(1000) as result,
    status::varchar(50) as status,
    rec_count::number(18,0) as rec_count,
    crtd_dt::timestamp_ntz(9) as crtd_dt
from result
