{{
    config(
        materialized= "incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('aspsdl_raw', 'sdl_rg_mds_log') }}
),

transformed as(
    select
        'rg' as cntry_cd,
        result as result,
        replace(replace(split_part(query, ' ', 2), ']', ''), '[', '')
            as table_name,
        case
            when result like 'records affected%' then 'success' else 'failure'
        end as status,
        case
            when result like 'records affected%'
                then cast(split_part(result, ' ', 3) as int)
        end as rec_count,
        current_timestamp()::timestamp_ntz(9) as crtd_dt
    from source
    where
        not result is null or not query is null
),

final as (
    select 
        cntry_cd::varchar(10) as cntry_cd, 
        result::varchar(255) as result, 
        table_name::varchar(1000) as table_name, 
        status::varchar(50) as status, 
        rec_count::number(18,0) as rec_count, 
        crtd_dt 
    from transformed
)

select * from final