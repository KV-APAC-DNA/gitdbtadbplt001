
--Import CTE
with source as (
    select *
    from {{ source('sgpsdl_raw', 'sdl_sg_ciw_mapping') }}
),

--Logical CTE

--Final CTE
final as 
(
    select
    condition_type::varchar(500) as condition_type,
    gl::varchar(10) as gl,
    gl_description::varchar(500) as gl_description,
    posted_where::varchar(500) as posted_where,
    purpose::varchar(500) as purpose,
    ciw_bucket::varchar(500) as ciw_bucket,
    cdl_dttm::varchar(255) as cdl_dttm,
    curr_date::timestamp_ntz(9) as crtd_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
    file_name::varchar(255) as file_name,
    run_id::number(14,0) as run_id
    from source
)

--Final select
select * from final