{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        pre_hook = "delete from {{this}} where (upper(trim(keyword))) in (select upper(trim(keyword)) as keyword from {{ source('ntasdl_raw', 'sdl_mds_kr_keyword_classifications') }});"
    )
}}

with source as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_keyword_classifications') }}
),
final as (
    select 
        TRIM(code)::varchar(500) as code,
        TRIM(keyword)::varchar(200) AS keyword,
        TRIM(keyword_group)::varchar(200) AS keyword_group,
        lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
    from source
)
select * from final
