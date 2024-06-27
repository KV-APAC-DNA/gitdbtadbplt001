{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["code"]
    )
}}

with source as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_brand_campaign_promotion') }}
),
final as
(
    select
        trim(code)::varchar(500) as code,
        trim(brand_code)::varchar(500) AS brand_code,
        trim(brand_name)::varchar(500) AS brand_name,
        trim(brand_id)::varchar(500) AS brand_id,
        lastchgdatetime::timestamp_ntz(9) as lastchgdatetime
    from source
)
select * from final