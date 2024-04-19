with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_ps_msl') }}
),
final as
(
    select distinct 
        code::varchar(500) as code,
        category::varchar(510) as category,
        product_description::varchar(510) as product_description,
        ean::number(31,0) as ean,
        retail_environment::varchar(510) as retail_environment,
        msl_flag::varchar(200) as msl_flag,
        msl_rank::number(31,0) as msl_rank
    from source
)
select * from final