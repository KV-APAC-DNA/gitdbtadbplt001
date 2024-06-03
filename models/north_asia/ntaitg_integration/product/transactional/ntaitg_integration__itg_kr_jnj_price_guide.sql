with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_kr_jnj_price_guide') }}
),
final as
(
    select
        prod_desc::varchar(255) as prod_desc,
        qty_of_bundle::varchar(20) as qty_of_bundle,
        (prod_desc || qty_of_bundle)::varchar(500) as prod_desc_and_qty_of_bundle,
        jnj_price_guide_line::float as jnj_price_guide_line,
        from_date::date as from_date,
        to_date::date as to_date
    from source
)
select * from final