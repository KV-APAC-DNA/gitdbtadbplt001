--Import CTE
with source as (
    select *
    from {{ source('aspsdl_raw', 'sdl_mds_ap_price_tracker_category_mapping') }}
),

--Logical CTE

--Final CTE
final as (
    select
        market::varchar(510) as market,
        stronghold::varchar(510) as stronghold,
        harmonized_category::varchar(510) as harmonized_category,
        input_category::varchar(510) as input_category,
        input_sub_category::varchar(510) as input_sub_category
    from source
)

--Final select
select * from final
