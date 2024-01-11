--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_ap_greenlight_skus') }}
),

--Logical CTE

--Final CTE
final as (
    select * from source
)

--Final select
select * from final