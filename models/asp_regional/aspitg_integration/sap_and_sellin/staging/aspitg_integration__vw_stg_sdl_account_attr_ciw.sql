--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_account_attr_ciw') }}
),

--Logical CTE

--Final CTE
final as (
    select * from source
)

--Final select
select * from final