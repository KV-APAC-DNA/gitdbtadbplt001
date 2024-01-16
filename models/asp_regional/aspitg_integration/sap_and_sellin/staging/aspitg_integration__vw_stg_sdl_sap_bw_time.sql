--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_time') }}
),

--Logical CTE

--Final CTE
final as (
    select * from source
)

--Final select
select * from final