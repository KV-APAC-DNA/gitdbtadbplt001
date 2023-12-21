--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_mara_extract') }}
),

--Logical CTE

--Final CTE
final as (
    select * from source 
)

--Final select 
select * from final