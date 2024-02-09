{{
    config(
        materialized="incremental",
        incremental_strategy="append"
        )
}}
--Import CTE
with source as (
    select *
    from {{ source('sgpsdl_raw', 'sdl_sg_tp_closed_year_bal') }}
),

--Logical CTE

--Final CTE
final as (
    select *
  from source
)

--Final select
select * from final
