{{
    config(
        materialized="incremental",
        incremental_strategy="append"
        )
}}
--Import CTE
with source as 
(
    select *
    from {{ source('sgpsdl_raw', 'sdl_sg_zuellig_sellout') }}
),
--Final CTE
final as (
    select *
  from source
)

--Final select
select * from final