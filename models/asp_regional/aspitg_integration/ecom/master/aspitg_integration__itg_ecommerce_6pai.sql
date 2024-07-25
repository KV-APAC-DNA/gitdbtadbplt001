--{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where (Year,Month) in (select distinct year,month from {{ source('aspsdl_raw', 'sdl_ecommerce_6pai') }});
        {% endif %}"
    )
}}
with source as
(
    select * from {{ source('aspsdl_raw', 'sdl_ecommerce_6pai') }}
),
final as
(
    select Source,
    Year,
    Month,
    Cluster,
    Market,
    KPI,
    Detail,
    Plan,
    Franchise,
    Score_Weighted,
    Score_NON_Weighted,
    Gap_vs_PM,
    Gap_vs_P3M,
    Gap_vs_Plan,
    Filename,
    crt_dttm 
    from source
)
select * from final