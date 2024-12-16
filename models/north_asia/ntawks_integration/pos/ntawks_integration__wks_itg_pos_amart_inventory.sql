--remove pre_hhok during deployment
{{
    config(
        materialized='incremental',
        incincremental_strategy='append',
        pre_hook = "delete from prod_dna_core.dbt_cloud_pr_5458_1580.ntaitg_integration__itg_pos where src_sys_cd like 'A-Mart%';"
    )
}}

with sdl_tw_pos_amart_inventory as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_amart_inventory') }}
    
),

transformed AS 
(
    SELECT * FROM sdl_tw_pos_amart_inventory
),
final as 
(
    select * from transformed
    

)   
select * from final