{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where rdd_exp >= (select distinct min(rdd_exp) from sdl_otif_glbl_con_reporting
        {% endif %}"
    )
}}
with sdl_otif_glbl_con_reporting as(
SELECT * FROM {{ source('aspsdl_raw', 'sdl_otif_glbl_con_reporting') }} 
)
select * from sdl_otif_glbl_con_reporting
