{{
    config(
        materialized='incremental',
        incremental_strategy= "append",
        unique_key= ["mnth_id"],
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}} WHERE mnth_id >= '202303';
        {% endif %}"
    )
}}

with source as
(
    select * from snapinditg_integration.itg_sku_recom_flag
),
final as
(
    SELECT * FROM source
    WHERE  mnth_id >= '202303'
)
select * from final