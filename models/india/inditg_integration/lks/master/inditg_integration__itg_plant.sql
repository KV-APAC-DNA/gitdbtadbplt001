{{
    config(
        materialized='incremental',
        incremental_strategy= "append",
        unique_key= ["plantcode"],
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}} ITG_PLANT
        USING {{ ref('indwks_integration__wks_lks_plant') }} WKS_LKS_PLANT
        WHERE ITG_PLANT.plantcode = WKS_LKS_PLANT.plantcode
        AND WKS_LKS_PLANT.CHNG_FLG='U';
        {% endif %}"
    )
}}

with source as
(
    select * from {{ ref('indwks_integration__wks_lks_plant') }}
),
final as 
(
    SELECT
    plantcode::number(38,0) as plantcode,
    plantid::varchar(50) as plantid,
    plantname::varchar(50) as plantname,
    shortname::varchar(50) as shortname,
    name2::varchar(200) as name2,
    statecode::number(18,0) as statecode,
    active::varchar(1) as active,
    createdby::number(18,0) as createdby,
    suppliercode::varchar(100) as suppliercode,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    file_name::varchar(255) as file_name
    FROM source
)
select * from final