{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}}
                where (mc_cycle_product_id) in (select mc_cycle_product_id
                from {{ source('hcposesdl_raw', 'sdl_hcp_osea_mc_cycle_product') }} stg_mc_cycle_product
                where stg_mc_cycle_product.mc_cycle_product_id = mc_cycle_product_id);
                {% endif %}"
    )
}}
with sdl_hcp_osea_mc_cycle_product
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_mc_cycle_product') }}
)
,
transformed as
(
    select
MC_CYCLE_PRODUCT_ID,
 (CASE WHEN UPPER(IS_DELETED) = 'TRUE' THEN 1 WHEN UPPER(IS_DELETED) IS NULL THEN 0 WHEN UPPER(IS_DELETED) = ' ' 
 THEN 0 ELSE 0 END) AS IS_DELETED,
CYCLE_PRODUCT,
RECORD_TYPE_ID,
CREATED_DATE,
CREATED_BY_ID,
LAST_MODIFIED_DATE,
LAST_MODIFIED_BY_ID,
SYSTEM_MODSTAMP,
MAY_EDIT,
CASE WHEN UPPER(IS_LOCKED) = 'TRUE' THEN 1 WHEN UPPER(IS_LOCKED) = 'FALSE' THEN 0 ELSE 0 END AS IS_LOCKED,	          
CHANNEL,
CHANNEL_LABEL,
CYCLE,
DETAIL_GROUP,
EXTERNAL_ID,
WEIGHT,
PRODUCT,
VEEVA_EXTERNAL_ID,
APPLICABLE_PRODUCT_METRICS,
MASTER_ALIGN_ID,
COUNTRY_CODE,
LEGACY_ID,
current_timestamp() as INSERTED_DATE,
       NULL as UPDATED_DATE 
from sdl_hcp_osea_mc_cycle_product
)
,
final as
(select 
	MC_CYCLE_PRODUCT_ID::VARCHAR(18) AS MC_CYCLE_PRODUCT_ID,
IS_DELETED::NUMBER(38,0) AS IS_DELETED,
CYCLE_PRODUCT::VARCHAR(255) AS CYCLE_PRODUCT,
RECORD_TYPE_ID::VARCHAR(18) AS RECORD_TYPE_ID,
CREATED_DATE::TIMESTAMP_NTZ(9) AS CREATED_DATE,
CREATED_BY_ID::VARCHAR(18) AS CREATED_BY_ID,
LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) AS LAST_MODIFIED_DATE,
LAST_MODIFIED_BY_ID::VARCHAR(18) AS LAST_MODIFIED_BY_ID,
SYSTEM_MODSTAMP::TIMESTAMP_NTZ(9) AS SYSTEM_MODSTAMP,
MAY_EDIT::VARCHAR(10) AS MAY_EDIT,
IS_LOCKED::NUMBER(38,0) AS IS_LOCKED,
CHANNEL::VARCHAR(50) AS CHANNEL,
CHANNEL_LABEL::VARCHAR(1300) AS CHANNEL_LABEL,
CYCLE::VARCHAR(1300) AS CYCLE,
DETAIL_GROUP::VARCHAR(100) AS DETAIL_GROUP,
EXTERNAL_ID::VARCHAR(100) AS EXTERNAL_ID,
WEIGHT::NUMBER(3,2) AS WEIGHT,
PRODUCT::VARCHAR(200) AS PRODUCT,
VEEVA_EXTERNAL_ID::VARCHAR(100) AS VEEVA_EXTERNAL_ID,
APPLICABLE_PRODUCT_METRICS::VARCHAR(10000) AS APPLICABLE_PRODUCT_METRICS,
MASTER_ALIGN_ID::VARCHAR(36) AS MASTER_ALIGN_ID,
COUNTRY_CODE::VARCHAR(2) AS COUNTRY_CODE,
LEGACY_ID::VARCHAR(255) AS LEGACY_ID,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date
	from transformed
)

select * from final 
