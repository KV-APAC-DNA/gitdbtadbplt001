{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}}
                where (sample_lot_id) in (select sample_lot_id
                from {{ source('hcposesdl_raw', 'sdl_hcp_osea_sample_lot') }} stg_sample_lot
                where stg_sample_lot.sample_lot_id = sample_lot_id);
                {% endif %}"
    )
}}
with sdl_hcp_osea_sample_lot
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_sample_lot') }}
)
,
transformed as
(
    select
    SAMPLE_LOT_ID,
OWNER_ID,
 (CASE WHEN UPPER(IS_DELETED) = 'TRUE' THEN 1 WHEN UPPER(IS_DELETED) IS NULL THEN 0 WHEN UPPER(IS_DELETED) = ' ' 
 THEN 0 ELSE 0 END) AS IS_DELETED,
 LOT_NAME,
CREATED_DATE,
CREATED_BY_ID,
LAST_MODIFIED_DATE,
LAST_MODIFIED_BY_ID,
SYSTEM_MODSTAMP,
MAY_EDIT,
CASE WHEN UPPER(IS_LOCKED) = 'TRUE' THEN 1 WHEN UPPER(IS_LOCKED) = 'FALSE' THEN 0 ELSE 0 END AS IS_LOCKED,	          
LAST_VIEWED_DATE,
LAST_REFERENCED_DATE,
SAMPLE_LOT_ID1,
ACTIVE,
SAMPLE_LOT,
U_M,
CALCULATED_QUANTITY,
EXPIRATION_DATE,
ALLOCATED_QUANTITY,
PRODUCT,
SUPPRESS_LOT,
COUNTRY_CODE,
BATCH_LOT_ID,
LEGACY_ID,
current_timestamp() as INSERTED_DATE,
       NULL as UPDATED_DATE 
from sdl_hcp_osea_sample_lot
)
,
final as
(select 
	SAMPLE_LOT_ID::VARCHAR(18) AS SAMPLE_LOT_ID,
OWNER_ID::VARCHAR(30) AS OWNER_ID,
IS_DELETED::NUMBER(38,0) AS IS_DELETED,
LOT_NAME::VARCHAR(100) AS LOT_NAME,
CREATED_DATE::TIMESTAMP_NTZ(9) AS CREATED_DATE,
CREATED_BY_ID::VARCHAR(18) AS CREATED_BY_ID,
LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) AS LAST_MODIFIED_DATE,
LAST_MODIFIED_BY_ID::VARCHAR(18) AS LAST_MODIFIED_BY_ID,
SYSTEM_MODSTAMP::TIMESTAMP_NTZ(9) AS SYSTEM_MODSTAMP,
MAY_EDIT::VARCHAR(10) AS MAY_EDIT,
IS_LOCKED::NUMBER(38,0) AS IS_LOCKED,
LAST_VIEWED_DATE::TIMESTAMP_NTZ(9) AS LAST_VIEWED_DATE,
LAST_REFERENCED_DATE::TIMESTAMP_NTZ(9) AS LAST_REFERENCED_DATE,
SAMPLE_LOT_ID1::VARCHAR(200) AS SAMPLE_LOT_ID1,
ACTIVE::VARCHAR(100) AS ACTIVE,
SAMPLE_LOT::VARCHAR(100) AS SAMPLE_LOT,
U_M::VARCHAR(100) AS U_M,
CALCULATED_QUANTITY::VARCHAR(20) AS CALCULATED_QUANTITY,
EXPIRATION_DATE::DATE AS EXPIRATION_DATE,
ALLOCATED_QUANTITY::NUMBER(18,0) AS ALLOCATED_QUANTITY,
PRODUCT::VARCHAR(50) AS PRODUCT,
SUPPRESS_LOT::VARCHAR(50) AS SUPPRESS_LOT,
COUNTRY_CODE::VARCHAR(2) AS COUNTRY_CODE,
BATCH_LOT_ID::VARCHAR(255) AS BATCH_LOT_ID,
LEGACY_ID::VARCHAR(255) AS LEGACY_ID,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date
	from transformed
)

select * from final 
