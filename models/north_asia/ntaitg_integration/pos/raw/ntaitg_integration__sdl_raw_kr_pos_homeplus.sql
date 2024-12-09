{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_homeplus') }}
    where filename not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_pos_homeplus__null_test') }}
    )
),
transformed as(
    select * from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
),

final as (
    SELECT POS_DATE,
	TRIM(STORE_CODE) AS STORE_CODE,
	PRODUCT_CODE,
	TRIM(BAR_CODE) AS BAR_CODE,
	UNIT_PRICE,
	NUMBER_OF_SALES,
	SALES_REVENUE,
	DATE_OF_PREPARATION,
	SERIAL_NUM,
	DISTRIBUTION_CODE,
	CUSTOMER_CODE,
	FILENAME,
	RUN_ID,
	CRT_DTTM,
	UPD_DTTM
    FROM transformed
)

select * from final