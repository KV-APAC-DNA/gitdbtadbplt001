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
    SELECT POS_DATE DATE NOT NULL AS POS_DATE,
	TRIM(STORE_CODE) VARCHAR(40) NOT NULL AS STORE_CODE,
	PRODUCT_CODE VARCHAR(40) AS PRODUCT_CODE,
	TRIM(BAR_CODE) VARCHAR(100) AS BAR_CODE,
	UNIT_PRICE VARCHAR(20) AS UNIT_PRICE,
	NUMBER_OF_SALES NUMBER(18,0) AS NUMBER_OF_SALES,
	SALES_REVENUE VARCHAR(20) AS SALES_REVENUE,
	DATE_OF_PREPARATION DATE AS DATE_OF_PREPARATION,
	SERIAL_NUM VARCHAR(40) AS SERIAL_NUM,
	DISTRIBUTION_CODE VARCHAR(40) DISTRIBUTION_CODE,
	CUSTOMER_CODE VARCHAR(40) AS CUSTOMER_CODE,
	FILENAME VARCHAR(100) AS FILENAME,
	RUN_ID VARCHAR(40) AS RUN_ID,
	CRT_DTTM TIMESTAMP_NTZ(9) AS CRT_DTTM,
	UPD_DTTM TIMESTAMP_NTZ(9) AS UPD_DTTM
    FROM transformed
)

select * from final