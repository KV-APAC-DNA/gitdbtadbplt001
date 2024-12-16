{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["audit_code","coalesce(data_period,'#')", "period_yyyymm","product_name","form_description","pack_size","units"]
    )
}}

with SDL_KR_IQVIA_OFFTAKE as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_iqvia_offtake_temp') }}
),
transformed as (
    SELECT  
 	AUDIT_CODE,
	DATA_PERIOD,
	PERIOD_YYYYMM,
	PRODUCT_NAME,
	PRODUCT_NAME_KOR,
	FORM_DESCRIPTION,
	PACK_SIZE,
	MOLECULE_DESC,
	MFR_NAME,
	MFR_NAME_KOR,
	MFR_TYPE,
	NHI_TYPE,
	CHC_1_CODE,
	CHC_1_DESC,
	CHC_2_CODE,
	CHC_2_DESC,
	CHC_3_CODE,
	CHC_3_DESC,
	CHC_4_CODE,
	CHC_4_DESC,
	ATC_1_CODE,
	ATC_1_DESC,
	ATC_2_CODE,
	ATC_2_DESC,
	ATC_3_CODE,
	ATC_3_DESC,
	ATC_4_CODE,
	ATC_4_DESC,
	UNITS,
	PRICE,
	VALUES_LC_SI_PRICE,
	VALUES_LC_SO_PRICE,
	FILE_NAME,
	CRTD_DTTM
FROM SDL_KR_IQVIA_OFFTAKE 
),

final as (
    select * from transformed
)
select * from final