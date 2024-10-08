with source as
(
    select * from {{ source('thasdl_raw', 'SDL_ECOM_CPAS') }}
),
final as
(
	Select 
		DATE::VARCHAR(50) as date,
		SHOP::VARCHAR(50) as shop,
		SPENDING::NUMBER(20,4) as spending,
		FILENAME::VARCHAR(255) as filename,
		CRTD_DTTM :: TIMESTAMP_NTZ(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from source
)
select * from final